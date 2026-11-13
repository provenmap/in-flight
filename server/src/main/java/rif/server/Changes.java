package rif.server;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.Catalog;
import rif.catalog.Json;
import rif.catalog.Result;
import rif.contracts.Change;
import rif.contracts.ChangeStatus;
import rif.contracts.Contracts;
import rif.contracts.ContractsException;
import rif.observer.SimulatorClient;

/**
 * Contracts in the loop, for the console and the MCP server: propose a catalog revision against the router's active
 * one, see who must change and the state of each migration, and approve it once they have merged. The schema gate
 * refuses the new versions until then.
 *
 * <p>In the demo, the consumers' repository is a scratch one this app owns, and merging a migration is a button: it
 * stands for the payments team merging its pull request and deploying, which is when payments reads the new version.
 */
public final class Changes {

    /** Where the contracts live, and what a merge in the demo also does. */
    public record Options(Contracts.Settings settings, boolean demo) {}

    private final Contracts contracts;
    private final Options options;
    private final SimulatorClient sim;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    public Changes(Options options, SimulatorClient sim) {
        this.options = options;
        this.contracts = new Contracts(options.settings());
        this.sim = sim;
    }

    public Contracts contracts() {
        return contracts;
    }

    public String routerUrl() {
        return options.settings().routerUrl();
    }

    /** The catalog the router routes by, or the reason there is none. */
    public Result<Catalog> active() {
        try {
            return Result.ok(contracts.activeCatalog(null));
        } catch (ContractsException e) {
            return Result.err(e.getMessage());
        }
    }

    /** Every change, newest last, each with its state. */
    public ArrayNode list() {
        ArrayNode out = Json.array();
        for (Change c : contracts.changes()) {
            out.add(statusJson(contracts.changeStatus(c.id())));
        }
        return out;
    }

    public ObjectNode status(String id) {
        return statusJson(contracts.changeStatus(id));
    }

    /** Proposes a catalog as the next revision of the router's. A refused proposal says why, and changes nothing. */
    public ObjectNode propose(Catalog proposed) {
        Result<Catalog> active = active();
        if (!active.isOk()) {
            throw new ContractsException(active.error());
        }
        Catalog next = proposed.revision() > active.value().revision() ? proposed : proposed.withRevision(active.value().revision() + 1);
        Contracts.Proposal p = contracts.propose(next, Contracts.ProposeOptions.DEFAULT);
        ObjectNode o = Json.object();
        o.put("refused", p.refused());
        o.put("text", p.text());
        if (p.change() != null) {
            o.set("change", status(p.change().id()));
        }
        return o;
    }

    /**
     * Demo only: merges every migration branch of a change into the consumers' repository, as their teams would, and
     * deploys them: payments then reads through its regenerated binding.
     */
    public ObjectNode merge(String id) {
        if (!options.demo()) {
            throw new ContractsException("merging is the consumers' teams' to do: this app only merges in the demo's own repository");
        }
        ChangeStatus s = contracts.changeStatus(id);
        List<String> merged = new ArrayList<>();
        for (ChangeStatus.MigrationState m : s.migrations()) {
            if (!m.merged()) {
                git("merge", "--no-ff", "-q", "-m", "Merge " + m.branch(), m.branch());
                merged.add(m.consumer());
            }
        }
        if (sim != null && s.migrations().stream().anyMatch(m -> m.consumer().equals("payments-service"))) {
            sim.call("POST", "/patch");
        }
        ObjectNode o = Json.object();
        ArrayNode ms = o.putArray("merged");
        merged.forEach(ms::add);
        o.set("change", status(id));
        return o;
    }

    /** Approves a change whose migrations have all merged: registers its versions and activates it on the router. */
    public ObjectNode approve(String id) {
        Contracts.Approval a = contracts.approve(id);
        ObjectNode o = Json.object();
        o.put("approved", a.approved());
        o.put("text", a.text());
        o.set("change", status(id));
        return o;
    }

    /** The regenerated binding of one migration, as a diff against what the consumer runs today. */
    public String diff(ChangeStatus.MigrationState m) {
        try {
            String base = git("merge-base", "HEAD", m.branch()).trim();
            return git("diff", base, m.branch(), "--", m.bindingPath());
        } catch (ContractsException e) {
            return "";
        }
    }

    private ObjectNode statusJson(ChangeStatus s) {
        ObjectNode o = Json.object();
        o.put("id", s.id());
        o.put("revision", s.revision());
        o.put("status", s.status().wire());
        o.put("createdAt", s.createdAt().toString());
        o.put("headline", s.headline());
        o.put("report", s.report());
        o.set("impact", s.impact());
        ArrayNode ms = o.putArray("migrations");
        for (ChangeStatus.MigrationState m : s.migrations()) {
            ObjectNode x = ms.addObject();
            x.put("consumer", m.consumer()).put("intent", m.intent()).put("bindingPath", m.bindingPath()).put("branch", m.branch())
                    .put("commit", m.commit()).put("merged", m.merged());
            if (m.pullRequest() == null) {
                x.putNull("pullRequest");
            } else {
                x.put("pullRequest", m.pullRequest());
            }
            x.put("diff", diff(m));
        }
        ArrayNode unbound = o.putArray("unbound");
        s.unbound().forEach(unbound::add);
        ArrayNode gate = o.putArray("gate");
        s.gate().forEach(g -> gate.addObject().put("event", g.event().show()).put("gate", g.gate().wire()));
        o.put("allMerged", s.allMerged());
        return o;
    }

    private String git(String... args) {
        List<String> cmd = new ArrayList<>(List.of("git", "-C", options.settings().root().toString(), "-c", "user.name=rif-demo",
                "-c", "user.email=rif-demo@users.noreply.github.com"));
        cmd.addAll(List.of(args));
        try {
            Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (p.waitFor() != 0) {
                throw new ContractsException(String.join(" ", args) + ": " + out.trim());
            }
            return out;
        } catch (IOException e) {
            throw new ContractsException("git: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ContractsException("interrupted");
        }
    }

    /** Declares an intent on the router; its answer, or why it refused. */
    public Router.Reply declare(String name, String predicate, String by) {
        ObjectNode body = Json.object().put("name", name).put("predicate", predicate);
        if (by != null && !by.isBlank()) {
            body.put("by", by);
        }
        return Router.call(http, "PUT", routerUrl() + "/intents/" + name, Json.write(body));
    }

    /** The router's routing tables and live state; null when it cannot be reached. */
    public JsonNode routerIntents() {
        Router.Reply r = Router.call(http, "GET", routerUrl() + "/health", null);
        return r.status() == 200 ? r.json() : null;
    }

    /** The router's control API, as this server reaches it. */
    public static final class Router {
        private Router() {}

        public record Reply(int status, String body) {
            public JsonNode json() {
                Result<JsonNode> j = Json.parse(body);
                return j.isOk() ? j.value() : Json.object().put("error", body);
            }
        }

        static Reply call(HttpClient http, String method, String url, String body) {
            try {
                HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(30)).header("Content-Type", "application/json");
                b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
                HttpResponse<String> res = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
                return new Reply(res.statusCode(), res.body());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new Reply(502, Json.write(Json.object().put("error", "interrupted")));
            } catch (Exception e) {
                return new Reply(502, Json.write(Json.object().put("error", "the router is not reachable at " + url)));
            }
        }
    }

    static Path cwd() {
        return Path.of("").toAbsolutePath();
    }
}
