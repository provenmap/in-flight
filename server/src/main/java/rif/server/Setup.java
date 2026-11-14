package rif.server;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.Catalog;
import rif.catalog.Json;
import rif.catalog.JsonPatch;
import rif.catalog.Result;
import rif.discovery.Discovery;
import rif.discovery.Drafter;
import rif.discovery.Sampler;
import rif.discovery.Validator;
import rif.kafka.Profile;
import rif.kafka.SchemaRegistry;
import rif.recovery.Redrive;

/**
 * The setup and recovery actions, once, for the console's API, the MCP tools and the CLI alike: discover, sample, draft,
 * validate, edit, activate, and redrive. Each answers with a line for a person and the same facts as data.
 */
public final class Setup {

    /** An answer: whether it worked, a line or a few for a person, and the data. */
    public record Answer(boolean ok, String text, ObjectNode data) {
        static Answer ok(String text, ObjectNode data) {
            return new Answer(true, text, data);
        }

        static Answer fail(String text) {
            return new Answer(false, text, Json.object().put("error", text));
        }
    }

    /** Records sampled per topic for a draft or a dry run. */
    static final int SAMPLE = 200;

    private final Engine engine;
    private final Map<String, Job> jobs = new ConcurrentHashMap<>();

    public Setup(Engine engine) {
        this.engine = engine;
    }

    public Engine engine() {
        return engine;
    }

    private Sampler sampler() {
        Profile p = engine.connected();
        return new Sampler(p.bootstrap(), engine.decoders());
    }

    private SchemaRegistry registry() {
        Profile p = engine.profile();
        return p == null || p.registry() == null || engine.mode() != Engine.Mode.APP ? null : SchemaRegistry.of(p.registry());
    }

    // ---- discover and sample ------------------------------------------------------------------------------------------

    public Answer discover() {
        try {
            Discovery.Found f = Discovery.discover(engine.connected().bootstrap(), registry(), sampler());
            return Answer.ok(Discovery.summary(f), Discovery.json(f));
        } catch (RuntimeException e) {
            return Answer.fail(e.getMessage());
        }
    }

    public Answer sample(String topic, int n, boolean reveal) {
        if (topic == null || topic.isBlank()) {
            return Answer.fail("sample needs a topic");
        }
        try {
            List<Sampler.Sampled> recs = sampler().sample(topic, n <= 0 ? 50 : n);
            Sampler.Shape s = Sampler.shape(topic, recs, reveal);
            ObjectNode data = Sampler.json(s);
            data.put("revealed", reveal);
            String fmt = String.join(", ", s.formats().entrySet().stream().map(e -> e.getValue() + " " + e.getKey()).toList());
            String line = recs.isEmpty() ? topic + " has no records to sample."
                    : "Sampled " + recs.size() + " records of " + topic + " (" + fmt + "): " + s.fields().size() + " fields"
                            + (s.eventTypes().isEmpty() ? "" : ", event types " + String.join(", ", s.eventTypes().keySet()))
                            + (s.unreadable() > 0 ? "; " + s.unreadable() + " could not be read" : "") + "." + (reveal ? "" : " Example values are masked.");
            return Answer.ok(line, data);
        } catch (RuntimeException e) {
            return Answer.fail(e.getMessage());
        }
    }

    // ---- draft, validate, edit, activate -------------------------------------------------------------------------------

    /** Drafts {@code flow} from the cluster (only {@code topics} when given) and saves it as the flow's draft. */
    public Answer draft(String flow, List<String> topics) {
        if (!Workspace.validName(flow)) {
            return Answer.fail("a flow's name is lowercase letters, digits and hyphens, such as checkout");
        }
        try {
            Sampler sampler = sampler();
            Discovery.Found f = Discovery.discover(engine.connected().bootstrap(), registry(), sampler);
            Map<String, List<Sampler.Sampled>> samples = new LinkedHashMap<>();
            for (Discovery.Topic t : f.flowTopics()) {
                if ((topics == null || topics.isEmpty() || topics.contains(t.name())) && t.records() > 0) {
                    samples.put(t.name(), sampler.sample(t.name(), SAMPLE));
                }
            }
            Drafter.Draft d = Drafter.draft(flow, f, samples, topics == null || topics.isEmpty() ? null : topics);
            ObjectNode saved = Drafter.json(d);
            saved.put("flow", flow);
            saved.put("profile", engine.profile().name());
            saved.put("created", Instant.now().toString());
            Report r = report(d.catalog(), f, samples);
            saved.set("validation", r.json());
            engine.workspace().saveDraft(flow, saved);
            long open = d.guesses().stream().filter(g -> g.confidence() < 0.9).count();
            String line = "Drafted " + flow + ": " + d.catalog().path("events").size() + " events on " + samples.size() + " topics, "
                    + d.catalog().path("consumers").size() + " consumer groups. " + (open == 0 ? "No open questions."
                    : open + " question" + (open == 1 ? "" : "s") + " to confirm, least sure first.")
                    + (d.skipped().isEmpty() ? "" : " Left out: " + String.join("; ", d.skipped()) + ".");
            return Answer.ok(line, saved);
        } catch (RuntimeException e) {
            return Answer.fail(e.getMessage());
        }
    }

    public Answer getDraft(String flow) {
        JsonNode d = engine.workspace().draft(flow);
        if (d == null) {
            return Answer.fail("there is no draft of " + flow + ": draft it first");
        }
        return Answer.ok("The draft of " + flow + ", with " + d.path("guesses").size() + " guesses.", (ObjectNode) d);
    }

    private record Report(Validator.Report report, JsonNode catalog) {
        ObjectNode json() {
            return Validator.json(report, catalog);
        }
    }

    private Report report(JsonNode catalog, Discovery.Found f, Map<String, List<Sampler.Sampled>> samples) {
        return new Report(Validator.validate(catalog, f, samples), catalog);
    }

    /**
     * Checks a catalog and dry-runs it on records sampled now: the one given, else the flow's draft, else the flow.
     */
    public Answer validate(String flow, JsonNode catalog) {
        try {
            JsonNode c = catalog != null ? catalog : resolve(flow);
            if (c == null) {
                return Answer.fail("validate needs a catalog, or a flow with a draft or a catalog");
            }
            Result<Catalog> parsed = Catalog.fromJson(c);
            if (!parsed.isOk()) {
                Validator.Report r = new Validator.Report(parsed.errors(), List.of(), List.of(), List.of(), false);
                return new Answer(false, Validator.text(r), Validator.json(r, (String) null));
            }
            Sampler sampler = sampler();
            Discovery.Found f = Discovery.discover(engine.connected().bootstrap(), registry(), null);
            Map<String, List<Sampler.Sampled>> samples = new LinkedHashMap<>();
            for (String t : parsed.value().topics()) {
                if (f.topic(t) != null) {
                    samples.put(t, sampler.sample(t, SAMPLE));
                }
            }
            Validator.Report r = Validator.validate(c, f, samples);
            if (catalog == null && flow != null && engine.workspace().draft(flow) != null) {
                ObjectNode d = (ObjectNode) engine.workspace().draft(flow);
                d.set("validation", Validator.json(r, c));
                engine.workspace().saveDraft(flow, d);
            }
            return new Answer(r.ok(), Validator.text(r), Validator.json(r, c));
        } catch (RuntimeException e) {
            return Answer.fail(e.getMessage());
        }
    }

    /** The draft's catalog when there is a draft, else the flow's. */
    private JsonNode resolve(String flow) {
        if (flow == null) {
            return null;
        }
        JsonNode d = engine.workspace().draft(flow);
        if (d != null) {
            return d.get("catalog");
        }
        return engine.workspace().flowJson(flow);
    }

    /**
     * Applies a JSON Patch to the flow's draft (the default) or to the active catalog, or answers one of the draft's
     * questions with one of its options. The result must still be a catalog; an active one is switched to at once.
     */
    public Answer edit(String flow, String target, JsonNode patch, String guess, String option) {
        if (flow == null) {
            return Answer.fail("edit needs a flow");
        }
        boolean active = "active".equals(target);
        Workspace ws = engine.workspace();
        JsonNode draft = active ? null : ws.draft(flow);
        JsonNode base = active ? ws.flowJson(flow) : draft == null ? null : draft.get("catalog");
        if (base == null) {
            return Answer.fail(active ? "there is no flow " + flow : "there is no draft of " + flow + ": draft it first, or edit the active catalog with target active");
        }
        JsonNode p = patch;
        if (guess != null) {
            if (active) {
                return Answer.fail("questions belong to a draft, not the active catalog");
            }
            JsonNode g = null;
            for (JsonNode x : draft.path("guesses")) {
                if (x.path("id").asText().equals(guess)) {
                    g = x;
                }
            }
            if (g == null) {
                return Answer.fail("the draft has no question " + guess);
            }
            JsonNode chosen = null;
            for (JsonNode o : g.path("options")) {
                if (o.path("label").asText().equals(option)) {
                    chosen = o;
                }
            }
            if (chosen == null) {
                List<String> labels = new ArrayList<>();
                g.path("options").forEach(o -> labels.add(o.path("label").asText()));
                return Answer.fail("question " + guess + " has no answer '" + option + "'; its answers: " + String.join(", ", labels));
            }
            p = chosen.get("patch");
        }
        if (p == null) {
            return Answer.fail("edit needs a patch, or a question and one of its answers");
        }
        Result<JsonNode> applied = JsonPatch.apply(base, p);
        if (!applied.isOk()) {
            return Answer.fail(applied.error());
        }
        Result<Catalog> checked = Catalog.fromJson(applied.value());
        ObjectNode data = Json.object();
        data.set("catalog", applied.value());
        if (!checked.isOk()) {
            ArrayNode errs = data.putArray("errors");
            checked.errors().forEach(errs::add);
            if (active) {
                return new Answer(false, "Not applied: the result is not a usable catalog:\n  " + String.join("\n  ", checked.errors()), data);
            }
        }
        if (active) {
            Result<Catalog> a = engine.activate(flow, applied.value());
            return a.isOk() ? Answer.ok("Applied to " + flow + " and switched to it.", data) : Answer.fail(a.error());
        }
        ObjectNode d = (ObjectNode) draft;
        d.set("catalog", applied.value());
        if (guess != null) {
            ArrayNode left = Json.array();
            for (JsonNode x : d.path("guesses")) {
                if (!x.path("id").asText().equals(guess)) {
                    left.add(x);
                }
            }
            d.set("guesses", left);
            ArrayNode answered = d.has("answered") ? (ArrayNode) d.get("answered") : d.putArray("answered");
            answered.addObject().put("id", guess).put("answer", option);
        }
        ws.saveDraft(flow, d);
        data.put("questionsLeft", d.path("guesses").size());
        String line = checked.isOk() ? "Applied to the draft of " + flow + "." : "Applied to the draft, which is not a usable catalog yet:\n  " + String.join("\n  ", checked.errors());
        return new Answer(checked.isOk(), line + " " + d.path("guesses").size() + " questions left.", data);
    }

    /** Saves the given catalog, or the flow's draft, as the flow, and switches the observer to it. */
    public Answer activate(String flow, JsonNode catalog) {
        try {
            JsonNode c = catalog;
            if (c == null) {
                JsonNode d = engine.workspace().draft(flow);
                c = d != null ? d.get("catalog") : engine.workspace().flowJson(flow);
            }
            if (c == null) {
                return Answer.fail("there is nothing to activate for " + flow + ": draft it, or give a catalog");
            }
            Result<Catalog> a = engine.activate(flow, c);
            if (!a.isOk()) {
                return Answer.fail("Not activated: " + a.error());
            }
            ObjectNode data = Json.object().put("flow", flow).put("file", engine.workspace().flowFile(flow).toString());
            return Answer.ok("Watching " + flow + " (" + a.value().events().size() + " events, " + a.value().services().size()
                    + " consumer groups). Saved as " + engine.workspace().flowFile(flow) + ", a file to keep in a repository.", data);
        } catch (RuntimeException e) {
            return Answer.fail(e.getMessage());
        }
    }

    // ---- redrive ------------------------------------------------------------------------------------------------------

    /** A redrive running in the background, and how it ended. */
    static final class Job {
        final AtomicBoolean stop = new AtomicBoolean();
        volatile CompletableFuture<Redrive.Report> future;
        volatile Instant started = Instant.now();
    }

    /**
     * Replays dead letters from {@code dlq} to where they came from. A dry run reads and writes nothing; a real run needs
     * a profile that allows writes, and runs in the background: its report comes back when it ends, or from status.
     */
    public Answer redrive(String dlq, Redrive.Filter filter, boolean dryRun, double perSecond, int limit, long waitMs) {
        var r = engine.running();
        if (r == null) {
            return Answer.fail("nothing is being watched: activate a flow first");
        }
        Catalog c = r.catalog();
        Catalog.Consumer owner = c.ownerOfDlq(dlq).orElse(null);
        if (owner == null) {
            return Answer.fail(dlq + " is not a dead-letter queue of " + r.flow() + ": its queues are " + String.join(", ", c.dlqs()));
        }
        if (engine.mode() == Engine.Mode.TRY) {
            return Answer.fail("the sample data redrives its queue with the simulator's own payments code: use the runbook's Redrive");
        }
        if (!dryRun && (r.profile() == null || !r.profile().allowWrites())) {
            return Answer.fail("profile " + (r.profile() == null ? "" : r.profile().name() + " ") + "does not allow writes to the cluster. A redrive writes "
                    + "dead letters back to their topics: allow it with connect (allowWrites true) once you mean to.");
        }
        String group = owner.redrive() != null ? owner.redrive() : owner.group() + "-redrive";
        Redrive.Options opts = new Redrive.Options(r.observer().bootstrap(), dlq, group, owner.reads(), filter, perSecond, limit, engine.decoders(), c);
        if (dryRun) {
            try {
                Redrive.Report rep = Redrive.dryRun(opts);
                return Answer.ok(Redrive.text(rep, dlq, c.display()), Redrive.json(rep));
            } catch (RuntimeException e) {
                return Answer.fail(e.getMessage());
            }
        }
        Job running = jobs.get(dlq);
        if (running != null && !running.future.isDone()) {
            return Answer.fail("a redrive of " + dlq + " is running: wait for it, or stop it");
        }
        Job job = new Job();
        job.future = CompletableFuture.supplyAsync(() -> Redrive.run(opts, job.stop));
        jobs.put(dlq, job);
        return redriveStatus(dlq, waitMs);
    }

    public Answer redriveStatus(String dlq, long waitMs) {
        Job job = jobs.get(dlq);
        if (job == null) {
            return Answer.ok("No redrive of " + dlq + " has run since the server started.", Json.object().put("running", false));
        }
        try {
            Redrive.Report rep = job.future.get(Math.max(0, waitMs), TimeUnit.MILLISECONDS);
            ObjectNode data = Redrive.json(rep).put("running", false);
            return Answer.ok(Redrive.text(rep, dlq, engine.running() == null ? null : engine.running().catalog().display()), data);
        } catch (java.util.concurrent.TimeoutException e) {
            String since = java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss").withZone(java.time.ZoneId.systemDefault()).format(job.started);
            return Answer.ok("A redrive of " + dlq + " is running, started at " + since + ". The money at risk drains as its consumer reads the dead "
                    + "letters again.",
                    Json.object().put("running", true).put("started", job.started.toString()));
        } catch (java.util.concurrent.ExecutionException e) {
            return Answer.fail("The redrive of " + dlq + " failed: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Answer.fail("interrupted");
        }
    }

    public Answer stopRedrive(String dlq) {
        Job job = jobs.get(dlq);
        if (job == null || job.future.isDone()) {
            return Answer.fail("no redrive of " + dlq + " is running");
        }
        job.stop.set(true);
        return redriveStatus(dlq, 15_000);
    }

    /** Parses the filter's text forms: an ISO time each for from and to. */
    public static Redrive.Filter filter(String error, String order, String from, String to) {
        return new Redrive.Filter(blank(error), blank(order), from == null || from.isBlank() ? null : Instant.parse(from),
                to == null || to.isBlank() ? null : Instant.parse(to));
    }

    private static String blank(String s) {
        return s == null || s.isBlank() ? null : s;
    }
}
