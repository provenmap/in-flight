package rif.contracts;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.Catalog;
import rif.catalog.EventRef;
import rif.catalog.Json;
import rif.catalog.Result;

/**
 * Change records live in {@code .rif/changes/} in the repository, each next to the proposed catalog it holds back:
 * {@code <id>.json} and {@code <id>.catalog.json}.
 */
public final class ChangeStore {

    /** Where change records live, relative to the repository root. */
    public static final String DIR = ".rif/changes";

    /** A change and the catalog it would activate. */
    public record Stored(Change change, Catalog catalog) {}

    private final Path dir;

    public ChangeStore(Path root) {
        this.dir = root.resolve(DIR);
    }

    public Path dir() {
        return dir;
    }

    public boolean exists(String id) {
        return Files.exists(dir.resolve(id + ".json"));
    }

    public void save(Change c, Catalog catalog) {
        try {
            Files.createDirectories(dir);
            Files.writeString(dir.resolve(c.id() + ".catalog.json"), Json.pretty(catalog.toJson()) + "\n");
            Files.writeString(dir.resolve(c.id() + ".json"), Json.pretty(toJson(c)) + "\n");
        } catch (IOException e) {
            throw new ContractsException("cannot write change " + c.id() + " in " + dir + ": " + e.getMessage(), e);
        }
    }

    public Result<Stored> load(String id) {
        Path f = dir.resolve(id + ".json");
        if (!Files.exists(f)) {
            return Result.err("no change '" + id + "' in " + dir);
        }
        try {
            Result<Change> change = Json.parse(Files.readString(f)).flatMap(ChangeStore::fromJson);
            if (!change.isOk()) {
                return Result.err(f + ": " + change.error());
            }
            Result<Catalog> catalog = Catalog.parse(Files.readString(dir.resolve(id + ".catalog.json")));
            return catalog.isOk() ? Result.ok(new Stored(change.value(), catalog.value())) : Result.err(id + ".catalog.json: " + catalog.error());
        } catch (IOException e) {
            return Result.err("cannot read change " + id + ": " + e.getMessage());
        }
    }

    /** Every change's id, sorted. */
    public List<String> ids() {
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(dir)) {
            return files.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(".json") && !n.endsWith(".catalog.json"))
                    .map(n -> n.substring(0, n.length() - ".json".length())).sorted().toList();
        } catch (IOException e) {
            throw new ContractsException("cannot list changes in " + dir + ": " + e.getMessage(), e);
        }
    }

    static ObjectNode toJson(Change c) {
        ObjectNode o = Json.object();
        o.put("id", c.id());
        o.put("revision", c.revision());
        o.put("status", c.status().wire());
        o.put("createdAt", c.createdAt().toString());
        o.put("headline", c.headline());
        o.put("report", c.report());
        o.set("impact", c.impact() == null ? Json.object() : c.impact());
        ArrayNode added = o.putArray("added");
        c.added().forEach(r -> added.add(r.show()));
        ArrayNode ms = o.putArray("migrations");
        for (Change.Migration m : c.migrations()) {
            ObjectNode x = ms.addObject();
            x.put("consumer", m.consumer());
            x.put("intent", m.intent());
            x.put("bindingPath", m.bindingPath());
            x.put("branch", m.branch());
            x.put("commit", m.commit());
            if (m.pullRequest() != null) {
                x.put("pullRequest", m.pullRequest());
            }
        }
        ArrayNode unbound = o.putArray("unbound");
        c.unbound().forEach(unbound::add);
        return o;
    }

    static Result<Change> fromJson(JsonNode j) {
        try {
            List<EventRef> added = new ArrayList<>();
            for (JsonNode a : j.path("added")) {
                added.add(EventRef.parse(a.asText()).value());
            }
            List<Change.Migration> migrations = new ArrayList<>();
            for (JsonNode m : j.path("migrations")) {
                migrations.add(new Change.Migration(Json.text(m, "consumer"), Json.text(m, "intent"), Json.text(m, "bindingPath"),
                        Json.text(m, "branch"), Json.text(m, "commit"), Json.text(m, "pullRequest")));
            }
            List<String> unbound = new ArrayList<>();
            j.path("unbound").forEach(u -> unbound.add(u.asText()));
            return Result.ok(new Change(Json.text(j, "id"), j.path("revision").asInt(), Change.Status.of(Json.text(j, "status")),
                    Instant.parse(Json.text(j, "createdAt")), Json.text(j, "headline"), Json.text(j, "report"), j.get("impact"),
                    added, migrations, unbound));
        } catch (RuntimeException e) {
            return Result.err("not a change record: " + e.getMessage());
        }
    }
}
