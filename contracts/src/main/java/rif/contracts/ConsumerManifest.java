package rif.contracts;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;

import rif.catalog.Json;
import rif.catalog.Result;

/**
 * A consumer of one intent, and where its generated binding lives. The catalog already says who reads by which
 * intent; a manifest adds what contracts needs to regenerate that consumer's binding in a pull request.
 *
 * @param bindingPath the generated source file, relative to the repository root
 * @param repo the GitHub repository the consumer lives in, or null
 */
public record ConsumerManifest(String name, String intent, String bindingPath, BindingGenerator.Target target, String repo) {

    /** Where manifests live, relative to the repository root. */
    public static final String DEFAULT_DIR = "contracts/consumers";

    /** Reads a manifest; {@code binding.class} names the class, and intent-bus's {@code binding.object} is read too. */
    public static Result<ConsumerManifest> read(JsonNode json) {
        List<String> errors = new ArrayList<>();
        String name = Json.text(json, "name");
        String intent = Json.text(json, "intent");
        JsonNode b = json == null ? null : json.get("binding");
        String path = Json.text(b, "path");
        String pkg = Json.text(b, "package");
        String cls = Json.text(b, "class") != null ? Json.text(b, "class") : Json.text(b, "object");
        if (name == null) {
            errors.add("'name' must be a string");
        }
        if (intent == null) {
            errors.add("'intent' must be a string");
        }
        if (path == null || pkg == null || cls == null) {
            errors.add("'binding' must give 'path', 'package' and 'class'");
        }
        return errors.isEmpty() ? Result.ok(new ConsumerManifest(name, intent, path, new BindingGenerator.Target(pkg, cls), Json.text(json, "repo")))
                : Result.err(errors);
    }

    /** Every manifest in a directory, by file name; none when the directory does not exist. A bad file fails them all. */
    public static List<ConsumerManifest> loadAll(Path dir) {
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        List<ConsumerManifest> out = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir)) {
            for (Path f : files.filter(p -> p.getFileName().toString().endsWith(".json")).sorted().toList()) {
                Result<ConsumerManifest> m = Json.parse(Files.readString(f)).flatMap(ConsumerManifest::read);
                if (!m.isOk()) {
                    throw new ContractsException(f + ": " + m.error());
                }
                out.add(m.value());
            }
        } catch (IOException e) {
            throw new ContractsException("cannot read consumer manifests in " + dir + ": " + e.getMessage(), e);
        }
        return out;
    }
}
