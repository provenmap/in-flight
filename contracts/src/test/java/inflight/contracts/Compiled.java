package inflight.contracts;

import java.io.IOException;
import java.lang.reflect.RecordComponent;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * A generated binding, compiled with the JDK's compiler against Jackson alone, loaded, and driven by reflection, the
 * way a consumer's build would compile it.
 */
final class Compiled {

    private final Class<?> binding;

    private Compiled(Class<?> binding) {
        this.binding = binding;
    }

    static Compiled compile(Path work, BindingGenerator.Target target, String source) throws IOException {
        Path src = work.resolve("src").resolve(target.packageName().replace('.', '/')).resolve(target.className() + ".java");
        Path out = work.resolve("classes");
        Files.createDirectories(src.getParent());
        Files.createDirectories(out);
        Files.writeString(src, source);

        String classpath = List.of(JsonNode.class, JsonParser.class, JsonProperty.class).stream().map(Compiled::jarOf)
                .collect(Collectors.joining(java.io.File.pathSeparator));
        JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diags = new DiagnosticCollector<>();
        try (StandardJavaFileManager fm = javac.getStandardFileManager(diags, null, StandardCharsets.UTF_8)) {
            List<String> options = List.of("--release", "21", "-classpath", classpath, "-d", out.toString(), "-proc:none",
                    "-encoding", "UTF-8", "-Xlint:all", "-Werror");
            boolean ok = javac.getTask(null, fm, diags, options, null, fm.getJavaFileObjects(src)).call();
            if (!ok) {
                throw new AssertionError("the generated binding does not compile:\n" + diags.getDiagnostics().stream()
                        .map(d -> d.getKind() + " line " + d.getLineNumber() + ": " + d.getMessage(null)
                                + (d.getKind() == Diagnostic.Kind.ERROR ? "\n    " + lineOf(source, d.getLineNumber()) : ""))
                        .collect(Collectors.joining("\n")));
            }
        }
        URLClassLoader loader = new URLClassLoader(new URL[] {out.toUri().toURL()}, Compiled.class.getClassLoader());
        try {
            return new Compiled(loader.loadClass(target.packageName() + "." + target.className()));
        } catch (ClassNotFoundException e) {
            throw new AssertionError(e);
        }
    }

    private static String lineOf(String source, long n) {
        List<String> lines = source.lines().toList();
        return n >= 1 && n <= lines.size() ? lines.get((int) n - 1).strip() : "";
    }

    private static String jarOf(Class<?> c) {
        try {
            return Path.of(c.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }
    }

    Object constant(String name) throws ReflectiveOperationException {
        return binding.getField(name).get(null);
    }

    /** The result of {@code decode}, as {@code Ok}, {@code UnknownEvent} or {@code Malformed} with its record. */
    Object decode(String cloudEvent) throws ReflectiveOperationException {
        return binding.getMethod("decode", String.class).invoke(null, cloudEvent);
    }

    /** The decoded event; fails when decoding did not give one. */
    Object event(String cloudEvent) throws ReflectiveOperationException {
        Object d = decode(cloudEvent);
        if (!d.getClass().getSimpleName().equals("Ok")) {
            throw new AssertionError("expected an event, got " + show(d));
        }
        return call(d, "event");
    }

    static Object call(Object o, String method) throws ReflectiveOperationException {
        return o.getClass().getMethod(method).invoke(o);
    }

    static String entityId(Object event) throws ReflectiveOperationException {
        return (String) call(event, "entityId");
    }

    /** Each effect as {@code AssignOrderTotal(120.00)}. */
    static List<String> effects(Object event) throws ReflectiveOperationException {
        List<?> effects = (List<?>) call(event, "effects");
        List<String> out = new java.util.ArrayList<>();
        for (Object e : effects) {
            out.add(show(e));
        }
        return out;
    }

    /** A record as {@code Name(a, b)}, recursively; an Optional as its value or {@code empty}. */
    static String show(Object o) throws ReflectiveOperationException {
        if (o instanceof Optional<?> opt) {
            return opt.isPresent() ? "Optional[" + show(opt.get()) + "]" : "empty";
        }
        if (o != null && o.getClass().isRecord()) {
            List<String> parts = new java.util.ArrayList<>();
            for (RecordComponent rc : o.getClass().getRecordComponents()) {
                parts.add(show(rc.getAccessor().invoke(o)));
            }
            return o.getClass().getSimpleName() + "(" + String.join(", ", parts) + ")";
        }
        return String.valueOf(o);
    }

    static List<String> componentNames(Object record) {
        return java.util.Arrays.stream(record.getClass().getRecordComponents()).map(RecordComponent::getName).toList();
    }
}
