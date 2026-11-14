package inflight.contracts;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;

import inflight.catalog.Catalog;
import inflight.catalog.Effect;
import inflight.catalog.FieldPath;
import inflight.catalog.FieldType;
import inflight.catalog.Intent;
import inflight.catalog.Model;
import inflight.catalog.Result;
import inflight.catalog.RoutingTable;

/**
 * Generates a consumer's binding for one intent: Java 21-compatible source with a record per event type version, a
 * decoder, and each event's typed effects on the intent's entity.
 *
 * <p>A consumer that folds effects needs no hand edits when a new event type joins its intent: the regenerated binding
 * is the adapter. A consumer that switches over the event records is told by the compiler which record is new. The
 * generated code depends only on the JDK and Jackson databind. Output is deterministic, so a migration pull request
 * shows only what changed.
 */
public final class BindingGenerator {

    private BindingGenerator() {}

    /** Where the generated class goes: its package and its simple name. */
    public record Target(String packageName, String className) {}

    /** Java's reserved words and literals: none can name a package part or a variable. */
    private static final Set<String> RESERVED_WORDS = Set.of(
            "abstract", "assert", "boolean", "break", "byte", "case", "catch", "char", "class", "const", "continue", "default", "do",
            "double", "else", "enum", "extends", "final", "finally", "float", "for", "goto", "if", "implements", "import", "instanceof",
            "int", "interface", "long", "native", "new", "package", "private", "protected", "public", "return", "short", "static",
            "strictfp", "super", "switch", "synchronized", "this", "throw", "throws", "transient", "try", "void", "volatile", "while",
            "true", "false", "null", "_");

    /** Contextual keywords, escaped too so a component name never reads like one. */
    private static final Set<String> CONTEXTUAL = Set.of("var", "yield", "record", "sealed", "permits", "when");

    /** Names a record component cannot take: Object's methods, and the accessors every event already has. */
    private static final Set<String> TAKEN = Set.of("clone", "finalize", "getClass", "hashCode", "notify", "notifyAll", "toString", "wait",
            "entityId", "effects");

    /** Type names the generated class uses, which a nested record must not shadow. */
    private static final Set<String> RESERVED_TYPES = Set.of("Meta", "Event", "Effect", "EventVersion", "Decoded", "Ok", "Undecodable",
            "R", "String", "Integer", "Long", "Boolean", "BigDecimal", "Instant", "List", "ArrayList", "Optional", "Object", "Override",
            "JsonNode", "ObjectMapper", "DeserializationFeature", "IOException", "DateTimeParseException", "RuntimeException", "Missing");

    private static final Pattern QUALIFIED = Pattern.compile("[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)*");
    private static final Pattern SIMPLE = Pattern.compile("[A-Z][A-Za-z0-9_]*");

    /** The binding for a declared intent of the catalog. */
    public static Result<String> generate(Catalog catalog, String intent, Target target) {
        if (catalog.semantics() == null) {
            return Result.err("catalog revision " + catalog.revision() + " says nothing about what its events mean, so it has no intents");
        }
        RoutingTable table = catalog.semantics().table(intent);
        if (table == null) {
            return Result.err("intent '" + intent + "' is not declared in catalog revision " + catalog.revision());
        }
        return generate(table, catalog, target);
    }

    /** The binding for a compiled intent, with the event types the catalog gives each of its routes. */
    public static Result<String> generate(RoutingTable table, Catalog catalog, Target target) {
        List<String> errors = new ArrayList<>();
        if (!QUALIFIED.matcher(target.packageName()).matches() || splitKeyword(target.packageName())) {
            errors.add("'" + target.packageName() + "' is not a Java package name");
        }
        if (!SIMPLE.matcher(target.className()).matches() || RESERVED_TYPES.contains(target.className())) {
            errors.add("'" + target.className() + "' cannot name the binding class: give a capitalised Java identifier that is not one of "
                    + String.join(", ", RESERVED_TYPES.stream().sorted().toList()));
        }
        if (catalog.semantics() == null) {
            return Result.err("catalog revision " + catalog.revision() + " says nothing about what its events mean");
        }
        Model model = catalog.semantics().model();
        Map<String, FieldType> attrs = model.entities().get(table.entity());
        if (attrs == null) {
            errors.add("entity '" + table.entity() + "' is not in catalog revision " + catalog.revision());
        }
        List<Model.Event> events = new ArrayList<>();
        for (RoutingTable.Route r : table.routes()) {
            model.find(r.event()).ifPresentOrElse(events::add,
                    () -> errors.add(r.event().show() + " is not in catalog revision " + catalog.revision()));
        }
        for (Model.Event e : events) {
            if (e.fields().containsKey("meta")) {
                errors.add(e.ref().show() + " has a field named 'meta', which the binding reserves");
            }
        }
        if (!errors.isEmpty()) {
            return Result.err(errors);
        }
        events.sort(Comparator.comparing((Model.Event e) -> e.ref().name()).thenComparingInt(e -> e.ref().version()));
        Gen gen = new Gen(table, catalog.revision(), attrs, target, events);
        String source = gen.source();
        return gen.problems.isEmpty() ? Result.ok(source) : Result.err(List.copyOf(new LinkedHashSet<>(gen.problems)));
    }

    private static boolean splitKeyword(String pkg) {
        return java.util.Arrays.stream(pkg.split("\\.")).anyMatch(RESERVED_WORDS::contains);
    }

    // ---- names and text ----------------------------------------------------------------------------------------------

    /** A field name as a Java identifier: invalid characters become {@code _}, and a keyword or taken name gains a {@code _}. */
    static String ident(String name) {
        StringBuilder sb = new StringBuilder();
        for (char c : name.toCharArray()) {
            sb.append(c < 128 && (Character.isLetterOrDigit(c) || c == '_') ? c : '_');
        }
        if (sb.isEmpty() || Character.isDigit(sb.charAt(0))) {
            sb.insert(0, '_');
        }
        String s = sb.toString();
        return RESERVED_WORDS.contains(s) || CONTEXTUAL.contains(s) || TAKEN.contains(s) ? s + "_" : s;
    }

    /** {@code adjustment} to {@code Adjustment}, {@code seen-at} to {@code SeenAt}. */
    static String cap(String s) {
        String out = java.util.Arrays.stream(s.split("[^A-Za-z0-9]")).filter(p -> !p.isEmpty())
                .map(p -> Character.toUpperCase(p.charAt(0)) + p.substring(1)).collect(Collectors.joining());
        if (out.isEmpty()) {
            return "Field";
        }
        return Character.isDigit(out.charAt(0)) ? "N" + out : out;
    }

    private static String article(String word) {
        return !word.isEmpty() && "AEIOUaeiou".indexOf(word.charAt(0)) >= 0 ? "an" : "a";
    }

    /** A Java string literal. */
    static String lit(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20 || c == 0x7f) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    /** Text safe inside a comment: no line breaks, no comment end, no backslash that could start a unicode escape. */
    static String doc(String s) {
        return s.replace("\\", "\\\\").replace("*/", "*&#47;").replace("\r", " ").replace("\n", " ");
    }

    // ---- the generator -----------------------------------------------------------------------------------------------

    private static final class Gen {
        private final RoutingTable table;
        private final int revision;
        private final String entity;
        private final Map<String, FieldType> attrs;
        private final Target target;
        private final List<Model.Event> events;
        private final StringBuilder sb = new StringBuilder();
        final List<String> problems = new ArrayList<>();

        Gen(RoutingTable table, int revision, Map<String, FieldType> attrs, Target target, List<Model.Event> events) {
            this.table = table;
            this.revision = revision;
            this.entity = table.entity();
            this.attrs = attrs;
            this.target = target;
            this.events = events;
        }

        private void line(String s) {
            sb.append(s).append('\n');
        }

        private void line() {
            sb.append('\n');
        }

        String source() {
            String cls = target.className();
            line("// Generated by in-flight contracts for intent '" + table.intent() + "' at catalog revision " + revision + ".");
            line("// Do not edit. `contracts propose` regenerates it, in a pull request, when the intent's routing changes.");
            line("package " + target.packageName() + ";");
            line();
            line("import java.io.IOException;");
            line("import java.math.BigDecimal;");
            line("import java.time.Instant;");
            line("import java.time.format.DateTimeParseException;");
            line("import java.util.ArrayList;");
            line("import java.util.List;");
            line("import java.util.Optional;");
            line();
            line("import com.fasterxml.jackson.databind.DeserializationFeature;");
            line("import com.fasterxml.jackson.databind.JsonNode;");
            line("import com.fasterxml.jackson.databind.ObjectMapper;");
            line();
            line("/** Typed events of intent {@code " + doc(table.intent()) + "}: " + doc(table.predicate()) + ", keyed by " + entity + ". */");
            line("public final class " + cls + " {");
            line();
            line("    public static final String INTENT = " + lit(table.intent()) + ";");
            line("    public static final String TOPIC = " + lit(Intent.aliasFor(table.intent())) + ";");
            line("    public static final int CATALOG_REVISION = " + revision + ";");
            line("    public static final String ENTITY = " + lit(entity) + ";");
            line("    public static final List<EventVersion> EVENTS = List.of("
                    + events.stream().map(e -> "new EventVersion(" + lit(e.ref().name()) + ", " + e.ref().version() + ")").collect(Collectors.joining(", "))
                    + ");");
            line();
            line("    private " + cls + "() {}");
            line();
            line("    /** One event type version this binding decodes. */");
            line("    public record EventVersion(String type, int version) {}");
            line();
            effects();
            line();
            line("    /** CloudEvent attributes every event carries. */");
            line("    public record Meta(String id, String source, Instant time, Optional<Long> sequence) {}");
            line();
            line("    /** An event of this intent: one record per event type version. */");
            line("    public sealed interface Event {");
            line("        Meta meta();");
            line();
            line("        /** The " + entity + " this event is about. */");
            line("        String entityId();");
            line();
            line("        /** What this event does to its " + entity + ". */");
            line("        List<Effect> effects();");
            line("    }");
            Set<String> classes = new LinkedHashSet<>();
            for (Model.Event e : events) {
                line();
                eventRecord(e, classes);
            }
            line();
            line("    /** What decoding gives: an event, or why there is none. */");
            line("    public sealed interface Decoded {}");
            line();
            line("    /** A decoded event. */");
            line("    public record Ok(Event event) implements Decoded {}");
            line();
            line("    /** Why a CloudEvent did not decode. */");
            line("    public sealed interface Undecodable extends Decoded {");
            line("        /** An event type version outside this binding's catalog revision. */");
            line("        record UnknownEvent(String eventType, int version) implements Undecodable {}");
            line();
            line("        /** An event this binding knows, whose content does not match its declared fields. */");
            line("        record Malformed(String eventType, int version, String reason) implements Undecodable {}");
            line("    }");
            line();
            decoder();
            line();
            reader();
            line("}");
            return sb.toString();
        }

        // ---- effects ------------------------------------------------------------------------------------------------

        private String effectRecord(Effect.Op op, String attr) {
            return (op == Effect.Op.ASSIGN ? "Assign" : "Adjust") + entity + cap(attr);
        }

        private void effects() {
            line("    /** One change to one attribute of " + article(entity) + " " + entity + ". Adjust carries a signed delta. */");
            if (attrs.isEmpty()) {
                line("    public interface Effect {}");
                return;
            }
            line("    public sealed interface Effect {");
            Set<String> names = new LinkedHashSet<>();
            for (Map.Entry<String, FieldType> a : attrs.entrySet()) {
                String type = baseType(a.getValue(), "", false);
                String assign = effectRecord(Effect.Op.ASSIGN, a.getKey());
                unique(names, assign, entity + " attributes");
                line("        record " + assign + "(" + type + " value) implements Effect {}");
                if (a.getValue().isNumeric()) {
                    String adjust = effectRecord(Effect.Op.ADD, a.getKey());
                    unique(names, adjust, entity + " attributes");
                    line("        record " + adjust + "(" + type + " delta) implements Effect {}");
                }
            }
            line("    }");
        }

        private void unique(Set<String> seen, String name, String where) {
            if (!seen.add(name)) {
                problems.add(where + ": two names become '" + name + "' in Java");
            }
        }

        // ---- event records ------------------------------------------------------------------------------------------

        private String className(Model.Event e) {
            return e.ref().name() + "V" + e.ref().version();
        }

        private static String baseType(FieldType t, String nested, boolean boxed) {
            return switch (t) {
                case FieldType.Obj o -> nested;
                case FieldType.Scalar s -> switch (s.kind()) {
                    case STRING -> "String";
                    case INT -> boxed ? "Integer" : "int";
                    case LONG -> boxed ? "Long" : "long";
                    case DECIMAL -> "BigDecimal";
                    case BOOLEAN -> boxed ? "Boolean" : "boolean";
                    case TIMESTAMP -> "Instant";
                };
            };
        }

        /** A component's type: optional fields are {@code Optional<T>}, always; nested objects are records named by their path. */
        private static String componentType(FieldType t, String nested) {
            return t.optional() ? "Optional<" + baseType(t, nested, true) + ">" : baseType(t, nested, false);
        }

        private record Nested(String name, Map<String, FieldType> fields) {}

        private List<Nested> nestedTypes(String prefix, Map<String, FieldType> fields) {
            List<Nested> out = new ArrayList<>();
            fields.forEach((k, t) -> {
                if (t instanceof FieldType.Obj o) {
                    String name = prefix + cap(k);
                    out.add(new Nested(name, o.fields()));
                    out.addAll(nestedTypes(name, o.fields()));
                }
            });
            return out;
        }

        private String params(String owner, String prefix, Map<String, FieldType> fields) {
            Map<String, String> seen = new HashMap<>();
            List<String> out = new ArrayList<>();
            fields.forEach((k, t) -> {
                String id = ident(k);
                String other = seen.put(id, k);
                if (other != null) {
                    problems.add(owner + ": fields '" + other + "' and '" + k + "' both become '" + id + "' in Java");
                }
                out.add(componentType(t, owner + "." + prefix + cap(k)) + " " + id);
            });
            return String.join(", ", out);
        }

        private void eventRecord(Model.Event e, Set<String> classes) {
            String cls = className(e);
            unique(classes, cls, "event records");
            RoutingTable.Route route = table.lookup(e.ref().name(), e.ref().version()).orElseThrow();
            String from = e.district() == null ? "" : ", from " + e.district();
            line("    /** " + doc(e.ref().name()) + " v" + e.ref().version() + from + " on " + doc(e.topic()) + ". Matched because: "
                    + doc(String.join("; ", route.reasons())) + ". */");
            String components = params(cls, "", e.fields());
            line("    public record " + cls + "(Meta meta" + (components.isEmpty() ? "" : ", " + components) + ") implements Event {");
            line();
            Access key = access(e.fields(), route.keyPath());
            line("        @Override");
            line("        public String entityId() {");
            line("            return " + (key.type().is(FieldType.Kind.STRING) ? key.expr() : "String.valueOf(" + key.expr() + ")") + ";");
            line("        }");
            line();
            List<Access> effs = e.effects().stream().filter(x -> x.attribute().entity().equals(entity)).map(x -> effectExpr(e, x)).toList();
            line("        @Override");
            line("        public List<Effect> effects() {");
            if (effs.stream().noneMatch(Access::optional)) {
                line("            return List.of(" + effs.stream().map(Access::expr).collect(Collectors.joining(", ")) + ");");
            } else {
                line("            List<Effect> out = new ArrayList<>();");
                for (Access a : effs) {
                    line(a.optional() ? "            " + a.expr() + ".ifPresent(out::add);" : "            out.add(" + a.expr() + ");");
                }
                line("            return List.copyOf(out);");
            }
            line("        }");
            List<Nested> nested = nestedTypes("", e.fields());
            Set<String> names = new LinkedHashSet<>();
            for (Nested n : nested) {
                unique(names, n.name(), cls + " nested records");
                if (RESERVED_TYPES.contains(n.name()) || n.name().equals(cls) || n.name().equals(target.className())) {
                    problems.add(cls + ": a nested object would be named '" + n.name() + "', which the binding already uses; rename the field");
                }
                line();
                line("        public record " + n.name() + "(" + params(cls, n.name(), n.fields()) + ") {}");
            }
            line("    }");
        }

        /** A Java expression reading a path inside an event record, whether it is an Optional, and the leaf type. */
        private record Access(String expr, boolean optional, FieldType type) {}

        private Access access(Map<String, FieldType> fields, FieldPath path) {
            List<String> segs = path.segments();
            FieldType t = fields.get(segs.get(0));
            String expr = ident(segs.get(0));
            boolean opt = t.optional();
            for (String seg : segs.subList(1, segs.size())) {
                FieldType next = ((FieldType.Obj) t).fields().get(seg);
                String accessor = ident(seg) + "()";
                if (opt) {
                    expr = expr + (next.optional() ? ".flatMap(v -> v." : ".map(v -> v.") + accessor + ")";
                } else {
                    expr = expr + "." + accessor;
                    opt = next.optional();
                }
                t = next;
            }
            return new Access(expr, opt, t);
        }

        private static String widen(String x, FieldType from, FieldType to) {
            if (from.is(FieldType.Kind.INT) && to.is(FieldType.Kind.LONG)) {
                return "(long) " + x;
            }
            if ((from.is(FieldType.Kind.INT) || from.is(FieldType.Kind.LONG)) && to.is(FieldType.Kind.DECIMAL)) {
                return "BigDecimal.valueOf(" + x + ")";
            }
            return x;
        }

        private static String negate(String x, FieldType to) {
            return to.is(FieldType.Kind.DECIMAL) ? x + ".negate()" : "-(" + x + ")";
        }

        /** The effect as an expression of type Effect, or of Optional of it when its source is optional. */
        private Access effectExpr(Model.Event e, Effect eff) {
            FieldType attrType = attrs.get(eff.attribute().attribute());
            String name = "Effect." + effectRecord(eff.op(), eff.attribute().attribute());
            return switch (eff.source()) {
                case Effect.Const c -> new Access("new " + name + "(" + constant(c.value(), attrType) + ")", false, attrType);
                case Effect.FromField f -> {
                    Access a = access(e.fields(), f.field());
                    String x = a.optional() ? "x" : a.expr();
                    String value = widen(x, a.type(), attrType);
                    if (eff.op() == Effect.Op.SUBTRACT) {
                        value = negate(value, attrType);
                    }
                    String made = "new " + name + "(" + value + ")";
                    yield a.optional() ? new Access(a.expr() + ".map(x -> " + made + ")", true, attrType) : new Access(made, false, attrType);
                }
            };
        }

        private static String constant(JsonNode v, FieldType t) {
            FieldType.Kind kind = t instanceof FieldType.Scalar s ? s.kind() : FieldType.Kind.STRING;
            return switch (kind) {
                case DECIMAL -> "new BigDecimal(" + lit(v.asText().trim()) + ")";
                case TIMESTAMP -> "Instant.parse(" + lit(v.asText()) + ")";
                case STRING -> lit(v.asText());
                case LONG -> v.asLong() + "L";
                case INT -> String.valueOf(v.asInt());
                case BOOLEAN -> String.valueOf(v.asBoolean());
            };
        }

        // ---- decoding -----------------------------------------------------------------------------------------------

        private void decoder() {
            line("    /** Decodes a CloudEvent (structured JSON). Events outside this binding's revision are reported, never guessed. */");
            line("    public static Decoded decode(String cloudEvent) {");
            line("        JsonNode ce;");
            line("        try {");
            line("            ce = R.MAPPER.readTree(cloudEvent);");
            line("        } catch (IOException | IllegalArgumentException e) {");
            line("            return new Undecodable.Malformed(\"?\", 0, \"not JSON: \" + e.getMessage());");
            line("        }");
            line("        if (ce == null || ce.isMissingNode()) {");
            line("            return new Undecodable.Malformed(\"?\", 0, \"not JSON: no content\");");
            line("        }");
            line("        String type = R.optField(ce, \"type\").filter(JsonNode::isTextual).map(JsonNode::asText).orElse(\"?\");");
            line("        int version = R.version(ce);");
            line("        try {");
            line("            return switch (type + \"@v\" + version) {");
            for (Model.Event e : events) {
                line("                case " + lit(e.ref().show()) + " -> new Ok(decode" + className(e) + "(ce));");
            }
            line("                default -> new Undecodable.UnknownEvent(type, version);");
            line("            };");
            line("        } catch (R.Missing e) {");
            line("            return new Undecodable.Malformed(type, version, e.getMessage());");
            line("        }");
            line("    }");
            for (Model.Event e : events) {
                String cls = className(e);
                line();
                line("    private static " + cls + " decode" + cls + "(JsonNode ce) {");
                line("        JsonNode d = R.obj(R.field(ce, \"data\", \"data\"), \"data\");");
                line("        return new " + cls + "(");
                List<String> args = new ArrayList<>();
                args.add("R.meta(ce)");
                args.addAll(fieldArgs(cls, "", e.fields(), "d", "data"));
                line(args.stream().map(a -> "                " + a).collect(Collectors.joining(",\n")) + ");");
                line("    }");
                for (Nested n : nestedTypes("", e.fields())) {
                    line();
                    line("    private static " + cls + "." + n.name() + " read" + cls + n.name() + "(JsonNode node) {");
                    line("        JsonNode o = R.obj(node, " + lit(nestedPath(e.fields(), n.name())) + ");");
                    line("        return new " + cls + "." + n.name() + "(");
                    line(fieldArgs(cls, n.name(), n.fields(), "o", nestedPath(e.fields(), n.name())).stream()
                            .map(a -> "                " + a).collect(Collectors.joining(",\n")) + ");");
                    line("    }");
                }
            }
        }

        /** The JSON path of a nested record, such as {@code data.adjustment}, found by its name. */
        private String nestedPath(Map<String, FieldType> fields, String name) {
            return nestedPathFrom(fields, "", "data", name);
        }

        private String nestedPathFrom(Map<String, FieldType> fields, String prefix, String at, String name) {
            for (Map.Entry<String, FieldType> f : fields.entrySet()) {
                if (f.getValue() instanceof FieldType.Obj o) {
                    String n = prefix + cap(f.getKey());
                    String path = at + "." + f.getKey();
                    if (n.equals(name)) {
                        return path;
                    }
                    String deeper = nestedPathFrom(o.fields(), n, path, name);
                    if (deeper != null) {
                        return deeper;
                    }
                }
            }
            return null;
        }

        private List<String> fieldArgs(String owner, String prefix, Map<String, FieldType> fields, String obj, String at) {
            List<String> out = new ArrayList<>();
            fields.forEach((k, t) -> {
                String path = at + "." + k;
                if (t.optional()) {
                    out.add("R.optField(" + obj + ", " + lit(k) + ").map(v -> " + valueExpr(owner, prefix + cap(k), t, "v", path) + ")");
                } else {
                    out.add(valueExpr(owner, prefix + cap(k), t, "R.field(" + obj + ", " + lit(k) + ", " + lit(path) + ")", path));
                }
            });
            return out;
        }

        private static String valueExpr(String owner, String nestedName, FieldType t, String v, String at) {
            return switch (t) {
                case FieldType.Obj o -> "read" + owner + nestedName + "(" + v + ")";
                case FieldType.Scalar s -> switch (s.kind()) {
                    case STRING -> "R.str(" + v + ", " + lit(at) + ")";
                    case INT -> "R.int32(" + v + ", " + lit(at) + ")";
                    case LONG -> "R.int64(" + v + ", " + lit(at) + ")";
                    case DECIMAL -> "R.decimal(" + v + ", " + lit(at) + ")";
                    case BOOLEAN -> "R.bool(" + v + ", " + lit(at) + ")";
                    case TIMESTAMP -> "R.instant(" + v + ", " + lit(at) + ")";
                };
            };
        }

        private void reader() {
            String[] lines = {
                "    /** Reads JSON values, naming the path of anything missing or of the wrong type. */",
                "    private static final class R {",
                "",
                "        static final ObjectMapper MAPPER = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);",
                "",
                "        private R() {}",
                "",
                "        static final class Missing extends RuntimeException {",
                "            private static final long serialVersionUID = 1L;",
                "",
                "            Missing(String message) {",
                "                super(message);",
                "            }",
                "        }",
                "",
                "        static Missing fail(String at, String want, JsonNode v) {",
                "            String got = v.toString();",
                "            return new Missing(at + \": expected \" + want + \", got \" + (got.length() > 40 ? got.substring(0, 40) : got));",
                "        }",
                "",
                "        static Optional<JsonNode> optField(JsonNode o, String k) {",
                "            JsonNode v = o.isObject() ? o.get(k) : null;",
                "            return v == null || v.isNull() ? Optional.empty() : Optional.of(v);",
                "        }",
                "",
                "        static JsonNode field(JsonNode o, String k, String at) {",
                "            return optField(o, k).orElseThrow(() -> new Missing(at + \" is missing\"));",
                "        }",
                "",
                "        static JsonNode obj(JsonNode v, String at) {",
                "            if (!v.isObject()) {",
                "                throw fail(at, \"an object\", v);",
                "            }",
                "            return v;",
                "        }",
                "",
                "        static String str(JsonNode v, String at) {",
                "            if (!v.isTextual()) {",
                "                throw fail(at, \"a string\", v);",
                "            }",
                "            return v.asText();",
                "        }",
                "",
                "        static long int64(JsonNode v, String at) {",
                "            try {",
                "                if (v.isNumber()) {",
                "                    return v.decimalValue().longValueExact();",
                "                }",
                "                if (v.isTextual()) {",
                "                    return Long.parseLong(v.asText().trim());",
                "                }",
                "            } catch (ArithmeticException | NumberFormatException e) {",
                "                throw fail(at, \"an integer\", v);",
                "            }",
                "            throw fail(at, \"an integer\", v);",
                "        }",
                "",
                "        static int int32(JsonNode v, String at) {",
                "            long n = int64(v, at);",
                "            if (n != (int) n) {",
                "                throw fail(at, \"a 32-bit integer\", v);",
                "            }",
                "            return (int) n;",
                "        }",
                "",
                "        static BigDecimal decimal(JsonNode v, String at) {",
                "            try {",
                "                if (v.isNumber()) {",
                "                    return v.decimalValue();",
                "                }",
                "                if (v.isTextual()) {",
                "                    return new BigDecimal(v.asText().trim());",
                "                }",
                "            } catch (NumberFormatException e) {",
                "                throw fail(at, \"a decimal\", v);",
                "            }",
                "            throw fail(at, \"a decimal\", v);",
                "        }",
                "",
                "        static boolean bool(JsonNode v, String at) {",
                "            if (!v.isBoolean()) {",
                "                throw fail(at, \"a boolean\", v);",
                "            }",
                "            return v.booleanValue();",
                "        }",
                "",
                "        static Instant instant(JsonNode v, String at) {",
                "            try {",
                "                return Instant.parse(str(v, at));",
                "            } catch (DateTimeParseException | Missing e) {",
                "                throw fail(at, \"an RFC 3339 time\", v);",
                "            }",
                "        }",
                "",
                "        static int version(JsonNode ce) {",
                "            try {",
                "                return optField(ce, \"schemaversion\").map(v -> int32(v, \"schemaversion\")).orElse(0);",
                "            } catch (Missing e) {",
                "                return 0;",
                "            }",
                "        }",
                "",
                "        static Meta meta(JsonNode ce) {",
                "            return new Meta(",
                "                    str(field(ce, \"id\", \"id\"), \"id\"),",
                "                    optField(ce, \"source\").filter(JsonNode::isTextual).map(JsonNode::asText).orElse(\"\"),",
                "                    instant(field(ce, \"time\", \"time\"), \"time\"),",
                "                    optField(ce, \"sequence\").map(v -> int64(v, \"sequence\")));",
                "        }",
                "    }",
            };
            for (String l : lines) {
                line(l);
            }
        }
    }
}
