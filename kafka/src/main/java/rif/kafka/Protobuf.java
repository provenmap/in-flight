package rif.kafka;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import rif.catalog.Result;

/**
 * Protobuf records through a registry. The schema arrives as {@code .proto} text, so it is parsed at run time into a
 * descriptor: proto3 and proto2 messages, nested messages and enums, repeated fields, maps, oneofs, and the well-known
 * types (Timestamp, Duration, the wrappers, Struct, Any, Empty). After the frame's schema id comes Confluent's list of
 * message indexes (which message of the file it is: a lone zero for the first), then the message.
 *
 * <p>Values come out as protobuf's own JSON mapping has them: 64-bit integers and decimals as numbers, enums by name,
 * bytes in base64, Timestamps as ISO text, wrappers as their value.
 */
final class Protobuf {

    private Protobuf() {}

    private static final Map<String, Descriptors.FileDescriptor> FILES = new ConcurrentHashMap<>();
    private static final JsonNodeFactory F = JsonNodeFactory.instance;

    static Result<ObjectNode> decode(String key, String schema, byte[] value, int offset) {
        Descriptors.FileDescriptor file;
        try {
            file = FILES.computeIfAbsent(key, k -> parse(schema));
        } catch (RuntimeException e) {
            return Result.err("the Protobuf schema cannot be read: " + e.getMessage());
        }
        try {
            CodedInputStream in = CodedInputStream.newInstance(value, offset, value.length - offset);
            List<Integer> path = indexes(in);
            Descriptors.Descriptor d = pick(file, path);
            if (d == null) {
                return Result.err("the record names message " + path + ", which its schema does not have");
            }
            int at = offset + in.getTotalBytesRead();
            DynamicMessage m = DynamicMessage.parseFrom(d, ByteString.copyFrom(value, at, value.length - at));
            return Result.ok(object(m));
        } catch (Exception e) {
            return Result.err("the Protobuf record cannot be read with its schema: " + e.getMessage());
        }
    }

    /** Confluent's message indexes: a zigzag count, then that many zigzag indexes; a count of zero means [0]. */
    static List<Integer> indexes(CodedInputStream in) throws java.io.IOException {
        int n = in.readSInt32();
        List<Integer> out = new ArrayList<>();
        if (n == 0) {
            out.add(0);
            return out;
        }
        for (int i = 0; i < n; i++) {
            out.add(in.readSInt32());
        }
        return out;
    }

    private static Descriptors.Descriptor pick(Descriptors.FileDescriptor file, List<Integer> path) {
        if (path.isEmpty() || path.get(0) >= file.getMessageTypes().size()) {
            return null;
        }
        Descriptors.Descriptor d = file.getMessageTypes().get(path.get(0));
        for (int i = 1; i < path.size(); i++) {
            if (path.get(i) >= d.getNestedTypes().size()) {
                return null;
            }
            d = d.getNestedTypes().get(path.get(i));
        }
        return d;
    }

    // ---- to JSON ----------------------------------------------------------------------------------------------------

    static ObjectNode object(Message m) {
        ObjectNode o = F.objectNode();
        for (Map.Entry<FieldDescriptor, Object> e : m.getAllFields().entrySet()) {
            FieldDescriptor f = e.getKey();
            String name = f.getJsonName();
            if (f.isMapField()) {
                ObjectNode map = o.putObject(name);
                for (Object entry : (List<?>) e.getValue()) {
                    Message me = (Message) entry;
                    FieldDescriptor kf = me.getDescriptorForType().findFieldByName("key");
                    FieldDescriptor vf = me.getDescriptorForType().findFieldByName("value");
                    map.set(String.valueOf(me.getField(kf)), value(vf, me.getField(vf)));
                }
            } else if (f.isRepeated()) {
                ArrayNode a = o.putArray(name);
                for (Object x : (List<?>) e.getValue()) {
                    a.add(value(f, x));
                }
            } else {
                o.set(name, value(f, e.getValue()));
            }
        }
        return o;
    }

    static JsonNode value(FieldDescriptor f, Object v) {
        return switch (f.getJavaType()) {
            case INT -> F.numberNode((Integer) v);
            case LONG -> F.numberNode((Long) v);
            case FLOAT -> F.numberNode((Float) v);
            case DOUBLE -> F.numberNode((Double) v);
            case BOOLEAN -> F.booleanNode((Boolean) v);
            case STRING -> F.textNode((String) v);
            case BYTE_STRING -> F.textNode(Base64.getEncoder().encodeToString(((ByteString) v).toByteArray()));
            case ENUM -> F.textNode(((Descriptors.EnumValueDescriptor) v).getName());
            case MESSAGE -> wellKnown((Message) v);
        };
    }

    private static JsonNode wellKnown(Message m) {
        String full = m.getDescriptorForType().getFullName();
        switch (full) {
            case "google.protobuf.Timestamp", "google.protobuf.Duration" -> {
                long seconds = (Long) field(m, "seconds", 0L);
                int nanos = (Integer) field(m, "nanos", 0);
                return full.endsWith("Timestamp") ? F.textNode(java.time.Instant.ofEpochSecond(seconds, nanos).toString())
                        : F.textNode(java.time.Duration.ofSeconds(seconds, nanos).toString());
            }
            case "google.protobuf.DoubleValue", "google.protobuf.FloatValue", "google.protobuf.Int64Value", "google.protobuf.UInt64Value",
                 "google.protobuf.Int32Value", "google.protobuf.UInt32Value", "google.protobuf.BoolValue", "google.protobuf.StringValue",
                 "google.protobuf.BytesValue" -> {
                FieldDescriptor vf = m.getDescriptorForType().findFieldByName("value");
                return value(vf, m.getField(vf));
            }
            default -> {
                return object(m);
            }
        }
    }

    private static Object field(Message m, String name, Object otherwise) {
        FieldDescriptor f = m.getDescriptorForType().findFieldByName(name);
        return f == null ? otherwise : m.getField(f);
    }

    // ---- .proto text to a descriptor ------------------------------------------------------------------------------------

    private static final Map<String, Descriptors.FileDescriptor> WELL_KNOWN = Map.of(
            "google/protobuf/timestamp.proto", com.google.protobuf.TimestampProto.getDescriptor(),
            "google/protobuf/duration.proto", com.google.protobuf.DurationProto.getDescriptor(),
            "google/protobuf/wrappers.proto", com.google.protobuf.WrappersProto.getDescriptor(),
            "google/protobuf/struct.proto", com.google.protobuf.StructProto.getDescriptor(),
            "google/protobuf/any.proto", com.google.protobuf.AnyProto.getDescriptor(),
            "google/protobuf/empty.proto", com.google.protobuf.EmptyProto.getDescriptor());

    static Descriptors.FileDescriptor parse(String text) {
        Parser p = new Parser(tokens(text));
        FileDescriptorProto proto = p.file();
        List<Descriptors.FileDescriptor> deps = new ArrayList<>();
        for (String d : proto.getDependencyList()) {
            Descriptors.FileDescriptor fd = WELL_KNOWN.get(d);
            if (fd == null) {
                throw new IllegalArgumentException("it imports " + d + ", which is not one of protobuf's well-known types");
            }
            deps.add(fd);
        }
        try {
            return Descriptors.FileDescriptor.buildFrom(proto, deps.toArray(Descriptors.FileDescriptor[]::new), false);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    static List<String> tokens(String text) {
        List<String> out = new ArrayList<>();
        int i = 0;
        while (i < text.length()) {
            char c = text.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
            } else if (text.startsWith("//", i)) {
                while (i < text.length() && text.charAt(i) != '\n') {
                    i++;
                }
            } else if (text.startsWith("/*", i)) {
                int end = text.indexOf("*/", i + 2);
                i = end < 0 ? text.length() : end + 2;
            } else if (c == '"' || c == '\'') {
                int j = i + 1;
                while (j < text.length() && text.charAt(j) != c) {
                    j += text.charAt(j) == '\\' ? 2 : 1;
                }
                out.add(text.substring(i, Math.min(j + 1, text.length())));
                i = j + 1;
            } else if (Character.isLetterOrDigit(c) || c == '_' || c == '.' || c == '-' || c == '+') {
                int j = i;
                while (j < text.length() && (Character.isLetterOrDigit(text.charAt(j)) || "_.-+".indexOf(text.charAt(j)) >= 0)) {
                    j++;
                }
                out.add(text.substring(i, j));
                i = j;
            } else {
                out.add(String.valueOf(c));
                i++;
            }
        }
        return out;
    }

    /** A recursive descent over the tokens: enough of the language to describe messages; options are skipped. */
    static final class Parser {
        private final List<String> t;
        private int i;
        private String pkg = "";
        private boolean proto3;

        Parser(List<String> tokens) {
            this.t = tokens;
        }

        private String peek() {
            return i < t.size() ? t.get(i) : "";
        }

        private String next() {
            if (i >= t.size()) {
                throw new IllegalArgumentException("the schema ends early");
            }
            return t.get(i++);
        }

        private void expect(String s) {
            String got = next();
            if (!got.equals(s)) {
                throw new IllegalArgumentException("expected " + s + " but found " + got);
            }
        }

        private static String unquote(String s) {
            return s.length() >= 2 ? s.substring(1, s.length() - 1) : s;
        }

        FileDescriptorProto file() {
            FileDescriptorProto.Builder f = FileDescriptorProto.newBuilder().setName("schema.proto");
            while (i < t.size()) {
                String w = next();
                switch (w) {
                    case "syntax", "edition" -> {
                        expect("=");
                        String v = unquote(next());
                        proto3 = v.equals("proto3");
                        f.setSyntax(proto3 ? "proto3" : "proto2");
                        expect(";");
                    }
                    case "package" -> {
                        pkg = next();
                        f.setPackage(pkg);
                        expect(";");
                    }
                    case "import" -> {
                        String n = next();
                        if (n.equals("public") || n.equals("weak")) {
                            n = next();
                        }
                        f.addDependency(unquote(n));
                        expect(";");
                    }
                    case "option" -> skipStatement();
                    case "message" -> f.addMessageType(message());
                    case "enum" -> f.addEnumType(enumType());
                    case "service" -> skipBlock();
                    case ";" -> { }
                    default -> throw new IllegalArgumentException("unexpected " + w);
                }
            }
            if (!proto3 && !f.hasSyntax()) {
                f.setSyntax("proto2");
            }
            return f.build();
        }

        private void skipStatement() {
            while (!next().equals(";")) {
                // an option, read and dropped
            }
        }

        private void skipBlock() {
            while (!peek().equals("{")) {
                next();
            }
            int depth = 0;
            do {
                String w = next();
                if (w.equals("{")) {
                    depth++;
                } else if (w.equals("}")) {
                    depth--;
                }
            } while (depth > 0);
        }

        private EnumDescriptorProto enumType() {
            EnumDescriptorProto.Builder e = EnumDescriptorProto.newBuilder().setName(next());
            expect("{");
            while (!peek().equals("}")) {
                String w = next();
                if (w.equals("option") || w.equals("reserved")) {
                    skipStatement();
                    continue;
                }
                if (w.equals(";")) {
                    continue;
                }
                expect("=");
                int n = Integer.decode(next());
                if (peek().equals("[")) {
                    while (!next().equals("]")) {
                        // value options
                    }
                }
                expect(";");
                e.addValue(EnumValueDescriptorProto.newBuilder().setName(w).setNumber(n));
            }
            expect("}");
            return e.build();
        }

        private DescriptorProto message() {
            DescriptorProto.Builder m = DescriptorProto.newBuilder().setName(next());
            expect("{");
            body(m);
            expect("}");
            return m.build();
        }

        private void body(DescriptorProto.Builder m) {
            List<Integer> optionals = new ArrayList<>();
            bodyItems(m, optionals);
            // proto3 optional: a synthetic oneof of one each, as protoc makes them, after every real oneof.
            for (int idx : optionals) {
                int index = m.getOneofDeclCount();
                m.addOneofDecl(OneofDescriptorProto.newBuilder().setName("_" + m.getField(idx).getName()));
                m.getFieldBuilder(idx).setOneofIndex(index).setProto3Optional(true);
            }
        }

        private void bodyItems(DescriptorProto.Builder m, List<Integer> optionals) {
            while (!peek().equals("}")) {
                String w = peek();
                switch (w) {
                    case "message" -> {
                        next();
                        m.addNestedType(message());
                    }
                    case "enum" -> {
                        next();
                        m.addEnumType(enumType());
                    }
                    case "option", "reserved", "extensions" -> {
                        next();
                        skipStatement();
                    }
                    case "extend" -> skipBlock();
                    case ";" -> next();
                    case "oneof" -> {
                        next();
                        int index = m.getOneofDeclCount();
                        m.addOneofDecl(OneofDescriptorProto.newBuilder().setName(next()));
                        expect("{");
                        while (!peek().equals("}")) {
                            if (peek().equals("option")) {
                                next();
                                skipStatement();
                                continue;
                            }
                            m.addField(field(m, null).setOneofIndex(index));
                        }
                        expect("}");
                    }
                    default -> {
                        String label = null;
                        if (w.equals("repeated") || w.equals("optional") || w.equals("required")) {
                            label = next();
                        }
                        if (peek().equals("map")) {
                            mapField(m);
                        } else {
                            FieldDescriptorProto.Builder f = field(m, label);
                            if ("optional".equals(label) && proto3) {
                                optionals.add(m.getFieldCount());
                            }
                            m.addField(f);
                        }
                    }
                }
            }
        }

        private FieldDescriptorProto.Builder field(DescriptorProto.Builder owner, String label) {
            String type = next();
            String name = next();
            expect("=");
            int number = Integer.decode(next());
            if (peek().equals("[")) {
                while (!next().equals("]")) {
                    // field options such as [deprecated = true]
                }
            }
            expect(";");
            FieldDescriptorProto.Builder f = FieldDescriptorProto.newBuilder().setName(name).setNumber(number);
            f.setLabel("repeated".equals(label) ? FieldDescriptorProto.Label.LABEL_REPEATED
                    : "required".equals(label) ? FieldDescriptorProto.Label.LABEL_REQUIRED : FieldDescriptorProto.Label.LABEL_OPTIONAL);
            FieldDescriptorProto.Type scalar = scalar(type);
            if (scalar != null) {
                f.setType(scalar);
            } else {
                // A message or an enum, resolved by the descriptor builder (allowUnknownDependencies is off, so it must exist).
                f.setTypeName(type);
            }
            return f;
        }

        private void mapField(DescriptorProto.Builder m) {
            expect("map");
            expect("<");
            String k = next();
            expect(",");
            String v = next();
            expect(">");
            String name = next();
            expect("=");
            int number = Integer.decode(next());
            if (peek().equals("[")) {
                while (!next().equals("]")) {
                    // options
                }
            }
            expect(";");
            String entry = Character.toUpperCase(name.charAt(0)) + camel(name.substring(1)) + "Entry";
            DescriptorProto.Builder e = DescriptorProto.newBuilder().setName(entry).setOptions(MessageOptions.newBuilder().setMapEntry(true));
            e.addField(FieldDescriptorProto.newBuilder().setName("key").setNumber(1).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL).setType(scalar(k)));
            FieldDescriptorProto.Builder vf = FieldDescriptorProto.newBuilder().setName("value").setNumber(2).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
            FieldDescriptorProto.Type vs = scalar(v);
            if (vs != null) {
                vf.setType(vs);
            } else {
                vf.setTypeName(v);
            }
            e.addField(vf);
            m.addNestedType(e);
            m.addField(FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(entry));
        }

        private static String camel(String s) {
            StringBuilder b = new StringBuilder();
            boolean up = false;
            for (char c : s.toCharArray()) {
                if (c == '_') {
                    up = true;
                } else {
                    b.append(up ? Character.toUpperCase(c) : c);
                    up = false;
                }
            }
            return b.toString();
        }

        static FieldDescriptorProto.Type scalar(String type) {
            return switch (type) {
                case "double" -> FieldDescriptorProto.Type.TYPE_DOUBLE;
                case "float" -> FieldDescriptorProto.Type.TYPE_FLOAT;
                case "int64" -> FieldDescriptorProto.Type.TYPE_INT64;
                case "uint64" -> FieldDescriptorProto.Type.TYPE_UINT64;
                case "int32" -> FieldDescriptorProto.Type.TYPE_INT32;
                case "fixed64" -> FieldDescriptorProto.Type.TYPE_FIXED64;
                case "fixed32" -> FieldDescriptorProto.Type.TYPE_FIXED32;
                case "bool" -> FieldDescriptorProto.Type.TYPE_BOOL;
                case "string" -> FieldDescriptorProto.Type.TYPE_STRING;
                case "bytes" -> FieldDescriptorProto.Type.TYPE_BYTES;
                case "uint32" -> FieldDescriptorProto.Type.TYPE_UINT32;
                case "sfixed32" -> FieldDescriptorProto.Type.TYPE_SFIXED32;
                case "sfixed64" -> FieldDescriptorProto.Type.TYPE_SFIXED64;
                case "sint32" -> FieldDescriptorProto.Type.TYPE_SINT32;
                case "sint64" -> FieldDescriptorProto.Type.TYPE_SINT64;
                default -> null;
            };
        }
    }
}
