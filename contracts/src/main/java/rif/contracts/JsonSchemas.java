package rif.contracts;

import java.util.Map;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.FieldType;
import rif.catalog.Json;
import rif.catalog.Model;

/** Derives the JSON Schema of an event version's CloudEvent {@code data} from the catalog. The catalog is the source. */
public final class JsonSchemas {

    private JsonSchemas() {}

    public static ObjectNode of(Model.Event e) {
        ObjectNode o = Json.object();
        String name = e.ref().name();
        int version = e.ref().version();
        o.put("$schema", "http://json-schema.org/draft-07/schema#");
        o.put("$id", "urn:rif:" + name + ":v" + version);
        o.put("title", name + " v" + version);
        o.put("description", "data of " + name + " v" + version + (e.district() == null ? "" : ", produced by " + e.district()) + " to " + e.topic());
        o.setAll(objectSchema(e.fields()));
        return o;
    }

    private static ObjectNode objectSchema(Map<String, FieldType> fields) {
        ObjectNode o = Json.object();
        o.put("type", "object");
        ObjectNode properties = o.putObject("properties");
        fields.forEach((k, t) -> properties.set(k, schemaOf(t)));
        ArrayNode required = o.putArray("required");
        fields.forEach((k, t) -> {
            if (!t.optional()) {
                required.add(k);
            }
        });
        o.put("additionalProperties", true);
        return o;
    }

    private static ObjectNode schemaOf(FieldType t) {
        return switch (t) {
            case FieldType.Obj obj -> objectSchema(obj.fields());
            case FieldType.Scalar s -> {
                ObjectNode o = Json.object();
                switch (s.kind()) {
                    case STRING -> o.put("type", "string");
                    case INT -> o.put("type", "integer").put("minimum", Integer.MIN_VALUE).put("maximum", Integer.MAX_VALUE);
                    case LONG -> o.put("type", "integer");
                    case DECIMAL -> o.put("type", "string").put("pattern", "^-?[0-9]+(\\.[0-9]+)?$");
                    case BOOLEAN -> o.put("type", "boolean");
                    case TIMESTAMP -> o.put("type", "string").put("format", "date-time");
                }
                yield o;
            }
        };
    }
}
