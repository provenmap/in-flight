package inflight.catalog;

import com.fasterxml.jackson.databind.JsonNode;

/** What an event does to one attribute of an entity. */
public record Effect(Op op, AttrRef attribute, Source source) {

    public enum Op {
        ASSIGN, ADD, SUBTRACT;

        public String show() {
            return name().toLowerCase();
        }

        static Op of(String text) {
            for (Op op : values()) {
                if (op.show().equals(text)) {
                    return op;
                }
            }
            return null;
        }
    }

    /** Where the new value comes from: a field of the event, or a constant. */
    public sealed interface Source {
        String show();
    }

    public record FromField(FieldPath field) implements Source {
        @Override
        public String show() {
            return "from " + field.show();
        }
    }

    public record Const(JsonNode value) implements Source {
        @Override
        public String show() {
            return "= " + Json.write(value);
        }
    }

    public String show() {
        return op.show() + " " + attribute.show() + " " + source.show();
    }
}
