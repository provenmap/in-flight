package rif.catalog;

/** {@code Entity.attribute}, such as {@code Order.total}. */
public record AttrRef(String entity, String attribute) {

    public String show() {
        return entity + "." + attribute;
    }

    @Override
    public String toString() {
        return show();
    }

    /** {@code Order.total}; anything else is not an attribute. */
    public static Result<AttrRef> parse(String text) {
        String[] parts = text.split("\\.", -1);
        return parts.length == 2 && !parts[0].isEmpty() && !parts[1].isEmpty()
                ? Result.ok(new AttrRef(parts[0], parts[1]))
                : Result.err("'" + text + "' is not Entity.attribute");
    }
}
