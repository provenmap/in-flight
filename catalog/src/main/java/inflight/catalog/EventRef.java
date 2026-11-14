package inflight.catalog;

import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** One event type at one version. Versions are immutable once published. */
public record EventRef(String name, int version) {

    private static final Pattern TEXT = Pattern.compile("^([A-Za-z][A-Za-z0-9]*)@v(\\d+)$");

    /** By name, then version. */
    public static final Comparator<EventRef> ORDER = Comparator.comparing(EventRef::name).thenComparingInt(EventRef::version);

    public String show() {
        return name + "@v" + version;
    }

    @Override
    public String toString() {
        return show();
    }

    /** {@code OrderPlaced@v2}, the way {@link #show()} writes it. */
    public static Result<EventRef> parse(String text) {
        Matcher m = TEXT.matcher(text.trim());
        return m.matches() ? Result.ok(new EventRef(m.group(1), Integer.parseInt(m.group(2))))
                : Result.err("'" + text + "' is not an event version such as OrderPlaced@v2");
    }
}
