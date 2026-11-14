package inflight.broker;

import java.nio.charset.StandardCharsets;

/** One header on a message, as the broker carried it. A message may carry the same key more than once. */
public record Header(String key, byte[] value) {

    public static Header of(String key, String value) {
        return new Header(key, value == null ? null : value.getBytes(StandardCharsets.UTF_8));
    }

    /** The value as UTF-8 text, empty when there is none. */
    public String text() {
        return value == null ? "" : new String(value, StandardCharsets.UTF_8);
    }
}
