package inflight.router;

/**
 * The record headers on every event in an intent topic. They say where the event came from and under which catalog
 * revision it was routed, so a consumer never has to trust the router's word for it. The payload itself is the
 * producer's CloudEvent, unchanged.
 */
public final class IntentHeaders {

    public static final String INTENT = "ib-intent";
    public static final String CATALOG_REVISION = "ib-catalog-revision";
    public static final String ENTITY = "ib-entity";
    public static final String SOURCE_TOPIC = "ib-source-topic";
    public static final String SOURCE_PARTITION = "ib-source-partition";
    public static final String SOURCE_OFFSET = "ib-source-offset";
    /** Present, as {@code true}, on an event released after a later one of its entity: it is delivered, not dropped. */
    public static final String LATE = "ib-late";

    private IntentHeaders() {}
}
