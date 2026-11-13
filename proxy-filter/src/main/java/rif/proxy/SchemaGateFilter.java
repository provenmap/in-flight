package rif.proxy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.kafka.common.record.internal.MemoryRecords;
import io.kroxylicious.kafka.common.record.internal.Record;
import io.kroxylicious.kafka.common.record.internal.RecordBatch;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;

/**
 * The broker-side half of the change gate. A produce request is refused whole if any record is a CloudEvent
 * whose {@code type} at {@code schemaversion} is not registered. So no client that writes through the proxy,
 * stock or not, can send a new event shape before {@code contracts approve} has registered it.
 * <p>
 * Records that are not structured CloudEvents with a {@code schemaversion} pass untouched, as do topics whose names
 * start with an underscore (internal topics). A client that writes to the brokers without the proxy is not gated:
 * keep producers behind the proxy, as network policy.
 * <p>
 * In report-only mode it refuses nothing: it logs what it would have refused, and why, and lets the request through,
 * so a team can put the gate in front of a live flow and see what it would do before it does it.
 */
public class SchemaGateFilter implements ProduceRequestFilter {

    private static final ObjectMapper JSON = new ObjectMapper();

    record EventVersion(String type, int version) {
        String show() {
            return type + " v" + version;
        }
    }

    private static final System.Logger LOG = System.getLogger(SchemaGateFilter.class.getName());

    private final SchemaLookup lookup;
    private final FilterDispatchExecutor dispatch;
    private final boolean reportOnly;
    /** What report-only mode would have refused, for tests and for the log. */
    final List<String> reported = new java.util.concurrent.CopyOnWriteArrayList<>();

    SchemaGateFilter(SchemaLookup lookup, FilterDispatchExecutor dispatch) {
        this(lookup, dispatch, false);
    }

    SchemaGateFilter(SchemaLookup lookup, FilterDispatchExecutor dispatch, boolean reportOnly) {
        this.lookup = lookup;
        this.dispatch = dispatch;
        this.reportOnly = reportOnly;
    }

    private RequestFilterResult report(RequestHeaderData header, ProduceRequestData request, FilterContext context, String why) {
        String topics = String.join(", ", request.topicData().stream().map(ProduceRequestData.TopicProduceData::name).toList());
        String line = "schema gate, report only: would refuse a produce to " + topics + ": " + why;
        reported.add(line);
        LOG.log(System.Logger.Level.WARNING, line);
        return context.requestFilterResultBuilder().forward(header, request).build();
    }

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request,
                                                                 FilterContext context) {
        Set<EventVersion> versions = eventVersions(request);
        if (versions.isEmpty()) {
            return context.forwardRequest(header, request);
        }
        List<EventVersion> ordered = new ArrayList<>(versions);
        List<CompletableFuture<Boolean>> checks = ordered.stream()
                .map(v -> lookup.isRegistered(v.type(), v.version()).toCompletableFuture())
                .toList();
        CompletionStage<List<EventVersion>> missing = CompletableFuture.allOf(checks.toArray(CompletableFuture[]::new))
                .thenApply(unused -> {
                    List<EventVersion> out = new ArrayList<>();
                    for (int i = 0; i < ordered.size(); i++) {
                        if (!checks.get(i).join()) {
                            out.add(ordered.get(i));
                        }
                    }
                    return out;
                });
        if (dispatch != null) {
            missing = dispatch.completeOnFilterDispatchThread(missing);
        }
        return missing.handle((absent, failure) -> {
            if (failure != null && reportOnly) {
                return report(header, request, context, "the schema registry is unreachable (" + failure.getMessage() + ")");
            }
            if (failure != null) {
                return context.requestFilterResultBuilder()
                        .errorResponse(header, request, Errors.POLICY_VIOLATION, "schema registry unreachable, so the schema gate refuses: " + failure.getMessage())
                        .build();
            }
            if (!absent.isEmpty()) {
                String names = String.join(", ", absent.stream().map(EventVersion::show).toList());
                if (reportOnly) {
                    return report(header, request, context, names + " not registered");
                }
                return context.requestFilterResultBuilder()
                        .errorResponse(header, request, Errors.INVALID_RECORD,
                                names + " not registered: a contract change is pending, so it cannot be produced yet")
                        .build();
            }
            return context.requestFilterResultBuilder().forward(header, request).build();
        });
    }

    /** Every (type, schemaversion) named by the CloudEvents in the request. */
    static Set<EventVersion> eventVersions(ProduceRequestData request) {
        Set<EventVersion> out = new LinkedHashSet<>();
        for (var topic : request.topicData()) {
            if (topic.name() != null && topic.name().startsWith("_")) {
                continue;
            }
            for (var partition : topic.partitionData()) {
                if (!(partition.records() instanceof MemoryRecords records)) {
                    continue;
                }
                for (RecordBatch batch : records.batches()) {
                    for (Record r : batch) {
                        EventVersion v = parse(r);
                        if (v != null) {
                            out.add(v);
                        }
                    }
                }
            }
        }
        return out;
    }

    private static EventVersion parse(Record r) {
        if (!r.hasValue() || r.valueSize() < 2) {
            return null;
        }
        ByteBuffer value = r.value().duplicate();
        byte[] bytes = new byte[value.remaining()];
        value.get(bytes);
        if (bytes[0] != '{') {
            return null;
        }
        try {
            JsonNode node = JSON.readTree(new String(bytes, StandardCharsets.UTF_8));
            JsonNode type = node.get("type");
            JsonNode version = node.get("schemaversion");
            if (type == null || !type.isTextual() || version == null) {
                return null;
            }
            int v = version.isNumber() ? version.asInt() : Integer.parseInt(version.asText());
            return new EventVersion(type.asText(), v);
        }
        catch (Exception e) {
            return null;
        }
    }
}
