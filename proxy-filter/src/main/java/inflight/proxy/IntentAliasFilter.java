package inflight.proxy;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.kroxylicious.kafka.common.message.ApiVersionsResponseData;
import io.kroxylicious.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import io.kroxylicious.kafka.common.message.CreateTopicsRequestData;
import io.kroxylicious.kafka.common.message.DeleteRecordsRequestData;
import io.kroxylicious.kafka.common.message.DeleteTopicsRequestData;
import io.kroxylicious.kafka.common.message.FetchRequestData;
import io.kroxylicious.kafka.common.message.FetchResponseData;
import io.kroxylicious.kafka.common.message.ListOffsetsRequestData;
import io.kroxylicious.kafka.common.message.ListOffsetsResponseData;
import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.OffsetCommitRequestData;
import io.kroxylicious.kafka.common.message.OffsetCommitResponseData;
import io.kroxylicious.kafka.common.message.OffsetDeleteRequestData;
import io.kroxylicious.kafka.common.message.OffsetDeleteResponseData;
import io.kroxylicious.kafka.common.message.OffsetFetchRequestData;
import io.kroxylicious.kafka.common.message.OffsetFetchResponseData;
import io.kroxylicious.kafka.common.message.OffsetForLeaderEpochRequestData;
import io.kroxylicious.kafka.common.message.OffsetForLeaderEpochResponseData;
import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.message.TxnOffsetCommitRequestData;
import io.kroxylicious.kafka.common.message.TxnOffsetCommitResponseData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.ConsumerGroupHeartbeatRequestFilter;
import io.kroxylicious.proxy.filter.CreateTopicsRequestFilter;
import io.kroxylicious.proxy.filter.DeleteRecordsRequestFilter;
import io.kroxylicious.proxy.filter.DeleteTopicsRequestFilter;
import io.kroxylicious.proxy.filter.FetchRequestFilter;
import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ListOffsetsRequestFilter;
import io.kroxylicious.proxy.filter.ListOffsetsResponseFilter;
import io.kroxylicious.proxy.filter.MetadataRequestFilter;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;
import io.kroxylicious.proxy.filter.OffsetCommitRequestFilter;
import io.kroxylicious.proxy.filter.OffsetCommitResponseFilter;
import io.kroxylicious.proxy.filter.OffsetDeleteRequestFilter;
import io.kroxylicious.proxy.filter.OffsetDeleteResponseFilter;
import io.kroxylicious.proxy.filter.OffsetFetchRequestFilter;
import io.kroxylicious.proxy.filter.OffsetFetchResponseFilter;
import io.kroxylicious.proxy.filter.OffsetForLeaderEpochRequestFilter;
import io.kroxylicious.proxy.filter.OffsetForLeaderEpochResponseFilter;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.TxnOffsetCommitRequestFilter;
import io.kroxylicious.proxy.filter.TxnOffsetCommitResponseFilter;

/**
 * Lets any Kafka client read an intent stream as {@code intent.<name>}.
 * <p>
 * The router writes each intent to a real topic, {@code __intent.<name>}. This filter renames it in every request a
 * consumer makes and back in every response, so the client never sees the real name. Intent streams are read-only:
 * produce, create, delete and delete-records naming an intent are refused whole.
 * <p>
 * Topics travel by name: Produce is capped at v12 and DescribeTopicPartitions is withdrawn, as in Kroxylicious's
 * multi-tenant filter. Fetch by topic id needs no rewrite: the id the client holds came from renamed metadata.
 */
public class IntentAliasFilter
        implements ApiVersionsResponseFilter,
        MetadataRequestFilter, MetadataResponseFilter,
        FetchRequestFilter, FetchResponseFilter,
        ListOffsetsRequestFilter, ListOffsetsResponseFilter,
        OffsetFetchRequestFilter, OffsetFetchResponseFilter,
        OffsetCommitRequestFilter, OffsetCommitResponseFilter,
        OffsetDeleteRequestFilter, OffsetDeleteResponseFilter,
        OffsetForLeaderEpochRequestFilter, OffsetForLeaderEpochResponseFilter,
        TxnOffsetCommitRequestFilter, TxnOffsetCommitResponseFilter,
        ConsumerGroupHeartbeatRequestFilter,
        ProduceRequestFilter, CreateTopicsRequestFilter, DeleteTopicsRequestFilter, DeleteRecordsRequestFilter {

    static final short MAX_PRODUCE_VERSION = 12;
    private static final Set<Short> WITHDRAWN = Set.of(ApiKeys.DESCRIBE_TOPIC_PARTITIONS.id);

    private final IntentAliasConfig alias;

    IntentAliasFilter(IntentAliasConfig alias) {
        this.alias = alias;
    }

    // ---- negotiation --------------------------------------------------------------------------

    @Override
    public CompletionStage<ResponseFilterResult> onApiVersionsResponse(short apiVersion, ResponseHeaderData header,
                                                                       ApiVersionsResponseData response, FilterContext context) {
        response.apiKeys().removeIf(k -> WITHDRAWN.contains(k.apiKey()));
        var produce = response.apiKeys().find(ApiKeys.PRODUCE.id);
        if (produce != null && produce.maxVersion() > MAX_PRODUCE_VERSION) {
            produce.setMaxVersion(MAX_PRODUCE_VERSION);
        }
        return context.forwardResponse(header, response);
    }

    // ---- reading: rename both ways ------------------------------------------------------------

    @Override
    public CompletionStage<RequestFilterResult> onMetadataRequest(short v, RequestHeaderData header, MetadataRequestData request, FilterContext context) {
        if (request.topics() != null) {
            request.topics().forEach(t -> toBroker(t::name, t::setName));
        }
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onMetadataResponse(short v, ResponseHeaderData header, MetadataResponseData response, FilterContext context) {
        response.topics().forEach(t -> toClient(t::name, t::setName));
        return context.forwardResponse(header, response);
    }

    @Override
    public CompletionStage<RequestFilterResult> onFetchRequest(short v, RequestHeaderData header, FetchRequestData request, FilterContext context) {
        request.topics().forEach(t -> toBroker(t::topic, t::setTopic));
        request.forgottenTopicsData().forEach(t -> toBroker(t::topic, t::setTopic));
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onFetchResponse(short v, ResponseHeaderData header, FetchResponseData response, FilterContext context) {
        response.responses().forEach(t -> toClient(t::topic, t::setTopic));
        return context.forwardResponse(header, response);
    }

    @Override
    public CompletionStage<RequestFilterResult> onListOffsetsRequest(short v, RequestHeaderData header, ListOffsetsRequestData request, FilterContext context) {
        request.topics().forEach(t -> toBroker(t::name, t::setName));
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onListOffsetsResponse(short v, ResponseHeaderData header, ListOffsetsResponseData response, FilterContext context) {
        response.topics().forEach(t -> toClient(t::name, t::setName));
        return context.forwardResponse(header, response);
    }

    @Override
    public CompletionStage<RequestFilterResult> onOffsetFetchRequest(short v, RequestHeaderData header, OffsetFetchRequestData request, FilterContext context) {
        if (request.topics() != null) {
            request.topics().forEach(t -> toBroker(t::name, t::setName));
        }
        request.groups().forEach(g -> {
            if (g.topics() != null) {
                g.topics().forEach(t -> toBroker(t::name, t::setName));
            }
        });
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onOffsetFetchResponse(short v, ResponseHeaderData header, OffsetFetchResponseData response, FilterContext context) {
        response.topics().forEach(t -> toClient(t::name, t::setName));
        response.groups().forEach(g -> g.topics().forEach(t -> toClient(t::name, t::setName)));
        return context.forwardResponse(header, response);
    }

    @Override
    public CompletionStage<RequestFilterResult> onOffsetCommitRequest(short v, RequestHeaderData header, OffsetCommitRequestData request, FilterContext context) {
        request.topics().forEach(t -> toBroker(t::name, t::setName));
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onOffsetCommitResponse(short v, ResponseHeaderData header, OffsetCommitResponseData response, FilterContext context) {
        response.topics().forEach(t -> toClient(t::name, t::setName));
        return context.forwardResponse(header, response);
    }

    @Override
    public CompletionStage<RequestFilterResult> onOffsetDeleteRequest(short v, RequestHeaderData header, OffsetDeleteRequestData request, FilterContext context) {
        request.topics().forEach(t -> toBroker(t::name, t::setName));
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onOffsetDeleteResponse(short v, ResponseHeaderData header, OffsetDeleteResponseData response, FilterContext context) {
        response.topics().forEach(t -> toClient(t::name, t::setName));
        return context.forwardResponse(header, response);
    }

    @Override
    public CompletionStage<RequestFilterResult> onOffsetForLeaderEpochRequest(short v, RequestHeaderData header, OffsetForLeaderEpochRequestData request,
                                                                              FilterContext context) {
        request.topics().forEach(t -> toBroker(t::topic, t::setTopic));
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onOffsetForLeaderEpochResponse(short v, ResponseHeaderData header, OffsetForLeaderEpochResponseData response,
                                                                                FilterContext context) {
        response.topics().forEach(t -> toClient(t::topic, t::setTopic));
        return context.forwardResponse(header, response);
    }

    @Override
    public CompletionStage<RequestFilterResult> onTxnOffsetCommitRequest(short v, RequestHeaderData header, TxnOffsetCommitRequestData request, FilterContext context) {
        request.topics().forEach(t -> toBroker(t::name, t::setName));
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onTxnOffsetCommitResponse(short v, ResponseHeaderData header, TxnOffsetCommitResponseData response,
                                                                           FilterContext context) {
        response.topics().forEach(t -> toClient(t::name, t::setName));
        return context.forwardResponse(header, response);
    }

    /** KIP-848 consumers subscribe by name in the heartbeat; assignments come back by topic id. */
    @Override
    public CompletionStage<RequestFilterResult> onConsumerGroupHeartbeatRequest(short v, RequestHeaderData header, ConsumerGroupHeartbeatRequestData request,
                                                                                FilterContext context) {
        if (request.subscribedTopicNames() != null) {
            request.setSubscribedTopicNames(request.subscribedTopicNames().stream().map(alias::toBroker).toList());
        }
        return context.forwardRequest(header, request);
    }

    // ---- writing: refused ---------------------------------------------------------------------

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(short v, RequestHeaderData header, ProduceRequestData request, FilterContext context) {
        boolean namesIntent = request.topicData().stream().anyMatch(t -> alias.isAlias(t.name()) || isBrokerSide(t.name()));
        return namesIntent ? refuse(header, request, context, "intent streams are read-only") : context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<RequestFilterResult> onCreateTopicsRequest(short v, RequestHeaderData header, CreateTopicsRequestData request, FilterContext context) {
        boolean namesIntent = request.topics().stream().anyMatch(t -> alias.isAlias(t.name()) || isBrokerSide(t.name()));
        return namesIntent ? refuse(header, request, context, "intent streams are created by the router") : context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<RequestFilterResult> onDeleteTopicsRequest(short v, RequestHeaderData header, DeleteTopicsRequestData request, FilterContext context) {
        boolean namesIntent = request.topicNames().stream().anyMatch(n -> alias.isAlias(n) || isBrokerSide(n))
                || request.topics().stream().anyMatch(t -> alias.isAlias(t.name()) || isBrokerSide(t.name()));
        return namesIntent ? refuse(header, request, context, "intent streams are deleted by the router") : context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<RequestFilterResult> onDeleteRecordsRequest(short v, RequestHeaderData header, DeleteRecordsRequestData request, FilterContext context) {
        boolean namesIntent = request.topics().stream().anyMatch(t -> alias.isAlias(t.name()) || isBrokerSide(t.name()));
        return namesIntent ? refuse(header, request, context, "intent streams are read-only") : context.forwardRequest(header, request);
    }

    // ---- helpers ------------------------------------------------------------------------------

    private CompletionStage<RequestFilterResult> refuse(RequestHeaderData header, io.kroxylicious.kafka.common.protocol.ApiMessage request,
                                                        FilterContext context, String why) {
        return context.requestFilterResultBuilder().errorResponse(header, request, Errors.TOPIC_AUTHORIZATION_FAILED, why).completed();
    }

    private boolean isBrokerSide(String name) {
        return name != null && !alias.toClient(name).equals(name);
    }

    private void toBroker(Supplier<String> get, Consumer<String> set) {
        String name = get.get();
        if (name != null && !name.isEmpty()) {
            set.accept(alias.toBroker(name));
        }
    }

    private void toClient(Supplier<String> get, Consumer<String> set) {
        String name = get.get();
        if (name != null && !name.isEmpty()) {
            set.accept(alias.toClient(name));
        }
    }
}
