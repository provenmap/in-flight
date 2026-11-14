package inflight.proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kafka.common.message.ApiVersionsResponseData;
import io.kroxylicious.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import io.kroxylicious.kafka.common.message.CreateTopicsRequestData;
import io.kroxylicious.kafka.common.message.FetchRequestData;
import io.kroxylicious.kafka.common.message.FetchResponseData;
import io.kroxylicious.kafka.common.message.ListOffsetsRequestData;
import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.OffsetCommitRequestData;
import io.kroxylicious.kafka.common.message.OffsetFetchRequestData;
import io.kroxylicious.kafka.common.message.OffsetFetchResponseData;
import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.testing.filter.context.MockFilterContext;

class IntentAliasFilterTest {

    private final IntentAliasFilter filter = new IntentAliasFilter(IntentAliasConfig.DEFAULT);

    private static FilterContext ctx(ApiMessage header, ApiMessage msg) {
        return MockFilterContext.builder(header, msg).build();
    }

    private static ApiMessage forwarded(CompletionStage<RequestFilterResult> r) throws Exception {
        RequestFilterResult res = r.toCompletableFuture().get();
        assertFalse(res.shortCircuitResponse(), "expected the request to be forwarded");
        return res.message();
    }

    private static ApiMessage forwardedResponse(CompletionStage<ResponseFilterResult> r) throws Exception {
        return r.toCompletableFuture().get().message();
    }

    @Test
    void theNamingRuleIsABijectionOnThePrefixes() {
        IntentAliasConfig a = IntentAliasConfig.DEFAULT;
        assertEquals("__intent.order-total-changes", a.toBroker("intent.order-total-changes"));
        assertEquals("intent.order-total-changes", a.toClient("__intent.order-total-changes"));
        assertEquals("orders.placed", a.toBroker("orders.placed"));
        assertEquals("orders.placed", a.toClient("orders.placed"));
        assertEquals("intent.", a.toBroker("intent."), "the bare prefix is not an intent");
    }

    @Test
    void metadataIsAskedUnderTheRealNameAndAnsweredUnderTheAlias() throws Exception {
        MetadataRequestData req = new MetadataRequestData();
        req.topics().add(new MetadataRequestData.MetadataRequestTopic().setName("intent.totals"));
        req.topics().add(new MetadataRequestData.MetadataRequestTopic().setName("orders.placed"));
        RequestHeaderData hdr = new RequestHeaderData().setRequestApiKey(ApiKeys.METADATA.id);
        MetadataRequestData out = (MetadataRequestData) forwarded(filter.onMetadataRequest((short) 12, hdr, req, ctx(hdr, req)));
        assertEquals(List.of("__intent.totals", "orders.placed"), out.topics().stream().map(MetadataRequestData.MetadataRequestTopic::name).toList());

        MetadataResponseData resp = new MetadataResponseData();
        resp.topics().add(new MetadataResponseData.MetadataResponseTopic().setName("__intent.totals"));
        resp.topics().add(new MetadataResponseData.MetadataResponseTopic().setName("orders.placed"));
        ResponseHeaderData rh = new ResponseHeaderData();
        MetadataResponseData back = (MetadataResponseData) forwardedResponse(filter.onMetadataResponse((short) 12, rh, resp, ctx(rh, resp)));
        assertEquals(List.of("intent.totals", "orders.placed"),
                back.topics().stream().map(MetadataResponseData.MetadataResponseTopic::name).sorted().toList());
    }

    @Test
    void listAllMetadataPassesThrough() throws Exception {
        MetadataRequestData req = new MetadataRequestData().setTopics(null);
        RequestHeaderData hdr = new RequestHeaderData();
        assertNull(((MetadataRequestData) forwarded(filter.onMetadataRequest((short) 12, hdr, req, ctx(hdr, req)))).topics());
    }

    @Test
    void fetchByNameIsRenamedBothWaysAndFetchByIdIsLeftAlone() throws Exception {
        FetchRequestData req = new FetchRequestData();
        req.topics().add(new FetchRequestData.FetchTopic().setTopic("intent.totals"));
        req.topics().add(new FetchRequestData.FetchTopic().setTopic(""));
        RequestHeaderData hdr = new RequestHeaderData();
        FetchRequestData out = (FetchRequestData) forwarded(filter.onFetchRequest((short) 12, hdr, req, ctx(hdr, req)));
        assertEquals(List.of("__intent.totals", ""), out.topics().stream().map(FetchRequestData.FetchTopic::topic).toList());

        FetchResponseData resp = new FetchResponseData();
        resp.responses().add(new FetchResponseData.FetchableTopicResponse().setTopic("__intent.totals"));
        ResponseHeaderData rh = new ResponseHeaderData();
        FetchResponseData back = (FetchResponseData) forwardedResponse(filter.onFetchResponse((short) 12, rh, resp, ctx(rh, resp)));
        assertEquals("intent.totals", back.responses().get(0).topic());
    }

    @Test
    void offsetsCommitAndFetchUnderTheRealNameAndComeBackUnderTheAlias() throws Exception {
        OffsetCommitRequestData commit = new OffsetCommitRequestData().setGroupId("g");
        commit.topics().add(new OffsetCommitRequestData.OffsetCommitRequestTopic().setName("intent.totals"));
        RequestHeaderData hdr = new RequestHeaderData();
        assertEquals("__intent.totals",
                ((OffsetCommitRequestData) forwarded(filter.onOffsetCommitRequest((short) 8, hdr, commit, ctx(hdr, commit)))).topics().get(0).name());

        OffsetFetchRequestData fetch = new OffsetFetchRequestData();
        OffsetFetchRequestData.OffsetFetchRequestGroup group = new OffsetFetchRequestData.OffsetFetchRequestGroup().setGroupId("g");
        group.topics().add(new OffsetFetchRequestData.OffsetFetchRequestTopics().setName("intent.totals"));
        fetch.groups().add(group);
        OffsetFetchRequestData f = (OffsetFetchRequestData) forwarded(filter.onOffsetFetchRequest((short) 8, hdr, fetch, ctx(hdr, fetch)));
        assertEquals("__intent.totals", f.groups().get(0).topics().get(0).name());

        OffsetFetchResponseData resp = new OffsetFetchResponseData();
        OffsetFetchResponseData.OffsetFetchResponseGroup rg = new OffsetFetchResponseData.OffsetFetchResponseGroup().setGroupId("g");
        rg.topics().add(new OffsetFetchResponseData.OffsetFetchResponseTopics().setName("__intent.totals"));
        resp.groups().add(rg);
        ResponseHeaderData rh = new ResponseHeaderData();
        OffsetFetchResponseData back = (OffsetFetchResponseData) forwardedResponse(filter.onOffsetFetchResponse((short) 8, rh, resp, ctx(rh, resp)));
        assertEquals("intent.totals", back.groups().get(0).topics().get(0).name());
    }

    @Test
    void listOffsetsIsRenamed() throws Exception {
        ListOffsetsRequestData req = new ListOffsetsRequestData();
        req.topics().add(new ListOffsetsRequestData.ListOffsetsTopic().setName("intent.totals"));
        RequestHeaderData hdr = new RequestHeaderData();
        assertEquals("__intent.totals",
                ((ListOffsetsRequestData) forwarded(filter.onListOffsetsRequest((short) 7, hdr, req, ctx(hdr, req)))).topics().get(0).name());
    }

    @Test
    void kip848HeartbeatsSubscribeUnderTheRealName() throws Exception {
        ConsumerGroupHeartbeatRequestData req = new ConsumerGroupHeartbeatRequestData().setGroupId("g")
                .setSubscribedTopicNames(List.of("intent.totals", "orders.placed"));
        RequestHeaderData hdr = new RequestHeaderData();
        ConsumerGroupHeartbeatRequestData out = (ConsumerGroupHeartbeatRequestData) forwarded(
                filter.onConsumerGroupHeartbeatRequest((short) 0, hdr, req, ctx(hdr, req)));
        assertEquals(List.of("__intent.totals", "orders.placed"), out.subscribedTopicNames());
    }

    @Test
    void producingToAnIntentStreamIsRefusedUnderEitherName() throws Exception {
        for (String name : List.of("intent.totals", "__intent.totals")) {
            ProduceRequestData req = new ProduceRequestData().setAcks((short) 1);
            req.topicData().add(new ProduceRequestData.TopicProduceData().setName(name));
            RequestHeaderData hdr = new RequestHeaderData().setRequestApiKey(ApiKeys.PRODUCE.id).setRequestApiVersion((short) 12);
            RequestFilterResult res = filter.onProduceRequest((short) 12, hdr, req, ctx(hdr, req)).toCompletableFuture().get();
            assertTrue(res.shortCircuitResponse(), name + " should be refused");
        }
    }

    @Test
    void producingToOrdinaryTopicsPassesThrough() throws Exception {
        ProduceRequestData req = new ProduceRequestData().setAcks((short) 1);
        req.topicData().add(new ProduceRequestData.TopicProduceData().setName("orders.placed"));
        RequestHeaderData hdr = new RequestHeaderData();
        assertEquals(req, forwarded(filter.onProduceRequest((short) 12, hdr, req, ctx(hdr, req))));
    }

    @Test
    void creatingAnIntentStreamIsRefused() throws Exception {
        CreateTopicsRequestData create = new CreateTopicsRequestData();
        create.topics().add(new CreateTopicsRequestData.CreatableTopic().setName("intent.x"));
        RequestHeaderData hdr = new RequestHeaderData().setRequestApiKey(ApiKeys.CREATE_TOPICS.id).setRequestApiVersion((short) 7);
        assertTrue(filter.onCreateTopicsRequest((short) 7, hdr, create, ctx(hdr, create)).toCompletableFuture().get().shortCircuitResponse());
    }

    @Test
    void produceIsCappedAtV12AndDescribeTopicPartitionsIsWithdrawn() throws Exception {
        ApiVersionsResponseData resp = new ApiVersionsResponseData();
        resp.apiKeys().add(new ApiVersionsResponseData.ApiVersion().setApiKey(ApiKeys.PRODUCE.id).setMinVersion((short) 3).setMaxVersion((short) 13));
        resp.apiKeys().add(new ApiVersionsResponseData.ApiVersion().setApiKey(ApiKeys.DESCRIBE_TOPIC_PARTITIONS.id).setMinVersion((short) 0).setMaxVersion((short) 0));
        resp.apiKeys().add(new ApiVersionsResponseData.ApiVersion().setApiKey(ApiKeys.FETCH.id).setMinVersion((short) 4).setMaxVersion((short) 17));
        ResponseHeaderData rh = new ResponseHeaderData();
        ApiVersionsResponseData out = (ApiVersionsResponseData) forwardedResponse(filter.onApiVersionsResponse((short) 3, rh, resp, ctx(rh, resp)));
        Map<Short, Short> max = out.apiKeys().stream().collect(Collectors.toMap(ApiVersionsResponseData.ApiVersion::apiKey, ApiVersionsResponseData.ApiVersion::maxVersion));
        assertEquals(Map.of(ApiKeys.PRODUCE.id, (short) 12, ApiKeys.FETCH.id, (short) 17), max);
    }

    @Test
    @SuppressWarnings("rawtypes")
    void theFactoryIsDiscoverableAndReadsItsConfig() throws Exception {
        List<String> names = ServiceLoader.load(FilterFactory.class).stream().map(p -> p.type().getName()).toList();
        assertTrue(names.contains("inflight.proxy.IntentAliasFilterFactory"), names.toString());
        IntentAliasConfig cfg = new ObjectMapper().readValue("{\"clientPrefix\":\"view.\",\"brokerPrefix\":\"__view.\"}", IntentAliasConfig.class);
        assertEquals("__view.a", cfg.toBroker("view.a"));
        assertEquals(IntentAliasConfig.DEFAULT, new ObjectMapper().readValue("{}", IntentAliasConfig.class));
    }
}
