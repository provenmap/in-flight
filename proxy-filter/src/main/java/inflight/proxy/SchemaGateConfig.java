package inflight.proxy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for {@link SchemaGateFilter}.
 *
 * @param registryUrl Apicurio Registry v3 API base, such as {@code http://apicurio:8080/apis/registry/v3}
 * @param group the artifact group event types are registered in
 * @param negativeCacheMs how long "not registered" is remembered before asking again
 * @param reportOnly log what the gate would refuse, and let it through: to try the gate on a live flow without risk
 */
public record SchemaGateConfig(String registryUrl, String group, long negativeCacheMs, boolean reportOnly) {

    public SchemaGateConfig(String registryUrl, String group, long negativeCacheMs) {
        this(registryUrl, group, negativeCacheMs, false);
    }

    @JsonCreator
    public SchemaGateConfig(@JsonProperty("registryUrl") String registryUrl,
                            @JsonProperty("group") String group,
                            @JsonProperty("negativeCacheMs") Long negativeCacheMs,
                            @JsonProperty("reportOnly") Boolean reportOnly) {
        this(registryUrl == null ? "http://apicurio:8080/apis/registry/v3" : registryUrl.replaceAll("/+$", ""),
                group == null ? "intent-bus" : group,
                negativeCacheMs == null ? 1000L : negativeCacheMs,
                reportOnly != null && reportOnly);
    }
}
