package rif.proxy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for {@link SchemaGateFilter}.
 *
 * @param registryUrl Apicurio Registry v3 API base, such as {@code http://apicurio:8080/apis/registry/v3}
 * @param group the artifact group event types are registered in
 * @param negativeCacheMs how long "not registered" is remembered before asking again
 */
public record SchemaGateConfig(String registryUrl, String group, long negativeCacheMs) {

    @JsonCreator
    public SchemaGateConfig(@JsonProperty("registryUrl") String registryUrl,
                            @JsonProperty("group") String group,
                            @JsonProperty("negativeCacheMs") Long negativeCacheMs) {
        this(registryUrl == null ? "http://apicurio:8080/apis/registry/v3" : registryUrl.replaceAll("/+$", ""),
                group == null ? "intent-bus" : group,
                negativeCacheMs == null ? 1000L : negativeCacheMs);
    }
}
