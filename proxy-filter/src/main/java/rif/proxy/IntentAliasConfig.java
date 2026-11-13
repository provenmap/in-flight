package rif.proxy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for {@link IntentAliasFilter}.
 *
 * @param clientPrefix the prefix clients use, {@code intent.} by default
 * @param brokerPrefix the prefix of the real topics the router writes, {@code __intent.} by default
 */
public record IntentAliasConfig(String clientPrefix, String brokerPrefix) {

    public static final IntentAliasConfig DEFAULT = new IntentAliasConfig("intent.", "__intent.");

    @JsonCreator
    public IntentAliasConfig(@JsonProperty("clientPrefix") String clientPrefix,
                             @JsonProperty("brokerPrefix") String brokerPrefix) {
        this.clientPrefix = clientPrefix == null || clientPrefix.isEmpty() ? "intent." : clientPrefix;
        this.brokerPrefix = brokerPrefix == null || brokerPrefix.isEmpty() ? "__intent." : brokerPrefix;
        if (this.clientPrefix.equals(this.brokerPrefix)) {
            throw new IllegalArgumentException("clientPrefix and brokerPrefix must differ");
        }
    }

    /** The name the broker knows for a name a client used. Names outside the client prefix pass through. */
    public String toBroker(String clientName) {
        return isAlias(clientName) ? brokerPrefix + clientName.substring(clientPrefix.length()) : clientName;
    }

    /** The name a client sees for a broker topic. Names outside the broker prefix pass through. */
    public String toClient(String brokerName) {
        return brokerName != null && brokerName.startsWith(brokerPrefix)
                ? clientPrefix + brokerName.substring(brokerPrefix.length())
                : brokerName;
    }

    public boolean isAlias(String clientName) {
        return clientName != null && clientName.startsWith(clientPrefix) && clientName.length() > clientPrefix.length();
    }
}
