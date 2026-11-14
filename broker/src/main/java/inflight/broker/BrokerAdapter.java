package inflight.broker;

import java.time.Duration;
import java.util.List;
import java.util.Set;

/**
 * One kind of broker. An adapter is a module of its own with the broker's client library, registered for
 * {@link java.util.ServiceLoader} in {@code META-INF/services/inflight.broker.BrokerAdapter}; {@link Brokers} finds it
 * by {@link #kind()}. It turns a {@link Profile} into a working connection and opens {@link Broker} sessions on it.
 *
 * <p>An adapter keeps whatever a connection needs beyond its address (credentials, TLS settings) itself, from
 * {@link #configure(Profile)}, keyed by the address: the rest of the tool passes only the address around.
 */
public interface BrokerAdapter {

    /** An authentication preset: its id, as profiles name it, and what it is for. */
    record Preset(String id, String help) {}

    /** The id profiles and {@link Endpoint}s name this adapter by, in lower case: {@code kafka}. */
    String kind();

    /** What people call it, for messages: {@code Kafka}. */
    String displayName();

    /** What a profile's address is, with an example, for messages: "the cluster's bootstrap servers, such as ...". */
    String addressHelp();

    /** The address to use when none is given, such as a local development broker. */
    String defaultAddress();

    /** What this adapter can do. See {@link Capability}: declare only what the broker keeps. */
    Set<Capability> capabilities();

    /** The authentication presets, the first being the default. */
    List<Preset> presets();

    /** The secret roles a profile may give, by name; a {@code username} may be plain, the rest must be references. */
    Set<String> secretRoles();

    /**
     * What is wrong with the broker-specific parts of a profile (its auth preset, secrets, settings), before anything
     * connects. {@link Profile#problems()} adds these to the checks every profile gets. Never echo a secret back.
     */
    List<String> problems(Profile p);

    /**
     * Makes every session opened on {@code p.address()} from now on connect as {@code p} says: resolves its secrets
     * (through {@link Secrets}) and keeps the result. Throws {@link IllegalStateException} naming the reference when a
     * secret cannot be read.
     */
    void configure(Profile p);

    /** Whether the broker at {@code address} answers, within about {@code timeout}. Throws with its reason when not. */
    ClusterInfo describe(String address, Duration timeout);

    /** A session with the broker at {@code address}, connected as the last {@link #configure} for it said. */
    Broker open(String address);
}
