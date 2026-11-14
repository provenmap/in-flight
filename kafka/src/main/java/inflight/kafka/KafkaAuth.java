package inflight.kafka;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import inflight.broker.BrokerAdapter.Preset;
import inflight.broker.Profile;
import inflight.broker.Secrets;

/** Kafka's authentication presets: what each needs from a profile, and the Java client settings it becomes. */
public final class KafkaAuth {

    /** The presets, by what people call them. */
    public static final List<Preset> PRESETS = List.of(
            new Preset("none", "Plaintext, no authentication: a local or private cluster"),
            new Preset("ssl", "TLS, no client authentication; add settings.ssl.truststore.location for a private CA"),
            new Preset("sasl-plain", "SASL/PLAIN over TLS: Confluent Cloud (an API key as username, its secret as password)"),
            new Preset("scram-sha-256", "SASL/SCRAM-SHA-256 over TLS: self-hosted, Redpanda, Aiven, MSK with SCRAM"),
            new Preset("scram-sha-512", "SASL/SCRAM-SHA-512 over TLS: self-hosted, MSK with SCRAM"),
            new Preset("mtls", "Mutual TLS: a client certificate in settings.ssl.keystore.location"),
            new Preset("msk-iam", "Amazon MSK with IAM: credentials from the AWS default chain (needs the aws-msk-iam-auth library)"));

    /** The secret roles a preset may take; a username may be plain text, the rest must be references. */
    public static final Set<String> SECRET_ROLES = Set.of("username", "password", "keystorePassword", "keyPassword", "truststorePassword");

    private KafkaAuth() {}

    /** What a preset needs that the profile does not give. */
    static List<String> problems(Profile p) {
        List<String> out = new ArrayList<>();
        switch (p.auth()) {
            case "sasl-plain", "scram-sha-256", "scram-sha-512" -> {
                if (!p.secrets().containsKey("username") || !p.secrets().containsKey("password")) {
                    out.add(p.auth() + " needs secrets.username and secrets.password");
                }
            }
            case "mtls" -> {
                if (!p.settings().containsKey("ssl.keystore.location")) {
                    out.add("mtls needs settings.ssl.keystore.location, the client's keystore");
                }
            }
            default -> { }
        }
        return out;
    }

    /**
     * The Java client settings a profile resolves to, secrets read now from where they are referenced. Fails with a
     * message naming the reference when one cannot be read.
     */
    public static Map<String, String> clientSettings(Profile profile) {
        Map<String, String> secrets = profile.secrets();
        Map<String, String> p = new LinkedHashMap<>(profile.settings());
        switch (profile.auth()) {
            case "none" -> { }
            case "ssl" -> p.put("security.protocol", "SSL");
            case "sasl-plain" -> sasl(p, secrets, "PLAIN", "org.apache.kafka.common.security.plain.PlainLoginModule");
            case "scram-sha-256" -> sasl(p, secrets, "SCRAM-SHA-256", "org.apache.kafka.common.security.scram.ScramLoginModule");
            case "scram-sha-512" -> sasl(p, secrets, "SCRAM-SHA-512", "org.apache.kafka.common.security.scram.ScramLoginModule");
            case "mtls" -> {
                p.put("security.protocol", "SSL");
                p.putIfAbsent("ssl.keystore.type", "PKCS12");
                if (secrets.containsKey("keystorePassword")) {
                    p.put("ssl.keystore.password", Secrets.resolve(secrets.get("keystorePassword")));
                }
                if (secrets.containsKey("keyPassword")) {
                    p.put("ssl.key.password", Secrets.resolve(secrets.get("keyPassword")));
                }
            }
            case "msk-iam" -> {
                try {
                    Class.forName("software.amazon.msk.auth.iam.IAMLoginModule");
                } catch (ClassNotFoundException e) {
                    throw new IllegalStateException("msk-iam needs Amazon's aws-msk-iam-auth library on the class path, which this build does not carry");
                }
                p.put("security.protocol", "SASL_SSL");
                p.put("sasl.mechanism", "AWS_MSK_IAM");
                p.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
                p.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
            }
            default -> throw new IllegalStateException("unknown auth preset " + profile.auth());
        }
        if (secrets.containsKey("truststorePassword")) {
            p.put("ssl.truststore.password", Secrets.resolve(secrets.get("truststorePassword")));
        }
        return p;
    }

    private static void sasl(Map<String, String> p, Map<String, String> secrets, String mechanism, String module) {
        p.put("security.protocol", "SASL_SSL");
        p.put("sasl.mechanism", mechanism);
        String user = Secrets.resolveOrPlain(secrets.get("username"));
        String pass = Secrets.resolve(secrets.get("password"));
        p.put("sasl.jaas.config", module + " required username=\"" + escape(user) + "\" password=\"" + escape(pass) + "\";");
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
