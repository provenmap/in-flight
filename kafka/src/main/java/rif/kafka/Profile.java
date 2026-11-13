package rif.kafka;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * A connection to one cluster, as the workspace keeps it: where it is, how it authenticates, its schema registry, and
 * whether this tool may write to it. Secrets are never held here, only references to them ({@link Secrets}), so a
 * profile can be saved, shown and passed through a tool call without leaking anything.
 *
 * @param auth one of {@link #PRESETS}
 * @param secrets what the preset needs, by role ({@code username}, {@code password}, {@code keystorePassword},
 *     {@code keyPassword}, {@code truststorePassword}): each a reference, except a username, which may be plain
 * @param settings further Java client settings that are not secret, such as {@code ssl.truststore.location}
 * @param registry the schema registry, or null
 * @param allowWrites whether actions that write to the cluster, such as a redrive, may run against it
 */
public record Profile(String name, String bootstrap, String auth, Map<String, String> secrets, Map<String, String> settings, Registry registry,
                      boolean allowWrites) {

    /** The authentication presets, by what people call them. */
    public static final List<String> PRESETS = List.of("none", "ssl", "sasl-plain", "scram-sha-256", "scram-sha-512", "mtls", "msk-iam");

    /** What each preset is for, for the console and the tools. */
    public static final Map<String, String> PRESET_HELP = presetHelp();

    /** The secret roles a preset may take; a username may be plain text, the rest must be references. */
    public static final Set<String> SECRET_ROLES = Set.of("username", "password", "keystorePassword", "keyPassword", "truststorePassword");

    /** A schema registry: Confluent's API, or Apicurio's (which also serves Confluent's under {@code /apis/ccompat/v7}). */
    public record Registry(String url, String username, String password) {
        public Registry {
            if (url != null && url.endsWith("/")) {
                url = url.substring(0, url.length() - 1);
            }
        }
    }

    public Profile {
        auth = auth == null || auth.isBlank() ? "none" : auth.toLowerCase(Locale.ROOT);
        secrets = secrets == null ? Map.of() : Map.copyOf(secrets);
        settings = settings == null ? Map.of() : Map.copyOf(settings);
    }

    public static Profile plain(String name, String bootstrap) {
        return new Profile(name, bootstrap, "none", Map.of(), Map.of(), null, false);
    }

    public Profile withRegistry(Registry r) {
        return new Profile(name, bootstrap, auth, secrets, settings, r, allowWrites);
    }

    public Profile withAllowWrites(boolean w) {
        return new Profile(name, bootstrap, auth, secrets, settings, registry, w);
    }

    /** What is wrong with this profile as given, before anything connects: empty when nothing is. */
    public List<String> problems() {
        List<String> out = new java.util.ArrayList<>();
        if (name == null || !name.matches("[a-z0-9][a-z0-9._-]{0,62}")) {
            out.add("name: lowercase letters, digits, dots, hyphens and underscores, such as staging");
        }
        if (bootstrap == null || bootstrap.isBlank()) {
            out.add("bootstrap: the cluster's bootstrap servers, such as kafka-1:9092,kafka-2:9092");
        }
        if (!PRESETS.contains(auth)) {
            out.add("auth: one of " + String.join(", ", PRESETS));
        }
        secrets.forEach((role, ref) -> {
            if (!SECRET_ROLES.contains(role)) {
                out.add("secrets." + role + ": not a secret this tool takes; it takes " + String.join(", ", SECRET_ROLES.stream().sorted().toList()));
            } else if (!role.equals("username") && !Secrets.isReference(ref)) {
                out.add("secrets." + role + ": give a reference, never the secret itself: " + Secrets.FORMS);
            }
        });
        settings.forEach((k, v) -> {
            String key = k.toLowerCase(Locale.ROOT);
            if (key.contains("password") || key.contains("jaas") || key.contains("secret") || key.contains("token")) {
                out.add("settings." + k + ": a secret goes in secrets, as a reference, not in settings");
            }
        });
        if (registry != null) {
            if (registry.url() == null || !registry.url().matches("https?://.+")) {
                out.add("registry.url: such as http://registry:8081");
            }
            if (registry.password() != null && !Secrets.isReference(registry.password())) {
                out.add("registry.password: give a reference, never the secret itself: " + Secrets.FORMS);
            }
        }
        switch (auth) {
            case "sasl-plain", "scram-sha-256", "scram-sha-512" -> {
                if (!secrets.containsKey("username") || !secrets.containsKey("password")) {
                    out.add(auth + " needs secrets.username and secrets.password");
                }
            }
            case "mtls" -> {
                if (!settings.containsKey("ssl.keystore.location")) {
                    out.add("mtls needs settings.ssl.keystore.location, the client's keystore");
                }
            }
            default -> { }
        }
        return out;
    }

    /**
     * The Java client settings this profile resolves to, secrets read now from where they are referenced. Fails with a
     * message naming the reference when one cannot be read.
     */
    public Map<String, String> clientSettings() {
        Map<String, String> p = new LinkedHashMap<>(settings);
        switch (auth) {
            case "none" -> { }
            case "ssl" -> p.put("security.protocol", "SSL");
            case "sasl-plain" -> sasl(p, "PLAIN", "org.apache.kafka.common.security.plain.PlainLoginModule");
            case "scram-sha-256" -> sasl(p, "SCRAM-SHA-256", "org.apache.kafka.common.security.scram.ScramLoginModule");
            case "scram-sha-512" -> sasl(p, "SCRAM-SHA-512", "org.apache.kafka.common.security.scram.ScramLoginModule");
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
            default -> throw new IllegalStateException("unknown auth preset " + auth);
        }
        if (secrets.containsKey("truststorePassword")) {
            p.put("ssl.truststore.password", Secrets.resolve(secrets.get("truststorePassword")));
        }
        return p;
    }

    private void sasl(Map<String, String> p, String mechanism, String module) {
        p.put("security.protocol", "SASL_SSL");
        p.put("sasl.mechanism", mechanism);
        String user = Secrets.resolveOrPlain(secrets.get("username"));
        String pass = Secrets.resolve(secrets.get("password"));
        p.put("sasl.jaas.config", module + " required username=\"" + escape(user) + "\" password=\"" + escape(pass) + "\";");
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static Map<String, String> presetHelp() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("none", "Plaintext, no authentication: a local or private cluster");
        m.put("ssl", "TLS, no client authentication; add settings.ssl.truststore.location for a private CA");
        m.put("sasl-plain", "SASL/PLAIN over TLS: Confluent Cloud (an API key as username, its secret as password)");
        m.put("scram-sha-256", "SASL/SCRAM-SHA-256 over TLS: self-hosted, Redpanda, Aiven, MSK with SCRAM");
        m.put("scram-sha-512", "SASL/SCRAM-SHA-512 over TLS: self-hosted, MSK with SCRAM");
        m.put("mtls", "Mutual TLS: a client certificate in settings.ssl.keystore.location");
        m.put("msk-iam", "Amazon MSK with IAM: credentials from the AWS default chain (needs the aws-msk-iam-auth library)");
        return Map.copyOf(m);
    }
}
