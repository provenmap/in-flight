package inflight.broker;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * A connection to one broker, as the workspace keeps it: which kind of broker, where it is, how it authenticates, its
 * schema registry, and whether this tool may write to it. Secrets are never held here, only references to them
 * ({@link Secrets}), so a profile can be saved, shown and passed through a tool call without leaking anything.
 *
 * <p>What the auth preset, the secrets and the settings may be is up to the broker's {@link BrokerAdapter}. In JSON the
 * address is still called {@code bootstrap}, as it was when Kafka was the only broker.
 *
 * @param broker the adapter's {@link BrokerAdapter#kind() kind}; {@value Brokers#DEFAULT_KIND} when not given
 * @param address where the broker is: for Kafka, the bootstrap servers
 * @param auth one of the adapter's {@link BrokerAdapter#presets() presets}
 * @param secrets what the preset needs, by role: each a reference, except a username, which may be plain
 * @param settings further client settings that are not secret, such as {@code ssl.truststore.location}
 * @param registry the schema registry, or null
 * @param allowWrites whether actions that write to the broker, such as a redrive, may run against it
 */
public record Profile(String name, String broker, String address, String auth, Map<String, String> secrets, Map<String, String> settings,
                      Registry registry, boolean allowWrites) {

    /** A schema registry: Confluent's API, or Apicurio's (which also serves Confluent's under {@code /apis/ccompat/v7}). */
    public record Registry(String url, String username, String password) {
        public Registry {
            if (url != null && url.endsWith("/")) {
                url = url.substring(0, url.length() - 1);
            }
        }
    }

    public Profile {
        broker = Brokers.normal(broker);
        auth = auth == null || auth.isBlank() ? "none" : auth.toLowerCase(Locale.ROOT);
        secrets = secrets == null ? Map.of() : Map.copyOf(secrets);
        settings = settings == null ? Map.of() : Map.copyOf(settings);
    }

    /** A profile on a broker of the default kind. */
    public Profile(String name, String address, String auth, Map<String, String> secrets, Map<String, String> settings, Registry registry,
                   boolean allowWrites) {
        this(name, Brokers.DEFAULT_KIND, address, auth, secrets, settings, registry, allowWrites);
    }

    public static Profile plain(String name, Endpoint e) {
        return new Profile(name, e.kind(), e.address(), "none", Map.of(), Map.of(), null, false);
    }

    public Profile withRegistry(Registry r) {
        return new Profile(name, broker, address, auth, secrets, settings, r, allowWrites);
    }

    public Profile withAllowWrites(boolean w) {
        return new Profile(name, broker, address, auth, secrets, settings, registry, w);
    }

    public Endpoint endpoint() {
        return new Endpoint(broker, address);
    }

    public BrokerAdapter adapter() {
        return Brokers.adapter(broker);
    }

    /** What is wrong with this profile as given, before anything connects: empty when nothing is. */
    public List<String> problems() {
        List<String> out = new java.util.ArrayList<>();
        if (name == null || !name.matches("[a-z0-9][a-z0-9._-]{0,62}")) {
            out.add("name: lowercase letters, digits, dots, hyphens and underscores, such as staging");
        }
        if (!Brokers.has(broker)) {
            out.add("broker: one of " + String.join(", ", Brokers.kinds()));
        }
        BrokerAdapter a = Brokers.has(broker) ? adapter() : null;
        if (address == null || address.isBlank()) {
            out.add("bootstrap: " + (a == null ? "where the broker is" : a.addressHelp()));
        }
        if (a != null) {
            List<String> presets = a.presets().stream().map(BrokerAdapter.Preset::id).toList();
            if (!presets.contains(auth)) {
                out.add("auth: one of " + String.join(", ", presets));
            }
        }
        secrets.forEach((role, ref) -> {
            if (a != null && !a.secretRoles().contains(role)) {
                out.add("secrets." + role + ": not a secret this tool takes; it takes " + String.join(", ", a.secretRoles().stream().sorted().toList()));
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
        if (a != null && out.stream().noneMatch(p -> p.startsWith("auth:"))) {
            out.addAll(a.problems(this));
        }
        return out;
    }
}
