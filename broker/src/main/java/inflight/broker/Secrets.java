package inflight.broker;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Secrets by reference, never by value: an environment variable's name, a file's path, or an entry in the macOS
 * keychain. A password typed into a chat would stay in its transcript; a reference says where the password is and is
 * safe to save, show and pass through a tool call.
 *
 * <pre>
 *   env:KAFKA_PASSWORD                 the environment variable KAFKA_PASSWORD
 *   file:/run/secrets/kafka            the file's contents, trailing newline dropped
 *   keychain:kafka-staging             the macOS keychain's generic password with that service
 *   keychain:kafka-staging/alice       ... and that account
 * </pre>
 */
public final class Secrets {

    public static final String FORMS = "env:NAME, file:/path or keychain:service[/account]";

    /** Where environment variables come from; tests replace it. */
    static volatile Function<String, String> env = System::getenv;

    private Secrets() {}

    public static boolean isReference(String ref) {
        if (ref == null) {
            return false;
        }
        return ref.matches("env:[A-Za-z_][A-Za-z0-9_]*") || ref.matches("file:.+") || ref.matches("keychain:[^/]+(/.+)?");
    }

    /** A username may be a reference or plain text. */
    public static String resolveOrPlain(String value) {
        if (value == null) {
            throw new IllegalStateException("a username is needed");
        }
        return isReference(value) ? resolve(value) : value;
    }

    /** The secret a reference points to, read now. Fails naming the reference, never the secret. */
    public static String resolve(String ref) {
        if (!isReference(ref)) {
            throw new IllegalStateException("not a secret reference (" + FORMS + ")");
        }
        int colon = ref.indexOf(':');
        String kind = ref.substring(0, colon);
        String where = ref.substring(colon + 1);
        String got = switch (kind) {
            case "env" -> env.apply(where);
            case "file" -> file(where);
            case "keychain" -> keychain(where);
            default -> null;
        };
        if (got == null || got.isEmpty()) {
            throw new IllegalStateException(ref + " is empty or not set");
        }
        return got;
    }

    /** Whether a reference can be read now, and if not why: null when it can. */
    public static String check(String ref) {
        try {
            resolve(ref);
            return null;
        } catch (IllegalStateException e) {
            return e.getMessage();
        }
    }

    private static String file(String path) {
        Path p = Path.of(path.startsWith("~/") ? System.getProperty("user.home") + path.substring(1) : path);
        try {
            String s = Files.readString(p, StandardCharsets.UTF_8);
            return s.endsWith("\n") ? s.substring(0, s.length() - 1) : s;
        } catch (IOException e) {
            throw new IllegalStateException("file:" + path + " cannot be read: " + e.getMessage());
        }
    }

    private static String keychain(String where) {
        String service = where.contains("/") ? where.substring(0, where.indexOf('/')) : where;
        String account = where.contains("/") ? where.substring(where.indexOf('/') + 1) : null;
        java.util.List<String> cmd = new java.util.ArrayList<>(java.util.List.of("security", "find-generic-password", "-s", service, "-w"));
        if (account != null) {
            cmd.addAll(java.util.List.of("-a", account));
        }
        try {
            Process p = new ProcessBuilder(cmd).redirectErrorStream(false).start();
            byte[] out = p.getInputStream().readAllBytes();
            if (!p.waitFor(10, TimeUnit.SECONDS) || p.exitValue() != 0) {
                throw new IllegalStateException("keychain:" + where + " is not in the keychain");
            }
            String s = new String(out, StandardCharsets.UTF_8);
            return s.endsWith("\n") ? s.substring(0, s.length() - 1) : s;
        } catch (IOException e) {
            throw new IllegalStateException("keychain:" + where + ": the keychain is only on macOS (" + e.getMessage() + ")");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("keychain:" + where + ": interrupted");
        }
    }

    /** For tests, an adapter's included: environment variables from a map. */
    public static void useEnv(Map<String, String> vars) {
        env = vars::get;
    }

    /** For tests: environment variables from the environment again. */
    public static void useRealEnv() {
        env = System::getenv;
    }
}
