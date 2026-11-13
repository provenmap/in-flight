package rif.server;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

import io.javalin.http.Context;
import io.javalin.http.Cookie;
import io.javalin.http.HttpStatus;
import io.javalin.http.SameSite;
import io.javalin.http.UnauthorizedResponse;

/**
 * The console's login when it listens beyond localhost: a token, given as {@code RIF_TOKEN} or made up at start and
 * printed with the console's address. Opening {@code /?token=...} once sets a cookie; the API also takes it as a bearer
 * token. Bound to localhost, nothing is asked. {@code /mcp} has its own guard, {@link McpHttp}.
 */
final class ConsoleAuth {

    static final String COOKIE = "rif_token";

    private final boolean required;
    private final String token;

    ConsoleAuth(String host, String token) {
        this.required = !McpHttp.isLoopback(host);
        this.token = token != null && !token.isBlank() ? token : required ? random() : null;
    }

    boolean required() {
        return required;
    }

    String token() {
        return token;
    }

    private static String random() {
        byte[] b = new byte[24];
        new SecureRandom().nextBytes(b);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
    }

    private boolean same(String given) {
        return given != null && MessageDigest.isEqual(given.getBytes(StandardCharsets.UTF_8), token.getBytes(StandardCharsets.UTF_8));
    }

    /** Lets the request through, or answers it: 401, or a redirect that drops the token from the address. */
    void guard(Context ctx) {
        if (!required || ctx.path().equals("/mcp")) {
            return;
        }
        String q = ctx.queryParam("token");
        if (same(q)) {
            Cookie c = new Cookie(COOKIE, token);
            c.setHttpOnly(true);
            c.setSameSite(SameSite.STRICT);
            c.setPath("/");
            ctx.cookie(c);
            if (ctx.method().name().equals("GET") && !ctx.path().startsWith("/api") && !ctx.path().equals("/ws")) {
                ctx.redirect(ctx.path(), HttpStatus.FOUND);
                ctx.skipRemainingHandlers();
            }
            return;
        }
        String auth = ctx.header("Authorization");
        if (same(ctx.cookie(COOKIE)) || auth != null && auth.startsWith("Bearer ") && same(auth.substring(7))) {
            return;
        }
        throw new UnauthorizedResponse("This console needs its login token: open the address printed when it started, with ?token=");
    }
}
