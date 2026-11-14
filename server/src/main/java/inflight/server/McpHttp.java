package inflight.server;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javalin.http.Context;
import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.server.McpStatelessServerHandler;
import io.modelcontextprotocol.spec.McpError;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpStatelessServerTransport;
import reactor.core.publisher.Mono;
import inflight.catalog.Json;

/**
 * MCP over Streamable HTTP on {@code /mcp}, stateless: each request is answered on its own, as JSON.
 *
 * <p>Bound to localhost it needs no token, but only answers requests addressed to localhost, which stops a web page from
 * reaching it through DNS rebinding. Bound beyond localhost it needs {@code INFLIGHT_MCP_TOKEN} as a bearer token, and is off
 * until one is set. A token, once set, is always required.
 */
public final class McpHttp implements McpStatelessServerTransport {

    private static final Logger log = LoggerFactory.getLogger(McpHttp.class);
    private static final Set<String> LOOPBACK = Set.of("localhost", "127.0.0.1", "::1", "[::1]");

    private final String token;
    private final boolean local;
    private volatile McpStatelessServerHandler handler;

    public McpHttp(String bindHost, String token) {
        this.token = token == null || token.isEmpty() ? null : token;
        this.local = LOOPBACK.contains(bindHost);
    }

    @Override
    public void setMcpHandler(McpStatelessServerHandler mcpHandler) {
        this.handler = mcpHandler;
    }

    @Override
    public Mono<Void> closeGracefully() {
        return Mono.empty();
    }

    /** Whether a bind address is this machine only. */
    public static boolean isLoopback(String host) {
        return LOOPBACK.contains(host);
    }

    static String hostname(String hostHeader) {
        if (hostHeader == null) {
            return "";
        }
        return hostHeader.startsWith("[") ? hostHeader.substring(0, hostHeader.indexOf(']') + 1) : hostHeader.split(":")[0];
    }

    private static boolean sameToken(String given, String want) {
        return MessageDigest.isEqual(given.getBytes(StandardCharsets.UTF_8), want.getBytes(StandardCharsets.UTF_8));
    }

    private static void refuse(Context ctx, int status, String error) {
        ctx.status(status).contentType("application/json").result(Json.write(Json.object().put("error", error)));
    }

    /** The guard: null when the request may go on, else it has been answered. */
    boolean allowed(Context ctx) {
        if (token == null && !local) {
            refuse(ctx, 403, "MCP over HTTP is off when the server listens beyond localhost: set INFLIGHT_MCP_TOKEN");
            return false;
        }
        if (local && !LOOPBACK.contains(hostname(ctx.header("Host")))) {
            refuse(ctx, 403, "MCP over HTTP answers only requests to localhost");
            return false;
        }
        String origin = ctx.header("Origin");
        if (local && origin != null && !origin.isEmpty()) {
            String host;
            try {
                host = URI.create(origin).getHost();
            } catch (IllegalArgumentException e) {
                host = null;
            }
            if (host == null || !LOOPBACK.contains(host) && !LOOPBACK.contains("[" + host + "]")) {
                refuse(ctx, 403, "MCP over HTTP refuses requests from other origins");
                return false;
            }
        }
        if (token != null) {
            String auth = ctx.header("Authorization");
            if (auth == null || !auth.startsWith("Bearer ") || !sameToken(auth.substring(7), token)) {
                ctx.header("WWW-Authenticate", "Bearer");
                refuse(ctx, 401, "a bearer token is required");
                return false;
            }
        }
        return true;
    }

    /** POST /mcp: one JSON-RPC request, answered as JSON; a notification, accepted. */
    void post(Context ctx) {
        if (!allowed(ctx)) {
            return;
        }
        McpStatelessServerHandler h = handler;
        McpTransportContext context = McpTransportContext.create(Map.of());
        try {
            McpSchema.JSONRPCMessage message = McpSchema.deserializeJsonRpcMessage(Mcp.JSON, ctx.body());
            if (message instanceof McpSchema.JSONRPCRequest request) {
                McpSchema.JSONRPCResponse response = h.handleRequest(context, request)
                        .contextWrite(c -> c.put(McpTransportContext.KEY, context)).block();
                ctx.status(200).contentType("application/json").result(Mcp.JSON.writeValueAsString(response));
            } else if (message instanceof McpSchema.JSONRPCNotification notification) {
                h.handleNotification(context, notification).contextWrite(c -> c.put(McpTransportContext.KEY, context)).block();
                ctx.status(202);
            } else {
                error(ctx, 400, McpError.builder(McpSchema.ErrorCodes.INVALID_REQUEST).message("The server accepts either requests or notifications").build());
            }
        } catch (Exception e) {
            log.warn("mcp: {}", e.getMessage());
            error(ctx, 400, McpError.builder(McpSchema.ErrorCodes.INVALID_REQUEST).message("Invalid message: " + e.getMessage()).build());
        }
    }

    /** GET and DELETE /mcp: this server holds no sessions and pushes nothing. */
    void other(Context ctx) {
        if (!allowed(ctx)) {
            return;
        }
        ctx.header("Allow", "POST");
        refuse(ctx, 405, "this MCP server is stateless: POST only");
    }

    private static void error(Context ctx, int status, McpError e) {
        try {
            ctx.status(status).contentType("application/json").result(Mcp.JSON.writeValueAsString(e));
        } catch (Exception ex) {
            refuse(ctx, status, e.getMessage());
        }
    }
}
