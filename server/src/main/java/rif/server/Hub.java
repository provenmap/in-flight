package rif.server;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.jetty.websocket.api.Callback;

import com.fasterxml.jackson.databind.node.ArrayNode;

import io.javalin.websocket.WsContext;
import rif.catalog.Json;
import rif.observer.Reader;

/**
 * Sends frames to viewers, in order. A new viewer gets its snapshot from the reader between two batches and joins the
 * broadcast at once, so it sees every message after the snapshot exactly once.
 *
 * <p>When the engine swaps its observer, every viewer is closed and reconnects, so each gets the new flow's snapshot.
 * With no observer, a viewer waits, connected, for one.
 */
public final class Hub {

    private volatile Reader reader;
    private final Set<WsContext> viewers = ConcurrentHashMap.newKeySet();
    private final Set<WsContext> waiting = ConcurrentHashMap.newKeySet();

    public Hub(Reader reader) {
        this.reader = reader;
    }

    /** A new reader, or none: every viewer is sent away to come back for the new snapshot. */
    public void rebind(Reader r) {
        this.reader = r;
        for (WsContext ws : List.copyOf(viewers)) {
            viewers.remove(ws);
            ws.closeSession(1012, "the flow changed");
        }
        for (WsContext ws : List.copyOf(waiting)) {
            waiting.remove(ws);
            ws.closeSession(1012, "a flow is ready");
        }
    }

    /** Every frame is a JSON array of messages. Called on the reader's thread. */
    public void publish(ArrayNode frame) {
        if (viewers.isEmpty()) {
            return;
        }
        String text = Json.write(frame);
        for (WsContext ws : viewers) {
            send(ws, text);
        }
    }

    public void join(WsContext ws) {
        Reader r = reader;
        if (r == null) {
            waiting.add(ws);
            return;
        }
        r.join(snapshot -> {
            if (send(ws, Json.write(Json.array().add(snapshot)))) {
                viewers.add(ws);
            }
        });
    }

    public void leave(WsContext ws) {
        viewers.remove(ws);
        waiting.remove(ws);
    }

    public int count() {
        return viewers.size();
    }

    private boolean send(WsContext ws, String text) {
        if (!ws.session.isOpen()) {
            viewers.remove(ws);
            return false;
        }
        ws.session.sendText(text, Callback.from(() -> {}, failure -> viewers.remove(ws)));
        return true;
    }

    public void close() {
        for (WsContext ws : viewers) {
            ws.closeSession();
        }
        for (WsContext ws : waiting) {
            ws.closeSession();
        }
        viewers.clear();
        waiting.clear();
    }
}
