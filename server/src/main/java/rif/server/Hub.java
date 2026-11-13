package rif.server;

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
 */
public final class Hub {

    private final Reader reader;
    private final Set<WsContext> viewers = ConcurrentHashMap.newKeySet();

    public Hub(Reader reader) {
        this.reader = reader;
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
        reader.join(snapshot -> {
            if (send(ws, Json.write(Json.array().add(snapshot)))) {
                viewers.add(ws);
            }
        });
    }

    public void leave(WsContext ws) {
        viewers.remove(ws);
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
        viewers.clear();
    }
}
