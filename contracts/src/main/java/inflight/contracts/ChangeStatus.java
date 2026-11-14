package inflight.contracts;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

import inflight.catalog.EventRef;

/**
 * Where a change stands: its impact, each migration and whether it has merged, and whether the schema gate would still
 * refuse each event version it adds.
 *
 * @param unbound affected consumers with no binding manifest
 * @param gate one per added event version
 */
public record ChangeStatus(String id, int revision, Change.Status status, Instant createdAt, String headline, String report, JsonNode impact,
                           List<MigrationState> migrations, List<String> unbound, List<GateCheck> gate) {

    public ChangeStatus {
        migrations = List.copyOf(migrations);
        unbound = List.copyOf(unbound);
        gate = List.copyOf(gate);
    }

    /**
     * One migration and whether it has merged: its pull request is merged, or, with no pull request, its branch is
     * merged into the repository's HEAD.
     */
    public record MigrationState(String consumer, String intent, String bindingPath, String branch, String commit, String pullRequest,
                                 boolean merged) {}

    /**
     * What the schema gate does with an event version today. {@code REFUSED}: it is not registered, so a producer
     * writing through the proxy is turned away. {@code ACCEPTED}: it is registered. {@code UNKNOWN}: there is no registry
     * to ask, or it could not be reached.
     */
    public enum Gate {
        REFUSED, ACCEPTED, UNKNOWN;

        public String wire() {
            return name().toLowerCase();
        }
    }

    public record GateCheck(EventRef event, Gate gate) {}

    public boolean allMerged() {
        return migrations.stream().allMatch(MigrationState::merged);
    }

    /** The status as a person reads it. */
    public String text() {
        List<String> lines = new ArrayList<>();
        lines.add(id + ": " + status.wire() + ", revision " + revision + ", proposed " + createdAt);
        if (headline != null) {
            lines.add(headline);
        }
        lines.add("Migrations:");
        if (migrations.isEmpty() && unbound.isEmpty()) {
            lines.add("  (none)");
        }
        for (MigrationState m : migrations) {
            lines.add("  " + m.consumer() + " (" + m.intent() + "): " + (m.pullRequest() != null ? m.pullRequest() : m.branch()) + " at " + m.commit()
                    + ", " + (m.merged() ? "merged" : "not merged"));
        }
        unbound.forEach(u -> lines.add("  " + u + ": affected, with no binding manifest to regenerate"));
        if (!gate.isEmpty()) {
            lines.add("Schema gate:");
            for (GateCheck g : gate) {
                lines.add("  " + g.event().show() + ": " + switch (g.gate()) {
                    case REFUSED -> "refused (not registered)";
                    case ACCEPTED -> "accepted (registered)";
                    case UNKNOWN -> "unknown (no registry to ask)";
                });
            }
        }
        lines.add(report);
        return String.join("\n", lines);
    }
}
