package rif.contracts;

import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

import rif.catalog.EventRef;

/**
 * A proposed catalog revision, held until every affected consumer's migration has merged.
 *
 * @param headline who must change first, or that nobody must
 * @param report the impact report, as a person reads it
 * @param impact the impact as JSON, in the shape {@code rif.catalog.Describe.impactJson} gives it
 * @param added the event versions the change brings, which the schema gate refuses until it is approved
 * @param migrations one per affected consumer with a binding manifest: the branch holding its regenerated binding
 * @param unbound affected consumers with no binding manifest: named in the report, with nothing to regenerate
 */
public record Change(String id, int revision, Status status, Instant createdAt, String headline, String report, JsonNode impact,
                     List<EventRef> added, List<Migration> migrations, List<String> unbound) {

    public Change {
        added = List.copyOf(added);
        migrations = List.copyOf(migrations);
        unbound = List.copyOf(unbound);
    }

    /** Pending until approved; approval is what activates the revision. */
    public enum Status {
        PENDING, APPROVED;

        public String wire() {
            return name().toLowerCase();
        }

        static Status of(String text) {
            return "approved".equals(text) ? APPROVED : PENDING;
        }
    }

    /**
     * A consumer's migration: the branch with its regenerated binding, and its pull request if one was opened.
     *
     * @param pullRequest the pull request's URL, or null when the branch was not pushed
     */
    public record Migration(String consumer, String intent, String bindingPath, String branch, String commit, String pullRequest) {}

    public Change withStatus(Status s) {
        return new Change(id, revision, s, createdAt, headline, report, impact, added, migrations, unbound);
    }
}
