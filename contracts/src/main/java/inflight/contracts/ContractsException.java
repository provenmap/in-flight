package inflight.contracts;

import java.io.Serial;

/**
 * A contracts command could not do what it was asked, and the message says why in a sentence a person can act on: an
 * unreadable catalog, a router or registry that cannot be reached, a git command that failed. Outcomes that are part
 * of the process, such as a refused proposal or an approval held for unmerged migrations, are results, not this.
 */
public final class ContractsException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    public ContractsException(String message) {
        super(message);
    }

    public ContractsException(String message, Throwable cause) {
        super(message, cause);
    }
}
