package inflight.contracts;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import inflight.catalog.Result;

/**
 * Git and GitHub CLI calls for migration branches and pull requests. Every call runs in the repository given, and a
 * failure is a result that names the command and what it printed.
 */
public final class Git {

    private final Path repo;

    public Git(Path repo) {
        this.repo = repo;
    }

    private Result<String> run(List<String> cmd) {
        try {
            Process p = new ProcessBuilder(cmd).directory(repo.toFile()).start();
            p.getOutputStream().close();
            CompletableFuture<String> err = CompletableFuture.supplyAsync(() -> read(p.getErrorStream()));
            String out = read(p.getInputStream());
            int code = p.waitFor();
            return code == 0 ? Result.ok(out.trim()) : Result.err(String.join(" ", cmd) + ": " + err.join().trim());
        } catch (IOException | UncheckedIOException e) {
            return Result.err(String.join(" ", cmd) + ": " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Result.err(String.join(" ", cmd) + ": interrupted");
        }
    }

    private Result<String> run(String... cmd) {
        return run(List.of(cmd));
    }

    private static String read(InputStream in) {
        try (in) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The current branch. */
    public Result<String> head() {
        return run("git", "rev-parse", "--abbrev-ref", "HEAD");
    }

    public boolean branchExists(String branch) {
        return run("git", "rev-parse", "--verify", "--quiet", "refs/heads/" + branch).isOk();
    }

    /** The branch is merged into the current checkout's HEAD. */
    public boolean isMerged(String branch) {
        return run("git", "merge-base", "--is-ancestor", branch, "HEAD").isOk();
    }

    /**
     * Commits files on a new branch from {@code base}, in a temporary worktree, so the caller's checkout is untouched.
     * Returns the short commit id.
     */
    public Result<String> commitOnBranch(String branch, String base, Map<String, String> files, String message) {
        Path wt;
        try {
            wt = Files.createTempDirectory("in-flight-migration");
            Files.delete(wt);
        } catch (IOException e) {
            return Result.err("cannot make a temporary worktree: " + e.getMessage());
        }
        Result<String> added = run("git", "worktree", "add", "-q", "-b", branch, wt.toString(), base);
        if (!added.isOk()) {
            return added;
        }
        try {
            for (Map.Entry<String, String> f : files.entrySet()) {
                Path p = wt.resolve(f.getKey());
                Files.createDirectories(p.getParent());
                Files.writeString(p, f.getValue());
            }
            Git inWt = new Git(wt);
            List<String> add = new ArrayList<>(List.of("git", "add", "--"));
            add.addAll(files.keySet());
            return inWt.run(add)
                    .flatMap(x -> inWt.run("git", "-c", "user.name=in-flight-contracts", "-c", "user.email=in-flight-contracts@users.noreply.github.com",
                            "commit", "-q", "-m", message))
                    .flatMap(x -> inWt.run("git", "rev-parse", "--short", "HEAD"));
        } catch (IOException e) {
            return Result.err("cannot write the regenerated files on " + branch + ": " + e.getMessage());
        } finally {
            run("git", "worktree", "remove", "--force", wt.toString());
        }
    }

    public Result<String> push(String branch) {
        return run("git", "push", "-q", "-u", "origin", branch);
    }

    /** Opens a pull request with the GitHub CLI; its output is the pull request's URL. */
    public Result<String> openPullRequest(String branch, String base, String title, String body) {
        return run("gh", "pr", "create", "--head", branch, "--base", base, "--title", title, "--body", body);
    }

    /** OPEN, CLOSED or MERGED. */
    public Result<String> pullRequestState(String url) {
        return run("gh", "pr", "view", url, "--json", "state", "-q", ".state");
    }
}
