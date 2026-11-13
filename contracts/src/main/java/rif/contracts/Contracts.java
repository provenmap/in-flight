package rif.contracts;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;

import rif.catalog.Catalog;
import rif.catalog.Describe;
import rif.catalog.EventRef;
import rif.catalog.Impact;
import rif.catalog.Model;
import rif.catalog.Result;

/**
 * Contracts ahead of breakage: the commands, as methods that return what happened instead of printing it.
 *
 * <pre>
 * generate   regenerate every consumer's binding from a catalog
 * register   register every event version of a catalog in the registry
 * propose    impact, compatibility, a migration branch (or pull request) per affected consumer; holds the change
 * approve    once every migration has merged: register the new schemas, activate the catalog on the router
 * status     where a change stands
 * </pre>
 *
 * <p>A refused proposal or a held approval is a result. A failure to do the work at all (an unreadable catalog, a router
 * or registry that cannot be reached, a git command that fails) is a {@link ContractsException}.
 *
 * <p>The schema gate binds only producers that write through the proxy: a producer that writes to the brokers
 * directly is not gated, so approval keeps a change out of the proxy's produce path, not out of the cluster.
 */
public final class Contracts {

    /**
     * Where contracts works.
     *
     * @param root the repository root: change records, binding paths and manifests are relative to it
     * @param routerUrl the router's control API, which serves and activates the catalog
     * @param apicurioUrl the registry's v3 API, or null to work without a registry: nothing is checked or registered
     * @param consumers the directory of consumer manifests
     */
    public record Settings(Path root, String routerUrl, String apicurioUrl, Path consumers) {

        public static final String DEFAULT_ROUTER = "http://localhost:8085";
        public static final String DEFAULT_APICURIO = "http://localhost:8080/apis/registry/v3";

        /** ROUTER_URL and APICURIO_URL, with their defaults, and manifests in {@code contracts/consumers}. */
        public static Settings fromEnv(Path root) {
            String router = System.getenv("ROUTER_URL");
            String apicurio = System.getenv("APICURIO_URL");
            return new Settings(root, router == null || router.isEmpty() ? DEFAULT_ROUTER : router,
                    apicurio == null || apicurio.isEmpty() ? DEFAULT_APICURIO : apicurio, root.resolve(ConsumerManifest.DEFAULT_DIR));
        }

        public Settings withoutRegistry() {
            return new Settings(root, routerUrl, null, consumers);
        }

        public Settings withConsumers(Path dir) {
            return new Settings(root, routerUrl, apicurioUrl, root.resolve(dir));
        }
    }

    /**
     * How to propose.
     *
     * @param activeCatalog the active catalog's file, or null to ask the router for it
     * @param push push each migration branch and open its pull request with {@code gh}
     * @param base the branch migrations start from and merge into, or null for the current branch
     */
    public record ProposeOptions(Path activeCatalog, boolean push, String base) {
        public static final ProposeOptions DEFAULT = new ProposeOptions(null, false, null);
    }

    /** One event version and what the registry said about it. */
    public record SchemaCheck(EventRef event, String verdict, boolean incompatible) {}

    /**
     * What a proposal did.
     *
     * @param report the impact report with the registry's verdicts
     * @param change the change it holds, or null when it was refused
     * @param text everything a person reads, top to bottom
     */
    public record Proposal(Analysis analysis, List<SchemaCheck> checks, String report, boolean refused, Change change, String text) {}

    /**
     * What an approval did.
     *
     * @param unmerged the migrations that hold it, when it is held
     * @param registered the event versions it registered
     */
    public record Approval(String id, boolean approved, List<Change.Migration> unmerged, List<EventRef> registered, String text) {}

    /** A binding written to disk. */
    public record Generated(String consumer, String path, int revision) {}

    private final Settings settings;
    private final Apicurio apicurio;
    private final ChangeStore store;
    private final Git git;

    public Contracts(Settings settings) {
        this.settings = settings;
        this.apicurio = settings.apicurioUrl() == null ? null : new Apicurio(settings.apicurioUrl());
        this.store = new ChangeStore(settings.root());
        this.git = new Git(settings.root());
    }

    public Settings settings() {
        return settings;
    }

    // ---- sources -----------------------------------------------------------------------------------------------------

    public static Catalog readCatalog(Path file) {
        Result<Catalog> c = Catalog.load(file);
        if (!c.isOk()) {
            throw new ContractsException(file + ": " + String.join("\n  ", c.errors()));
        }
        return c.value();
    }

    /** The catalog in this file, or, with none, the one the router has active. */
    public Catalog activeCatalog(Path file) {
        if (file != null) {
            return readCatalog(file);
        }
        Http.Response r;
        try {
            r = Http.get(settings.routerUrl() + "/catalog");
        } catch (ContractsException e) {
            throw new ContractsException("router not reachable at " + settings.routerUrl() + "; give the active catalog's file instead", e);
        }
        if (!r.ok()) {
            throw new ContractsException("router has no active catalog (HTTP " + r.status() + ")");
        }
        Result<Catalog> c = r.json().flatMap(Catalog::fromJson);
        if (!c.isOk()) {
            throw new ContractsException("the router's active catalog: " + c.error());
        }
        return c.value();
    }

    public List<ConsumerManifest> manifests() {
        return ConsumerManifest.loadAll(settings.consumers());
    }

    private static String binding(ConsumerManifest m, Catalog catalog) {
        Result<String> source = BindingGenerator.generate(catalog, m.intent(), m.target());
        if (!source.isOk()) {
            throw new ContractsException("consumer " + m.name() + ": " + source.error());
        }
        return source.value();
    }

    // ---- generate and register ---------------------------------------------------------------------------------------

    /** Regenerates every consumer's binding from the catalog, in place. */
    public List<Generated> generate(Catalog catalog) {
        List<Generated> out = new ArrayList<>();
        for (ConsumerManifest m : manifests()) {
            String source = binding(m, catalog);
            Path path = settings.root().resolve(m.bindingPath());
            try {
                Files.createDirectories(path.getParent());
                Files.writeString(path, source);
            } catch (IOException e) {
                throw new ContractsException("cannot write " + path + ": " + e.getMessage(), e);
            }
            out.add(new Generated(m.name(), m.bindingPath(), catalog.revision()));
        }
        return out;
    }

    /** Registers every event version the catalog gives a meaning to. Events without fields have no schema to register. */
    public List<EventRef> register(Catalog catalog) {
        Apicurio registry = requireRegistry();
        List<EventRef> out = new ArrayList<>();
        if (catalog.semantics() != null) {
            for (Model.Event e : catalog.semantics().model().events()) {
                out.add(orFail(registry.register(e)));
            }
        }
        return out;
    }

    private Apicurio requireRegistry() {
        if (apicurio == null) {
            throw new ContractsException("no registry: set APICURIO_URL");
        }
        return apicurio;
    }

    private static <T> T orFail(Result<T> r) {
        if (!r.isOk()) {
            throw new ContractsException(r.error());
        }
        return r.value();
    }

    // ---- propose -----------------------------------------------------------------------------------------------------

    public Proposal propose(Path proposedCatalog, ProposeOptions options) {
        return propose(readCatalog(proposedCatalog), options);
    }

    /**
     * Works out the impact, asks the registry about each new event version, and, unless something refuses it, commits
     * each affected consumer's regenerated binding on its own branch and holds the change until those merge.
     */
    public Proposal propose(Catalog proposed, ProposeOptions options) {
        Catalog active = activeCatalog(options.activeCatalog());
        Analysis analysis = Analysis.of(active, proposed, manifests());

        List<SchemaCheck> checks = new ArrayList<>();
        for (Model.Event e : analysis.added()) {
            if (apicurio == null) {
                checks.add(new SchemaCheck(e.ref(), "not checked (no registry)", false));
                continue;
            }
            checks.add(switch (apicurio.check(e)) {
                case Apicurio.Verdict.Compatible c -> new SchemaCheck(e.ref(), "compatible", false);
                case Apicurio.Verdict.AlreadyRegistered a -> new SchemaCheck(e.ref(), "already registered", false);
                case Apicurio.Verdict.Incompatible i -> new SchemaCheck(e.ref(), "INCOMPATIBLE: " + i.reason(), true);
            });
        }
        List<String> errors = new ArrayList<>(analysis.impact().errors());
        checks.stream().filter(SchemaCheck::incompatible).forEach(c -> errors.add(c.event().show() + " is incompatible: " + c.verdict().substring("INCOMPATIBLE: ".length())));
        Impact found = analysis.impact();
        Impact impact = new Impact(found.changes(), found.intents(), found.unaffected(), List.copyOf(errors), found.breaks());
        String report = withChecks(impact.report(), checks);

        List<String> text = new ArrayList<>();
        text.add("Proposal: catalog revision " + active.revision() + " -> " + proposed.revision());
        text.add(report);
        text.add(impact.headline());
        if (!errors.isEmpty()) {
            text.add("The proposal is refused. Nothing was changed.");
            return new Proposal(analysis, checks, report, true, null, String.join("\n", text));
        }

        String id = "rev" + proposed.revision();
        if (store.exists(id)) {
            throw new ContractsException("change " + id + " already exists; see `contracts status " + id + "`");
        }
        String baseBranch = options.base() != null ? options.base() : orFail(git.head());

        // Everything is generated and every branch name checked before the first commit, so a failure leaves nothing behind.
        List<Analysis.Affected> bound = analysis.affected().stream().filter(Analysis.Affected::bound).toList();
        Map<Analysis.Affected, String> branches = new LinkedHashMap<>();
        Map<Analysis.Affected, String> sources = new LinkedHashMap<>();
        for (Analysis.Affected a : bound) {
            long same = bound.stream().filter(x -> x.consumer().equals(a.consumer())).count();
            String branch = "rif/" + id + "/" + a.consumer() + (same > 1 ? "." + a.intent().intent() : "");
            if (git.branchExists(branch)) {
                throw new ContractsException("branch " + branch + " already exists; delete it to propose again");
            }
            branches.put(a, branch);
            sources.put(a, binding(a.manifest(), proposed));
        }

        List<Change.Migration> migrations = new ArrayList<>();
        for (Analysis.Affected a : bound) {
            ConsumerManifest m = a.manifest();
            String branch = branches.get(a);
            String title = m.name() + ": adapt to intent " + m.intent() + " at catalog revision " + proposed.revision();
            String body = prBody(m, a.intent(), proposed, report);
            String commit = orFail(git.commitOnBranch(branch, baseBranch, Map.of(m.bindingPath(), sources.get(a)), title + "\n\n" + body));
            String pr = null;
            if (options.push()) {
                orFail(git.push(branch));
                pr = orFail(git.openPullRequest(branch, baseBranch, title, body));
            }
            text.add(m.name() + ": regenerated " + m.bindingPath() + " on branch " + branch + " (" + commit + ")"
                    + (pr == null ? "" : ", pull request " + pr));
            migrations.add(new Change.Migration(m.name(), m.intent(), m.bindingPath(), branch, commit, pr));
        }
        List<String> unbound = analysis.affected().stream().filter(a -> !a.bound()).map(Analysis.Affected::consumer).distinct().toList();
        for (String u : unbound) {
            text.add(u + ": affected, with no binding manifest in " + settings.root().relativize(settings.consumers()) + "; nothing to regenerate");
        }

        List<EventRef> added = analysis.added().stream().map(Model.Event::ref).toList();
        JsonNode impactJson = Describe.impactJson(impact);
        Change change = new Change(id, proposed.revision(), Change.Status.PENDING, Instant.now(), impact.headline(), report, impactJson,
                added, migrations, unbound);
        store.save(change, proposed);

        text.add("");
        if (migrations.isEmpty()) {
            text.add("No binding needs regenerating. Activate with: contracts approve " + id);
        } else {
            text.add("Change " + id + " is pending." + (added.isEmpty() ? ""
                    : " " + added.stream().map(EventRef::show).collect(Collectors.joining(", ")) + " cannot be produced through the proxy until it is approved."));
            text.add("Merge " + (options.push() ? "the pull requests" : String.join(", ", migrations.stream().map(Change.Migration::branch).toList()))
                    + ", then run: contracts approve " + id);
        }
        return new Proposal(analysis, checks, report, false, change, String.join("\n", text));
    }

    /** The registry's verdicts go after the catalog changes, before the intents they reach. */
    private static String withChecks(String report, List<SchemaCheck> checks) {
        if (checks.isEmpty()) {
            return report;
        }
        List<String> section = new ArrayList<>();
        section.add("Schema compatibility (Apicurio):");
        checks.forEach(c -> section.add("  - " + c.event().show() + ": " + c.verdict()));
        int at = report.indexOf("\nIntents affected:");
        return at < 0 ? report + "\n" + String.join("\n", section)
                : report.substring(0, at) + "\n" + String.join("\n", section) + report.substring(at);
    }

    private static String prBody(ConsumerManifest m, Impact.IntentImpact i, Catalog proposed, String report) {
        List<String> changes = new ArrayList<>();
        for (Impact.RouteArrival a : i.arrivals()) {
            String head = "- `" + a.route().event().show() + "` from `" + a.route().topic() + "`: " + String.join("; ", a.route().reasons());
            changes.add(switch (a.arrival()) {
                case NEW_TYPE -> head + " (a type this consumer has never read)";
                case SAME_READS -> head + " (read the same way as `" + a.previous().show() + "`)";
                case NEW_READS -> head + " (read differently from `" + a.previous().show() + "`: " + String.join("; ", a.differences()) + ")";
            });
        }
        i.removed().forEach(r -> changes.add("- `" + r.event().show() + "` leaves the intent"));
        i.rekeyed().forEach(r -> changes.add("- `" + r.event().show() + "` is now keyed by `" + r.keyPath().show() + "` from `" + r.topic() + "`"));
        return """
                Catalog revision %d changes what intent `%s` (`%s`) delivers to `%s`.

                %s

                This pull request regenerates `%s`, the consumer's typed binding, so it decodes the new events and maps
                them to typed effects on `%s`. Code that folds `effects()` needs no change. Code that switches over the
                event records is told by the compiler which record is new.

                The new events cannot be produced through the proxy until this merges and `contracts approve rev%d` runs:
                their schemas stay unregistered, and the schema gate refuses an unregistered version.

                <details><summary>Impact report</summary>

                ```
                %s
                ```
                </details>""".formatted(proposed.revision(), i.intent(), i.predicate(), m.name(), String.join("\n", changes), m.bindingPath(),
                i.entity(), proposed.revision(), report.strip());
    }

    // ---- approve -----------------------------------------------------------------------------------------------------

    public Approval approve(String id) {
        return approve(id, null);
    }

    /**
     * Approves a change once every migration has merged: registers the event versions it adds and activates its catalog
     * on the router. Held, with the migrations still open, until then.
     *
     * @param activeCatalog the active catalog's file, or null to ask the router for it
     */
    public Approval approve(String id, Path activeCatalog) {
        ChangeStore.Stored stored = orFail(store.load(id));
        Change change = stored.change();
        Catalog catalog = stored.catalog();
        if (change.status() == Change.Status.APPROVED) {
            return new Approval(id, true, List.of(), List.of(), id + " is already approved");
        }
        List<Change.Migration> open = change.migrations().stream().filter(m -> !merged(m)).toList();
        if (!open.isEmpty()) {
            String held = open.stream().map(m -> "  - " + m.consumer() + ": " + (m.pullRequest() != null ? m.pullRequest() : m.branch()))
                    .collect(Collectors.joining("\n"));
            return new Approval(id, false, open, List.of(), id + " is held: not merged yet:\n" + held);
        }

        Catalog active = activeCatalog(activeCatalog);
        if (catalog.revision() <= active.revision()) {
            throw new ContractsException("catalog revision " + active.revision() + " is already active; " + id + " (revision "
                    + catalog.revision() + ") is stale: propose again against it");
        }
        Model before = active.semantics() == null ? new Model(Map.of(), List.of()) : active.semantics().model();
        List<Model.Event> added = catalog.semantics() == null ? List.of() : Impact.diff(before, catalog.semantics().model()).added();
        List<EventRef> registered = new ArrayList<>();
        if (apicurio != null) {
            for (Model.Event e : added) {
                registered.add(orFail(apicurio.register(e)));
            }
        }
        Http.Response r = Http.put(settings.routerUrl() + "/catalog", catalog.toJson());
        if (!r.ok()) {
            throw new ContractsException("router refused revision " + catalog.revision() + ": HTTP " + r.status() + ": " + r.body());
        }
        store.save(change.withStatus(Change.Status.APPROVED), catalog);
        String names = added.stream().map(e -> e.ref().show()).collect(Collectors.joining(", "));
        String text = id + " approved: catalog revision " + catalog.revision() + " is active"
                + (added.isEmpty() ? "." : apicurio == null ? "; " + names + " not registered (no registry)." : "; " + names + " may now be produced.");
        return new Approval(id, true, List.of(), registered, text);
    }

    private boolean merged(Change.Migration m) {
        if (m.pullRequest() != null) {
            Result<String> state = git.pullRequestState(m.pullRequest());
            return state.isOk() && state.value().equals("MERGED");
        }
        return git.isMerged(m.branch());
    }

    // ---- status ------------------------------------------------------------------------------------------------------

    /** Every change, by id. */
    public List<Change> changes() {
        return store.ids().stream().map(i -> orFail(store.load(i)).change()).toList();
    }

    public ChangeStatus changeStatus(String id) {
        Change c = orFail(store.load(id)).change();
        List<ChangeStatus.MigrationState> migrations = c.migrations().stream()
                .map(m -> new ChangeStatus.MigrationState(m.consumer(), m.intent(), m.bindingPath(), m.branch(), m.commit(), m.pullRequest(), merged(m)))
                .toList();
        List<ChangeStatus.GateCheck> gate = c.added().stream().map(ref -> new ChangeStatus.GateCheck(ref, gate(ref))).toList();
        return new ChangeStatus(c.id(), c.revision(), c.status(), c.createdAt(), c.headline(), c.report(), c.impact(), migrations, c.unbound(), gate);
    }

    private ChangeStatus.Gate gate(EventRef ref) {
        if (apicurio == null) {
            return ChangeStatus.Gate.UNKNOWN;
        }
        try {
            return apicurio.isRegistered(ref) ? ChangeStatus.Gate.ACCEPTED : ChangeStatus.Gate.REFUSED;
        } catch (ContractsException e) {
            return ChangeStatus.Gate.UNKNOWN;
        }
    }
}
