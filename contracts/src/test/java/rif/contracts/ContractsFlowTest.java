package rif.contracts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import rif.catalog.Catalog;
import rif.catalog.EventRef;
import rif.catalog.Json;

/**
 * Propose, then approve, against a temporary git repository, with a local stand-in for the router and the registry.
 * Nothing is pushed and no pull request is opened: migrations are branches, merged by hand as a reviewer would.
 */
class ContractsFlowTest {

    @TempDir
    Path tmp;

    Path repo;
    Path proposedFile;
    Catalog active;
    Catalog proposed;
    FakeServices services;

    static final EventRef V2 = new EventRef("OrderPlaced", 2);
    static final String BRANCH = "rif/rev2/payments-service";

    @BeforeEach
    void setUp() throws Exception {
        repo = Files.createDirectories(tmp.resolve("repo"));
        ContractsTest.initRepo(repo);
        active = Fixtures.parse(Fixtures.shippedActiveJson());
        proposed = Fixtures.parse(Fixtures.shippedProposedJson());
        proposedFile = tmp.resolve("proposed.json");
        Files.writeString(proposedFile, Json.pretty(proposed.toJson()));
        services = new FakeServices(active.toJson()).registered(active);
    }

    @AfterEach
    void tearDown() {
        services.close();
    }

    /** The repository with this module's manifest for payments-service committed in it. */
    void commitManifests() throws Exception {
        Path dir = Files.createDirectories(repo.resolve(ConsumerManifest.DEFAULT_DIR));
        Files.copy(Fixtures.MANIFESTS.resolve("payments-service.json"), dir.resolve("payments-service.json"));
        ContractsTest.sh(repo, "git", "add", ".");
        ContractsTest.sh(repo, "git", "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false", "commit", "-q", "-m", "manifests");
    }

    Contracts contracts() {
        return new Contracts(new Contracts.Settings(repo, services.routerUrl(), services.apicurioUrl(), repo.resolve(ConsumerManifest.DEFAULT_DIR)));
    }

    @Test
    void proposeHoldsTheChangeUntilPaymentsMigrationMergesThenApproveRegistersAndActivates() throws Exception {
        commitManifests();
        Contracts c = contracts();

        Contracts.Proposal p = c.propose(proposedFile, Contracts.ProposeOptions.DEFAULT);
        assertFalse(p.refused(), p.text());
        assertEquals(List.of(new Contracts.SchemaCheck(V2, "compatible", false)), p.checks());
        assertTrue(p.text().startsWith("Proposal: catalog revision 1 -> 2\n"), p.text());
        assertTrue(p.report().contains("Schema compatibility (Apicurio):\n  - OrderPlaced@v2: compatible\nIntents affected:"), p.report());
        assertTrue(p.text().contains("payments-service must change before this ships, or its events pile up in payments.dlq."), p.text());
        assertTrue(p.text().contains("payments-service: regenerated simulator/src/main/java/rif/simulator/generated/OrderTotalChanges.java on branch " + BRANCH), p.text());
        assertTrue(p.text().contains("inventory-service: affected, with no binding manifest in contracts/consumers; nothing to regenerate"), p.text());
        assertTrue(p.text().contains("Change rev2 is pending. OrderPlaced@v2 cannot be produced through the proxy until it is approved."), p.text());

        Change change = p.change();
        assertEquals("rev2", change.id());
        assertEquals(Change.Status.PENDING, change.status());
        assertEquals(List.of(V2), change.added());
        assertEquals(List.of("inventory-service"), change.unbound());
        assertEquals(1, change.migrations().size());
        Change.Migration m = change.migrations().get(0);
        assertEquals(BRANCH, m.branch());
        assertNull(m.pullRequest());
        assertFalse(Files.exists(repo.resolve(m.bindingPath())), "the checkout is untouched");
        assertTrue(services.activated.isEmpty());
        assertFalse(services.versions.contains("OrderPlaced/2"), "proposing registers nothing");
        ContractsException again = assertThrows(ContractsException.class, () -> c.propose(proposedFile, Contracts.ProposeOptions.DEFAULT));
        assertEquals("change rev2 already exists; see `contracts status rev2`", again.getMessage());

        ChangeStatus pending = c.changeStatus("rev2");
        assertEquals(Change.Status.PENDING, pending.status());
        assertEquals(List.of(new ChangeStatus.GateCheck(V2, ChangeStatus.Gate.REFUSED)), pending.gate());
        assertFalse(pending.migrations().get(0).merged());
        assertEquals("payments-service must change before this ships, or its events pile up in payments.dlq.", pending.headline());
        assertEquals("payments-service", pending.impact().at("/mustChangeFirst/0/consumer").asText());
        assertTrue(pending.text().contains("OrderPlaced@v2: refused (not registered)"), pending.text());

        Contracts.Approval held = c.approve("rev2");
        assertFalse(held.approved());
        assertEquals(List.of(m), held.unmerged());
        assertEquals("rev2 is held: not merged yet:\n  - payments-service: " + BRANCH, held.text());
        assertTrue(services.activated.isEmpty(), "a held change activates nothing");

        ContractsTest.sh(repo, "git", "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false", "merge", "-q", "--no-edit", BRANCH);
        assertEquals(Fixtures.binding(proposed, Fixtures.SIMULATOR), Files.readString(repo.resolve(m.bindingPath())),
                "the merged binding is the generator's output for the proposed catalog");

        Contracts.Approval approved = c.approve("rev2");
        assertTrue(approved.approved(), approved.text());
        assertEquals(List.of(V2), approved.registered());
        assertEquals("rev2 approved: catalog revision 2 is active; OrderPlaced@v2 may now be produced.", approved.text());
        assertTrue(services.versions.contains("OrderPlaced/2"));
        assertEquals(1, services.activated.size());
        assertEquals(2, services.activated.get(0).get("revision").asInt());

        ChangeStatus done = c.changeStatus("rev2");
        assertEquals(Change.Status.APPROVED, done.status());
        assertTrue(done.allMerged());
        assertEquals(List.of(new ChangeStatus.GateCheck(V2, ChangeStatus.Gate.ACCEPTED)), done.gate());
        assertEquals("rev2 is already approved", c.approve("rev2").text());
        assertEquals(List.of("rev2"), c.changes().stream().map(Change::id).toList());
        assertTrue(c.propose(proposedFile, Contracts.ProposeOptions.DEFAULT).refused(), "revision 2 is now the active one");
    }

    @Test
    void anIncompatibleSchemaRefusesTheProposalAndChangesNothing() throws Exception {
        commitManifests();
        services.incompatible = "required property 'total' changed type";
        Contracts.Proposal p = contracts().propose(proposedFile, Contracts.ProposeOptions.DEFAULT);
        assertTrue(p.refused());
        assertNull(p.change());
        assertTrue(p.report().contains("  ! OrderPlaced@v2 is incompatible: HTTP 409: required property 'total' changed type"), p.report());
        assertTrue(p.text().endsWith("The proposal is refused. Nothing was changed."), p.text());
        assertFalse(Files.exists(repo.resolve(ChangeStore.DIR)));
        assertFalse(new Git(repo).branchExists(BRANCH));
    }

    @Test
    void aProposalThatIsNotNewerThanTheActiveCatalogIsRefused() throws Exception {
        Contracts.Proposal p = contracts().propose(active, Contracts.ProposeOptions.DEFAULT);
        assertTrue(p.refused());
        assertTrue(p.report().contains("  ! proposed revision 1 is not newer than the active revision 1"), p.report());
    }

    @Test
    void withoutManifestsOrARegistryEveryConsumerIsStillNamedAndApprovalNeedsNoMerge() throws Exception {
        Path activeFile = tmp.resolve("active.json");
        Files.writeString(activeFile, Json.pretty(active.toJson()));
        Contracts c = new Contracts(new Contracts.Settings(repo, services.routerUrl(), null, repo.resolve(ConsumerManifest.DEFAULT_DIR)));

        Contracts.Proposal p = c.propose(proposedFile, new Contracts.ProposeOptions(activeFile, false, null));
        assertFalse(p.refused(), p.text());
        assertEquals(List.of(new Contracts.SchemaCheck(V2, "not checked (no registry)", false)), p.checks());
        assertEquals(List.of(), p.change().migrations());
        assertEquals(List.of("payments-service", "inventory-service"), p.change().unbound());
        assertTrue(p.text().contains("No binding needs regenerating. Activate with: contracts approve rev2"), p.text());
        assertFalse(services.requests.stream().anyMatch(r -> r.startsWith("GET /catalog")), "the active catalog came from the file");

        assertEquals(ChangeStatus.Gate.UNKNOWN, c.changeStatus("rev2").gate().get(0).gate());
        Contracts.Approval a = c.approve("rev2");
        assertTrue(a.approved(), a.text());
        assertEquals(List.of(), a.registered());
        assertEquals("rev2 approved: catalog revision 2 is active; OrderPlaced@v2 not registered (no registry).", a.text());
        assertEquals(1, services.activated.size());
        assertFalse(services.requests.stream().anyMatch(r -> r.contains("/apis/registry")), "no registry was called");
    }

    @Test
    void generateWritesEveryBindingFromTheCatalog() throws Exception {
        commitManifests();
        List<Contracts.Generated> g = contracts().generate(proposed);
        assertEquals(List.of(new Contracts.Generated("payments-service", "simulator/src/main/java/rif/simulator/generated/OrderTotalChanges.java", 2)), g);
        assertEquals(Fixtures.binding(proposed, Fixtures.SIMULATOR), Files.readString(repo.resolve(g.get(0).path())));
    }

    @Test
    void registerPutsEveryVersionInTheRegistryAndSetsNoCompatibilityRuleOfItsOwn() throws Exception {
        services.artifacts.clear();
        services.versions.clear();
        List<EventRef> refs = contracts().register(proposed);
        assertEquals(6, refs.size());
        assertTrue(services.versions.containsAll(List.of("OrderPlaced/1", "OrderPlaced/2", "OrderDispatched/1")), services.versions.toString());
        // Compatibility is the catalog's to judge, by who reads what: a blanket BACKWARD rule would refuse every migrated breaking change.
        assertTrue(services.requests.stream().noneMatch(r -> r.endsWith("/rules")), services.requests.toString());
        assertTrue(services.requests.contains("POST /apis/registry/v3/groups/intent-bus/artifacts/OrderPlaced/versions"), "v2 is a version of an artifact v1 created");
        Apicurio registry = new Apicurio(services.apicurioUrl());
        assertTrue(registry.isRegistered(V2));
        assertTrue(registry.check(proposed.semantics().model().find(V2).orElseThrow()) instanceof Apicurio.Verdict.AlreadyRegistered);
    }

    @Test
    void anUnreachableRouterSaysSo() {
        Contracts c = new Contracts(new Contracts.Settings(repo, "http://127.0.0.1:1", null, repo.resolve(ConsumerManifest.DEFAULT_DIR)));
        ContractsException e = assertThrows(ContractsException.class, () -> c.propose(proposedFile, Contracts.ProposeOptions.DEFAULT));
        assertTrue(e.getMessage().startsWith("router not reachable at http://127.0.0.1:1"), e.getMessage());
    }
}
