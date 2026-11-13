package rif.contracts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import rif.catalog.Catalog;
import rif.catalog.Impact;
import rif.catalog.Intent;
import rif.catalog.Model;
import rif.catalog.Result;
import rif.catalog.Semantics;

/**
 * Everything a proposed catalog revision would do against the active one, worked out before anything is registered or
 * activated: the impact, the event versions it adds, and every consumer it reaches.
 *
 * <p>Who is reached comes from the proposed catalog, which declares its consumers and the intent each reads by, and from
 * the consumer manifests, which add consumers the catalog does not list. A consumer with no manifest is still reached;
 * it just has no binding to regenerate.
 *
 * @param impact the catalog's impact, with this proposal's own refusals added to its errors: a revision that is not
 *     newer, and an intent the active catalog declares that the proposal would break
 * @param added the event versions the proposal adds
 * @param affected each consumer of each affected intent, in the order of the impact's intents
 */
public record Analysis(Catalog active, Catalog proposed, Impact impact, List<Model.Event> added, List<Affected> affected) {

    public Analysis {
        added = List.copyOf(added);
        affected = List.copyOf(affected);
    }

    /** A consumer an affected intent reaches, and its manifest, or null when it has none. */
    public record Affected(String consumer, Impact.IntentImpact intent, ConsumerManifest manifest) {
        public boolean bound() {
            return manifest != null;
        }
    }

    public boolean blocked() {
        return !impact.errors().isEmpty();
    }

    public static Analysis of(Catalog active, Catalog proposed, List<ConsumerManifest> manifests) {
        if (proposed.semantics() == null) {
            throw new ContractsException("the proposed catalog says nothing about what its events mean (entities, fields, keys, effects), "
                    + "so there is nothing to check it against");
        }
        Model before = active.semantics() == null ? new Model(Map.of(), List.of()) : active.semantics().model();
        List<String> refusals = new ArrayList<>();
        if (proposed.revision() <= active.revision()) {
            refusals.add("proposed revision " + proposed.revision() + " is not newer than the active revision " + active.revision());
        }

        // An intent the active catalog declares and the proposal drops is still checked: its readers still read it.
        Semantics after = proposed.semantics();
        List<String> broken = new ArrayList<>();
        if (active.semantics() != null) {
            for (Intent i : active.semantics().intents()) {
                if (after.table(i.name()) == null) {
                    Result<Semantics> kept = after.declare(i);
                    if (kept.isOk()) {
                        after = kept.value();
                    } else {
                        kept.errors().forEach(e -> broken.add("would break " + e));
                    }
                }
            }
        }

        Function<String, List<Impact.Reader>> readers = intent -> {
            List<Impact.Reader> out = new ArrayList<>(proposed.readers().apply(intent));
            for (ConsumerManifest m : manifests) {
                if (m.intent().equals(intent) && out.stream().noneMatch(r -> r.name().equals(m.name()))) {
                    out.add(new Impact.Reader(m.name(), null));
                }
            }
            return out;
        };
        Impact found = Impact.analyse(before, after, readers);
        List<String> errors = new ArrayList<>(refusals);
        errors.addAll(found.errors());
        errors.addAll(broken);
        Impact impact = new Impact(found.changes(), found.intents(), found.unaffected(), List.copyOf(errors), found.breaks());

        List<Affected> affected = new ArrayList<>();
        for (Impact.IntentImpact i : impact.intents()) {
            for (Impact.Reader r : i.consumers()) {
                ConsumerManifest m = manifests.stream().filter(x -> x.name().equals(r.name()) && x.intent().equals(i.intent())).findFirst().orElse(null);
                affected.add(new Affected(r.name(), i, m));
            }
        }
        return new Analysis(active, proposed, impact, Impact.diff(before, after.model()).added(), affected);
    }
}
