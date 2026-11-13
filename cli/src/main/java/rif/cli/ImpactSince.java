package rif.cli;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import rif.catalog.Catalog;
import rif.catalog.Describe;
import rif.catalog.EventRef;
import rif.catalog.Impact;
import rif.catalog.Result;

/**
 * Impact for a pull request: every event version a catalog adds since its base, each worked out as {@code impact}
 * does, in one report. Exits 1 when any of them needs a consumer to change first, so CI can hold the pull request.
 */
final class ImpactSince {

    private ImpactSince() {}

    static int run(String[] args) {
        if (args.length < 1) {
            throw new Main.Failure("impact --since needs the base catalog, such as: impact --since base.json flows/checkout.json");
        }
        Catalog base = load(args[0]);
        Catalog head = args.length > 1 ? load(args[1]) : Main.loadCatalog(null);
        List<EventRef> added = new ArrayList<>();
        head.events().forEach(e -> {
            if (base.find(e.ref()).isEmpty()) {
                added.add(e.ref());
            }
        });
        if (added.isEmpty()) {
            System.out.println("No new event versions since the base: nothing to check.");
            return 0;
        }
        if (head.semantics() == null) {
            System.out.println("The catalog adds " + String.join(", ", added.stream().map(EventRef::show).toList()) + ", but says nothing about what "
                    + "events mean, so who they reach cannot be worked out. " + Describe.NO_SEMANTICS);
            return 0;
        }
        boolean safe = true;
        List<String> out = new ArrayList<>();
        for (EventRef ref : added) {
            Result<Impact> i = Impact.of(ref, head.semantics(), head.readers());
            if (!i.isOk()) {
                out.add(ref.show() + ": " + i.error());
                safe = false;
                continue;
            }
            safe &= i.value().safe();
            out.add("## " + ref.show() + "\n\n" + i.value().headline() + "\n\n" + i.value().report());
        }
        System.out.println((safe ? "Safe to ship: " : "Not safe to ship yet: ") + added.size() + " new event version" + (added.size() == 1 ? "" : "s")
                + " (" + String.join(", ", added.stream().map(EventRef::show).toList()) + ").\n\n" + String.join("\n\n", out));
        return safe ? 0 : 1;
    }

    private static Catalog load(String file) {
        Result<Catalog> c = Catalog.load(Path.of(file));
        if (!c.isOk()) {
            throw new Main.Failure(file + " is not a usable catalog:\n  " + String.join("\n  ", c.errors()));
        }
        return c.value();
    }
}
