package rif.cli;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import rif.catalog.Catalog;
import rif.contracts.Change;
import rif.contracts.Contracts;
import rif.contracts.ContractsException;

/**
 * {@code contracts}: contracts ahead of breakage.
 *
 * <pre>
 *   contracts propose &lt;catalog.json&gt; [--push] [--catalog active.json] [--base branch] [--no-registry]
 *   contracts approve &lt;id&gt; [--catalog active.json] [--no-registry]
 *   contracts status [&lt;id&gt;]
 *   contracts generate [--catalog catalog.json]
 *   contracts register [--catalog catalog.json]
 * </pre>
 *
 * The active catalog is the router's ({@code ROUTER_URL}) unless {@code --catalog} names a file.
 */
final class ContractsCommand {

    private ContractsCommand() {}

    static final String USAGE = "usage: contracts propose <catalog.json> [--push] [--catalog active.json] [--base branch] [--no-registry]"
            + " | approve <id> [--catalog active.json] | status [<id>] | generate [--catalog F] | register [--catalog F]";

    static int run(String[] args) {
        List<String> positional = new ArrayList<>();
        Map<String, String> opts = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a.equals("--push") || a.equals("--no-registry")) {
                opts.put(a, "true");
            } else if (a.startsWith("--") && i + 1 < args.length) {
                opts.put(a, args[++i]);
            } else {
                positional.add(a);
            }
        }
        Contracts.Settings settings = Contracts.Settings.fromEnv(Path.of("").toAbsolutePath());
        if (opts.containsKey("--no-registry")) {
            settings = settings.withoutRegistry();
        }
        Contracts c = new Contracts(settings);
        Path active = opts.containsKey("--catalog") ? Path.of(opts.get("--catalog")) : null;
        try {
            String cmd = positional.isEmpty() ? "" : positional.get(0);
            switch (cmd) {
                case "propose" -> {
                    if (positional.size() < 2) {
                        throw new Main.Failure(USAGE);
                    }
                    Contracts.Proposal p = c.propose(Path.of(positional.get(1)), new Contracts.ProposeOptions(active, opts.containsKey("--push"), opts.get("--base")));
                    System.out.println(p.text());
                    return p.refused() ? 1 : 0;
                }
                case "approve" -> {
                    if (positional.size() < 2) {
                        throw new Main.Failure(USAGE);
                    }
                    Contracts.Approval a = active == null ? c.approve(positional.get(1)) : c.approve(positional.get(1), active);
                    System.out.println(a.text());
                    return a.approved() ? 0 : 1;
                }
                case "status" -> {
                    if (positional.size() > 1) {
                        System.out.println(c.changeStatus(positional.get(1)).text());
                    } else {
                        List<Change> all = c.changes();
                        if (all.isEmpty()) {
                            System.out.println("no changes");
                        }
                        all.forEach(ch -> System.out.println(ch.id() + ": " + ch.status().name().toLowerCase()));
                    }
                    return 0;
                }
                case "generate" -> {
                    Catalog catalog = c.activeCatalog(active);
                    c.generate(catalog).forEach(g -> System.out.println(g.consumer() + ": " + g.path() + " at catalog revision " + g.revision()));
                    return 0;
                }
                case "register" -> {
                    c.register(c.activeCatalog(active)).forEach(r -> System.out.println("registered " + r.show()));
                    return 0;
                }
                default -> throw new Main.Failure(USAGE);
            }
        } catch (ContractsException e) {
            throw new Main.Failure(e.getMessage());
        }
    }
}
