# Merging intent-bus into in-flight, on the JVM

Written 2026-11-13. Status: built the same day, phases 0 to 7; [HANDOVER.md](HANDOVER.md) has how each was shown done
and where the build differs from this plan, and [PLAN.md](PLAN.md) the design as built. The four decisions at the end
are still the owner's. [REFACTOR.md](REFACTOR.md) is the earlier move from Scala to TypeScript, which this plan
superseded for the backend: it moved to Java.

## What this is for

Two attempts exist, each half of one product:

| | in-flight (this repo) | intent-bus (`../intent-bus`) |
| --- | --- | --- |
| Does | watches a flow on Kafka and says what is stuck, for whom, worth how much; redrives a dead-letter queue | lets a consumer subscribe by typed intent; refuses a schema change until every consumer it reaches is ready |
| Has | a console, an MCP server, watch mode on anyone's Kafka, a simulator | a router that makes an intent a real topic, a proxy filter, contracts with generated bindings |
| Lacks | any way to prevent the incident it shows | any way to see what is happening, and any interface but a shell script |
| Stack today | TypeScript on Node, React | Scala 3, Kafka Streams, a Java filter for Kroxylicious |
| State | milestones 1 to 3, unreleased, no GitHub repo | milestones 1 to 3, parked on a branch, public repo |

The direction both serve: **Kafka's real capabilities, usable by someone who is not a Kafka expert.** Each removes a
different piece of required expertise. The merge puts them on one catalog, in one console, as one loop.

## Why the JVM

The two pieces that make an intent real cannot run on Node:

- **The router** is a Kafka Streams application. Streams gives it exactly-once delivery and durable state for
  per-entity ordering; there is no Streams for Node.
- **The proxy filter** is a Kroxylicious plugin, loaded through a Java API.

So the backend moves to the JVM, which is what the showcase's own language rule says to do when the core needs a
runtime API only one language has. Nothing is released, so there is no user to migrate.

**Java 21 for the backend, TypeScript for the console.** Decided 2026-11-13, over Scala 3.

- **The audience reads Java.** The repo exists to be read and judged by Kafka developers, and most of them work
  in Java. A project about removing expert-only walls should not put one in front of its own code.
- **Every API it touches is Java:** Kafka Streams, the Kroxylicious filter API, the MCP SDK. intent-bus already
  calls the Java Streams DSL from Scala, because the Scala wrapper is not published for Scala 3.
- **Java 21 covers what Scala was buying.** The intent language and compiler need algebraic types and pattern
  matching: records, sealed interfaces and switch patterns.
- **One language on the JVM side**, and a lighter toolchain for a reader: a JDK and the Maven wrapper, no sbt.
- **The cost:** intent-bus's Scala does not come in unchanged. About 2,400 lines of main code and 700 of tests are
  rewritten in Java; only its 600-line proxy filter, already Java, moves as it is. This repo's first build in
  Scala (commit `4e5cf21`) becomes a reference to read, not a starting point. The first merged demo arrives later
  than it would in Scala. That is accepted: the language outlasts the porting work.
- The console stays React and TypeScript. It runs in a browser; "convert everything" means everything server-side.
  Its Playwright tests stay with it.

Defaults that follow, each to be confirmed in phase 0: Maven with its wrapper, multi-module; Jackson for JSON;
`BigDecimal` for money; Javalin for HTTP and WebSocket; JUnit 5, with Testcontainers for the tests that need Kafka.

## The product after the merge

One catalog describes a flow: who produces what, what each event means, who cares about what, and where failures
land. Everything else reads it.

```
            declare                subscribe               watch                change               recover
  catalog ───────────▶ intents ───────────▶ intent topics ───────▶ console ───────────▶ contracts ───────────▶ redrive, rehearse
  entities, events,    typed predicates,    router + proxy:        what is stuck,       impact, generated      replay dead letters;
  effects, consumers   compiled             a stock client reads   for whom, how much   bindings, the gate     try a build on past events
                                            intent.<name>
```

Each step is useful without the next. That matters for anyone pointing it at a Kafka they already run:

| Level | They add | They get | Touches their cluster |
| --- | --- | --- | --- |
| 0. Watch | a catalog: topics, consumer groups, dead-letter queues, where the value is | what is stuck and lagging, in their own terms | reads only |
| 1. Meaning | entities, fields, keys, effects, intents | `impact` on every proposed event version, in CI | not at all |
| 2. Subscribe | the router | consumers read `__intent.<name>`, ordered per entity | writes derived topics |
| 3. Gate | the proxy in front of producers | an undeclared event shape cannot be produced | in the produce path |

Watch mode today is level 0, and the work done on 2026-11-13 is level 1. Nothing in the merge may make a higher
level a precondition for a lower one.

## The console

Today the console tells one story: an incident, and its recovery. The merged console tells the whole loop, and its
demo makes one schema change twice.

**Navigation.** Four views over the same flow map, which stays the centre of the page.

| View | Answers | New or existing |
| --- | --- | --- |
| **Flow** | What is moving, what is stuck, what is it worth | existing; gains a toggle between topics and intents on the edges |
| **Intents** | Who cares about what, and what would this predicate match | new |
| **Changes** | What will this schema change do, who must act, is it safe to ship | new |
| **Incidents** | What is stuck and why, redrive it, what did the last one cost | existing runbook, stuck table and timeline |

**Flow.** The map draws an edge per topic today. Switched to intents, it draws an edge per intent from the districts
that produce its matches to the districts that read by it, labelled with the predicate. The same particles move on
either. A district's card lists the intents it reads by.

**Intents.** The declared intents, each with its matches, the reason for each match, what a reader takes from each,
and its readers. Above them, one input: type a predicate and it compiles as you type. Matches light up on the map;
an error names the entity, attribute or event type the catalog does not have. This is the entry point for someone
who knows their business and not their topics. With the router running, a declared intent also shows its live
stream and lag.

**Changes.** A proposed catalog revision, or a single new event version, and its impact: who it reaches, who reads
it the same way, who must change first and why, each with its generated binding as a diff and the state of its pull
request. With the gate running, the view shows whether the new shape can be produced yet, and holds the approval.

**Incidents.** As today. Each stuck event gains two facts: the intent its reader was reading by, and whether the
catalog had predicted it. "Predicted by the proposal for OrderPlaced v2, shipped without approval" reads very
differently from "a version the catalog has never seen".

**The demo.** One schema change, `OrderPlaced` v2, shipped two ways:

1. **Around the catalog.** The orders team writes v2 straight to the topic. Payments dead-letters every order,
   money at risk climbs, payments is patched, the queue is redriven, the counter drains. This is today's demo.
2. **Through the catalog.** The same change is proposed. Changes shows payments must change and inventory need
   not, with payments' regenerated binding as a pull request. The gate refuses v2 until it merges. Then v2 ships,
   and nothing gets stuck.

The closing screen compares them: money stuck and time to recover the first way, zero the second. The first run
needs no narration to make the case for the second.

"Patch payments" stops being a magic button. In the second run the patch *is* the generated binding, merged.

## What merges into what

**The catalog.** One file format. This repo's catalog already carries intent-bus's `entities`, `fields`, `keys`,
`effects` and `intents` (the 2026-11-13 work). Still to reconcile:

- `revision`, which contracts needs to tell a proposal from the active catalog.
- `producer` (intent-bus) against `district` (here): a district produces; keep `district`, drop `producer`.
- **Value, customer and order become meaning.** Today each event declares three paths. They generalise to three
  roles the catalog names once: the **case** in flight (an entity: `Order`), the **party** waiting on it
  (`Customer`), and the **measure** it is worth (an attribute: `Order.total`). An event's case and party come from
  its keys. Its worth is the measure's value for its case: from the event's own effect on the measure when it has
  one, else from what the observer has already seen for that case. The explicit `value` path stays as an override,
  because an observer that starts mid-flow has seen nothing yet. A worth that cannot be established is counted as
  unknown, as now, never guessed. This is what makes money one measure among others.

**Modules** (Maven, one build, all Java):

| Module | From | What it is |
| --- | --- | --- |
| `catalog` | this repo's `src/pricing` + `src/intents`, intent-bus's `intent-model` and the analysis half of `contracts` | the file format, its checks, the intent language and compiler, impact, pricing. Pure: no Kafka |
| `router` | intent-bus, rewritten from Scala | Kafka Streams: one application per intent, a derived topic keyed by entity |
| `proxy-filter` | intent-bus, as it is | the `intent.<name>` alias and the schema gate |
| `contracts` | intent-bus, rewritten from Scala | registry check, binding generation, pull requests, approval |
| `observer` | this repo | the ledger, the readers, the poller; learns intent topics and per-intent lag |
| `simulator` | this repo + intent-bus's `demo` | the order-to-cash services, now reading by intent; both ways of shipping v2 |
| `server` | this repo | HTTP, WebSocket, MCP over HTTP, the built console |
| `cli` | this repo + intent-bus's `demo/ib` | one entry point for every command |
| `ui/` | this repo | the console, with the three new views |

**Behaviour that must survive from each side.**

- From this repo, everything [REFACTOR.md](REFACTOR.md) lists under "What must not change" (the accounting, redrive
  semantics, the WebSocket protocol, the control API, the test hooks), plus what was added since: watch mode and
  all it reads, the unpriced dead letter, the host and token rules for MCP over HTTP.
- From intent-bus, its README's "Guarantees, stated exactly": per-entity order within the grace window, late events
  delivered and marked, at-least-once by default and exactly-once on request, provenance headers, and the gate's
  honest limit (it binds only producers that write through the proxy).
- From the 2026-11-13 TypeScript port, three things the Scala original does not have: a consumer declared with an
  intent in the catalog, what a reader takes from each match (`reads`), and how a new version lands on readers
  (same reads, new reads, new type). The Java `catalog` is written from the TypeScript, which has them, with the
  Scala original as the cross-check.

**MCP.** One server. The existing tools stay. Added: `intents`, `explain_intent`, `impact` (ported), then
`declare_intent`, `tail_intent`, `propose_change`, `change_status` and `approve_change`, the last guarded by the
same merged-pull-request check as the console's button. Read-only in watch mode, as now.

## Phases

Each ends green and runnable. The TypeScript backend keeps working until phase 6, so there is always a demo.

**0. Scaffold, and prove the three unknowns.** One Maven build on JDK 21 with CI, and intent-bus's proxy filter
brought in as its first module, with its two test suites rewritten from munit to JUnit. Then spike what has not
been done in Java here before:

- an MCP server on the official Java SDK, over stdio and Streamable HTTP, with the token and host rules;
- the WebSocket server at 500 events a second (the first Scala build used Javalin);
- distribution without `npx`: a container image and a single launcher.

A gap in any of them stops the plan here for a decision.

**1. One catalog.** Merge the formats, port the 2026-11-13 additions and the watch-mode pricing. The Vitest tests in
`src/pricing` and `src/intents` are the specification: port each to JUnit before the code it tests, then add
intent-bus's three model suites. Everything later depends on this module, so it is first. Done when both repos'
catalogs load and `impact OrderPlaced@v2` prints what it prints today.

**2. Observer, simulator, server.** Port from the TypeScript, test by test. Done when the existing console runs
unchanged against the Java server and the Playwright tests pass on it. The WebSocket
protocol is the contract, as it was in the last port.

**3. MCP.** The current tool list on the JVM, over both transports. Done when the MCP end-to-end test passes.

**4. Subscribe by intent.** The router is rewritten in Java against intent-bus's two router suites, and joins the
Compose stack with the proxy. The simulator's services read by intent
through the proxy. The observer watches intent topics. Fix intent-bus's known bug on the way: all intents share one
Streams application, so an intent declared at runtime misses events that already exist; each gets its own. Done
when payments reads `intent.order-total-changes` and the incident demo still passes.

**5. Contracts in the loop.** Contracts is rewritten in Java, with one change: the binding generator emits Java,
not Scala. Then propose, impact, generated binding, pull request, approval, the gate. The simulator
gains the second way of shipping v2. Done when the same change jams one way and passes clean the other, in a test.

**6. The console.** Intents, Changes, the intent toggle on the map, the two extra facts on a stuck event, the
two-run demo and its comparison. Then retire the TypeScript backend: delete `src/`, the Node tooling for it, and the
npm packaging. Rewrite README, PLAN and HANDOVER.

**7. Rehearse.** The old milestone 4, now selected by intent: name an intent, a time range and a build; its past
events run through the build on a branch topic, and the difference in output comes back.

## What was unverified, and what was found

- **The MCP Java SDK** covers stdio and stateless Streamable HTTP (2.1.0); the host and token rules sit in front of it.
- **intent-bus's router and contracts** were not run as they stood; they were rewritten in Java against their suites
  and run end to end in containers.
- **The end-to-end and browser tests** all pass on the Java build.
- **The demo's weight:** three containers and one JVM (`demo` runs the router, the simulator and the server together).
  The JVM starts in about two seconds once the stack is up.

## Costs, stated plainly

- **This is the second rewrite of the backend in two weeks, and a rewrite of intent-bus as well.** About 3,700
  lines of TypeScript backend and 900 of tests, plus 2,400 lines of Scala and 700 of tests, become Java. The tests
  keep it mechanical, but it is weeks, and it adds no feature until phase 4. The console's 2,200 lines stay.
- **`npx in-flight` goes away.** A reader needs Docker, or a JDK. The one-line `claude mcp add` becomes a
  `docker run` or a `java -jar`.
- **The 2026-11-13 TypeScript intents code is thrown away as code.** Its design, its catalog format and its 18
  tests carry over.

## Decisions that are the owner's

1. **The name.** With money as one measure among others, "revenue-in-flight" names the demo, not the product.
   (Settled 2026-11-14: renamed in-flight.)
2. **The intent-bus repository.** It is public. Archive it with a pointer here once phase 4 passes, or keep it.
   Either way this repo's NOTICE credits it for the filter and the designs rewritten from it.
3. **The first run of the demo.** Keep "around the catalog" as the opening act, as this plan assumes, or lead with
   the clean run.
4. **Branch `merge-intent-bus`.** Commit the 2026-11-13 work as the specification for phase 1, or leave it
   uncommitted until then.
