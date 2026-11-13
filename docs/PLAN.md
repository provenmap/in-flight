# revenue-in-flight: the design

Status: the merge with intent-bus is built ([MERGE.md](MERGE.md), phases 0 to 7, 2026-11-13). The backend is Java 21
in one Maven build; the console is React and TypeScript. Earlier builds: Scala (commit `4e5cf21`), then TypeScript on
Node (`e039183`, [REFACTOR.md](REFACTOR.md)); both are history to read, not code to run.

The direction: **Kafka's real capabilities, usable by someone who is not a Kafka expert.** One catalog describes a
flow. Watching says what is stuck in the reader's own terms; intents remove the need to know topics and fields;
contracts remove the need to coordinate a schema change by hand.

```
            declare                subscribe               watch                change               recover
  catalog ───────────▶ intents ───────────▶ intent topics ───────▶ console ───────────▶ contracts ───────────▶ redrive, rehearse
  entities, events,    typed predicates,    router + proxy:        what is stuck,       impact, generated      replay dead letters;
  effects, consumers   compiled             a stock client reads   for whom, how much   bindings, the gate     try a build on past events
                                            intent.<name>
```

## Pieces

| Module | What it does |
| --- | --- |
| `catalog` | The file format and its checks; the intent language, compiler and impact analysis; pricing; CloudEvents. Pure: no Kafka |
| `kafka` | Client settings (`RIF_KAFKA_*`) and the admin calls every part shares |
| `router` | One Kafka Streams application per intent: route, repartition by entity, order within the grace window, write `__intent.<name>`. A registry topic keeps the active catalog and runtime intents. Control API on 8085 |
| `proxy-filter` | Kroxylicious: `intent.<name>` ⇄ `__intent.<name>` in every request and response, writes to intents refused; the schema gate refuses unregistered CloudEvent versions |
| `contracts` | Propose, approve, status; Apicurio; Java binding generation; migration branches and pull requests |
| `simulator` | Order traffic and the payments, inventory and shipping services; dead-letter queues; redrive; the gated producer; rehearsal; control API on 8090 |
| `observer` | The ledger, the reader (flow topics, dead-letter queues, intent topics) and the poller (group offsets, log ends) |
| `server` | WebSocket, the simulator relay, the console's API (intents, changes), MCP over Streamable HTTP, the console |
| `cli` | `up`, `down`, `demo`, `simulate`, `serve`, `watch`, `router`, `mcp`, `intents`, `explain`, `impact`, `contracts`, `rehearse` |
| `ui/` | The console: four views over one flow map |

## The flow

| Event | Topic | From | Read by (intent) |
| --- | --- | --- | --- |
| `OrderPlaced` v1, v2 | `orders.placed` | Orders | payments-service (`order-total-changes`), inventory-service (`orders-placed`) |
| `PaymentAuthorized` | `payments.authorized` | Payments | shipping-service (`ready-to-ship`) |
| `PaymentDeclined` | `payments.declined` | Payments | nobody |
| `StockReserved` | `inventory.reserved` | Inventory | shipping-service (`ready-to-ship`) |
| `OrderDispatched` | `shipping.dispatched` | Shipping | the customers (`order-dispatched`) |

The breaking change: `OrderPlaced` v2 turns `total` from a decimal string into `{ amount, currency }`. The shipped
catalog is revision 2, the one that adds v2. The demo's router starts at revision 1, the catalog without it.

## The catalog

- **Districts, events, consumers, dead-letter queues, redrive groups**, as before. `districts` may be left out (then
  each district an event or consumer names is one); an event's `producer`, intent-bus's name, is read as its district.
- **`revision`**, so contracts can tell a proposal from the active catalog.
- **Meaning:** `entities` with typed attributes; per event version, typed `fields`, the `keys` naming the entities it
  is about, and `effects` on attributes. All optional; an event without them is watched and priced, and no intent
  matches it.
- **Roles instead of paths.** `roles` names the case in flight (`Order`), the party waiting on it (`Customer`) and the
  measure it is worth (`Order.total`). An event's order and customer come from its keys; its worth from its own effect
  on the measure, else from what was seen for its case. An explicit `value`, `customer` or `order` path overrides,
  because an observer that starts mid-flow has seen nothing yet. A worth that cannot be established is unknown.
- **Intents**, each `{name, predicate, by?}`, and a consumer's `intent`. A consumer with an intent and no `reads` reads
  the intent's topics.

## Money at risk

- **Stuck:** the value of every dead-lettered event not yet recovered.
- **In lag:** for each service's group, the events between its committed offset and the log end polled with it, on
  its topics or, when it reads by intent, on the intent's topic. Valued from the events the observer saw.
- **Recovered:** a flow event whose `redriveof` names a stuck one; the district that dead-lettered an order producing
  for it again; or the queue's redrive group committing past it. On restart, dead letters below the redrive group's
  commit count as already recovered.
- The headline counts each order once, at the most it is worth anywhere it is held up.

## Intents

- **Compiled** against the catalog's meaning into routing tables: the versions an intent matches and why, their
  topics, its key entity, what a reader takes from each match. Anything unresolved fails.
- **Routed** by the router, one Streams application per intent (`<APPLICATION_ID>-<intent>`), each reading from the
  start, so a runtime intent sees existing events; a catalog change restarts only the intents whose routes changed.
  Order per entity within `GRACE_MS`; late events released and marked; provenance headers on every event.
- **Read** by stock clients as `intent.<name>` through the proxy. The demo's services read their intents through the
  proxy and write straight to Kafka.
- **A version the active catalog does not know** (produced around the gate) follows its type's newest route, so its
  readers dead-letter it visibly; intent-bus skipped it, which lost the order without a trace. A type the catalog has
  never declared stays outside every intent.

## Contracts

- **Propose:** the next revision against the router's active one. Impact intent by intent; each new version checked
  with the registry; for each consumer that must change and has a binding manifest, its binding regenerated on a
  branch in a temporary worktree (or a pull request with `--push`). Consumers the catalog names without a manifest are
  reported as reached. A change record goes in `.rif/changes`.
- **The gate:** a version the registry does not have cannot be produced through the proxy.
- **Approve:** refused until every migration merged; then the new versions are registered and the revision activated
  on the router.
- **The registry holds approved shapes and no compatibility rule.** intent-bus set BACKWARD on each artifact, which
  refuses every breaking change however its readers are prepared. Compatibility is judged by the catalog's impact
  analysis, which knows who reads which field.
- **Bindings are Java:** one record per event version, nested records for nested fields, `Optional` for optional
  ones, a sealed `Effect` per attribute, and `decode(String)` with Jackson. Payments' checked-in binding
  (`simulator/.../generated/OrderTotalChanges.java`) is exactly the generator's output; a test checks it. Patched
  Payments reads through it: the patch is the binding.

## The console

Four views over the same flow map:

- **Flow:** the map, edges labelled by topic or by intent; the runbook; activity.
- **Intents:** the declared intents, their matches and readers, live state and lag; a predicate input that compiles as
  you type, lights its matches up on the map and names what the catalog lacks; declaring one; its live tail.
- **Changes:** propose; each change's headline, the gate per new version, each migration's binding as a diff, merged
  or open; merge (demo) and approve; the two runs compared.
- **Incidents:** the runbook, and what the catalog says about each kind of stuck event: the intent its reader reads by,
  and whether the catalog predicted it, knew the version and foresaw nothing, or never saw it.

## The demo, two ways

1. **Around the catalog:** orders straight to Kafka; v2 jams Payments; patch and redrive drain it.
2. **Through the catalog:** propose revision 2; orders through the gate; v2 refused (each refused order goes out as v1);
   merge Payments' migration (which deploys it); approve; v2 ships with nothing stuck.

`demo` runs the router, the simulator and the server in one process on the stack `up` starts. Merging in the demo is
done in a scratch consumers' repository under `.rif/demo/repo`.

## Rehearse

Milestone 4's replay, selected by intent: an intent, a time range and a build. The intent's past events go through the
running build and the candidate; the candidate's output goes to a branch topic `rehearse.<id>.<consumer>`; the
counts and the events they disagree on come back. Only Payments' code is in the simulator, as `strict` and `patched`.

## Watch mode

`watch my-flow.json`: the observer, the console and the read-only MCP tools on a catalog and a Kafka of one's own.
Structured and binary CloudEvents, plain JSON, Schema Registry framing; this app's, Kafka Connect's and Spring Kafka's
dead-letter headers; recovery without `redriveof`; secured clusters through `RIF_KAFKA_*`. Not yet: Avro and Protobuf,
several currencies, sampling for very busy clusters.

## Checks

- **Unit (195, `./mvnw verify`):** the catalog (TypeScript's suites and intent-bus's model suites), the proxy filters,
  the router (TopologyTestDriver), contracts (compiling the bindings it generates), the services, the ledger, the MCP
  tools and their HTTP guard.
- **End to end (22, `./mvnw verify -Pe2e`, Testcontainers):** the simulator; the observer (healthy $0, jam, recovery,
  restart, 500 orders a second); watch mode; MCP over stdio (the jar) and HTTP; subscribing by intent through the
  router and the proxy, including a runtime intent and rehearsal; the same change shipped both ways.
- **Browser (6, `npm run test:browser`):** the console against the jar, including the intents view.

## Not in this build

- An agent that heals on its own; the tools to do it are there (`impact`, `rehearse`, `propose_change`).
- Rehearsal of consumers whose code is not in the simulator.
- Whether a live business-flow view belongs in ProvenMap itself is still open.
