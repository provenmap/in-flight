# Revenue in Flight

Kafka's real capabilities, usable by someone who is not a Kafka expert. One catalog describes a flow: who produces
what, what each event means, who cares about what, and where failures land. From it you can see what is stuck and
what it is worth, subscribe to what you care about instead of to topics, and refuse a schema change until every
consumer it reaches is ready.

![The console during an incident: money at risk climbing as Payments dead-letters every new order](docs/images/incident.png)

An operations console watches order-to-cash live from Kafka: Orders, Payments, Inventory and Shipping are districts
on a flow map, each edge as thick as the money crossing it a minute, and every event a particle sized by its value.
The demo makes one schema change, `OrderPlaced` v2, twice:

1. **Around the catalog.** The orders team writes v2 straight to Kafka. Payments cannot read it and dead-letters
   every order, money at risk climbs, Payments is patched, the dead-letter queue is redriven, the counter drains.
2. **Through the catalog.** The same change is proposed. The console shows Payments must change and Inventory need
   not, with Payments' regenerated binding as a migration. The schema gate refuses v2 until that migration merges and
   the change is approved. Then v2 ships, and nothing gets stuck.

The closing screen compares them: money stuck and time to recover the first way, $0 the second.

Nothing on screen is canned. The simulator runs real Kafka services with real consumer groups, reading through a real
proxy; the router is a Kafka Streams application per intent; the gate is a Kroxylicious filter checking a schema
registry. An MCP server gives an agent the same view and the same controls. [The guide](docs/guide.md) walks through
the console with screenshots.

## Run the demo

You need Docker and a JDK 21. Node 22 is needed only to build the console.

```sh
npm ci && npm run build                 # the console, which the jar carries
./mvnw package                          # one jar: cli/target/revenue-in-flight.jar
bin/revenue-in-flight up                # Kafka 4.4, the Apicurio registry, the Kroxylicious proxy with our filters
bin/revenue-in-flight demo              # in a second shell: router, simulator and console; open http://localhost:8088
```

`bin/revenue-in-flight` finds a JDK 21 and runs the jar; `java -jar cli/target/revenue-in-flight.jar` is the same.
`demo` starts the router at revision 1 of the catalog (before `OrderPlaced` v2), registers its versions, and runs
the services reading by intent through the proxy. `down` stops the stack and deletes its data and the demo's state.

The console has four views over the same flow map:

| View | Answers |
| --- | --- |
| **Flow** | what is moving, what is stuck, what it is worth; edges labelled by topic or by the intent their reader subscribes to |
| **Intents** | who cares about what. Type a predicate and it compiles as you type: its matches light up on the map, and an error names what the catalog does not have |
| **Changes** | what a schema change will do, who must change first and why, each migration's binding as a diff, merged or not, and whether the gate lets it through |
| **Incidents** | the runbook and every stuck event, each with the intent its reader reads by and whether the catalog had predicted it |

For the first run, ship `OrderPlaced` v2 from the runbook, then patch and redrive. For the second, open Changes:
propose, switch the orders team to ship through the gate, ship v2 (the gate refuses it, so orders keep going out as
v1), merge Payments' migration, approve, and watch v2 flow with nothing stuck.

Without the proxy and the registry, the incident alone runs on Kafka:

```sh
bin/revenue-in-flight up kafka
bin/revenue-in-flight simulate          # order traffic and the three services, reading their topics
bin/revenue-in-flight serve             # the console on 8088
```

## Ask before you break anything

`impact` reads the catalog only, so it needs no Kafka:

```sh
bin/revenue-in-flight impact OrderPlaced@v2
```

```
payments-service must change before this ships, or its events pile up in payments.dlq.
  ...
  - order-total-changes: changes(Order.total)
      ! OrderPlaced@v2 from orders.placed, read differently from OrderPlaced@v1:
          Order.total: from total (decimal) -> from total.amount (decimal)
      consumers: payments-service
  - orders-placed: emits(OrderPlaced)
      + OrderPlaced@v2 from orders.placed, read the same way as OrderPlaced@v1
      consumers: inventory-service
```

It exits 1 when a consumer must change first, so it can gate a pull request that adds a version to the catalog.

## Read by intent, not by topic

Kafka expects every consumer to know which topics hold what it needs, and which fields to read from each version of
each event. The catalog carries that knowledge instead. It says what each event means: the entities it is about, and
what it does to their attributes.

```json
"entities": { "Order": { "total": "decimal", "status": "string" }, "Customer": {} },
"events": [{
  "type": "OrderPlaced", "version": 2, "topic": "orders.placed", "district": "orders",
  "fields": { "orderId": "string", "customerId": "string", "total": { "amount": "decimal", "currency": "string" } },
  "keys": { "Order": "orderId", "Customer": "customerId" },
  "effects": [{ "op": "assign", "attribute": "Order.total", "from": "total.amount" }]
}],
"intents": [{ "name": "order-total-changes", "predicate": "changes(Order.total)" }],
"consumers": [{ "group": "payments-service", "district": "payments", "intent": "order-total-changes", "dlq": "payments.dlq" }]
```

| Predicate | Means |
| --- | --- |
| `changes(Order.total)` | every event version with an effect on that attribute, from whichever topics |
| `emits(OrderPlaced)` | every version of that event type |
| `about(Order)` | every event that carries that entity's key |
| `and`, `or`, `not`, `( )` | combine them; `not` binds tightest, then `and`, then `or` |

An intent is compiled when the catalog loads. It fails on an entity, attribute or event type the catalog does not
have; nothing is matched by guessing. The **router** then makes each intent a real topic: one Kafka Streams
application per intent reads the topics its matches come from and writes `__intent.<name>`, keyed by the intent's
entity, in order per entity. A consumer subscribes with a stock Kafka client to `intent.<name>` through the
**proxy**, which renames it both ways and refuses writes to it.

- **Order:** per entity, by event time, within the grace window (`GRACE_MS`, 1.5 s). A later event older than one
  already released is delivered at once and marked `ib-late`; nothing is dropped.
- **Delivery:** at least once by default, exactly once with `PROCESSING_GUARANTEE=exactly_once_v2`.
- **Provenance:** every event carries `ib-intent`, `ib-catalog-revision`, `ib-entity`, `ib-source-topic`,
  `ib-source-partition` and `ib-source-offset`.
- **An intent declared at runtime** sees the events that already exist: each intent is its own application, reading
  from the start.
- **A version the active catalog does not know** was produced around the gate. It goes to the intents that route its
  type, so its readers meet it and dead-letter it where it can be seen and recovered, instead of losing the order.

```sh
bin/revenue-in-flight intents                          # each intent: what it matches, why, who reads by it
bin/revenue-in-flight explain "changes(Order.payment)" # try a predicate without declaring it
bin/revenue-in-flight router                           # the router alone, control API on 8085
```

## Changes, and the gate

A proposal is the catalog's next revision. `contracts propose` works out its impact, checks each new version against
the schema registry, and for each consumer that must change, regenerates its typed binding (Java: a record per event
version, decoded from the CloudEvent, with its typed effects on the intent's entity) on a branch, or as a pull request
with `--push`. The **schema gate** in the proxy refuses to produce any version the registry does not have, so the new
version cannot be produced until `contracts approve` finds every migration merged, registers it, and activates the
revision on the router.

```sh
bin/revenue-in-flight contracts propose catalog/order-to-cash.json
bin/revenue-in-flight contracts status rev2
bin/revenue-in-flight contracts approve rev2       # held until every migration has merged
```

The gate binds only producers that write through the proxy: keep producers behind it, as network policy. The
registry holds approved shapes and carries no compatibility rule of its own: whether a change is compatible is the
catalog's to judge, from who reads which field.

## Rehearse a build on the past

Name an intent, a time range and a build: the intent's past events run through that build beside the one running
now, the candidate's output goes to a branch topic no reader sees, and the difference comes back.

```sh
bin/revenue-in-flight rehearse order-total-changes patched 30
```

```
Rehearsed 312 events of intent.order-total-changes through payments-service: strict {PaymentAuthorized=196,
dead-lettered=105, PaymentDeclined=11}, patched {PaymentAuthorized=289, PaymentDeclined=23}. They disagree on 105
events. The candidate's output is on rehearse.r3f9a1c02.payments-service.
```

The figures are illustrative; the wording is what it prints. The simulator has the code of Payments only, as `strict`
(OrderPlaced v1) and `patched` (its regenerated binding).

## Watch your own Kafka

Each step is useful without the next, and none needs a higher one:

| Level | You add | You get | Touches your cluster |
| --- | --- | --- | --- |
| 0. Watch | a catalog: topics, consumer groups, dead-letter queues, where the value is | what is stuck and lagging, in your own terms | reads only |
| 1. Meaning | entities, fields, keys, effects, intents | `impact` on every proposed event version, in CI | not at all |
| 2. Subscribe | the router | consumers read `intent.<name>`, ordered per entity | writes derived topics |
| 3. Gate | the proxy in front of producers | an undeclared event shape cannot be produced | in the produce path |

```sh
BOOTSTRAP_SERVERS=broker-1:9092 bin/revenue-in-flight watch my-flow.json
```

The observer reads CloudEvents (structured or binary), plain JSON and Schema Registry framed JSON, and the
dead-letter headers of this app, Kafka Connect and Spring Kafka. A catalog with `roles` (`case`, `party`, `measure`)
needs no value paths: an event's order and customer come from its keys, and its worth from its effect on the measure,
else from what has been seen for its order. A worth that cannot be established is shown as unknown, never guessed.
The observer only reads: it never joins or commits for your groups. [The guide](docs/guide.md#watch-your-own-kafka)
has the catalog format and the settings for SASL and TLS.

## Drive it from an agent

```sh
claude mcp add revenue-in-flight -- java -jar /path/to/revenue-in-flight.jar mcp
claude mcp add revenue-in-flight -- docker run --rm -i --network host revenue-in-flight mcp
```

The stdio server runs an observer of its own; `serve` and `demo` also answer MCP over Streamable HTTP on
`http://localhost:8088/mcp`. With no simulator (`watch`), only the tools that read are offered.

| Tool | What it gives |
| --- | --- |
| `value_at_risk`, `stuck_events`, `last_episode` | money held up, stuck and in lag; each stuck event and its error; the latest recovery |
| `intents`, `explain_intent`, `impact` | what each intent matches and who reads by it; what a predicate would match; who a version reaches and who must change first |
| `declare_intent`, `tail_intent` | declare an intent on the router; the latest events on its topic |
| `propose_change`, `change_status`, `approve_change` | contracts in the loop; approval is refused while any migration is open |
| `rehearse` | a build on the intent's past, and the difference |
| `simulator_state`, `break_schema`, `restore_schema`, `patch_payments`, `unpatch_payments`, `redrive`, `set_rate` | the console's controls |

The resource `rif://flow` describes the landscape: districts, topics, who produces and who reads each one, and the
intents.

## How it works

Java 21 for the backend, one Maven build; TypeScript and React for the console.

| Module | What it is |
| --- | --- |
| `catalog` | the file format and its checks, the intent language and compiler, impact, pricing. Pure: no Kafka. The shipped flow is `catalog/order-to-cash.json` |
| `router` | Kafka Streams: one application per intent, a derived topic keyed by entity, ordered per entity; control API on 8085 |
| `proxy-filter` | the Kroxylicious filters: the `intent.<name>` alias and the schema gate |
| `contracts` | registry check, binding generation, migration branches and pull requests, approval |
| `observer` | the ledger (stuck, lag, recovered), the reader and the poller; it learns intent topics and per-intent lag |
| `simulator` | the order-to-cash services, reading by intent through the proxy when there is one; both ways of shipping v2; redrive; rehearsal |
| `server` | HTTP, WebSocket, MCP over HTTP, the console |
| `cli` | one entry point for every command, and the runnable jar |
| `ui/` | React, shadcn/ui and D3: the flow map, money at risk over time, the four views |

Money at risk is stuck money plus money in consumer lag, each order counted once at the most it is worth anywhere it
is held up. A redrive is one pass over the dead-letter queue in its own consumer group; the first record that still
fails stops its partition without moving its offset. [docs/PLAN.md](docs/PLAN.md) has the design.

## Configuration

| Variable | Default | |
| --- | --- | --- |
| `BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka |
| `PORT`, `CONTROL_PORT`, `ROUTER_PORT` | `8088`, `8090`, `8085` | the console, the simulator, the router |
| `SIMULATOR_URL` | `http://localhost:8090` | where `serve` and `mcp` find the simulator; `none` to watch |
| `RIF_INTENTS_VIA` | none | the proxy, for `simulate`: its services then read by intent through it |
| `RIF_PROXY`, `APICURIO_URL`, `ROUTER_URL` | `localhost:9192`, `http://localhost:8080/apis/registry/v3`, `http://localhost:8085` | the proxy, the registry and the router, for `demo` and `contracts` |
| `RIF_CATALOG` | the shipped one | the catalog file |
| `RIF_KAFKA_*` | none | Java client settings: `RIF_KAFKA_SECURITY_PROTOCOL=SASL_SSL` sets `security.protocol` |
| `HOST` | `localhost` | the interface servers listen on |
| `RIF_MCP_TOKEN` | none | a bearer token for MCP over HTTP; required when `HOST` is not localhost |
| `GRACE_MS`, `PROCESSING_GUARANTEE`, `STATE_DIR` | `1500`, `at_least_once`, a temporary folder | the router |

## Docker

```sh
docker build -t revenue-in-flight .
docker run --rm --network host revenue-in-flight demo
```

The image listens on all interfaces, so MCP over HTTP stays off until `RIF_MCP_TOKEN` is set.

## Develop

```sh
./mvnw verify                  # every module's unit tests
./mvnw verify -Pe2e            # end to end, on Kafka, the registry and the proxy in containers (needs Docker)
npm run lint && npm run typecheck
npm run dev:ui                 # the console with hot reload, against serve on 8088
npm run test:browser           # Playwright against the jar and the Kafka in BOOTSTRAP_SERVERS
npm run docs:screenshots       # retakes the guide's screenshots
```

The end-to-end suites each start their own Kafka (or whole stack) in containers. The browser tests start their own
simulator, which refuses to share a Kafka with another one: stop the demo first, or point `BOOTSTRAP_SERVERS` at
another Kafka.

## Credits

The intent language, the router and the contracts were first built in
[intent-bus](https://github.com/provenmap/intent-bus) and rewritten here in Java; its proxy filter is used as it was.

## License

Apache 2.0. See [LICENSE](LICENSE).
