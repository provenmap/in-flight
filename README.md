# Revenue in Flight

An event pipeline you see in money. An operations console watches order-to-cash live from Kafka: Orders, Payments,
Inventory and Shipping are districts on a flow map, each edge as thick as the money crossing it a minute, and every
event a particle sized by its value. The orders team ships a new `OrderPlaced` shape, Payments cannot read it, and
orders pile up red in its dead-letter queue while the counter runs: money stuck, customers waiting. Patch Payments,
redrive the queue, and the pile drains green into Shipping while the counter falls to $0.

![The console during an incident: money at risk climbing as Payments dead-letters every new order](docs/images/incident.png)

Nothing on screen is canned. The simulator runs real Kafka services with real consumer groups and a real
dead-letter queue, and the observer prices every event it reads from the topics.

An MCP server gives an agent the same view and the same controls: money at risk, the stuck events and their errors,
the last recovery, and the buttons.

It also watches a flow of your own: describe it in a catalog, point it at your Kafka, and it shows the same money at
risk with no simulator and no controls. [The guide](docs/guide.md) walks through the console, an incident and
recovery, and watching your own Kafka, with screenshots.

## Run the demo

You need Docker and Node 22 or later.

```sh
npm install && npm run build
npx revenue-in-flight up         # Kafka 4.4 in Docker, KRaft, port 9092
npx revenue-in-flight simulate   # in a second shell: order traffic and the three services
npx revenue-in-flight serve      # in a third shell: then open http://localhost:8088
```

The package is not on npm yet. Once it is, `npx revenue-in-flight` works without the clone.

Before breaking anything, ask what the change will do. This reads the catalog only, so it needs no Kafka:

```sh
npx revenue-in-flight impact OrderPlaced@v2
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

Then follow the incident runbook, and watch exactly that happen:

1. **Ship OrderPlaced v2.** `total` becomes `{ amount, currency }`. Payments dead-letters every new order, the
   dead-letter tank fills, and money at risk climbs. Inventory reads only the items, so it keeps working.
2. **Patch payments.** New orders flow again, but the dead letters are still stuck.
3. **Redrive DLQ.** The stuck orders replay through the patched Payments, turn green and fly on to Shipping. The
   counter drains to $0 and the banner reports what was recovered and how long it was stuck.

Around the map: money at risk over the last 1, 5 or 15 minutes, split into stuck and lag; an activity timeline of
what changed; every stuck order with its error, searchable and exportable as CSV; each consumer group's progress and
lag; the live event stream; throughput by topic; and the details of any district you select. It follows the system's
light or dark setting, with a toggle. `npx revenue-in-flight down` stops Kafka and clears its data.

## Read by intent, not by topic

Kafka expects every consumer to know which topics hold what it needs, and which fields to read from each version
of each event. The catalog can carry that knowledge instead. It says what each event means: the entity it is
about, and what it does to that entity's attributes.

```json
"entities": { "Order": { "total": "decimal", "status": "string" } },
"events": [{
  "type": "OrderPlaced", "version": 2, "topic": "orders.placed",
  "fields": { "orderId": "string", "total": { "amount": "decimal", "currency": "string" } },
  "keys": { "Order": "orderId" },
  "effects": [{ "op": "assign", "attribute": "Order.total", "from": "total.amount" }]
}],
"intents": [{ "name": "order-total-changes", "predicate": "changes(Order.total)" }],
"consumers": [{ "group": "payments-service", "district": "payments", "intent": "order-total-changes", "dlq": "payments.dlq" }]
```

A consumer then declares an intent: what it cares about, as a typed predicate.

| Predicate | Means |
| --- | --- |
| `changes(Order.total)` | every event version with an effect on that attribute, from whichever topics |
| `emits(OrderPlaced)` | every version of that event type |
| `about(Order)` | every event that carries that entity's key |
| `and`, `or`, `not`, `( )` | combine them; `not` binds tightest, then `and`, then `or` |

An intent is compiled when the catalog loads. It fails on an entity, attribute or event type the catalog does not
have, and nothing is matched by guessing. A consumer that names an intent and lists no topics reads the topics its
intent draws on.

```sh
npx revenue-in-flight intents                          # each intent: what it matches, why, who reads by it
npx revenue-in-flight explain "changes(Order.payment)" # try a predicate without declaring it
npx revenue-in-flight impact OrderPlaced@v2            # who a version reaches, and who must change first
```

`impact` compares the catalog with a version against the catalog without it. For each intent the version newly
matches, it says how the version lands on that intent's consumers:

- **read the same way:** the fields they read are where they were. Nothing to do.
- **read differently:** a field they read has moved or changed type. They must change first, or it dead-letters.
- **a new type:** an event type they have never been sent.

It exits 1 when a consumer must change first, so it can gate a pull request that adds a version to the catalog.
The whole section is optional: a catalog with no `entities` is priced and watched exactly as before.

What this does not do yet: route events. intent-bus, which this model was ported from, materialises each intent
as a Kafka topic that a stock client subscribes to; here an intent is checked and analysed, and each consumer still
subscribes to its topics.

## Watch your own Kafka

```sh
BOOTSTRAP_SERVERS=broker-1:9092 npx revenue-in-flight watch my-flow.json
```

The catalog names your districts, the topic of each event type, the paths to its value, customer and order, and
each service's consumer group and dead-letter queue. The observer reads the following:

- **Events:** CloudEvents (one JSON object, or attributes in `ce_` headers), plain JSON, and JSON from a Schema
  Registry serializer.
- **Dead-letter headers:** its own, Kafka Connect's and Spring Kafka's.

A schema version the catalog doesn't list yet is priced from the newest listed version of its type that can read
it. A dead letter nothing can price still counts as stuck, with its value shown as unknown. A stuck order counts as
recovered in any of these cases:

- an event marks it with `redriveof`;
- the district that dead-lettered it produces an event for the same order;
- the queue's redrive group commits past it.

The observer only reads: it never joins or commits for your groups. [The guide](docs/guide.md#watch-your-own-kafka)
has the catalog format, the settings for SASL and TLS, and what isn't supported yet (Avro, Protobuf, more than one
currency).

## Drive it from an agent

```sh
claude mcp add revenue-in-flight -- npx revenue-in-flight mcp
```

The stdio server runs an observer of its own, so it needs Kafka and `simulate` running, but not `serve`. `serve` also
answers MCP over Streamable HTTP on `http://localhost:8088/mcp`. With `SIMULATOR_URL=none` (and `watch`), only the
read-only tools are offered.

| Tool | What it gives |
| --- | --- |
| `value_at_risk` | the headline: money, orders and customers held up, split into stuck and consumer lag, by district and group |
| `stuck_events` | the stuck dead letters, filterable by district, each with the error that stopped it |
| `last_episode` | the latest recovery: value, events, time from first stuck to last recovered |
| `intents` | each declared intent: the event versions it matches and why, their topics, what a consumer reads from each, who reads by it |
| `explain_intent` | compiles a predicate such as `changes(Order.total)` and shows what it would match |
| `impact` | what one event version reaches, and which consumers must change before it ships |
| `simulator_state` | traffic rate, which `OrderPlaced` version ships, whether Payments is patched, the last redrive |
| `break_schema`, `restore_schema`, `patch_payments`, `unpatch_payments`, `redrive`, `set_rate` | the view's controls, with the same 409 for a second redrive and 502 when the simulator is down |

The resource `rif://flow` describes the landscape: districts, topics, who produces and who reads each one.

A session goes like this:

> **You:** Break the schema, tell me what it costs, then fix it.
>
> **Agent** calls `impact` with `OrderPlaced@v2` first: *payments-service must change before this ships, or its
> events pile up in payments.dlq.* Then `break_schema`: *Orders now ship OrderPlaced v2. Unpatched payments dead-letters every one.*
> A little later, `value_at_risk`: *$2,914.37 at risk: 31 orders, 31 customers waiting (stuck $2,914.37, in lag
> $0.00).* And `stuck_events` with `district: payments`: each one `OrderPlaced@v2`, failing with
> *'total' is an object, expected a decimal string*.
>
> **Agent** calls `patch_payments`, then `redrive`, then `value_at_risk` until it reads *Nothing held up: $0.00 at
> risk.* Then `last_episode`: *Recovered $2,914.37: 31 orders, stuck 9.8 s.*

The figures are illustrative; the wording is what the tools return.

## How it works

| Folder | What it does |
| --- | --- |
| `catalog/order-to-cash.json` | the flow: districts, each event type's topic and where its value, customer and order are, who reads what, the dead-letter queues and the redrive group |
| `src/pricing` | loads and checks the catalog, reads and writes CloudEvents, decodes what other producers write, prices an event by its declared paths |
| `src/intents` | what the events mean and the intents declared over them: the predicate language, the compiler, and the impact of a new event version on each consumer |
| `src/simulator` | order traffic, the payments, inventory and shipping services with their dead-letter queues, the redrive, and the control API on 8090 |
| `src/observer` | reads every dead-letter queue from the start and every flow topic from the end, and keeps the ledger: stuck, lag, recovered |
| `src/server` | the observer over WebSocket, the controls passed to the simulator, the view, MCP over HTTP, on 8088 |
| `src/mcp` | the MCP server |
| `ui` | React, shadcn/ui and D3: the flow map (laid out from the catalog, SVG with particles on a canvas), money at risk over time, the runbook and the tables |

Money at risk is stuck money plus money in consumer lag, each order counted once at the most it is worth anywhere
it is held up. Lag is bounded by the end offset polled with each group's commit, so a healthy flow reads $0.
A redrive is one pass over the dead-letter queue in its own consumer group, up to the end it had when it started; the
first record that still fails stops its partition without moving its offset. It never re-publishes to the source
topic, so Inventory never sees an order twice. [docs/PLAN.md](docs/PLAN.md) has the design.

## Configuration

| Variable | Default | |
| --- | --- | --- |
| `BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka |
| `PORT` | `8088` | `serve` |
| `CONTROL_PORT` | `8090` | `simulate` |
| `SIMULATOR_URL` | `http://localhost:8090` | where `serve` and `mcp` find the simulator; `none` to watch with no simulator |
| `RIF_CATALOG` | the demo's | the catalog file, for `serve` and `mcp` (`watch` takes it as an argument) |
| `RIF_KAFKA_*` | none | librdkafka settings: `RIF_KAFKA_SECURITY_PROTOCOL=SASL_SSL` sets `security.protocol` |
| `HOST` | `localhost` | the interface `serve` and `simulate` listen on |
| `RIF_MCP_TOKEN` | none | a bearer token for MCP over HTTP; required when `HOST` is not localhost |

## Docker

```sh
docker build -t revenue-in-flight .
docker run --rm --network host revenue-in-flight simulate
docker run --rm --network host revenue-in-flight serve
```

The image listens on all interfaces, so MCP over HTTP stays off until `RIF_MCP_TOKEN` is set.

## Develop

```sh
npm test                 # unit tests
npm run lint && npm run typecheck
npm run dev:ui           # the view with hot reload, against serve on 8088
npm run test:e2e         # against Kafka in BOOTSTRAP_SERVERS: the client, simulator, observer, MCP, watch mode
npm run build && npm run test:browser   # Playwright: break and recover, details, theme, phone width
npm run docs:screenshots # rebuilds and retakes the guide's screenshots
```

The end-to-end tests start their own simulators, which join the catalog's consumer groups. A simulator refuses to
start while another one is attached to the same Kafka, so stop the demo before running them, or point them at
another Kafka. CI runs the unit tests on every push, and the end-to-end and browser tests on
Kafka in Compose for pull requests and `main`.

## License

Apache 2.0. See [LICENSE](LICENSE).
