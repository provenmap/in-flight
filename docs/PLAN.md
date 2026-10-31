# revenue-in-flight: implementation plan

Status: milestones 1 to 3 are built, first in Scala and since 2026-10-30 in TypeScript on Node (see
[REFACTOR.md](REFACTOR.md)). On 2026-10-31 the observer learned to watch a flow of one's own, with no simulator
(Watch mode, below). Milestone 4, the agent that heals and proves, comes next, on the MCP server.

Stack: TypeScript on Node 22, Confluent's Kafka client (librdkafka), Fastify, React + Vite with shadcn/ui, Tailwind
and D3, an MCP server over stdio and Streamable HTTP, one npm package with a CLI. The 3D view of the first build was
replaced on 2026-10-31 by a 2D operations console.

## The story the build has to tell

Order-to-cash runs live. Orders, Payments, Inventory and Shipping are districts, and every event flows between
them as a particle, brighter the more it is worth. The orders team ships a new `OrderPlaced` shape. Payments
cannot read it, so orders pile up red at Payments and a counter runs: money stuck, customers waiting. Payments is
patched, its dead-letter queue is redriven, the pile drains, and the counter turns into money recovered.

Everything on screen comes from a real Kafka. Nothing is canned animation.

## Pieces

| Folder | What it does |
| --- | --- |
| `catalog/` and `src/pricing` | The event catalog: each event type's topic, district, and where its value and customer are. Shared CloudEvents helpers |
| `src/simulator` | Order traffic plus three real Kafka services: payments, inventory, shipping. Each service dead-letters what it cannot read. A control API breaks the schema, patches payments and redrives its dead-letter queue |
| `src/observer` | Reads every topic and dead-letter queue, prices each event from the catalog, tracks money stuck and money in consumer lag |
| `src/server` | Streams the observer over WebSocket, passes controls to the simulator, serves the view and MCP over HTTP |
| `src/mcp` | The MCP server: what the view shows and the controls it has, as tools and a resource |
| `ui/` | The operations console: the flow map, money at risk over time, the runbook, activity and the detail tables |

## The flow

| Event | Topic | From | Read by | Value | Customer |
| --- | --- | --- | --- | --- | --- |
| `OrderPlaced` v1 | `orders.placed` | Orders | payments-service, inventory-service | `total` | `customerId` |
| `OrderPlaced` v2 | `orders.placed` | Orders | same | `total.amount` | `customerId` |
| `PaymentAuthorized` | `payments.authorized` | Payments | shipping-service | `amount` | `customerId` |
| `PaymentDeclined` | `payments.declined` | Payments | nobody | `amount` | `customerId` |
| `StockReserved` | `inventory.reserved` | Inventory | shipping-service | `value` | `customerId` |
| `OrderDispatched` | `shipping.dispatched` | Shipping | the customer | `orderTotal` | `customerId` |

Values are declared per event type, as a path into the event. They are never guessed.

The breaking change: `OrderPlaced` v2 turns `total` from a decimal string into `{ amount, currency }`.
Payments reads `total`, so it fails and dead-letters. Inventory reads only the items, so it keeps working. One
schema change, one jam, in one district, which is what the view has to make obvious.

## Money at risk

- **Stuck:** the value of every dead-lettered event not yet recovered, and its distinct customers.
- **In lag:** for each consumer group, the value of events between its committed offset and the end of the log.
- **Recovered:** a redriven event that succeeds carries the original event id. The observer marks it recovered and
  the counter reports the value and the time since the first event got stuck.
- **Restarts:** the observer reads the dead-letter queues from the start but the flow topics from the end, so it
  cannot rely on seeing the redriven events again. The catalog names each queue's redrive consumer group, and that
  group's committed offsets are the record of what was dealt with: on start, a dead letter below the committed offset
  counts as already recovered, with no time to recover. While running, `redriveof` marks recovery as it happens.

The headline counter is stuck plus lag, with customers counted once.

## The view

- A flow map, left to right as money moves: Orders, then Payments and Inventory, then Shipping, then Customers.
  Each edge is as thick as the money crossing it a minute, so a jam shows as money going into Payments and not out.
- Each event is a particle on the edge from its district to each district that reads it, sized by value.
- A dead-lettered event becomes a red block in the dead-letter tank above Payments. A recovered block turns green
  and drops back into Payments, and its redriven event flies on to Shipping.
- Money at risk heads the page, with its split into stuck and lag and its history. An incident runbook holds the
  controls: ship v2, patch payments, redrive, and traffic.
- Around them: an activity timeline, every stuck order with its error, consumer groups, the live event stream,
  throughput by topic, and a detail panel for each district.
- Colour is state: ink blue moving, green recovered, ochre waiting in lag, red stuck. Light and dark themes.

## Watch mode

`revenue-in-flight watch my-flow.json`, or `SIMULATOR_URL=none`: the observer, the view and the MCP server on a
catalog of one's own and a Kafka of one's own, read-only, with no controls. What it takes to be useful on a real
pipeline, and how each is met:

- **Formats.** Structured and binary CloudEvents, plain JSON, and Schema Registry framed JSON (`src/pricing/records.ts`).
  A record that does not name its type and version takes the catalog's types for its topic, newest first, the first
  whose declared paths resolve. Still a declared path, never a guessed value.
- **Schema changes nobody declared.** A version missing from the catalog is priced by the newest declared version
  of its type that reads it. A dead letter nothing can price is still stuck, counted with its value unknown, and
  recovered when its queue's redrive group commits past it.
- **Dead-letter conventions.** This app's headers, Kafka Connect's and Spring Kafka's, or names the catalog gives.
- **Recovery without `redriveof`.** The district that dead-lettered an order producing for that order again.
- **Secured clusters.** `RIF_KAFKA_*` environment variables pass any librdkafka setting.
- **Any shape of flow.** The map lays itself out from the catalog: longest path from a source, sinks last, a tank
  over the district holding the most dead letters.

Not yet: Avro and Protobuf, several currencies, the value of lag for events from before the observer started, and
sampling for clusters busier than one process can read.

## Checks

- Unit tests for value extraction, the services' decoders, the shipping join, the observer's accounting and the
  MCP tools.
- End-to-end tests against Kafka for the simulator, the observer and the MCP server, over stdio and HTTP.
- A browser test with Playwright: the page renders, particles flow, breaking the schema raises the counter,
  patching and redriving drains it.
- An end-to-end test of watch mode: plain JSON, Kafka Connect dead letters, recovery by order, an unpriceable dead
  letter recovered by its redrive group's commit.
- The guide's screenshots come from a Playwright script on the real thing, `npm run docs:screenshots`.

## Not in this build

- Milestone 4: the agent that branches the topic, replays the patched consumer, diffs the output and opens the PR.
  Patch and redrive are buttons until then.
- Whether a live business-flow view belongs in ProvenMap itself is still open, as the idea's risk list says.
