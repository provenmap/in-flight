# revenue-in-flight: implementation plan

Status: milestones 1 to 3 are built, first in Scala and since 2026-10-30 in TypeScript on Node (see
[REFACTOR.md](REFACTOR.md)). Milestone 4, the agent that heals and proves, comes next, on the MCP server.

Stack: TypeScript on Node 22, Confluent's Kafka client (librdkafka), Fastify, React + Vite with react-three-fiber,
Tailwind and shadcn/ui, an MCP server over stdio and Streamable HTTP, one npm package with a CLI.

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
| `ui/` | The 3D landscape in react-three-fiber, and a 2D fallback on a canvas |

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

- Districts are platforms in a diamond, Orders on the left and Shipping on the right, with customers beyond.
- Each event is a particle on the arc from its district to each district that reads it. Size and brightness
  scale with value.
- A dead-lettered event becomes a red block on a pile beside its district. A recovered block turns green and
  rejoins the flow.
- The counter sits above the scene. Controls below: break the schema, patch payments, redrive, restore,
  traffic rate.
- Without WebGL, or on request, the same scene is drawn top-down on a 2D canvas.

## Checks

- Unit tests for value extraction, the services' decoders, the shipping join, the observer's accounting and the
  MCP tools.
- End-to-end tests against Kafka for the simulator, the observer and the MCP server, over stdio and HTTP.
- A browser test with Playwright: the page renders, particles flow, breaking the schema raises the counter,
  patching and redriving drains it.

## Not in this build

- Milestone 4: the agent that branches the topic, replays the patched consumer, diffs the output and opens the PR.
  Patch and redrive are buttons until then.
- Whether a live business-flow view belongs in ProvenMap itself is still open, as the idea's risk list says.
