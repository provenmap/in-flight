# Revenue in Flight

An event pipeline you see in money. Order-to-cash runs live from Kafka in 3D: Orders, Payments, Inventory and
Shipping are districts, and every event flies between them as a particle, brighter the more it is worth. The orders
team ships a new `OrderPlaced` shape, Payments cannot read it, and orders pile up red at Payments while a counter
runs: money stuck, customers waiting. Patch Payments, redrive its dead-letter queue, and the pile drains green into
Shipping while the counter falls to $0.

Nothing on screen is canned. The simulator runs real Kafka services with real consumer groups and a real
dead-letter queue, and the observer prices every event it reads from the topics.

An MCP server gives an agent the same view and the same controls: money at risk, the stuck events and their errors,
the last recovery, and the buttons.

## Run the demo

You need Docker and Node 22 or later.

```sh
npm install && npm run build
npx revenue-in-flight up         # Kafka 4.4 in Docker, KRaft, port 9092
npx revenue-in-flight simulate   # in a second shell: order traffic and the three services
npx revenue-in-flight serve      # in a third shell: then open http://localhost:8088
```

The package is not on npm yet. Once it is, `npx revenue-in-flight` works without the clone.

Then follow the hint under the counter:

1. **Ship OrderPlaced v2.** `total` becomes `{ amount, currency }`. Payments dead-letters every new order, the red
   pile grows, and money at risk climbs. Inventory reads only the items, so it keeps working.
2. **Patch payments.** New orders flow again, but the dead letters are still stuck.
3. **Redrive DLQ.** The stuck orders replay through the patched Payments, turn green and fly on to Shipping. The
   counter drains to $0 and the banner reports what was recovered and how long it was stuck.

`?view=2d` draws the same scene on a 2D canvas, which is also the fallback without WebGL. `npx revenue-in-flight
down` stops Kafka and clears its data.

## Drive it from an agent

```sh
claude mcp add revenue-in-flight -- npx revenue-in-flight mcp
```

The stdio server runs an observer of its own, so it needs Kafka and `simulate` running, but not `serve`. `serve` also
answers MCP over Streamable HTTP on `http://localhost:8088/mcp`.

| Tool | What it gives |
| --- | --- |
| `value_at_risk` | the headline: money, orders and customers held up, split into stuck and consumer lag, by district and group |
| `stuck_events` | the stuck dead letters, filterable by district, each with the error that stopped it |
| `last_episode` | the latest recovery: value, events, time from first stuck to last recovered |
| `simulator_state` | traffic rate, which `OrderPlaced` version ships, whether Payments is patched, the last redrive |
| `break_schema`, `restore_schema`, `patch_payments`, `unpatch_payments`, `redrive`, `set_rate` | the view's controls, with the same 409 for a second redrive and 502 when the simulator is down |

The resource `rif://flow` describes the landscape: districts, topics, who produces and who reads each one.

A session goes like this:

> **You:** Break the schema, tell me what it costs, then fix it.
>
> **Agent** calls `break_schema`: *Orders now ship OrderPlaced v2. Unpatched payments dead-letters every one.*
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
| `src/pricing` | loads and checks the catalog, reads and writes CloudEvents, prices an event by its declared paths |
| `src/simulator` | order traffic, the payments, inventory and shipping services with their dead-letter queues, the redrive, and the control API on 8090 |
| `src/observer` | reads every dead-letter queue from the start and every flow topic from the end, and keeps the ledger: stuck, lag, recovered |
| `src/server` | the observer over WebSocket, the controls passed to the simulator, the view, MCP over HTTP, on 8088 |
| `src/mcp` | the MCP server |
| `ui` | React and react-three-fiber: the 3D landscape, the 2D fallback, the counter and the controls |

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
| `SIMULATOR_URL` | `http://localhost:8090` | where `serve` and `mcp` find the simulator |
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
npm run test:e2e         # against Kafka in BOOTSTRAP_SERVERS: the client, simulator, observer, MCP
npm run build && npm run test:browser   # Playwright: 3D, 2D, phone width, break and recover
```

The end-to-end tests start their own simulators, which join the catalog's consumer groups. A simulator refuses to
start while another one is attached to the same Kafka, so stop the demo before running them, or point them at
another Kafka. CI runs the unit tests on every push, and the end-to-end and browser tests on
Kafka in Compose for pull requests and `main`.

## License

Apache 2.0. See [LICENSE](LICENSE).
