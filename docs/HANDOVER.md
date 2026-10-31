# Handover: revenue-in-flight

Written 2026-10-30 at the end of a session that ran in the intent-bus repo, and updated the same day after the
build moved from Scala to TypeScript ([REFACTOR.md](REFACTOR.md)). Start here, then read [PLAN.md](PLAN.md), which
holds the design.

## Where things stand

- **The idea.** `revenue-in-flight` in the showcase ledger, doc at
  `/Users/murali/source-others/pmap-showcase/data/ideas/revenue-in-flight.md`. The pitch: order-to-cash runs live
  in 3D, a schema change jams Payments with a running "money stuck" counter, a fix drains it. The view is now a 2D
  operations console (2026-10-31), at the user's request: the 3D one felt like an amateur demo.
- **Ledger stage:** still `chosen`. The doc header says `founded`, but the record does not.
- **GitHub:** no repo yet. `provenmap/revenue-in-flight` does not exist. Creating it is public and was
  deliberately left for the user to approve. Once created, record it with
  `showcase-ideas.js --link revenue-in-flight --repo … --fleet …` and `--stage revenue-in-flight founded`.
- **npm:** not published. `npm pack` was checked from a clean install; publishing is public, so it waits for the user.
- **This directory:** branch `port-typescript`. Commit `4e5cf21` holds the Scala build, `e039183` the TypeScript
  port. Not committed yet: the 2D console (2026-10-31), watch mode, and the guide with its screenshots
  ([guide.md](guide.md)). 66 unit tests, five end-to-end suites and the browser test pass.

| Path | State |
| --- | --- |
| `catalog/order-to-cash.json` | districts (customers is a `sink`), six event specs with value/customer/order paths, consumers with groups, DLQs and the payments redrive group. Unchanged |
| `src/kafka.ts` | a promise layer over the Confluent client's librdkafka API: `Consumer` (assign, subscribe, poll, pause, exact commits, committed, watermarks), `Writer`, `Admin` |
| `src/pricing/` | `catalog.ts` (zod schema, same error messages, cross-checks, optional `deadLetterHeaders`), `cloudevents.ts` (build, parse, `redriveof`, dead-letter header names), `records.ts` (decodes structured and binary CloudEvents, plain and Schema Registry JSON), `pricing.ts` (by type, or by topic, and undeclared versions) |
| `src/simulator/` | `orders.ts` (traffic, 2,000 customers, weighted shop range), `services.ts` (payments, inventory, shipping join), `events.ts` (JVM hashes, bit for bit), `loop.ts` (service loop with DLQ), `redrive.ts`, `main.ts` (control API on Fastify, 8090) |
| `src/observer/` | `ledger.ts` (pure accounting), `reader.ts` and `poller.ts` (Kafka, Admin offsets, simulator state), `observer.ts` (the three together) |
| `src/server/` | Fastify on 8088: WebSocket hub, simulator proxy, the built view, MCP over HTTP |
| `src/mcp/` | the MCP server (`server.ts`) and its HTTP guard (`http.ts`) |
| `src/cli.ts` | `up`, `simulate`, `serve`, `watch [catalog]`, `mcp`, `down` |
| `ui/` | React + Vite + shadcn/ui: `model.ts` (scene, history, rates, recent events), `useStream.ts`, `timeline.ts`, `map/` (geometry, district facts, `FlowMap.tsx`), `panels/` (top bar, headline and chart, runbook, activity, data tabs, district sheet), `components/ui/` (shadcn) |
| `test/e2e/` | against Kafka: the client, the simulator, the observer, the MCP server over stdio and HTTP |
| `test/browser/` | Playwright: break and recover, district details and tabs, theme, phone width |
| `test/docs/` | the guide's screenshots, `npm run docs:screenshots`, with `playwright.docs.config.ts` |

### What was verified on the TypeScript build

Break, then 11 s of v2 orders: 91 orders dead-lettered at payments, none at inventory. A redrive before the patch
stopped at the first unreadable record and committed nothing. Patch, then redrive, recovered all 91 and left the
redrive group at the end of every partition. A restarted observer counted every dead letter as recovered and $0
stuck. At 500 orders a second for 5 s, a viewer saw all 2,550 orders. The old plain-module view ran unchanged
against the new server, then the new UI passed the same browser test. The react-three-fiber scene was compared
with the wrapped three.js scene during a jam and mid-recovery: the same picture, and labels in the same pixels.

### The view

An operations console, rebuilt on 2026-10-31 to replace the 3D scene, with standard shadcn/ui components, SVG and D3.

- **Design:** colour is state. Ink blue is money moving, banknote green recovered, ochre waiting in lag, seal red
  stuck; tokens are in `ui/src/styles.css`, light and dark, following the system with a toggle kept in
  `localStorage` as `rif.theme`. IBM Plex Sans for the interface, Plex Sans Condensed with tabular figures for money.
- **The flow map** (`ui/src/map/`): laid out from the catalog (`layoutOf` in `geometry.ts`), districts left to right as money moves. Each edge is as thick as the money
  crossing it a minute (`model.rate()`, over 30 s). Nodes, edges, labels and the dead-letter tank are SVG; particles
  and tank blocks are drawn on a canvas between two SVG layers, so thousands stay smooth and labels stay readable.
  Select a district for its detail panel.
- **The tank:** one is drawn, over the district holding the most dead letters (with none, the first queue with a
  redrive group). Dead letters rise from Payments into `payments.dlq` above it. On recovery they turn green in a wave,
  15 ms apart (3 ms once the wave runs past 5 s), and drop back into Payments; each redriven particle leaves as its
  block turns green.
- **Money at risk:** the counter is the observer's money at risk plus recoveries the latest stats already counted but
  the wave has not played yet, so it drains in step with the tank. The frame loop writes it directly; React
  re-renders only on snapshots and stats. Beside it, its history over 1, 5 or 15 minutes, stuck under lag.
- **Runbook:** incident status, the three steps with their controls, order traffic, and the hint. In watch mode the
  Watch panel (`panels/Watch.tsx`) takes its place: each queue, what it holds, and what will count as recovery.
- **District cards** (`map/facts.ts`): the demo's own words when a simulator is attached, generic ones otherwise.
- **Activity** (`ui/src/timeline.ts`): changes worked out from one stats message to the next, since the page opened.
- **Tabs:** dead letters (search, newest or largest, CSV export), consumer groups, live events (pausable), and
  throughput by topic.
- **For tests:** `window.rif` exposes `model`, `view` (`map`), `frames`, `particles` and `blocks`. `data-testid`s:
  `at-risk`, `at-risk-sub`, `stuck`, `lag`, `recovered`, `hint`, `break`, `patch`, `redrive`, `rate`, `sim-state`,
  `connection`, and `activity`, `dead-letters`, `groups`, `live-events`, `throughput`.
- **Editing it:** `npm run dev:ui` serves it with hot reload on Vite's port and passes `/ws`, `/api` and `/mcp` to
  `serve` on 8088. `npm run build` writes `dist/ui`, which `serve` serves.

### The observer's WebSocket protocol

Unchanged from the Scala build, message for message. `ws://localhost:8088/ws`. Every frame is a JSON **array** of
messages. The first frame holds one `snapshot`. Timestamps are ISO 8601 with milliseconds.

- `snapshot`: `mode` (`demo` or `watch`), `flow` (`districts[]` of `{id, name, sink}`, `topics[]` of
  `{topic, from, to[], events[]}`, `dlqs[]` of `{dlq, district, group, redrive}`, `groups[]` of `{group, district}`),
  `stuck[]` (the `dead` messages still stuck, capped at 3,000), `stats`.
- `event`: `{id, event, topic, district, to[], value, customer, orderId, redrive}`. `to` is empty for
  `payments.declined`, and `["customers"]` for dispatches.
- `dead`: `{id, dlq, district, event, value, priced, customer, orderId, error, at}`. `district` is the service that
  dead-lettered it (payments). `priced` is false for one the catalog cannot price: value 0 means unknown there.
- `recovered`: `{id, district, value, afterMs}`. `afterMs` is null for one found already redriven at start-up.
- `stats`, every 500 ms: `atRisk {value, orders, customers, unpriced}` (the headline), `stuck {value, events,
  customers, unpriced, since, byDistrict}`, `lag {value, events, byGroup}`, `recovered {value, events}`, `lastEpisode {value, events,
  tookMs}` or null, `unpriced`, and `sim` (the simulator's `/state`, or null when it is down).

Controls: `POST /api/sim/{break|restore|patch|unpatch|redrive}` and `POST /api/sim/rate?perSecond=N` (0 to 500).
The simulator answers 409 to a second redrive while one runs, and the server answers 502 when the simulator is down.

### The MCP server

Tools `value_at_risk`, `stuck_events`, `last_episode`, `simulator_state`, `break_schema`, `restore_schema`,
`patch_payments`, `unpatch_payments`, `redrive`, `set_rate`, and the resource `rif://flow`. Each tool answers with a
line to read and the same facts as `structuredContent`; the controls fail with the simulator's 409 and the 502.
With no simulator (`SIMULATOR_URL=none`) the simulator tools are not registered at all.

- **stdio:** `revenue-in-flight mcp` runs an observer of its own, so it needs Kafka and the simulator but not
  `serve`. Start it before a break if `last_episode` should see the recovery: episodes are seen live, not rebuilt.
- **HTTP:** `POST /mcp` on `serve`, stateless. On localhost it needs no token but refuses other hosts and origins
  (DNS rebinding). Listening beyond localhost (`HOST`), it is off until `RIF_MCP_TOKEN` is set, then needs it as a
  bearer token.

### Running it

```sh
npm install && npm run build
npx revenue-in-flight up         # Kafka in Compose, port 9092
npx revenue-in-flight simulate   # in another shell: control API on 8090
npx revenue-in-flight serve      # in another shell: open http://localhost:8088
npx revenue-in-flight down       # clears Kafka
npx revenue-in-flight watch my-flow.json   # a flow of one's own, read-only
```

Environment: `BOOTSTRAP_SERVERS`, `PORT`, `CONTROL_PORT`, `SIMULATOR_URL` (`none` to watch), `HOST`,
`RIF_MCP_TOKEN`, `RIF_UI_DIR`, `RIF_CATALOG`, `RIF_KAFKA_*` (librdkafka settings).

Tests: `npm test` (units), `npm run test:e2e` and `npm run test:browser` (need Kafka in `BOOTSTRAP_SERVERS`, and
`npm run build` first for the browser). The end-to-end tests start their own simulators on ports 28090 to 48090,
using the catalog's consumer groups. A simulator refuses to start while any service group already has members, so a
test run cannot split partitions with a running demo or drain its dead letters: stop the demo first.

## Decisions already made

- **Scope of this build:** milestones 1 to 3, the simulator, the live view and money at risk. Milestone 4, the agent
  that heals and proves, comes later. Until then "patch payments" and "redrive" are buttons and MCP tools.
- **Stack:** TypeScript on Node 22 (strict, ESM), `@confluentinc/kafka-javascript` through its librdkafka-style API,
  Fastify 5, zod 4, big.js for money, the MCP TypeScript SDK, React 19 + Vite 8, shadcn/ui on Radix, D3 (scale,
  shape, array), Tailwind 4, IBM Plex, TanStack Query for the controls, Vitest and Playwright. Dependencies
  were installed with `npm --before` two weeks back. TypeScript is 6.0, because typescript-eslint does not support 7.
- **Ports:** Kafka 9092, the server and the view on 8088, the simulator's control API on 8090. The view calls the
  simulator through the server, which avoids CORS. Both listen on localhost unless `HOST` says otherwise.
- **The jam:** `OrderPlaced` v2 makes `total` an object `{ amount, currency }`. Payments reads `total` and
  dead-letters. Inventory reads only items and keeps working. Exactly one district jams.
- **Redrive without duplicates:** the patched payments service reads `payments.dlq` with its own consumer group.
  It does not re-publish to `orders.placed`, which inventory would read twice. A redriven success carries the original
  event id, so the observer can mark it recovered.
- **Values are declared, never guessed:** each event type names the path to its value and its customer in the
  catalog. Money is decimal (big.js), never floating point, until it goes on the wire.
- **Restarts:** the catalog names each DLQ's redrive consumer group. On start-up, the observer counts dead letters
  below that group's committed offset as already recovered, because it cannot see the redriven events again from the
  end of the flow topics. `PLAN.md` records it under Money at risk.
- **Lag is bounded by the polled end:** an event newer than the end offset polled with the group's commit is in
  flight, not lag. That keeps the value and the count consistent, and the healthy headline at $0.
- **Watch mode is read-only:** no controls in the view, none in MCP, and the observer never joins or commits for
  the flow's groups. A dead letter it cannot price is stuck with its value unknown, never $0: the incident this tool
  exists for is a schema change nobody declared yet.
- **Redrive semantics:** one pass, up to the end offsets at its start, in group `payments-service-redrive`, with
  `assign` and no rebalance. The first record that still fails pauses its partition and leaves its offset
  uncommitted. The decline decision is Scala's `MurmurHash3.stringHash` of the order id, ported bit for bit, so a
  redrive decides the same way, even for dead letters the Scala build wrote.

## Next steps, in order

1. **Commit the console, watch mode and the guide** (the user's call), then the GitHub repo and an npm publish once
   the user approves them.
2. **Watch mode, next:** Avro and Protobuf through Schema Registry, more than one currency, and trying it on a real
   pipeline.
3. **Milestone 4 on the MCP server:** `branch_and_replay`, `replay_result` and `open_fix_pr`, as REFACTOR.md lists.

## What to reuse from intent-bus

In `/Users/murali/source-others/public-repos/intent-bus`, on branch `build/milestones-1-3`: the order-to-cash event
shapes in `demo/catalog/rev1.json`, which this flow extends. Its Scala helpers were ported here and are no longer
needed.

## Gotchas learned the hard way

- **The Kafka client's two APIs:** the KafkaJS-style consumer cannot assign partitions; use `src/kafka.ts`. Its
  `consume(n)` waits the poll timeout for every record unless told to wait only for the first, which `Consumer`
  does. `commitCb` (an async commit that resolves when the broker has it) exists but is missing from the typings.
- **Lag in tests:** a simulator sharing an event loop with the observer and the test lags its own commits, so
  the healthy flow shows one order in lag. The observer test runs the simulator in a process of its own, as in
  production.
- **One simulator per Kafka:** two would split the services' partitions, and one's redrive would drain the other's
  dead letters, so `startSimulator` refuses while a service group has members. To test beside a running demo, use a
  second broker: the port used a throwaway `apache/kafka:4.4.0` container with the Compose file's settings and the
  external listener moved to 9192.
- **Stop services with a signal, not a kill:** a killed service never leaves its consumer group, and the next one
  waits out the 45 s session timeout before it gets partitions. The CLI closes cleanly on SIGINT and SIGTERM, and the
  Playwright config stops its servers with SIGINT.
- **Docker Desktop host networking** carries traffic from a container to the host, not from the host into a
  container on host networking. Probe such a container from another one on the host network.
- **Playwright MCP** writes snapshots and screenshots into `.playwright-mcp/` in the workspace root, and can only
  save files under the workspace. It is ignored here.
- **Images in the VS Code panel:** past about 20 images in one session the panel freezes. Prefer
  `browser_snapshot` and `browser_evaluate` over screenshots, and look at only the ones that matter. WebGL renders
  in headless Chromium with SwiftShader.
- **Kafka CLI from the host:** `docker run --rm --network host apache/kafka:4.4.0 /opt/kafka/bin/…` works,
  because Docker host networking is on.

## Left open in intent-bus

intent-bus is parked at milestone 3, with commit `e97f0f3` on `build/milestones-1-3`. Two changes there are
uncommitted: `demo/ib` finds JDK 21, and `docker-compose.yml` adds the Apicurio UI on port 8888. A known bug is not
fixed yet. An intent declared at runtime misses events that already exist, because all intents share one
Streams application. The fix is one application id per intent.
