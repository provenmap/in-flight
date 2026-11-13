# revenue-in-flight: refactoring plan, Scala to TypeScript

Status: done 2026-10-30, and superseded on 2026-11-13 by the move to Java ([MERGE.md](MERGE.md)): the TypeScript backend it describes is deleted. Phases 0 to 6 are complete, every parity check below holds, and the Scala build is deleted.
[PLAN.md](PLAN.md) holds the design and [HANDOVER.md](HANDOVER.md) the state of the build; this file says how the
build moved to the agreed stack without losing anything it already did, and, under "How it went", where the port
differed from this plan.

## Why

The first build chose Scala 3 and sbt to match intent-bus. The same day, the showcase settled its stack for founded
apps (`pmap-showcase/.claude/skills/showcase-fleet/references/reinventing.md`):

- **TypeScript on Node by default.** Another language only when the core needs a runtime API only it has.
  revenue-in-flight needs none: it produces, consumes, seeks, pauses and reads offsets, all of which Node's Kafka
  clients do. It uses no Kafka Streams. intent-bus stays on the JVM because its router does.
- **An MCP server in every app**, with the same capabilities as the UI, over stdio and Streamable HTTP.
- **Fastify** for the backend, **React + Vite** for the UI, **Three.js through react-three-fiber** for 3D.
- **One npm package**: `npx revenue-in-flight serve | mcp | simulate`, plus a container image.
- **One end-to-end test per milestone**, green in GitHub Actions.

The port is small (about 1,500 lines of Scala and 1,000 of web code) and the design does not change, so it is
cheaper now than after milestone 4.

## What must not change

The port is a refactor: the behaviour below is the contract, and the parity checks at the end prove it.

- **The catalog file**, `catalog/order-to-cash.json`, as is: districts, event specs with value, customer and order
  paths, consumers, DLQs, redrive groups.
- **The flow and the jam.** Six event types on the same topics; `OrderPlaced` v2 turns `total` into
  `{ amount, currency }`; payments dead-letters it, inventory keeps working.
- **The accounting** in PLAN.md's "Money at risk": stuck, lag bounded by the polled end offset, recovered via
  `redriveof`, and restarts that count dead letters below the redrive group's committed offset as already recovered.
- **Redrive semantics**: one pass up to the end offsets at its start, group `payments-service-redrive`, `assign`
  with no rebalance, pause the partition at the first record that still fails and leave its offset uncommitted; the
  decline decision is a hash of the order id.
- **The WebSocket protocol**, message for message, as HANDOVER.md documents it (`snapshot`, `event`, `dead`,
  `recovered`, `stats`, frames as arrays). Keeping it exact lets the old web view run against the new server, which
  is the cheapest parity test there is.
- **The control API**: `POST /api/sim/{break|restore|patch|unpatch|redrive}`, `POST /api/sim/rate?perSecond=N`,
  the 409 on a second redrive and the 502 when the simulator is down.
- **The test hooks**: `window.rif` and every `data-testid` HUD element and control.
- **Ports**: Kafka 9092, the app on 8088, the simulator's control API on 8090.

## The target layout

One package; one folder per board component, so the code reads like the board. The product (pricing, observer,
server, MCP, UI) and the demo system it watches (the simulator) live side by side but run as separate processes,
as now.

```
package.json            bin: revenue-in-flight → dist/cli.js
catalog/order-to-cash.json
src/
  pricing/              catalog load and validation, value and customer extraction, CloudEvents
  simulator/            orders, payments/inventory/shipping services, the DLQ loop, redrive, control API (8090)
  observer/             ledger (pure accounting), reader and poller (Kafka, offsets, simulator state)
  server/               Fastify on 8088: REST, WebSocket, simulator proxy, static UI, MCP over HTTP
  mcp/                  the MCP server: tools and resources over the same services
  cli/                  serve · mcp · simulate · up · down
ui/                     React + Vite, built into dist/ui
test/e2e/               one end-to-end test per milestone, against the Compose Kafka
docker-compose.yml      unchanged: Kafka 4.4.0 in KRaft mode
Dockerfile
```

| Concern | Choice |
| --- | --- |
| Runtime | Node 22 LTS, TypeScript strict, ESM |
| Kafka | Confluent's JavaScript client (`@confluentinc/kafka-javascript`, librdkafka). Not KafkaJS, which is unmaintained. Phase 0 proves it covers what we use |
| HTTP and WebSocket | Fastify, `@fastify/websocket`, `@fastify/static` |
| MCP | `@modelcontextprotocol/sdk`: stdio, and Streamable HTTP on `/mcp` bound to localhost, bearer token (`RIF_MCP_TOKEN`) beyond |
| Validation | zod, for the catalog file and the MCP tools' inputs |
| UI | React, Vite, react-three-fiber and drei, Tailwind, shadcn/ui for the HUD and controls, TanStack Query for REST |
| Tests | Vitest for units, Playwright for the browser, an MCP client in the end-to-end tests |
| CI | GitHub Actions: lint, type check and units on every push; end-to-end with Compose on pull requests and `main` |

## File by file

| Scala today | TypeScript target | Notes |
| --- | --- | --- |
| `catalog/.../Catalog.scala` | `src/pricing/catalog.ts` | zod schema replaces the hand validation; same error messages |
| `catalog/.../Pricing.scala` | `src/pricing/pricing.ts` | value and customer by declared path, never guessed |
| `catalog/.../CloudEvents.scala` | `src/pricing/cloudevents.ts` | build, parse, `redriveof`, dead-letter header names |
| `simulator/.../Orders.scala`, `Events.scala` | `src/simulator/orders.ts`, `events.ts` | 2,000 customers, the weighted shop range, v1 and v2 shapes |
| `simulator/.../Services.scala` | `src/simulator/services.ts` | payments, inventory, the shipping join; decoders unchanged |
| `simulator/.../Kafka.scala` | `src/simulator/loop.ts` | the service loop with its DLQ |
| `simulator/.../Redrive.scala` | `src/simulator/redrive.ts` | the semantics above, exactly |
| `simulator/.../Main.scala` | `src/simulator/main.ts` | control API on Fastify, port 8090 |
| `observer/.../Ledger.scala` | `src/observer/ledger.ts` | pure; port first, with its tests |
| `observer/.../Reader.scala` | `src/observer/reader.ts`, `poller.ts` | consumers, Admin offsets, simulator state |
| `observer/.../Main.scala` (Javalin) | `src/server/*.ts` | Fastify; `Broadcaster` becomes a small WebSocket hub |
| `web/model.js` | `ui/src/model.ts` | scene as data stays framework-free; typed, otherwise as is |
| `web/app.js` | `ui/src/App.tsx`, `hud/*`, `useStream.ts` | HUD and controls as components; the WebSocket in one hook |
| `web/view3d.js` | `ui/src/view3d/*` | react-three-fiber, in two steps: see phase 5 |
| `web/view2d.js` | `ui/src/View2D.tsx` | the canvas drawing kept, mounted in a component |
| `web/labels.js`, `style.css` | components, Tailwind | |
| `*Suite.scala` (munit) | `src/**/*.test.ts` (Vitest) | ported test by test before the code they test: they are the spec |

## The MCP server

Parity with the UI, on the same services, never by driving the UI.

| Kind | Name | What it gives |
| --- | --- | --- |
| tool | `value_at_risk` | the headline: money, orders and customers at risk; stuck and lag split; by district and group |
| tool | `stuck_events` | the stuck dead letters, filterable by district, with each one's error |
| tool | `last_episode` | the latest recovery: value, events, time to recover |
| resource | `flow` | districts, topics, who reads what: the landscape as data |
| tool | `simulator_state` | traffic rate, broken or not, patched or not, redrive running |
| tool | `break_schema`, `restore_schema`, `patch_payments`, `unpatch_payments`, `redrive`, `set_rate` | the UI's controls; the same 409 and 502 rules |

Milestone 4 adds `branch_and_replay`, `replay_result` and `open_fix_pr`, on this server, after the port.

## Phases

Each phase ends green and runnable. The Scala build keeps working until phase 6, so there is always a working demo.

**0. Scaffold and prove the Kafka client** (the one real risk).
`package.json`, strict `tsconfig`, ESLint, Vitest, the CI skeleton. A spike against the Compose Kafka shows that
`@confluentinc/kafka-javascript` can do each of: manual `assign` without a group rebalance, `seek`, `pause` and
`resume` per partition, commit a chosen offset, read record headers, and through Admin read a group's committed
offsets and a topic's end offsets. Any gap stops the port here for a decision; the fallback is the client's
lower-level, node-rdkafka-style API, not KafkaJS.

**1. Pricing.** Port `CatalogSuite` and `PricingSuite`, then the code. Loads `catalog/order-to-cash.json` unchanged.

**2. Simulator.** Port `ServicesSuite`, then orders, services, the DLQ loop, redrive and the control API. Re-run
HANDOVER.md's live check: break for 11 s; a redrive before the patch stops at the first unreadable record and
moves no offsets; patch plus redrive recovers every stuck order.

**3. Observer and server.** Port `LedgerSuite`, then the ledger, reader, poller and the Fastify server. Parity test:
the **old** web view, served by the new server, behaves as before. The observer-restart check from HANDOVER.md
($0 stuck, every dead letter recovered) passes.

**4. MCP.** The table above, over stdio (`npx revenue-in-flight mcp`) and HTTP on `/mcp`. End-to-end test: an MCP
client breaks the schema, waits, reads `value_at_risk` above zero, patches, redrives, and reads it back to $0 with
`last_episode` matching.

**5. UI.** Vite + React shell with the HUD and controls first, `model.ts` and the 2D view mounted as they are.
Then the 3D view in two steps: wrap the existing three.js scene in a component so it runs unchanged, then move it
to react-three-fiber piece by piece (districts, arcs, instanced particles, piles, bloom), checking each against the
wrapped version. The Playwright test planned as HANDOVER.md's step 6 is written here, against the new UI: renders,
particles flow, break raises the counter, patch and redrive drain it, phone width without horizontal scroll.

**6. Package, document, retire Scala.**
- The CLI replaces the driver script planned as HANDOVER.md's step 5: `up` and `down` wrap Compose, `serve` runs
  the app with the built UI on 8088, `simulate` runs the simulator, `mcp` runs the stdio server.
- Dockerfile; the README with the demo, `claude mcp add revenue-in-flight -- npx revenue-in-flight mcp`, and an
  agent session driving it.
- CI runs the end-to-end tests for milestones 1 to 3 and the MCP test on Compose.
- Delete `build.sbt`, `project/`, `catalog/src`, `simulator/src`, `observer/src` and every `target/`. Update PLAN.md's
  stack lines and HANDOVER.md.

Then milestone 4 on the new stack.

## Parity checks

The port is done when all of these hold on the TypeScript build, and only then is Scala deleted.

- [x] Every ported Vitest test passes, with no Scala test left untranslated. All 46 Scala tests are ported, one
  renamed (there is no classpath copy of the catalog any more); 55 unit tests in all.
- [x] Break, 11 s of v2 orders: stuck money and orders counted, inventory still working. Checked by hand (91 orders
  dead-lettered, inventory none) and in `test/e2e/simulator.test.ts` and `observer.test.ts`.
- [x] A redrive before the patch moves no offsets and stops at the first unreadable record (`simulator.test.ts`).
- [x] Patch, then redrive: every stuck order recovered, the counter at $0, the episode banner with value and time
  (`observer.test.ts`, the browser test).
- [x] Observer restart after a recovery: $0 stuck, every dead letter counted as recovered (`observer.test.ts`).
- [x] A healthy flow reads $0 at risk (lag bounded by the polled end) (`observer.test.ts`).
- [x] The old web view works against the new server unchanged (phase 3): the browser test passed on it before the
  new UI replaced it.
- [x] The new UI: 3D, 2D and phone width, no console errors, `window.rif` and every `data-testid` present
  (`test/browser/view.spec.ts`).
- [x] The MCP end-to-end test passes over stdio and HTTP (`test/e2e/mcp.test.ts`).
- [x] `npx revenue-in-flight serve` works from a clean machine with only Docker and Node: the `npm pack` tarball,
  installed into an empty folder, ran `up`, `simulate`, `serve` and `mcp`, and passed the browser test. The package
  is not on npm yet.

## Risks

- **The Kafka client.** Redrive needs assign, pause and exact commits; the observer needs Admin offsets. Phase 0
  exists to find a gap before anything depends on it.
- **The 3D view's look.** Bloom and the recovery wave are tuned by hand. Wrapping first and converting piece by
  piece keeps a working reference to compare against.
- **Throughput on one Node thread.** The simulator runs up to 500 events a second and the observer broadcasts every
  one. Batching into frames, as the protocol already does, should hold; phase 3 measures it at 500/s.
- **Scope creep.** No new features during the port. Milestone 4 waits until the parity checks pass.

## How it went

Where the port differed from the plan, and what was measured.

- **The Kafka client.** Phase 0 found the gap the plan feared: the client's KafkaJS-style consumer can only
  subscribe, not assign. Everything uses the librdkafka-style API instead, the plan's fallback, behind a small
  promise layer in `src/kafka.ts`. It assigns partitions at chosen offsets rather than seeking, which covers seek.
  Its `consume(n)` waits the poll timeout for every record by default, so a steady trickle held a batch open until
  it was full; it is set to wait only for the first. `test/e2e/kafka-client.test.ts` keeps proving each capability.
- **Decisions match the Scala build.** The decline decision ports Scala's `MurmurHash3.stringHash` and the carrier
  Java's `String.hashCode`, bit for bit, checked against values from the JVM, so a redrive of dead letters written by
  the Scala build decides as it did.
- **Throughput.** At 500 orders a second for 5 s, a viewer of the observer saw all 2,550 orders placed, with stats
  every 500 ms. Payments, charging one order at a time, builds real lag, as before.
- **Labels in 3D.** drei's `Html` renders each label into a React root of its own and dropped one of them, so labels
  are plain DOM over the canvas, projected every frame, as `CSS2DRenderer` did. They match the wrapped scene to the
  pixel.
- **Bloom** is three's `UnrealBloomPass` and `OutputPass`, as tuned in the Scala-era view, not
  `@react-three/postprocessing`, whose bloom looks different. The wrapped scene was checked against the new one with
  a jam and mid-recovery, then deleted.
- **Hosts.** The simulator and the server listen on localhost by default (`HOST` changes it; the image sets
  `0.0.0.0`). MCP over HTTP refuses requests addressed to other hosts or from other origins on localhost, and beyond
  localhost it is off until `RIF_MCP_TOKEN` is set.
- **Lint.** The React Hooks plugin's React Compiler rules assume components that never mutate; the scene is
  imperative by design, so only the classic hooks rules run.
