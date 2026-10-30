# Handover: revenue-in-flight, first build

Written 2026-10-30 at the end of a session that ran in the intent-bus repo. Start here, then read
[PLAN.md](PLAN.md), which holds the design.

## Where things stand

- **The idea.** `revenue-in-flight` in the showcase ledger, doc at
  `/Users/murali/source-others/pmap-showcase/data/ideas/revenue-in-flight.md`. The pitch: order-to-cash runs live
  in 3D, a schema change jams Payments with a running "money stuck" counter, a fix drains it.
- **Ledger stage:** still `chosen`. The doc header says `founded`, but the record does not.
- **GitHub:** no repo yet. `provenmap/revenue-in-flight` does not exist. Creating it is public and was
  deliberately left for the user to approve. Once created, record it with
  `showcase-ideas.js --link revenue-in-flight --repo … --fleet …` and `--stage revenue-in-flight founded`.
- **This directory:** `git init` done on `main`, nothing committed. **Steps 1 to 3 are built and tested** (2026-10-30,
  second session): 46 unit tests pass (`sbt test`), and the whole story was run on real Kafka. **Step 4, the web view,
  is built** and was checked in a real browser in 3D, 2D and at phone width.

| Path | State |
| --- | --- |
| `catalog/order-to-cash.json` | districts (customers is a `sink`), six event specs with value/customer/order paths, consumers with groups, DLQs and the payments redrive group |
| `catalog/src/.../rif/catalog` | `Catalog` (load, validate), `CloudEvents` (build, parse, `redriveof`), `DeadLetter` header names, `Pricing` |
| `simulator/src/.../rif/simulator` | `Orders` (traffic, 2,000 customers, weighted shop range), `Services` (payments, inventory, shipping join), `Kafka` (service loop with DLQ), `Redrive`, `Main` (control API on 8090) |
| `observer/src/.../rif/observer` | `Ledger` (pure accounting), `Reader` and `Poller` (Kafka, Admin offsets, simulator state), `Main` (Javalin on 8088, `Broadcaster`) |
| `observer/src/main/resources/web/` | the view: `index.html`, `style.css`, `app.js` (WebSocket, HUD, controls, view choice), `model.js` (scene as data, layout, value scale, recovery wave), `view3d.js` (three.js), `view2d.js` (canvas fallback), `labels.js` |
| `*/src/main/resources/simplelogger.properties` | Kafka client logging at warn |

### What was verified live

Break, then 11 s of v2 orders, took stuck money to $9,817.11 across 58 orders and 58 customers. Inventory kept
working. A redrive before the patch stopped at the first unreadable record and moved no offsets. Patch, then redrive,
recovered all 58, the counter dropped to $0 and the episode read "$9,817.11 recovered, stuck 11.0 s". After an
observer restart, the snapshot showed $0 stuck and all 129 dead letters recovered.

### The view (step 4)

- **Picking a view:** 3D by default. Use `?view=2d` or `?view=3d`, or the toggle, which is remembered in
  `localStorage` as `rif.view`. With no WebGL, or if the three.js import from jsdelivr fails, the page draws in 2D
  and says so.
- **Particles:** size and brightness scale with value on a log scale from $10 to $10,000 (`worth()` in
  `model.js`). Brightness grows with the square of that, so only dear orders bloom. Redriven events are green, and
  declined payments drop off amber.
- **The pile:** red blocks on open ground left of their district (`PILE` in `model.js`). Each block drops in as its
  dead letter arrives. On recovery the blocks turn green in a wave, 15 ms apart (3 ms once the wave runs past
  5 s), and lift back into Payments. Each redriven particle leaves Payments as its block turns green.
- **HUD:** the counter is the observer's money at risk, plus recoveries the latest stats already counted but the wave
  has not played yet, so it drains in step with the pile without overshooting. The banner shows the last episode.
  The hint suggests the next control and the suggested button pulses.
- **For tests:** `window.rif` exposes `model`, `view`, `frames`, `particles` and `blocks`. The HUD and controls
  carry `data-testid`s: `at-risk`, `at-risk-sub`, `stuck`, `lag`, `recovered`, `hint`, `break`, `patch`,
  `redrive`, `rate`, `sim-state`, `connection`.
- **Editing it:** the observer serves `web/` from the classpath, so run `sbt observer/copyResources` after an edit
  (`compile` does not copy resources), then reload.

Checked in the browser: a 28 s jam built 221 blocks and $44,881. A redrive drained it in a green river from Payments
to Shipping over about 3 s, with the counter falling without ever rising above its pre-redrive value. There are no
console errors and no horizontal scroll at 390 px.

### The observer's WebSocket protocol

`ws://localhost:8088/ws`. Every frame is a JSON **array** of messages. The first frame holds one `snapshot`.

- `snapshot`: `flow` (`districts[]` of `{id, name, sink}`, `topics[]` of `{topic, from, to[], events[]}`, `dlqs[]`),
  `stuck[]` (the `dead` messages still stuck, capped at 3,000), `stats`.
- `event`: `{id, event, topic, district, to[], value, customer, orderId, redrive}`. `to` is empty for
  `payments.declined`, and `["customers"]` for dispatches.
- `dead`: `{id, dlq, district, event, value, customer, orderId, error, at}`. `district` is the service that
  dead-lettered it (payments).
- `recovered`: `{id, district, value, afterMs}`. `afterMs` is null for one found already redriven at start-up.
- `stats`, every 500 ms: `atRisk {value, orders, customers}` (the headline), `stuck {value, events, customers,
  since, byDistrict}`, `lag {value, events, byGroup}`, `recovered {value, events}`, `lastEpisode {value, events,
  tookMs}` or null, `unpriced`, and `sim` (the simulator's `/state`, or null when it is down).

Controls: `POST /api/sim/{break|restore|patch|unpatch|redrive}` and `POST /api/sim/rate?perSecond=N` (0 to 500).
The simulator answers 409 to a second redrive while one runs, and the observer answers 502 when the simulator is down.

### Running it by hand until step 5's script exists

```sh
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
docker compose up -d --wait
sbt -batch "export simulator/Runtime/fullClasspath" "export observer/Runtime/fullClasspath" | grep '^/' > target/cp/all
java -cp "$(sed -n 1p target/cp/all)" rif.simulator.Main   # then, in another shell:
java -cp "$(sed -n 2p target/cp/all)" rif.observer.Main
```

Kafka was left running with this session's topics and data. `docker compose down` clears it.

## Decisions already made

- **Scope of this build:** milestones 1 to 3, the simulator, the 3D view and money at risk. Milestone 4, the agent
  that heals and proves, comes later. Until then "patch payments" and "redrive" are buttons.
- **Stack:** Scala 3 and sbt, matching intent-bus. Kafka clients 4.4.0, ujson 4.4.3, munit 1.3.6,
  Javalin 6.7.0 for HTTP, WebSocket and static files. Javalin 7 was skipped because its API changed.
- **Web:** no build step. Plain ES modules with an import map to three.js 0.186.1 on jsdelivr, using
  `OrbitControls` and `CSS2DRenderer` from `examples/jsm`. The 2D canvas fallback is part of milestone 2, not optional.
- **Ports:** Kafka 9092, observer and web on 8088, simulator control API on 8090. The web UI calls the
  simulator through the observer, which avoids CORS.
- **The jam:** `OrderPlaced` v2 makes `total` an object `{ amount, currency }`. Payments reads `total` and
  dead-letters. Inventory reads only items and keeps working. Exactly one district jams.
- **Redrive without duplicates:** the patched payments service reads `payments.dlq` with its own consumer group.
  It does not re-publish to `orders.placed`, which inventory would read twice. A redriven success carries the original
  event id, so the observer can mark it recovered.
- **Values are declared, never guessed:** each event type names the path to its value and its customer in the
  catalog.
- **Restarts (added this session):** the catalog names each DLQ's redrive consumer group. On start-up, the observer
  counts dead letters below that group's committed offset as already recovered, because it cannot see the redriven
  events again from the end of the flow topics. `PLAN.md` records it under Money at risk.
- **Lag is bounded by the polled end:** an event newer than the end offset polled with the group's commit is in
  flight, not lag. That keeps the value and the count consistent, and the healthy headline at $0.
- **Redrive semantics:** one pass, up to the end offsets at its start, in group `payments-service-redrive`, with
  `assign` and no rebalance. The first record that still fails pauses its partition and leaves its offset
  uncommitted. The decline decision is a hash of the order id, so a redrive decides the same way.

## Next steps, in order

Steps 1 to 4 (catalog, simulator, observer, web view) are done. Start at 5.

5. **A driver script,** like intent-bus's `demo/ib`: `up`, `simulator`, `observer`, `build`, `down`. The `build`
   command needs `copyResources` too, or its exported classpath to include fresh `web/` files.
6. **Browser test with Playwright.** Check that the page renders, the counter rises after break, and drains after
   patch and redrive. Use `window.rif` and the `data-testid`s above, not screenshots.
7. **README,** then the GitHub repo once the user approves it.

## What to reuse from intent-bus

In `/Users/murali/source-others/public-repos/intent-bus`, on branch `build/milestones-1-3`:

- **CloudEvent helper.** `demo/src/main/scala/intentbus/demo/CloudEvents.scala` covers the envelope, with
  `schemaversion` and `sequence`.
- **Producer patterns.** `demo/src/main/scala/intentbus/demo/Produce.scala` has topic creation, scrambled sends
  and error handling.
- **Order-to-cash data.** `demo/catalog/rev1.json` holds the event shapes this flow extends.
- **The driver script.** `demo/ib` finds JDK 21 itself and exports sbt classpaths, so apps start without sbt
  overhead.

## Gotchas learned the hard way

- **JDK:** Homebrew's `openjdk@21` is keg-only and `/usr/bin/java` is a stub. Set
  `JAVA_HOME=/opt/homebrew/opt/openjdk@21` for sbt and `java`, or copy `demo/ib`'s lookup.
- **Kafka Streams** does not leave its group on close by default. Not used here, but relevant if it comes in.
- **Playwright MCP** writes snapshots and screenshots into `.playwright-mcp/` in the workspace root, and can only
  save files under the workspace. It is ignored here.
- **Images in the VS Code panel:** past about 20 images in one session the panel freezes. Prefer
  `browser_snapshot` and `browser_evaluate` over screenshots, and look at only the ones that matter. WebGL renders
  in headless Chromium, but verify the 3D view with a few screenshots, not many.
- **Kafka CLI from the host:** `docker run --rm --network host apache/kafka:4.4.0 /opt/kafka/bin/…` works,
  because Docker host networking is on.

## Left open in intent-bus

intent-bus is parked at milestone 3, with commit `e97f0f3` on `build/milestones-1-3`. Two changes there are
uncommitted: `demo/ib` finds JDK 21, and `docker-compose.yml` adds the Apicurio UI on port 8888. A known bug is not
fixed yet. An intent declared at runtime misses events that already exist, because all intents share one
Streams application. The fix is one application id per intent.
