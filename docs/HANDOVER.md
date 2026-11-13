# Handover: revenue-in-flight

Start here, then read [PLAN.md](PLAN.md) for the design. [MERGE.md](MERGE.md) is the plan this state came from;
[REFACTOR.md](REFACTOR.md) is the earlier move from Scala to TypeScript, now history.

## 2026-11-13: intent-bus merged, on the JVM

All of MERGE.md's phases, 0 to 7, are built. On branch `merge-intent-bus`, **not committed** (the owner's call,
MERGE.md's decision 4). The TypeScript backend is deleted; the console stays React and TypeScript.

- **Java 21, Maven 3.9.16 through `./mvnw`.** Modules: `catalog`, `kafka`, `proxy-filter`, `router`, `contracts`,
  `simulator`, `observer`, `server`, `cli`. One jar, `cli/target/revenue-in-flight.jar` (about 110 MB), carries the
  console (from `dist/ui`, so `npm run build` first) and the Compose stack. `bin/revenue-in-flight` finds a JDK 21.
  On this machine the JDK is Homebrew's keg-only `openjdk@21`: `/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home`.
- **Libraries:** Kafka clients and Streams 4.4.0, Kroxylicious API 0.25.1, Jackson 2.22.4, Javalin 7.2.4, the MCP Java
  SDK 2.1.0 (`mcp-core` with `mcp-json-jackson2`), slf4j-simple 2.0.20, JUnit 6.1.3, Testcontainers 2.0.5.
- **Verified:** 195 unit tests (`./mvnw verify`); 22 end-to-end tests on Testcontainers (`./mvnw verify -Pe2e`); 6
  browser tests against the jar (`npm run test:browser`); typecheck and lint; the Docker image builds and runs; the
  whole demo was driven by hand in a browser on a private copy of the stack, both runs, end to end.
- **The demo:** `bin/revenue-in-flight up`, then `bin/revenue-in-flight demo` (router, simulator, server in one JVM;
  starts in about two seconds). `up kafka` plus `simulate` and `serve` still run the incident on Kafka alone.

### Phase by phase

| Phase | Done when (MERGE.md) | How it was shown |
| --- | --- | --- |
| 0 | one build, the proxy filter in it, the three unknowns proven | the filter's two suites in JUnit (18); MCP over stdio and stateless Streamable HTTP with the SDK (McpIT); WebSocket at 500/s (ObserverIT); the jar, the launcher and the image |
| 1 | both repos' catalogs load; `impact OrderPlaced@v2` prints what it printed | 74 catalog tests (TypeScript's and intent-bus's suites); the output compared line for line with the TypeScript CLI's |
| 2 | the existing console runs unchanged on the Java server; Playwright passes | the 5 browser tests passed on the unchanged console before it gained its views |
| 3 | the MCP end-to-end test passes | McpIT, over stdio (the jar) and HTTP |
| 4 | payments reads `intent.order-total-changes`; the incident demo passes | IntentsIT: router, proxy and registry in containers |
| 5 | the same change jams one way and passes clean the other, in a test | TwoWaysIT |
| 6 | the console's views; the TypeScript backend retired; docs rewritten | the browser test of the intents view; `src/` deleted |
| 7 | rehearse, selected by intent | IntentsIT's rehearsal test; `rehearse` in the CLI and MCP |

### Where it differs from MERGE.md, and why

- **Router: unknown versions are routed, not skipped.** A version the active catalog does not know, of a type an intent
  routes, follows its type's newest route. intent-bus skipped it, which with services reading by intent made "around
  the catalog" orders vanish without a trace instead of jamming Payments. It is still counted as uncatalogued.
- **Registry: no compatibility rule.** Contracts no longer sets BACKWARD on new artifacts: it refused `OrderPlaced` v2
  outright (a string turned object), whatever its readers' migrations. The catalog's impact analysis judges it.
- **Services read through the proxy but write straight to Kafka**, so the gate binds only the orders team, the producer
  the demo puts behind it. Through the proxy, the gate would refuse Payments' own dead letters of v2.
- **A refused v2 order goes out as v1**: the orders team's deploy is held at the gate, not its orders.
- **The shipped catalog is revision 2**, the one that adds `OrderPlaced` v2; the demo derives revision 1 by taking v2
  out. It keeps explicit value paths beside `roles`; derived values are tested with them removed.
- **Merging in the demo** happens in a scratch git repository under `.rif/demo/repo`, never in this one. A real flow
  uses `contracts propose --push` and pull requests.
- **Module layout:** modules at the root; the shipped flow stays at `catalog/order-to-cash.json`, beside the `catalog`
  module's pom, which bundles it. A small `kafka` module holds what the simulator and observer share.
- **The catalog format** lets `flow`, `districts`, `consumers` and `currency` be left out, so intent-bus's catalogs
  load as they are; its `producer` is read as `district`.
- **MCP tools** added beyond the plan: `rehearse`. Reference errors from the compiler now name the intent, as the
  TypeScript did; intent-bus's suites were adapted to that.

### Open, for the owner

MERGE.md's four decisions are untouched: the product's name, what becomes of the intent-bus repository (NOTICE now
credits it), which run of the demo leads (the code assumes "around the catalog" first), and committing this branch.
Also: no GitHub repository yet; the guide's screenshots predate the new views (`npm run docs:screenshots` retakes the
old ones; the new views need the full stack, which the screenshot script does not start).

## Running it

```sh
npm ci && npm run build && ./mvnw package
bin/revenue-in-flight up && bin/revenue-in-flight demo     # http://localhost:8088
bin/revenue-in-flight down                                 # the stack's data and .rif/demo
```

Tests: `./mvnw verify`; `./mvnw verify -Pe2e` (Docker; each suite starts its own Kafka, or Kafka, Apicurio and the
proxy on fixed ports 29192 to 29195); `npm run test:browser` (needs Kafka in `BOOTSTRAP_SERVERS` and the jar).

## Gotchas learned the hard way

- **A foreign simulator on the Compose Kafka.** On 2026-11-13 a TypeScript simulator (an `rdkafka` client) from outside
  this session held `shipping-service` on `localhost:9092`, so a new simulator refused to start there. It was left
  alone. The browser tests ran on a throwaway `apache/kafka:4.4.0` on 9392, and the demo on a copy of the stack with
  Kafka 9592, the registry 8580 and the proxy 9692 (`docker compose -p rif-verify` with the ports edited).
- **One simulator per Kafka:** two would split the services' partitions and drain each other's dead letters, so a
  simulator refuses to start while a service group has members.
- **Stop with a signal, and before Kafka:** the CLI closes cleanly on SIGINT and SIGTERM (services leave their groups,
  the router's applications leave theirs). Taking Kafka down first leaves the JVM waiting on it.
- **The proxy advertises fixed ports**, so in tests it runs on fixed host ports, and suites using it take turns.
- **Kroxylicious loads the filter from a jar** that only the Java build makes (`stack/rif-proxy-filter.jar`, ignored
  by git). `up` copies it out of the CLI jar when it is missing.
- **Javalin 7** declares routes in `config.routes`, not on the app; `HandlerType` is not an enum.
- **The MCP Java SDK validates tool inputs** against their JSON Schema before the handler runs.
- **Jackson with `USE_BIG_DECIMAL_FOR_FLOATS`** reads `40.0` as `4E+1` through `asText()`; compare numbers as numbers.
- **Playwright MCP** writes into `.playwright-mcp/` in the workspace, which is ignored. Past about 20 images in one
  session the VS Code panel freezes: prefer `browser_evaluate` and snapshots.
- **Kafka CLI from the host:** `docker run --rm --network host apache/kafka:4.4.0 /opt/kafka/bin/…`.

## intent-bus

Parked at milestone 3 (`e97f0f3` on `build/milestones-1-3`), with two uncommitted changes of its own. Everything in it
is now here: its filters as they were, its model, router and contracts rewritten in Java, its known bug fixed (one
Streams application per intent).
