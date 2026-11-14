# Handover: in-flight

Start here, then read [PLAN.md](PLAN.md) for the design. [MERGE.md](MERGE.md) is the plan this state came from;
[REFACTOR.md](REFACTOR.md) is the earlier move from Scala to TypeScript, now history.

## 2026-11-14, later: renamed in-flight, and public

The product stopped being only about money, so the name went with it (PRODUCT.md decision 4, settled). Not
kafka-in-flight, which was tried first: Apache's trademark policy does not allow "Kafka" in a product's name, and
nothing had been pushed, so it was renamed again before publishing. Everything
is renamed: the repository (`provenmap/in-flight`, public), the command and launcher (`bin/in-flight`),
the MCP server name, the workspace (`~/.in-flight`), the image, the Homebrew formula, the jar, the UI title,
the Java packages (`rif.*` → `in-flight.*`, group `io.github.provenmap.inflight`), the variables (`RIF_` → `INFLIGHT_`), the
dead-letter headers (`in-flight-error`, …), group names and the console's cookie.

- **What still works from before:** `RIF_` variables are read when the `INFLIGHT_` name is not set (`inflight.catalog.Env`,
  also `RIF_JAR` in the launcher), and `~/.revenue-in-flight` is used when there is no `~/.in-flight`. Old
  `rif-` dead-letter headers are not read: nothing outside throwaway test stacks ever wrote them.
- **The video is no longer committed** (`docs/usage.mp4` is git-ignored; it was taken out of the unpushed history
  before the first push). The README has a marker where GitHub's player goes once the file is uploaded there; the
  same file goes on YouTube. Both uploads are the owner's.
- **The clone's folder** is still `public-repos/revenue-in-flight` on this machine: renaming it under an open editor
  would break the workspace. Rename it when nothing has it open.

Checked: the full Maven suite (235), typecheck, lint, the console and setup browser tests, and the guide's
screenshots retaken (they show the new name).

**The e2e suite (`./mvnw verify -Pe2e`) did not pass that day; fixed since.** IntentsIT (3 of 6) and TwoWaysIT
failed because the proxy never ran: the move to Java 25 (`3b770e3`) compiled the filters for Java 25, and the
Kroxylicious 0.25.1 image runs Java 21, so it exited at startup on an `UnsupportedClassVersionError`. Docker kept
the fixed ports open, so the tests' wait passed and services reading `intent.<name>` through it processed nothing.
`proxy-filter` now compiles for Java 21 (its pom says why); raise it only with the proxy's own Java. The
`no_such_guarantee` errors in the log are RouterServiceTest refusing a bad setting on purpose, not a failure.

## 2026-11-14: not only money, and the video opens on a preview

The engine always tracked a case, a party and a measure; the product only said money. Now a catalog can count
something else (PRODUCT.md phase 8; README and guide, "Not only money"):

- **Four optional catalog keys:** `unit`, `measureName`, `count`, `nouns`. `Catalog.Worth` holds them;
  `Catalog.display()` resolves them, with the roles and the currency, into a `Display` that formats every figure and
  noun. The console (`shown` and `setMeasure` in `ui/src/model.ts`), the MCP answers, the webhook, redrive reports,
  Causes and the dry run all use it; the flow message, `/api/stuck/causes` and validation carry it as `measure`.
- **`count: true`** makes every event worth one in `Pricing`, ignoring any value path; a counted event still needs its
  order and customer. A `unit` with a `currency` is refused, and so is `count` with a currency.
- **Confirm** offers *Not money: count the events* when no record names a currency. Other units are set with Edit
  JSON or `edit_catalog`.
- **MCP:** `value_at_risk` is titled "Value at risk" (the tool's name is unchanged) and its data has `measure`.

Checked: the full Maven suite (catalog 87 with 7 new in `DisplayTest`), typecheck, lint, the console browser tests (6)
and the setup browser test (which retook the four setup screenshots, whose wording changed). By hand: the shipped flow
recast as counted shipments, run on the simulator and broken, reads "Shipments at risk", "442 shipments and 396
recipients waiting", and the same over MCP, with no dollar sign. In counted mode some lines say the number twice
("543 shipments at risk: 543 shipments"), which is true but redundant.

The usage video now opens on a 4.5 s preview of the end result, recorded live by a second window while the take runs,
and fills its forms faster; it stays 1:52 and stays about money.

## 2026-11-13, evening: the usage video, and what making it found

[docs/usage.mp4](usage.mp4) (1:52) is made, as [VIDEO.md](VIDEO.md) planned: the real app set up on a sample shop, no
simulator. `npm run docs:video` remakes it. Fixed on the way, each seen in a take:

- **The launcher missed Homebrew's JDKs** other than `openjdk@25` (plain `openjdk`, now 27, and newer kegs), so
  `bin/in-flight` failed on a machine with a JDK 27. It now tries every Homebrew keg and `/usr/lib/jvm/*`.
- **Redrive texts:** a running redrive said "since 2026-11-13T14:57:27.934782Z … watch value_at_risk drain" in the
  console; it now gives the local time and plain words. Reports say "$5,665.06", not "USD 5665.06".
- **The map's dead-letter tank** clipped a long queue name and its figures (`fulfilment-service.DLT`); name and
  figures are now on two lines.
- **Errors by cause** kept showing the last causes for up to 15 s after everything recovered; it now refetches the
  moment nothing is stuck.
- **"What the catalog says"** showed only "Unknown" for a drafted catalog without meanings; it now shows only for
  flows with intents, like the Intents and Changes tabs.

Checked: typecheck, lint, the unit tests of catalog, kafka, discovery, recovery, observer and server (128). Not run
this time: the setup browser test (its Kafka, `in-flight-setup-kafka`, was in use by someone else) and the e2e suite.

## 2026-11-13, later: PRODUCT.md built, a tool for your own Kafka

All of [PRODUCT.md](PRODUCT.md)'s phases, 0 to 7, are built on the plan's defaults for the owner's decisions:
self-hosted with Apicurio or Confluent's registry first, then Confluent Cloud; redrive in, opt-in per profile; config
in the home directory, catalogs exportable; the simulator kept as `try`. Not committed. Nothing is published: a
release needs the GitHub repository (decision 5) and the product's name (decision 4).

- **New modules:** `discovery` (topics, groups, dead-letter queues, sampling and field shapes, drafting with
  questions, the dry run) and `recovery` (the generic redrive). `kafka` gained connection profiles, secret references
  (`env:`, `file:`, `keychain:`), the registry client and the decoders (JSON, CloudEvents, Avro, Protobuf parsed at run
  time). `server` gained `Workspace`, `Engine` (swaps the observer without a restart), `Setup` (every setup action,
  shared by HTTP, MCP and the CLI), `ConsoleAuth`, `Alerts` and `Causes`.
- **Libraries added:** Apache Avro 1.12.2, protobuf-java 4.36.1. No Confluent serializers (their licence).
- **The front door:** no arguments starts the app on the workspace (`~/.in-flight`, `--home`, `INFLIGHT_HOME`)
  and opens the console, on Connect when nothing is configured. `demo` is `try` (still answers to `demo`). `mcp` serves
  the workspace's tools and the console on :8088; with `SIMULATOR_URL` set (or `--try`), the sample data's.

| Phase | Done when (PRODUCT.md) | How it was shown |
| --- | --- | --- |
| 0 | a reader's first command is about their cluster | README and the CLI's usage lead with it; `useRuns` lifted into `App` |
| 1 | no config, to a cluster, to another, no restart, by an agent | `ProductIT` (MCP over HTTP; two Kafkas); missing topics waited for |
| 2 | discovery finds every topic, group and DLQ; sampling shows Avro | `ProductIT` on Kafka and Apicurio in Testcontainers, a sample application with Avro on one topic; `DecodersTest` |
| 3 | an agent and a person reach a live view with every value right | `ProductIT` (cents and Avro decimals priced alike); the setup browser test (`playwright.setup.config.ts`) clicks through Connect, Discover, Confirm in the real jar |
| 4 | broken, fixed, redriven from console and MCP; a restart keeps the episode | `ProductIT` (refused until the profile allows writes; dry run first); the browser test's dry run; `LedgerTest` for the estimate and the restart |
| 5 | nothing but the README to a watched flow | the image runs the app by default; `packaging/jpackage.sh` made a 129 MB macOS download that ran with no JDK; the release workflow and Homebrew formula are written but cannot run until there is a repository |
| 6 | a breaking change through CI and the gate, no demo-only code | `impact --since` and `action.yml` (CliTest); the gate's `reportOnly` (SchemaGateFilterTest); approval reads pull requests through `gh`, as before, and propose/approve now work outside `try`; `rehearse --candidate` for any command or image |
| 7 | Protobuf, currencies, busy topics, the guide | `ProtobufTest`; `minorUnits`, `currency`, `currencyField`, `rates` (AmountsTest); sampling bounded by count, bytes and time; the guide rewritten with new screenshots |

### Where it falls short of the plan, and why

- **Phase 6's "done when"** is shown piece by piece, not as one test of the sample application shipping a breaking
  change through CI and the gate: that needs the router and the proxy in front of a repository with real pull
  requests. The pieces are each tested; the whole is not.
- **MSK IAM** is a preset whose settings are right, but Amazon's `aws-msk-iam-auth` is not bundled (it brings the AWS
  SDK); the preset says so when chosen.
- **Elicitation** works over stdio only: the HTTP server is stateless, so its open questions come back in results.
- **The action's default image**, `ghcr.io/provenmap/in-flight`, assumes the repository's name.
- **Protobuf** imports of files other than the well-known types are not followed.
- **Redrive with a filter** keeps what it redrove beyond its commit in the commit's metadata (`in-flight-redriven:17-20`),
  so a later pass does not write them twice.

### Running it

```sh
npm ci && npm run build && ./mvnw package
bin/in-flight                                   # the app, http://localhost:8088
bin/in-flight up && bin/in-flight try   # the sample data
```

Tests: `./mvnw verify`; `./mvnw verify -Pe2e` (now with `ProductIT`); `npm run test:browser`; the setup flow:
`test/setup/seed.sh && npx playwright test -c playwright.setup.config.ts && test/setup/seed.sh down` (a throwaway
Kafka on 9392, kept off the Compose one, which another session's simulator may be using).

## 2026-11-13: intent-bus merged, on the JVM

All of MERGE.md's phases, 0 to 7, are built. On branch `merge-intent-bus`, **not committed** (the owner's call,
MERGE.md's decision 4). The TypeScript backend is deleted; the console stays React and TypeScript.

- **Java 25, Maven 3.9.16 through `./mvnw`.** Modules: `catalog`, `kafka`, `proxy-filter`, `router`, `contracts`,
  `simulator`, `observer`, `server`, `cli`. One jar, `cli/target/in-flight.jar` (about 110 MB), carries the
  console (from `dist/ui`, so `npm run build` first) and the Compose stack. `bin/in-flight` finds a JDK 25 or newer.
  Install Temurin 25 or set `JAVA_HOME` to a JDK 25+ installation.
- **Libraries:** Kafka clients and Streams 4.4.0, Kroxylicious API 0.25.1, Jackson 2.22.4, Javalin 7.2.4, the MCP Java
  SDK 2.1.0 (`mcp-core` with `mcp-json-jackson2`), slf4j-simple 2.0.20, JUnit 6.1.3, Testcontainers 2.0.5.
- **Verified:** 195 unit tests (`./mvnw verify`); 22 end-to-end tests on Testcontainers (`./mvnw verify -Pe2e`); 6
  browser tests against the jar (`npm run test:browser`); typecheck and lint; the Docker image builds and runs; the
  whole demo was driven by hand in a browser on a private copy of the stack, both runs, end to end.
- **The demo:** `bin/in-flight up`, then `bin/in-flight demo` (router, simulator, server in one JVM;
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
- **Merging in the demo** happens in a scratch git repository under `.in-flight/demo/repo`, never in this one. A real flow
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
bin/in-flight up && bin/in-flight demo     # http://localhost:8088
bin/in-flight down                                 # the stack's data and .in-flight/demo
```

Tests: `./mvnw verify`; `./mvnw verify -Pe2e` (Docker; each suite starts its own Kafka, or Kafka, Apicurio and the
proxy on fixed ports 29192 to 29195); `npm run test:browser` (needs Kafka in `BOOTSTRAP_SERVERS` and the jar).

## Gotchas learned the hard way

- **A foreign simulator on the Compose Kafka.** On 2026-11-13 a TypeScript simulator (an `rdkafka` client) from outside
  this session held `shipping-service` on `localhost:9092`, so a new simulator refused to start there. It was left
  alone. The browser tests ran on a throwaway `apache/kafka:4.4.0` on 9392, and the demo on a copy of the stack with
  Kafka 9592, the registry 8580 and the proxy 9692 (`docker compose -p in-flight-verify` with the ports edited).
- **One simulator per Kafka:** two would split the services' partitions and drain each other's dead letters, so a
  simulator refuses to start while a service group has members.
- **Stop with a signal, and before Kafka:** the CLI closes cleanly on SIGINT and SIGTERM (services leave their groups,
  the router's applications leave theirs). Taking Kafka down first leaves the JVM waiting on it.
- **The proxy advertises fixed ports**, so in tests it runs on fixed host ports, and suites using it take turns.
- **Kroxylicious loads the filter from a jar** that only the Java build makes (`stack/in-flight-proxy-filter.jar`, ignored
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
