# Handover: in-flight

Where the build stands and how to work on it. [PLAN.md](PLAN.md) is the design, [guide.md](guide.md) the user's
guide. Git history has how it got here.

## What it is

A console and an MCP server for what is moving through your Kafka: point it at a cluster, it discovers the flow,
drafts a catalog for you to confirm, then shows what is stuck, for whom, worth how much, and redrives dead-letter
queues. Money is the default measure; a catalog can count anything else (`unit`, `measureName`, `count`, `nouns`).
Shipping schema changes safely (intents, the router, contracts, the gate) is the step after that, for teams that want
it. The simulated order-to-cash business is kept as `try` and for tests.

Java 25 backend, one Maven build (`./mvnw`); the console is React and TypeScript in `ui/`. Public at
`provenmap/in-flight`.

## Running it

```sh
npm ci && npm run build && ./mvnw package       # the console first: the jar carries dist/ui
bin/in-flight                                   # the app on the workspace, http://localhost:8088
bin/in-flight up && bin/in-flight try           # the sample data
bin/in-flight down                              # the stack's data and .in-flight/demo
```

`bin/in-flight` finds a JDK 25 or newer (Homebrew kegs, `/usr/lib/jvm/*`, or `JAVA_HOME`). The workspace is
`~/.in-flight` (`--home`, `INFLIGHT_HOME`). Old `RIF_` variables and `~/.revenue-in-flight` are still read when the
new ones are absent.

## Tests

- `./mvnw verify`: unit tests.
- `./mvnw verify -Pe2e`: end to end on Testcontainers (Docker). Each suite starts its own Kafka, or Kafka, Apicurio and
  the proxy on fixed ports 29192 to 29195. `ProductIT` is the product path with no simulator.
- `npm run test:browser`: the console against the jar (needs Kafka in `BOOTSTRAP_SERVERS`).
- The setup flow in a browser: `test/setup/seed.sh && npx playwright test -c playwright.setup.config.ts && test/setup/seed.sh down`
  (a throwaway Kafka on 9392). It also retakes `docs/images/setup-*.png`.
- `npm run typecheck`, `npm run lint`.

## Modules

`catalog` (pure: format, intents, impact, pricing, `Display`), `broker` (the broker interface, profiles, secret
references), `codec` (decoders, registry client), `kafka` (the Kafka adapter), `discovery`, `recovery`, `observer`,
`server`, `cli`, and for the change loop `router`, `proxy-filter`, `contracts`, `simulator`. observer, discovery,
recovery and server have no Kafka import: the enforcer fails the build if a Kafka library reaches them. Adding a
broker: the `add-broker` skill in `.claude/skills/`.

## Known gaps

- **Safe changes end to end:** impact in CI, the gate's report-only mode, approval from real pull requests and
  rehearsal of any command or image are each tested, but not as one run of a real application shipping a breaking
  change through CI and the gate.
- **MSK IAM** is a preset; Amazon's `aws-msk-iam-auth` is not bundled.
- **Elicitation** works over stdio only; the HTTP server is stateless, so open questions come back in results.
- **Protobuf** imports of files other than the well-known types are not followed.
- **Counted flows** sometimes say a number twice ("543 shipments at risk: 543 shipments").
- **Brokers:** offsets are dense longs per shard, so Kinesis and Pub/Sub do not fit the interface yet.

## Open, for the owner

- The first release: tag `v*` to run `.github/workflows/release.yml`; the Homebrew tap needs `HOMEBREW_TAP_TOKEN`.
- The usage video's uploads (GitHub and YouTube); the README has a marker where the player goes.
- The simulator's future: keep `try` as it is, or reduce it to test fixtures.

## Gotchas

- **The proxy runs Java 21.** Kroxylicious 0.25.1's image does, so `proxy-filter` compiles for 21 (its pom says why).
  Built for a newer Java, the proxy exits at startup, Docker keeps its ports open, and the e2e intent suites process
  nothing. Raise it only with the proxy's own Java.
- **One simulator per Kafka:** two split the services' partitions and drain each other's dead letters, so a simulator
  refuses to start while a service group has members. Another session's simulator may hold the Compose Kafka on 9092;
  use a throwaway Kafka on another port rather than touching it.
- **Stop with a signal, and before Kafka:** the CLI closes cleanly on SIGINT and SIGTERM. Taking Kafka down first leaves
  the JVM waiting on it.
- **The proxy advertises fixed ports**, so in tests it runs on fixed host ports, and suites using it take turns.
- **Kroxylicious loads the filter from a jar** (`stack/in-flight-proxy-filter.jar`, git-ignored). `up` copies it out of
  the CLI jar when it is missing.
- **Javalin 7** declares routes in `config.routes`, not on the app; `HandlerType` is not an enum.
- **The MCP Java SDK validates tool inputs** against their JSON Schema before the handler runs.
- **Jackson with `USE_BIG_DECIMAL_FOR_FLOATS`** reads `40.0` as `4E+1` through `asText()`; compare numbers as numbers.
- **`no_such_guarantee` errors** in the test log are RouterServiceTest refusing a bad setting on purpose.
- **Playwright MCP** writes into `.playwright-mcp/` (ignored). Past about 20 images in one session the VS Code panel
  freezes: prefer `browser_evaluate` and snapshots.
- **Kafka CLI from the host:** `docker run --rm --network host apache/kafka:4.4.0 /opt/kafka/bin/…`.
