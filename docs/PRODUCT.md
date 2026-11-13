# From a demo to a tool someone can use

Written 2026-11-13. Status: built, phases 0 to 7, on the plan's defaults for the owner's decisions (confirmed by the
owner on 2026-11-13); publishing a release waits on decision 5. [HANDOVER.md](HANDOVER.md) says where it stands and
how each phase was shown done. [PLAN.md](PLAN.md) is the earlier design, still true of the meaning layer.

## What is wrong today

Someone who opens this repository cannot tell what it is for, where to start, or how to use it on their own system.
The owner tried and could not. The reasons, from that attempt and from reading the code:

| # | Problem | Where it shows |
| --- | --- | --- |
| 1 | **The front door is a simulator.** The README, the guide, `demo` and the runbook all start from a fake order-to-cash business. The real use, watching your own Kafka, is a section near the end of the guide | README, `cli demo`, `Runbook.tsx` |
| 2 | **No way in.** Before anything shows, you hand-write a catalog JSON: districts, events, topics, JSON paths to the value, customer and order, consumer groups, dead-letter queues | `watch my-flow.json` |
| 3 | **No setup anywhere.** Connection settings are environment variables (`BOOTSTRAP_SERVERS`, `RIF_KAFKA_*`), the catalog is a file argument, and any change means a restart. The console has no screen for any of it | `Server.Options.fromEnv`, `Observer` takes one fixed catalog |
| 4 | **The MCP server can only look and drive the demo.** No tool connects, discovers or configures; an agent cannot set the tool up for you | `Mcp.java`: read tools plus the simulator's controls |
| 5 | **Most real clusters can't be read.** Avro and Protobuf go undecoded: their dead letters show a value of unknown, and their flow events go uncounted. One currency only | `Reader`, guide's "Not supported yet" |
| 6 | **Brittle on a real cluster.** Every catalog topic must exist before it starts; nothing survives a restart (the chart, episodes, recovery times); lag is valued only from events seen since start | `Reader`, `Ledger` (in memory) |
| 7 | **Recovery is the simulator's.** "Redrive" replays the demo's own Payments queue. A real dead-letter queue can be watched, never acted on | `simulator/Redrive` |
| 8 | **The safe-change loop is demo-shaped.** "Merge the migrations (demo)" merges in a scratch repo; rehearsal only runs the simulator's Payments code; the gate needs every producer behind a proxy before it does anything | `Changes`, `Rehearsal`, the proxy |
| 9 | **Hard to install.** A JDK 25 or Docker, a 110 MB jar built from source, no release, no GitHub repository; `up` starts the demo's stack, not anything a user needs | `bin/revenue-in-flight`, `up` |
| 10 | **Open to anyone on the network if bound beyond localhost.** The console and its API have no authentication; only MCP over HTTP has a token | `Server`, `Api` |
| 11 | **A bug:** the Changes view counts the second run only while it is on screen, so following the guide (ship v2 from the Flow view) undercounts the orders held at the gate | `useRuns` in `Changes.tsx` |

## What it becomes

**A console and an MCP server for the money moving through your Kafka.** You point it at a cluster; it finds the
flow; you confirm what the money is; it shows what is stuck, for whom, worth how much, and lets you recover it.
Shipping schema changes safely stays, as the step after that for teams that want it.

The first five minutes, whether a person does them in the console or an agent does them over MCP:

```
  connect ──────────▶ discover ──────────▶ confirm ──────────▶ watch ──────────▶ act
  a cluster, its      topics, consumer       the money field,     what is stuck,     redrive a queue,
  registry, tested    groups, dead-letter    the order and        for whom, worth    alert, ship a
                      queues, field shapes   customer, the teams  how much           change safely
```

- `revenue-in-flight` with no arguments starts the app and opens the console. With nothing configured, it opens on
  **Connect**, not on a flow.
- The catalog is still the heart of it, but it is **drafted for you** from what the cluster shows, then saved as a
  file you can keep in a repository. Writing one by hand stays possible.
- The simulator stays only as **Try it with sample data** (`revenue-in-flight try`), clearly labelled.

## MCP: set it up by asking

The MCP server becomes the easiest way to configure the tool. An agent in Claude Code, or any MCP client, can take
someone from nothing to a watched flow in one conversation: "connect to our staging Kafka and watch the checkout
flow".

**Install, one line:**

```sh
claude mcp add revenue-in-flight -- revenue-in-flight mcp      # stdio; also serves the console on 8088
```

Streamable HTTP stays, behind its token and host rules, for a shared instance.

**Tools, grouped by what they touch:**

| Tool | Does | Writes |
| --- | --- | --- |
| `status` | What is connected, which flow is active, whether it is healthy, what is missing | no |
| `connect` | Tests a connection (bootstrap servers, an auth preset, a schema registry) and saves it as a profile | config only |
| `discover` | Topics, partitions, consumer groups and what each reads, dead-letter queue candidates and why, registry subjects | no |
| `sample_topic` | The field shapes of a topic's recent records: paths, types, how often present, decoded through the registry. Example values are masked unless `reveal` is set | no |
| `draft_catalog` | A catalog from what was discovered, with a confidence and a question for every guess it made | no |
| `validate_catalog` | Checks a catalog, then dry-runs it on sampled records: does each path resolve, what would each event be worth | no |
| `edit_catalog` | Applies a JSON Patch to the draft, or the active catalog, and validates the result | config only |
| `activate` | Saves a catalog and switches the running observer to it, no restart | config only |
| `value_at_risk`, `stuck_events`, `last_episode`, `rif://flow` | As today | no |
| `redrive` | Replays dead letters to their source topic, filtered by error, order or time, with a dry run first | **the cluster**, opt-in |
| intents, impact, changes | As today, on the active catalog | as today |

The rules they follow:

- **Secrets never go through a tool call.** `connect` takes references: an environment variable's name, a file
  path, or the macOS keychain. A password typed into a chat would end up in its transcript.
- **Reading is the default.** Tools that change config carry `readOnlyHint: false`. Tools that write to the cluster
  carry `destructiveHint`, and work only when the profile allows writes; they always offer a dry run first.
- **Ask, don't guess, when the client can.** Where the client supports elicitation, `draft_catalog` asks its open
  questions directly ("Which field is the order's value: `amount` or `total_cents`?"); otherwise they come back in
  the result for the agent to ask.
- **A prompt, `set-up-a-flow`,** walks an agent through status, connect, discover, sample, draft, validate and
  activate in order.
- **Every MCP action is also an HTTP endpoint and a console screen**, so nothing is agent-only.

## How the pieces change

| Piece | Change |
| --- | --- |
| **Workspace** (new, in `server`) | A home directory (`~/.revenue-in-flight`, or `--home`) holding `config.json` (connection profiles, the active flow, what is allowed), `flows/<name>.json` (catalogs), and `state/` (ledger snapshots). Secrets by reference only |
| **Engine** (new, in `server`) | Owns the running observer and swaps it when the connection or catalog changes; viewers get a new snapshot. Replaces the fixed observer in `Server` |
| `discovery` (new module) | Admin-client reads of topics and groups; dead-letter queue candidates by name (`.dlq`, `-dlt`, `.DLT`, `dlq-`), headers (Kafka Connect's, Spring's, ours) and who writes them; sampling; field-shape inference |
| `catalog` | A draft carries guesses with confidence and open questions. Value paths may be in minor units (`total_cents`). A currency per event or a currency field |
| `observer` | Decoders as a small interface: JSON, CloudEvents, Avro through a registry (the Confluent wire format and Apicurio's), then Protobuf. Topics that don't exist yet are waited for, not fatal. The ledger snapshots to `state/` and loads on restart. Lag on unseen events is valued from each topic's recent average, marked as an estimate |
| `recovery` (new module) | A redrive for any dead-letter queue: reads under its own group, writes each record back to its source topic with a `redriveof` header, rate-limited, filtered, dry run first. Replaces the simulator's as what the console's button does |
| `server` | Setup endpoints matching the MCP tools; a login token for the console whenever it is bound beyond localhost |
| `ui/` | **Connect** (profiles, auth presets: none, SASL/PLAIN for Confluent Cloud, SCRAM, mTLS, MSK IAM), **Discover** (what was found), **Confirm** (one screen of the draft's questions), then the existing views. The runbook becomes per-queue actions for real queues. The demo-only controls appear only under `try` |
| `simulator` | Kept for `try` and for tests. Nothing else depends on it |
| `cli` | No arguments: start the app. `try` replaces `demo`. `connect`, `discover`, `draft`, `validate` mirror the tools for scripts and CI |
| Packaging | A container image as the main way in, then self-contained downloads (jpackage, so no JDK is needed) and a Homebrew tap, published per release |

## Phases

Each ends green and usable. Tests run against Kafka and Apicurio in Testcontainers, fed by a small **sample
application** in the tests: a producer and consumers that know nothing of this tool, standing in for someone's real
services. The simulator is not used to test the real path.

**0. The front door.** Fix the Changes bug (lift `useRuns` into `App`). `revenue-in-flight` with no arguments starts
the app; `demo` becomes `try` and says it is sample data. The README leads with "watch your own Kafka" and moves the
demo below it. Done when a reader's first command is about their cluster, not the simulator.

**1. Workspace and live reconfiguration.** The workspace directory, connection profiles with secret references, the
engine that swaps the observer without a restart, missing topics tolerated. `status` and `connect` over MCP and HTTP.
Done when a running instance moves from no config, to a cluster, to another cluster without restarting, and an
agent can do it.

**2. Discover.** The `discovery` module, sampling, and the decoder interface with JSON, CloudEvents and Avro.
`discover` and `sample_topic` over MCP. Done when, on the sample application's cluster with Avro on one topic, discovery
finds every topic, group and dead-letter queue, and sampling shows the Avro fields.

**3. Draft and confirm.** `draft_catalog`, `validate_catalog`, `edit_catalog`, `activate`, the `set-up-a-flow`
prompt, elicitation where supported; the console's Connect, Discover and Confirm screens. Done when an agent, given
only "watch the checkout flow", reaches a live view of the sample application with every value correct, and a person
can do the same in the console without writing any JSON.

**4. Act on a real incident.** The `recovery` module and its redrive, opt-in per profile; errors grouped by cause in
the incident view; ledger snapshots; estimated lag value; a webhook when money at risk crosses a threshold. Done
when the sample application's consumer is broken, fixed and its queue redriven from the console and over MCP, and a
restart in the middle keeps the episode.

**5. Install in one line.** The container image, self-contained downloads, the Homebrew tap, a release workflow.
`claude mcp add` works with the installed binary. Done when someone with neither a JDK nor this repository goes
from nothing to a watched flow following the README alone. Needs the GitHub decision below.

**6. Safe changes for real teams.** Impact in CI on a catalog kept in a repository (a GitHub Action around
`impact`). The gate gains a **report-only mode** that logs what it would refuse, so a team can try it without
risk. Approval reads the real pull requests' state instead of the demo's merge button. Rehearsal runs a consumer
given as a container image or a command, not just the simulator's Payments. Done when the sample application ships a
breaking change through CI and the gate, with no demo-only code involved.

**7. The rest of "not supported".** Protobuf; several currencies; sampling for very busy topics. Then the guide
rewritten around connect, discover and confirm, new screenshots. The **usage video** has its own plan,
[VIDEO.md](VIDEO.md), and can be made as soon as phase 3 is done.

## What this costs

- Phases 0 to 3 are the product. They are mostly new code (discovery, drafting, the engine, three screens) on top
  of what exists. The observer, catalog and console views carry over.
- Avro adds Apache Avro and a registry client; the Confluent serializers are avoided for their licence. Protobuf
  needs schema parsing at runtime, which is why it waits until phase 7.
- A redrive that writes to someone's cluster is the first feature that can do harm. It stays off until a profile
  allows it, and always dry-runs first.
- The simulator and the demo stop being the centre. The two-run story remains under `try`.

## Decisions that are the owner's

1. **Who is first.** Confluent Cloud, MSK or self-hosted decides which auth preset and which registry come first.
   The plan assumes self-hosted with Apicurio or Confluent's registry, then Confluent Cloud.
2. **Redrive in the first release, or watch-only.** The plan includes it in phase 4, behind an opt-in.
3. **Where config lives by default.** The plan says the user's home directory, with the catalog exportable to a
   repository; teams may prefer the repository to be the source.
4. **The name**, still open from MERGE.md: "revenue-in-flight" fits money, less so other measures.
5. **A public GitHub repository and releases**, which phase 5 needs.
6. **The simulator's future** after phase 3: keep `try` as it is, or reduce it to test fixtures.
