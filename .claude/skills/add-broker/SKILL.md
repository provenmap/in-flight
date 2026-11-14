---
name: add-broker
description: Add support for a message broker other than Kafka to in-flight by writing a broker adapter behind the inflight.broker interface — or decide that a broker cannot be supported, and say why. Use when asked to support, add, integrate or write an adapter for RabbitMQ, Pulsar, NATS JetStream, Amazon SQS or Kinesis, Google Pub/Sub, Azure Service Bus, Redis Streams or any other broker, or when asked whether in-flight can watch one.
---

# Add a broker adapter

in-flight's observer, discovery, sampling, redrive, intent tail and rehearse reach a broker only through the
interfaces in `broker/src/main/java/inflight/broker/`. An adapter is a Maven module of its own that implements them
for one broker and is found at run time through `java.util.ServiceLoader`. `kafka/` is the one adapter so far, and the
worked example to copy.

The interface is **experimental**: it was extracted from Kafka with only Kafka behind it. When a broker does not fit,
say so plainly and stop; do not bend the broker to fit or fake a capability. A missing capability turns a feature off.
A false one gives wrong numbers about the user's own money.

Work through the steps in order. Steps 1 and 2 can end the job, and that is a fine outcome to report.

## 1. Is it Kafka already?

Many brokers speak Kafka's protocol: Redpanda, WarpStream, AutoMQ, Amazon MSK, Confluent Cloud, Aiven, Azure Event
Hubs (its Kafka endpoint), Pulsar with its Kafka protocol handler (KoP). For these, **no adapter is needed**. Connect
with the Kafka adapter and the right auth preset (`KafkaAuth.PRESETS` in `kafka/src/main/java/inflight/kafka/`), and
tell the user. Only go on if the broker has to be reached through its own protocol.

## 2. Decide what the broker can do (the deciding step)

Read `broker/src/main/java/inflight/broker/Capability.java`. Answer each question from the broker's documentation,
not from memory of how it usually behaves, and write the answers down for the user before writing any code.

| Capability | The question | Needed by |
| --- | --- | --- |
| `OBSERVE` | Can a client read messages **with no side effects at all**? Reading must not take the message from any other reader, must not move any group's position, and must not mark anything redelivered or reorder anything. | the observer (the whole headline), discovery's sampling, `sample`, the intent tail, `rehearse` |
| `GROUP_LAG` | Can the committed position of *another* application's group be read per shard, and are offsets **dense**, so that `end - committed` counts messages? | the "in lag" half of value at risk |
| `REDRIVE` | Can the tool read a dead-letter stream under a group of its own, commit how far it got (with a short text note), and write messages back to a stream? | redrive |

The two that decide most cases:

- **No `OBSERVE`, no adapter worth having.** The observer refuses to start without it (`Reader.run`), because reading
  a classic queue steals the messages a real service was waiting for. A classic RabbitMQ queue, SQS and Google
  Pub/Sub subscriptions deliver each message to one reader and remove or hide it. Look for a side-effect-free route
  before giving up, and check it in the broker's documentation:
  - **RabbitMQ**: *streams* (`x-queue-type: stream`) can be read from any offset without consuming: yes. For a
    classic queue, a queue of the tool's own bound to the same exchange gets a copy, but only for messages published
    after it is bound, and only if the user agrees to create it. That changes their broker, so ask first.
  - **Azure Service Bus**: *peek* browses a queue or subscription by sequence number without locking or removing
    anything, so `OBSERVE` looks possible. Confirm that peek also sees the dead-letter sub-queue.
  - **SQS**: only with an SNS topic in front, through a subscription of the tool's own. Otherwise no.
  - **Pub/Sub**: a subscription of the tool's own on the topic. Seek by time exists, but there are no offsets (see
    below).

  If there is no route, stop and report: "in-flight can't watch &lt;broker&gt; without taking messages from your
  services. A counts-only mode (queue depth and dead-letter depth from the management API) would need the observer
  extended first." Do not build that mode as part of an adapter.

- **Offsets must be `long`, only growing, per shard.** The ledger and its saved state count in them (see the
  "Offsets" section of `Broker.java`). Kafka partitions, RabbitMQ stream offsets, NATS JetStream stream sequences and
  Service Bus sequence numbers fit. Kinesis sequence numbers (128-bit, sparse) and Pub/Sub (none at all) do not. Pulsar
  needs checking: its `MessageId` is a ledger id and an entry id, which do not make one number in general; a
  per-partition index exists only when the cluster enables broker entry metadata. For a broker that does not fit, stop
  and report that the interface needs opaque positions first. Do not truncate or hash them.

Dense offsets matter only for `GROUP_LAG`. If offsets grow but skip numbers (for example after compaction), declare
`OBSERVE` without `GROUP_LAG`; lag then reads zero rather than wrong.

Tell the user which capabilities the adapter will declare and what that means in the console. Without `GROUP_LAG`
there is no lag figure, only what is stuck. Without `REDRIVE` the Redrive action fails with a clear message.

## 3. Map the broker's concepts

| in-flight | Means | Kafka | Find the broker's equivalent |
| --- | --- | --- | --- |
| stream | what a catalog calls a topic | topic | Pulsar topic, RabbitMQ stream, JetStream stream or subject |
| `Shard` | an ordered part of a stream, numbered from 0 | partition | Pulsar partition, a JetStream stream (one shard) |
| offset | a message's place in its shard | offset | |
| `Group` | an application that reads and remembers where it got to | consumer group | Pulsar subscription, JetStream durable consumer |
| `Commit` | a group's position on a shard, plus an optional note | committed offset and metadata | |
| `Header` | key and bytes, repeats allowed, in order | record header | message properties, AMQP headers |
| key | the message key, or null | record key | Pulsar key, routing key |
| timestamp | when the broker says it was written, epoch ms | record timestamp | publish time |

Dead letters matter to the product. Find where the broker puts failed messages (a dead-letter topic, a DLX) and which
headers it adds. If they differ from Kafka Connect's and Spring's, the catalog's `deadLetterHeaders` can name them, so
the adapter does not need to translate them.

## 4. Write the module

Copy the shape of `kafka/`:

```
<kind>/pom.xml                                         depends on broker + the broker's Java client
<kind>/src/main/java/inflight/<kind>/<Kind>Adapter.java     implements BrokerAdapter
<kind>/src/main/java/inflight/<kind>/<Kind>Broker.java      implements Broker (one session)
<kind>/src/main/java/inflight/<kind>/<Kind>Cursor.java      implements Cursor
<kind>/src/main/java/inflight/<kind>/<Kind>Writer.java      implements Writer        (only with REDRIVE)
<kind>/src/main/java/inflight/<kind>/<Kind>Auth.java        presets -> client settings
<kind>/src/main/resources/META-INF/services/inflight.broker.BrokerAdapter   one line: inflight.<kind>.<Kind>Adapter
```

Read the javadoc of `Broker`, `BrokerAdapter`, `Cursor` and `Writer` before writing each method. They are the
contract. The rules that are easiest to get wrong:

- `observe()` really has no side effects (step 2). If the client needs a subscription to read at all, make it
  non-durable or exclusive to the tool, never one of the user's. Name it so `groups()` can leave it out, the way
  Kafka's observing consumers use `Kafka.OBSERVER_GROUP_PREFIX`.
- `shards(streams)` returns nothing for streams that do not exist, rather than failing. Results are sorted by stream,
  then index.
- `ends` is the offset the *next* message will get, not the last one's.
- `offsetsAt(shards, at)` falls back to the shard's end when nothing was written at or after `at`.
- `committed(group)` is empty for a group that does not exist. Only throw when the broker fails.
- `Cursor.seek` *adds* shards and keeps the places of the shards it does not name. The observer relies on this when
  new topics appear.
- `Cursor.poll` throws `Cursor.Woken` after `wakeup()` from another thread. That is how the observer stops.
- `Cursor.commit` is only for cursors from `consume(group)`. An observing cursor throws if asked.
- `Writer.send` returns only once the broker has the message durably.
- Every broker failure is an `IllegalStateException` carrying the broker's own reason. The console shows it as it is.
- Methods for a capability the adapter does not declare throw `IllegalStateException` saying so (for example
  "RabbitMQ classic queues cannot be read without consuming them").

Profiles and credentials (`Profile`, `Secrets`, `BrokerAdapter.configure`):

- A profile's address is `Profile.address()`. In JSON it is still called `bootstrap`, for compatibility. Say in
  `addressHelp()` what it is for this broker (a service URL, a connection string).
- `presets()` lists the auth presets, the first being the default. `secretRoles()` lists the secret names they take.
  `problems(p)` checks what a preset needs. Never echo a secret back in a problem.
- `configure(p)` resolves secrets now with `Secrets.resolve` and keeps the client settings keyed by the address (see
  `Kafka.settings`). Sessions opened later on that address use them. Nothing else in the tool passes credentials
  around.
- Schema registries are broker-independent (`codec/`). Leave them alone.

## 5. Wire it into the build

1. Root `pom.xml`: add `<module><kind></module>` after `kafka`, and the module to `dependencyManagement`.
2. `cli/pom.xml`: add it as a dependency next to `kafka`. That is the only place adapters belong: the CLI assembles
   the app, and its shaded jar merges `META-INF/services` files.
3. Do **not** add it to `observer`, `discovery`, `recovery`, `server`, `codec` or `broker`. They are broker-neutral,
   and `maven-enforcer-plugin` fails the build if a Kafka library or the `kafka` module reaches them. If an adapter
   brings in a client that should be banned the same way, add it to the `broker-neutral` rule in the root pom.
4. Run `./mvnw verify`.

A profile chooses the adapter with `"broker": "<kind>"` (REST and MCP `connect`) or `connect --broker <kind>` (CLI).
`watch` and `rehearse` without a profile use `INFLIGHT_BROKER`.

## 6. Test it

There is no shared conformance suite yet. Write `<kind>/src/test/java/inflight/<kind>/<Kind>BrokerIT.java`, tagged
`@Tag("e2e")`, that starts the broker with Testcontainers (see `cli/src/test/java/inflight/e2e/E2e.java` for how the
Kafka suites do it) and checks each rule below against the real broker. These are the promises the rest of the tool
depends on:

- [ ] **No side effects.** Write 10 messages. A separate real consumer group of the broker's own reads 3 and commits.
      Then `observe()` reads all 10. The other group still gets messages 4 to 10, in order, none marked redelivered,
      and its committed position is unchanged.
- [ ] `streams()` lists a created stream; `shards()` returns its shards sorted, and nothing for a missing stream.
- [ ] `starts`/`ends` before and after writing n messages: `ends` grows by exactly n (with `GROUP_LAG`) or at least
      grows (without).
- [ ] `offsetsAt` with a time before the first message gives the start; with a time in the future, the end.
- [ ] `seek` then `poll` returns messages from exactly that offset, with key, value, headers (repeats kept) and timestamp
      as written. A second `seek` on another shard keeps the first shard's place.
- [ ] `wakeup()` from another thread makes a blocked `poll` throw `Cursor.Woken`.
- [ ] `groups()` lists a real group with its members, and never the tool's own observing readers.
- [ ] `committed(group)` matches what that group committed; it is empty for an unknown group.
- [ ] With `REDRIVE`: `consume(g)` + `commit` with a note, then `committed(g)` returns the offset and the note;
      `writer().send` lands a message that `observe()` then reads back.
- [ ] `BrokerAdapter.describe` answers for a live broker and throws with a reason for a dead address.
- [ ] Unit test (no Docker) for `problems` and the presets, like `kafka/src/test/java/inflight/kafka/ProfileTest.java`.

Run them with `./mvnw verify -Pe2e -pl <kind> -am`.

Then try it through the product, on a broker with a small flow on it:

```sh
java -jar cli/target/in-flight.jar connect dev <address> --broker <kind> --test
java -jar cli/target/in-flight.jar connect dev <address> --broker <kind>
java -jar cli/target/in-flight.jar discover
java -jar cli/target/in-flight.jar sample <stream>
java -jar cli/target/in-flight.jar draft <flow> <stream>...
```

Then activate the flow in the console (`java -jar cli/target/in-flight.jar`), break a consumer so it dead-letters,
and check that value at risk climbs, that the stuck events name the right customers and errors, and that a redrive dry
run reports what it would write.

## 7. What stays Kafka-only

Tell the user these won't work on the new broker, and don't try to adapt them as part of the adapter:

- **Intents** (`router/`, Kafka Streams) and **the schema gate** (`proxy-filter/`, a Kroxylicious proxy for Kafka's
  protocol).
- **The sample data** (`simulator/`, `in-flight try`, `in-flight up`): a Kafka shop.
- **The console's Connect screen** is still Kafka-shaped. `ui/src/setup/Connect.tsx` hardcodes which secrets and
  settings each preset takes. It keeps a profile's `broker` when editing, but cannot pick one. `/api/presets?broker=<kind>`
  already returns an adapter's presets and the list of kinds, so a broker picker is UI work only. Until then, connect
  with the CLI or MCP `connect`, which both take a `broker`.

## 8. Report

Finish with what the adapter declares and what that means for the user in plain words (for example: "Stuck money and
redrive work; the lag figure is off because this broker's offsets skip numbers"), which test rules passed against a
real broker, and anything you stopped on in step 2.
