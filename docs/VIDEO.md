# The usage video

Written 2026-11-13. Status: made the same day, once [PRODUCT.md](PRODUCT.md) was built: [usage.mp4](usage.mp4), 1:52,
recorded on the real app against a sample shop, never the simulator. Two earlier takes on the demo were dropped.
Remade 2026-11-14 to open on a preview of the end result, with the forms filled in faster to keep it under two minutes.
`npm run docs:video` remakes it from scratch.

## How to remake it

```sh
npm run build && ./mvnw package     # the jar the video records
npm run docs:video                  # about three minutes; writes docs/usage.mp4
```

[scripts/video/take.sh](../scripts/video/take.sh) starts its own Kafka and Apicurio ([stack.yml](../scripts/video/stack.yml),
ports 9592 and 8580), runs the sample shop ([Shop.java](../scripts/video/Shop.java): JSON orders in cents, Avro payments
through the registry, a fulfilment service that breaks on `POST :8099/break`), starts the app on an empty workspace, and
lets [record.mjs](../scripts/video/record.mjs) drive it, callouts and all. Everything it starts is removed afterwards.
It refuses to start while 9592, 8580, 8099 or 8088 is taken. To change what a callout says or how long it stays,
edit `record.mjs`; the storyboard below is what it follows.

## What it is for

Someone who has never seen this project watches it and can answer three questions:

1. **What is it for?** The money held up in their Kafka, in business terms, and changes shipped without a jam.
2. **Where do I start?** The install line and the first command.
3. **How do I use it?** Connect, confirm, watch, act: where to look on each screen, what to click, and why.

At most **two minutes**. **No voice:** every explanation is on-screen text, in callouts with arrows pointing at the
thing they explain. It is **a real screen recording** of the app, not screenshots stitched together.

## When

After PRODUCT.md phase 3, when Connect, Discover and Confirm exist. It is recorded against the **sample application**
from that plan's tests (a producer and consumers that know nothing of this tool), never the simulator. A second,
shorter video on shipping a change safely can follow phase 6.

## Storyboard

| Time | Step | On screen | Callouts say |
| --- | --- | --- | --- |
| 0:00 | Preview | The console as it ends up, recorded live: money at risk climbing, Fulfilment dead-lettering | "By the end of this video: the money stuck in your Kafka, counted live" |
| 0:04 | Why | An illustration: a team ships a new event version, Kafka carries it, the next service fails, orders pile up in a dead-letter queue | "Orders pile up here, unpaid. What is it costing? Who should have known?" |
| 0:12 | What | Two cards: see the money stuck; ship changes without the jam | "Here is how to use it" |
| 0:20 | 1 · Start | A terminal: the install line, then `revenue-in-flight`; the console opens | What each command does; "it opens in your browser" |
| 0:35 | 2 · Connect | The Connect screen: bootstrap servers, an auth preset, the registry; Test, then Save | Where secrets come from (a reference, never pasted) |
| 0:55 | 3 · Discover | Topics, consumer groups, dead-letter queues found, with why each was recognised | "It found your flow; nothing was written to your cluster" |
| 1:10 | 4 · Confirm | The draft's questions: which field is the money, the order, the customer | One bubble per question, pointing at the choice |
| 1:25 | 5 · Watch | The live view: money at risk, the map, then the sample consumer breaks and money climbs; the dead letters say why | Money at risk; a team's service; why it failed |
| 1:45 | 6 · Act | Redrive the queue once the consumer is fixed; the counter drains | "Dry run first, then redrive" |
| 1:52 | End | Try it yourself: the install line, `revenue-in-flight`, `claude mcp add revenue-in-flight -- revenue-in-flight mcp`, the guide | |

## How it looks

- A step label at the top centre (`2 · Connect`).
- For each point, a yellow bubble with a curved arrow to the element, and a pulsing ring around it. Dim the rest of
  the page when a whole panel is meant. One idea per bubble.
- Each bubble stays up for `max(2.6 s, 0.25 s × words + 1.1 s)`. Fit the two minutes by cutting words, not by
  speeding the video up.
- Forms are filled in faster than other steps are shown (`FORM` in `record.mjs`: a 0.42 s glide, 18 ms a key), because
  the values matter and the typing doesn't. Elsewhere the cursor glides in 0.7 s (`PACE`).
- A visible cursor that glides to what is clicked, with a ripple on the click.
- Dark theme, 1440 × 900, H.264 MP4 (`crf 22`, `yuv420p`, `+faststart`), about 5 MB.
- No callout claims something the screen doesn't show at that moment: "nothing stuck" only while the headline says so.

## How it is made

- **One continuous Playwright recording** (`recordVideo`) drives the app with real clicks and waits for real state
  through the `data-testid`s, the way the browser tests do.
- **[scripts/video/overlay.js](../scripts/video/overlay.js)** draws the step label, bubbles, arrows, rings and cursor
  over any page. Install it with `page.evaluate(source)` after every navigation and every `setContent`.
- **The terminal:** the commands are really run, their output captured with timestamps by
  [scripts/video/capture.mjs](../scripts/video/capture.mjs), and replayed in a terminal window in the recording with
  long waits shortened. A live web terminal (`ttyd`) was refused in the session that tried it, because it serves a
  writable shell over HTTP. A recorder such as VHS would also do.
- **The preview** it opens on is recorded by a second browser window that opens the console once the shop breaks,
  while the main take carries on, so it costs the take no time. `record.mjs` writes where the clip starts to
  `teaser.json`; without it (the console never showed money stuck), `take.sh` encodes the take alone.
- **ffmpeg** drops the first frame (white before the page paints), puts the preview first with a half-second fade into
  the take, and encodes.
- **Check a take with one contact sheet** (`ffmpeg … tile=3x4`), not a dozen screenshots; long image-heavy sessions
  freeze the VS Code panel.
- **A private stack** on its own ports (`docker compose -p rif-video`, Kafka 9592, registry 8580, proxy 9692), so no
  one else's Kafka is touched, torn down after each take.

## Learned from the parked takes

- **`page.setContent` keeps the same `window`.** An overlay guarded by a window global thinks it is installed after
  its styles were wiped. Guard it on an element in the document (`overlay.js` does).
- **Overlay elements must be `position: fixed`.** Otherwise they become flex items of the page and squeeze its layout.
- **Every take needs a fresh stack and app state.** The demo left the router at revision 2 with the change approved,
  and the second take failed on a disabled button.
- **Wait for consumers to catch up before changing what they read.** Going back to v1 and unpatching at once
  dead-lettered v2 orders still in flight, and the next scene showed money stuck where the callout said none.
- **The Changes view's two-run card undercounts if you leave the view.** PRODUCT.md phase 0 fixes it.
- **Takes ran 93 to 149 seconds.** The waits on real state (proposing, redriving) are the slack to watch.
- **Voice was tried and dropped** at the owner's request: on-screen text only.
- **Callouts must let clicks through** (`pointer-events: none`), or a ring over a button swallows the click.
- **Inputs come prefilled** (the profile's name, the flow's name): clear before typing.
- **Kill background Java by name, not by `$!`:** `cd … && nohup java … &` makes `$!` the subshell; and a script's
  background jobs ignore SIGINT, so stop them with SIGTERM. A shop left over from a previous take answered on the
  control port while its topics were gone, and nothing ever got stuck.
- **Don't claim $0 on a healthy flow:** real consumers commit every so often, so a few orders are always in lag and
  money at risk is rarely exactly zero. The callout says "Nothing stuck" instead.

## Done when

Someone who has never seen the project watches it once, then without help runs the first command, connects to a
cluster and reaches a watched flow.

## Decisions that are the owner's

1. **Where it lives:** in the repository (`docs/usage.mp4`, linked from the README), as a release asset, or on a site.
2. **Which cluster is on screen:** a local one, or a managed service named on screen (Confluent Cloud, MSK).
3. **A second video** on shipping a change safely, after phase 6, or one video only.
