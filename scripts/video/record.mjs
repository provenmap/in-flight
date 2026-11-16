/* global window */
// The usage video (docs/VIDEO.md): one continuous recording of the real app set up on someone's shop, with callouts.
// Run by scripts/video/take.sh, which first starts a fresh Kafka (9592), registry (8580) and shop (8099).
// Usage: node scripts/video/record.mjs <work dir>: the raw recording goes to <work dir>/take, the app's workspace to
// <work dir>/home, and the app's output, replayed in the terminal scene, to <work dir>/app.ndjson.
import { spawn } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const REPO = fileURLToPath(new URL('../..', import.meta.url)).replace(/\/$/, '');
const HERE = `${process.argv[2].replace(/\/$/, '')}/`;
const out = `${HERE}take`;
const APP = 'http://localhost:8088', SHOP = 'http://localhost:8099';
const W = 1440, H = 900;
const overlay = readFileSync(`${REPO}/scripts/video/overlay.js`, 'utf8');
const sleep = ms => new Promise(r => setTimeout(r, ms));
let v0 = Date.now();
const marks = [];
const mark = name => { marks.push({ name, t: +((Date.now() - v0) / 1000).toFixed(1) }); console.log(`${((Date.now() - v0) / 1000).toFixed(1)}s ${name}`); };
const shop = path => fetch(`${SHOP}/${path}`, { method: 'POST' });

// The app, started as the terminal will show it, on an empty workspace; its output kept as it ran.
const app = spawn('node', [`${REPO}/scripts/video/capture.mjs`, `${HERE}app.ndjson`, REPO, '--', 'bin/in-flight', '--no-open'],
  { stdio: 'ignore', env: { ...process.env, INFLIGHT_HOME: `${HERE}home` } });
for (let i = 0; i < 120; i++) {
  if ((await fetch(`${APP}/api/status`).then(r => r.ok, () => false))) break;
  await sleep(500);
}
await sleep(1000);

const browser = await chromium.launch();
const context = await browser.newContext({ viewport: { width: W, height: H }, recordVideo: { dir: out, size: { width: W, height: H } }, colorScheme: 'dark' });
const page = await context.newPage();
v0 = Date.now();
const errors = [];
page.on('pageerror', e => errors.push(e.message));
page.on('console', m => { if (m.type() === 'error' && !m.text().includes('WebSocket')) errors.push(m.text()); });
const install = () => page.evaluate(overlay);
const step = t => page.evaluate(t => window.__v.step(t), t);
const clear = () => page.evaluate(() => window.__v.clear());
const words = html => html.replace(/<[^>]+>/g, ' ').split(/\s+/).filter(Boolean).length;
const read = html => Math.max(2200, words(html) * 220 + 900);
async function note(sel, html, side = 'bottom', o = {}) {
  if (sel && !sel.startsWith('text:')) await page.locator(sel.split(' >> up ')[0]).first().scrollIntoViewIfNeeded().catch(() => {});
  await page.evaluate(n => window.__v.note(n), { sel, html, side, ...o });
}
async function say(sel, html, side, o = {}) { await note(sel, html, side, o); await sleep(o.hold ?? read(html)); }
// How fast the cursor glides, keys are typed, and the page is let settle after a click: PACE where the viewer is
// following a step, FORM while filling in a form, where the values matter and the typing doesn't.
const PACE = { glide: 700, key: 32, settle: 350 }, FORM = { glide: 420, key: 18, settle: 180 };
async function point(loc, o = PACE) {
  await loc.scrollIntoViewIfNeeded();
  const b = await loc.boundingBox();
  const x = b.x + b.width / 2, y = b.y + b.height / 2;
  await page.evaluate(([x, y, ms]) => window.__v.cursor(x, y, ms), [x, y, o.glide]);
  await sleep(o.glide);
  return [x, y];
}
async function click(target, o = PACE) {
  const loc = typeof target === 'string' ? page.locator(target).first() : target;
  const [x, y] = await point(loc, o);
  await page.evaluate(([x, y]) => window.__v.click(x, y), [x, y]);
  await loc.click();
  await sleep(o.settle);
}
async function type(css, text, o = FORM) {
  await click(css, o);
  await page.locator(css).first().fill('');
  await page.locator(css).first().pressSequentially(text, { delay: o.key });
  await sleep(o.settle * 0.7);
}
const tid = t => `[data-testid="${t}"]`;
async function until(fn, ms = 30_000, what = '') {
  for (const end = Date.now() + ms; Date.now() < end; await sleep(250)) if (await fn().catch(() => false)) return true;
  console.log(`until timed out: ${what}`); return false;
}
const text = async t => (await page.locator(tid(t)).first().innerText().catch(() => '')) ?? '';

// The preview the video opens on: the console as it ends up, the money climbing, with one bubble. A second window
// records it while the main take carries on, so it costs the take no time; take.sh puts it first, from teaser.json.
const TEASER = 4.5;
async function teaser() {
  const ctx = await browser.newContext({ viewport: { width: W, height: H }, recordVideo: { dir: `${HERE}teaser`, size: { width: W, height: H } }, colorScheme: 'dark' });
  const p = await ctx.newPage();
  const t0 = Date.now();
  try {
    await p.goto(APP);
    await p.locator(tid('at-risk')).waitFor({ timeout: 30_000 });
    await p.evaluate(overlay);
    await p.evaluate(() => { window.__v.hideCursor(true); window.__v.step('Preview'); });
    for (const end = Date.now() + 30_000; Date.now() < end; await sleep(250)) {
      if (/\$[\d,.]+ for \d+ s/.test(await p.locator(tid('stuck')).first().innerText().catch(() => ''))) break;
    }
    await p.evaluate(n => window.__v.note(n), { sel: `${tid('at-risk')} >> up [data-slot=card]`, side: 'bottom', dx: -150, width: 440,
      html: 'By the end of this video: <b>the money stuck in your Kafka</b>, counted live.' });
    const start = +((Date.now() - t0) / 1000 + 0.5).toFixed(2);
    await sleep((TEASER + 1) * 1000);
    const video = p.video();
    await ctx.close();
    writeFileSync(`${HERE}teaser.json`, JSON.stringify({ file: await video.path(), start, dur: TEASER }));
    console.log(`teaser recorded from ${start}s`);
  } catch (e) {
    console.log(`teaser skipped: ${String(e.message).split('\n')[0]}`);
    await ctx.close().catch(() => {});
  }
}

// How it is built: a few seconds of the app's public architecture board on ProvenMap, before the end card. Like the
// preview, a second window records it while the take carries on (the page's load and its cookie banner stay off
// camera), and take.sh splices it in at the `board` mark from board.json. If provenmap.com does not answer, the video
// goes on without it.
const BOARD_URL = 'https://provenmap.com/boards/paws/in-flight-the-money-moving-through-your-kafka?view=canvas';
const BOARD = 4.8;
async function board() {
  const ctx = await browser.newContext({ viewport: { width: W, height: H }, recordVideo: { dir: `${HERE}board`, size: { width: W, height: H } }, colorScheme: 'dark' });
  const p = await ctx.newPage();
  const t0 = Date.now();
  try {
    await p.goto(BOARD_URL, { waitUntil: 'domcontentloaded', timeout: 20_000 });
    await p.getByRole('button', { name: 'Deny' }).click({ timeout: 8000 }).catch(() => {});
    const engine = '.react-flow__node:has-text("In Flight server and engine")';
    await p.locator(engine).first().waitFor({ timeout: 20_000 });
    await p.waitForLoadState('networkidle', { timeout: 10_000 }).catch(() => {});
    await sleep(800);
    await p.evaluate(overlay);
    await p.evaluate(() => { window.__v.hideCursor(true); window.__v.step('How it is built'); });
    const nodeId = await p.locator(engine).first().getAttribute('data-id');
    await p.evaluate(n => window.__v.note(n), { sel: `.react-flow__node[data-id="${nodeId}"]`, side: 'right', width: 380,
      html: 'Its architecture, <b>mapped on ProvenMap</b>.<small>The board is public.</small>' });
    const start = +((Date.now() - t0) / 1000 + 0.3).toFixed(2);
    await sleep((BOARD + 0.6) * 1000);
    const video = p.video();
    await ctx.close();
    return { file: await video.path(), start, dur: BOARD };
  } catch (e) {
    console.log(`board skipped: ${String(e.message).split('\n')[0]}`);
    await ctx.close().catch(() => {});
    return null;
  }
}

// ------------------------------------------------------------------ Why
const base = `*{box-sizing:border-box}body{margin:0;background:#0b0f17;color:#e6edf3;font-family:ui-sans-serif,system-ui,-apple-system,sans-serif;height:100vh;overflow:hidden}`;
await page.setContent(`<style>${base}
  h1{font-size:44px;text-align:center;margin:96px 0 0;font-weight:700} h1 em{color:#f87171;font-style:normal}
  .row{position:absolute;top:320px;left:0;right:0;display:flex;justify-content:center;align-items:center}
  .box{width:220px;height:110px;border-radius:16px;border:2px solid #334155;background:#111827;display:flex;flex-direction:column;align-items:center;justify-content:center;font-size:24px;font-weight:600;opacity:0;animation:in .5s forwards}
  .box small{font-size:15px;color:#94a3b8;font-weight:400;margin-top:6px}
  .arrow{width:150px;height:4px;background:#475569;position:relative;opacity:0;animation:in .4s forwards}
  .arrow:after{content:'';position:absolute;right:-2px;top:-8px;border:10px solid transparent;border-left:14px solid #475569}
  #a1{animation-delay:.6s} #b2{animation-delay:.9s} #a2{animation-delay:1.2s} #b3{animation-delay:1.5s}
  #b3{position:relative} #b3.bad{border-color:#f87171;background:#2a1215}
  .x{position:absolute;top:-22px;right:-22px;width:44px;height:44px;border-radius:50%;background:#ef4444;color:#fff;font-size:28px;display:flex;align-items:center;justify-content:center;opacity:0;animation:in .3s 2.6s forwards}
  .down{position:absolute;left:50%;top:112px;width:4px;height:70px;background:#ef4444;opacity:0;animation:in .3s 2.9s forwards}
  .dlq{position:absolute;left:50%;transform:translateX(-50%);top:190px;width:250px;border:2px dashed #ef4444;border-radius:14px;padding:10px;opacity:0;animation:in .3s 3s forwards}
  .dlq b{display:block;font-size:15px;color:#fca5a5;margin-bottom:6px;text-align:center}
  .card{font:13px ui-monospace,Menlo,monospace;background:#1f2937;border-radius:6px;padding:4px 8px;margin:4px 0;opacity:0;animation:in .2s forwards;display:flex;justify-content:space-between}
  .bubble{position:absolute;left:calc(50% + 380px);top:540px;width:330px;background:#facc15;color:#0b0f17;font-size:24px;font-weight:700;line-height:1.3;border-radius:18px;padding:18px 22px;opacity:0;animation:in .4s 4.4s forwards}
  .bubble:before{content:'';position:absolute;left:-26px;top:30px;border:14px solid transparent;border-right:16px solid #facc15}
  @keyframes in{to{opacity:1}}
</style>
<h1>A service on Kafka starts failing. <em>Orders quietly pile up.</em></h1>
<div class="row">
  <div class="box" id="b1">Checkout<small>places orders</small></div>
  <div class="arrow" id="a1"></div>
  <div class="box" id="b2">Kafka<small>carries them</small></div>
  <div class="arrow" id="a2"></div>
  <div class="box" id="b3">Fulfilment<small>ships them</small><span class="x">✕</span><span class="down"></span>
    <div class="dlq"><b>dead-letter queue</b>${Array.from({ length: 6 }, (_, i) => `<div class="card" style="animation-delay:${3.2 + i * 0.22}s"><span>ord-${10412 + i * 7}</span><span>$${[129.99, 48, 312.5, 75.2, 18.9, 240][i]}</span></div>`).join('')}</div>
  </div>
</div>
<div class="bubble">Nobody is looking here.<br>How much money is stuck?<br>Who is waiting?</div>
<script>setTimeout(() => document.getElementById('b3').classList.add('bad'), 2600)</script>`);
await install();
await page.evaluate(() => window.__v.hideCursor(true));
mark('why');
await sleep(7400);

await page.setContent(`<style>${base}
  .wrap{max-width:1100px;margin:110px auto 0;text-align:center}
  h1{font-size:64px;margin:0;font-weight:800;letter-spacing:-.02em} h2{font-size:26px;color:#94a3b8;font-weight:400;margin:14px 0 56px}
  .two{display:grid;grid-template-columns:1fr 1fr;gap:36px;text-align:left}
  .c{border:2px solid #334155;border-radius:20px;padding:28px 30px;background:#111827;opacity:0;transform:translateY(16px);animation:in .5s forwards}
  .c:nth-child(2){animation-delay:1.2s}
  .c .i{width:56px;height:56px;border-radius:14px;display:flex;align-items:center;justify-content:center;font-size:30px;font-weight:800;margin-bottom:16px}
  .c h3{font-size:30px;margin:0 0 10px} .c p{font-size:21px;line-height:1.45;color:#cbd5e1;margin:0}
  .next{margin-top:52px;font-size:24px;color:#facc15;font-weight:700;opacity:0;animation:in .5s 2.6s forwards}
  @keyframes in{to{opacity:1;transform:none}}
</style>
<div class="wrap"><h1>In Flight</h1><h2>The money moving through your Kafka, live</h2>
<div class="two">
  <div class="c"><div class="i" style="background:#1e3a8a;color:#bfdbfe">⌁</div><h3>Point it at your Kafka</h3><p>It finds your topics, services and dead-letter queues, and asks you only what the money is.</p></div>
  <div class="c"><div class="i" style="background:#7f1d1d;color:#fecaca">$</div><h3>See what is stuck, and fix it</h3><p>Which orders are held up, worth how much, and why. Replay them once the service is fixed.</p></div>
</div>
<div class="next">Here is how to use it →</div></div>`);
await install();
await page.evaluate(() => window.__v.hideCursor(true));
mark('what');
await sleep(5600);

// ------------------------------------------------------------------ 1 · Start
await page.setContent(`<style>${base}
  body{display:flex;align-items:center;justify-content:center;background:#0f172a}
  .win{width:1180px;height:600px;border-radius:14px;background:#0b0f17;border:1px solid #334155;box-shadow:0 30px 80px rgba(0,0,0,.6);overflow:hidden;display:flex;flex-direction:column}
  .bar{height:38px;background:#1e293b;display:flex;align-items:center;gap:8px;padding:0 14px;color:#94a3b8;font-size:14px}
  .bar i{width:12px;height:12px;border-radius:50%;display:inline-block} .bar span{margin-left:auto;margin-right:auto}
  pre{margin:0;padding:18px 22px;font:19px/1.55 ui-monospace,SFMono-Regular,Menlo,monospace;color:#e2e8f0;white-space:pre;overflow:hidden;flex:1}
  .p{color:#22d3ee;font-weight:700} .d{color:#4ade80;font-weight:700} .dim{color:#64748b} .ok{color:#4ade80} .cur{background:#e2e8f0;color:#0b0f17}
  .line{display:block;overflow:hidden;text-overflow:ellipsis}
</style>
<div class="win"><div class="bar"><i style="background:#ef4444"></i><i style="background:#f59e0b"></i><i style="background:#22c55e"></i><span>in-flight — zsh</span></div><pre id="t"></pre></div>
<script>
  const t = document.getElementById('t');
  window.type_ = async (cmd, id) => { const s = document.createElement('span'); s.className = 'line'; s.id = id; s.innerHTML = '<span class="p">in-flight</span> <span class="d">$</span> <span class="cmd"></span><span class="cur"> </span>'; t.appendChild(s); const c = s.querySelector('.cmd'); for (const ch of cmd) { c.textContent += ch; await new Promise(r => setTimeout(r, 55)); } s.querySelector('.cur').remove(); };
  window.out_ = (text, id) => { for (const l of text.replace(/\\x1b\\[[0-9;?]*[A-Za-z]/g, '').split('\\n')) { if (!l.trim()) continue; const s = document.createElement('span'); s.className = 'line' + (/console is on/.test(l) ? ' ok' : / (WARN|INFO) /.test(l) ? ' dim' : ''); s.textContent = l; if (id && l.includes(id.match)) s.id = id.id; t.appendChild(s); } };
</script>`);
await install();
await page.evaluate(() => window.__v.hideCursor(true));
await step('1 · Start it');
mark('start');
await page.evaluate(() => window.type_('bin/in-flight', 'cmd'));
for (const c of readFileSync(`${HERE}app.ndjson`, 'utf8').trim().split('\n').map(l => JSON.parse(l))) {
  if (c.d === undefined) continue;
  await sleep(120);
  await page.evaluate(([d]) => window.out_(d, { match: 'console is on', id: 'open-line' }), [c.d]);
  if (c.d.includes('console is on')) break;
}
await say('#cmd', 'One command starts it. You need Java 25, or use the Docker image.', 'bottom', { dx: 260, hold: 3000 });
await clear();
await say('#open-line', 'It opens in your browser. Nothing is set up yet, so it starts on <b>Connect</b>.', 'bottom', { dx: -120, hold: 3200 });
await clear();

// ------------------------------------------------------------------ 2 · Connect
await page.goto(APP);
await install();
await page.locator(tid('connect-form')).waitFor({ timeout: 30_000 });
await step('2 · Connect');
mark('connect');
await page.evaluate(() => window.__v.hideCursor(false));
await type(tid('connect-name'), 'staging');
await type(tid('connect-bootstrap'), 'localhost:9592');
await note(tid('connect-auth'), 'Pick how your cluster signs in. Passwords stay in env vars or the keychain, never here.', 'right', { width: 380 });
await sleep(2900);
await clear();
await type(tid('connect-registry'), 'http://localhost:8580');
await note(tid('connect-allow-writes'), 'Off by default: it only reads. Allow writes so it can replay dead letters later.', 'right', { width: 380 });
await sleep(1500);
await click(tid('connect-allow-writes'), FORM);
await sleep(1000);
await clear();
await click(tid('connect-test'), FORM);
await until(async () => (await text('connect-result')).includes('Connected'), 30_000, 'connect');
await say(tid('connect-result'), 'Tested before it is saved.', 'right', { hold: 2000 });
await clear();
await click(tid('connect-save'), FORM);

// ------------------------------------------------------------------ 3 · Discover
await page.locator(tid('discover-view')).waitFor({ timeout: 30_000 });
await until(async () => (await text('discover-topics')).includes('shop.orders'), 30_000, 'topics');
await step('3 · Discover');
mark('discover');
await sleep(600);
await say(tid('discover-topics'), 'It found the shop\'s topics, and which service reads each one.', 'right', { width: 360 });
await clear();
await say(tid('discover-dlqs'), 'And its dead-letter queue, with the reason it thinks so.', 'right', { width: 360 });
await clear();
await click(tid('topic-shop.orders'));
await until(async () => (await text('sample-fields')).includes('total_cents'), 30_000, 'sample');
await say(tid('sample-fields'), 'Pick a topic to see its fields. Values stay masked.', 'left', { width: 340 });
await clear();
await type(tid('flow-name'), 'shop');
await click(tid('draft-button'));

// ------------------------------------------------------------------ 4 · Confirm
await page.locator(tid('confirm-view')).waitFor({ timeout: 60_000 });
await step('4 · Confirm');
mark('confirm');
await sleep(800);
const GUESS = '[data-testid^="guess-"]:not([data-testid$="-option"]):not([data-testid="guesses"])';
const money = page.locator(GUESS, { hasText: 'total_cents the value of' }).first();
await money.evaluate(e => e.scrollIntoView({ block: 'center', behavior: 'smooth' }));
await sleep(700);
await note('text:Is total_cents the value of ShopOrders', 'It guessed what the money is: <b>total_cents</b>, in cents. Each guess says how sure it is.', 'right', { width: 380 });
await sleep(3600);
await clear();
const dlqQ = page.locator(GUESS, { hasText: 'dead-letter what it cannot read' }).first();
if (await dlqQ.count()) {
  await dlqQ.evaluate(e => e.scrollIntoView({ block: 'center', behavior: 'smooth' }));
  await sleep(600);
  await note('text:dead-letter what it cannot read', 'Answer what it isn\'t sure of with one click.', 'right', { width: 340 });
  await sleep(1800);
  await click(dlqQ.locator('[data-testid$="-option"]').first(), FORM);
  await sleep(600);
  await clear();
}
const teams = page.locator('[data-testid="guess-districts-option"]').first();
if (await teams.count()) {
  await click(teams, FORM);
  await sleep(400);
}
await note(tid('activate-button'), 'Then start watching. No JSON written.', 'top');
await sleep(2000);
await click(tid('activate-button'));
await clear();

// ------------------------------------------------------------------ 5 · Watch
await page.locator(tid('connection')).filter({ hasText: 'Live' }).waitFor({ timeout: 60_000 });
await step('5 · Watch');
mark('watch');
await page.evaluate(() => window.scrollTo({ top: 0, behavior: 'smooth' }));
await sleep(500);
await sleep(1000);
await say(`${tid('at-risk')} >> up [data-slot=card]`, '<b>Money at risk</b>: orders held up right now. A few in flight is normal. <b>Nothing stuck</b>: all is well.', 'bottom', { dim: true, dx: -150 });
await clear();
await say('[data-district]', 'Your services, as the money flows through them.', 'top', { dim: false, dx: -200, hold: 2600 });
await clear();
await shop('break');
mark('broken');
const preview = teaser();
await note('[data-district="fulfilment"]', 'Fulfilment starts failing: its warehouse API times out.', 'top', { dx: 120, width: 360 });
await until(async () => /\$[\d,.]+ for \d+ s/.test(await text('stuck')), 20_000, 'stuck');
await sleep(2600);
await clear();
await page.evaluate(() => window.scrollTo({ top: 0, behavior: 'smooth' }));
await sleep(500);
await say(tid('at-risk'), 'Every failed order is counted, live: how much, how many customers.', 'right', { hold: 3800, width: 360 });
await clear();
await click(tid('view-incidents'));
await sleep(500);
await say(tid('causes-view'), 'Grouped by why: one cause, every order it stopped.', 'left', { width: 340 });
await clear();

// ------------------------------------------------------------------ 6 · Act
await step('6 · Recover');
mark('act');
const boardClip = board();
await shop('fix');
await say(null, 'The fulfilment team fixes the service.<small>Now get the stuck orders moving again.</small>', 'bottom', { hold: 2800 });
await clear();
const q = 'fulfilment-service.DLT';
await click(tid(`redrive-dry-${q}`));
await page.locator(tid(`redrive-report-${q}`)).waitFor({ timeout: 30_000 });
await say(tid(`redrive-report-${q}`), '<b>Dry run</b> first: what it would replay, and where to.', 'left', { width: 340 });
await clear();
await click(tid(`redrive-run-${q}`));
await note(tid(`redrive-confirm-yes-${q}`), 'It asks before writing anything.', 'left', { width: 300 });
await sleep(1800);
await click(tid(`redrive-confirm-yes-${q}`));
await clear();
await until(async () => await page.locator(tid('recovered')).isVisible() && (await text('stuck')).includes('Nothing stuck'), 60_000, 'recovered');
mark('recovered');
await page.evaluate(() => window.scrollTo({ top: 0, behavior: 'smooth' }));
await sleep(500);
await sleep(300);
await say(tid('recovered'), 'Recovered: every order, and how long they were stuck.', 'right', { hold: 3600, width: 340 });
await clear();
await step('');

// ------------------------------------------------------------------ How it is built
// Nothing is shown here in the take: take.sh puts the board clip in at this mark.
const clip = await boardClip;
mark('board');
if (clip) {
  writeFileSync(`${HERE}board.json`, JSON.stringify({ ...clip, at: marks.at(-1).t }));
  console.log(`board recorded from ${clip.start}s, spliced in at ${marks.at(-1).t}s`);
}

// ------------------------------------------------------------------ End
await page.setContent(`<style>${base}
  .wrap{max-width:1380px;margin:110px auto 0} h1{font-size:52px;margin:0 0 40px;text-align:center}
  .r{display:grid;grid-template-columns:800px 60px 1fr;align-items:center;margin:16px 0;opacity:0;animation:in .4s forwards}
  .r code{white-space:nowrap;font:600 20px ui-monospace,Menlo,monospace;background:#111827;border:1px solid #334155;border-radius:10px;padding:12px 16px;color:#e2e8f0}
  .r .a{height:3px;background:#facc15;position:relative;margin:0 8px}.r .a:after{content:'';position:absolute;right:-4px;top:-7px;border:8px solid transparent;border-left:12px solid #facc15}
  .r span{font-size:22px;padding-left:14px;color:#cbd5e1}
  .r:nth-of-type(2){animation-delay:.3s}.r:nth-of-type(3){animation-delay:.6s}
  p{text-align:center;color:#94a3b8;font-size:21px;margin-top:44px;opacity:0;animation:in .4s 1.2s forwards} p b{color:#facc15}
  p.repo{margin-top:18px;font:600 26px ui-monospace,Menlo,monospace;animation-delay:1.5s} p.repo b{color:#e2e8f0}
  @keyframes in{to{opacity:1}}
</style>
<div class="wrap"><h1>Try it yourself</h1>
<div class="r"><code>bin/in-flight</code><i class="a"></i><span>connect your Kafka, in the browser</span></div>
<div class="r"><code>claude mcp add in-flight -- in-flight mcp</code><i class="a"></i><span>or let your agent set it up</span></div>
<div class="r"><code>bin/in-flight try</code><i class="a"></i><span>no Kafka handy? sample data</span></div>
<p>Step by step: <b>docs/guide.md</b></p>
<p class="repo"><b>github.com/provenmap/in-flight</b></p></div>`);
mark('end');
await sleep(5600);
mark('stop');

await preview;
const video = page.video();
await context.close();
await browser.close();
app.kill('SIGTERM');
console.log('VIDEO', await video.path());
console.log('MARKS', JSON.stringify(marks));
console.log('ERRORS', JSON.stringify(errors));
