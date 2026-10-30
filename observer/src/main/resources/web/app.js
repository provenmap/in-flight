// Connects to the observer, keeps the model, drives the HUD and controls, and picks the 3D or 2D view.
import { Model, money, moneyShort, size, heat } from './model.js';
import { View2D } from './view2d.js';

const $ = sel => document.querySelector(sel);
const stage = $('#stage');
const model = new Model();
let view = null;
let connected = false;
let shownRisk = 0;
let labels = { districts: {}, pile: null };

// ---- Choosing a view -------------------------------------------------------------------------------------------

function webglWorks() {
  try {
    const c = document.createElement('canvas');
    return !!(c.getContext('webgl2') || c.getContext('webgl'));
  } catch {
    return false;
  }
}

function preferred() {
  const asked = new URLSearchParams(location.search).get('view');
  if (asked === '2d' || asked === '3d') return asked;
  try { return localStorage.getItem('rif.view') ?? '3d'; } catch { return '3d'; }
}

async function useView(kind) {
  let next = null;
  if (kind === '3d' && webglWorks()) {
    try {
      const { View3D } = await import('./view3d.js');
      next = new View3D(stage, model);
    } catch (e) {
      console.warn('3D view unavailable, drawing in 2D:', e);
      toast('3D is unavailable here, so the view is drawn in 2D');
    }
  }
  next ??= new View2D(stage, model);
  view?.dispose();
  view = next;
  if (model.flow) view.setFlow(model.flow);
  view.setLabels(labels);
  document.querySelectorAll('.view-toggle button').forEach(b => b.classList.toggle('on', b.dataset.view === view.kind));
  document.body.dataset.view = view.kind;
}

document.querySelectorAll('.view-toggle button').forEach(b => b.addEventListener('click', () => {
  try { localStorage.setItem('rif.view', b.dataset.view); } catch { /* private mode: the choice just isn't kept */ }
  useView(b.dataset.view);
}));

// ---- The observer's stream -------------------------------------------------------------------------------------

function connect(attempt = 0) {
  const ws = new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws`);
  ws.onopen = () => { attempt = 0; };
  ws.onmessage = m => {
    const now = performance.now();
    for (const msg of JSON.parse(m.data)) {
      if (msg.type === 'snapshot') {
        model.snapshot(msg, now);
        view?.setFlow(model.flow);
        connected = true;
        setConnection('live');
        refresh();
      } else {
        model.apply(msg, now);
        if (msg.type === 'stats') refresh();
      }
    }
  };
  ws.onclose = () => {
    connected = false;
    setConnection('down');
    setTimeout(() => connect(attempt + 1), Math.min(5000, 500 * 2 ** attempt));
  };
}

function setConnection(state) {
  const el = $('.conn');
  el.classList.toggle('live', state === 'live');
  el.classList.toggle('down', state === 'down');
  el.querySelector('.conn-text').textContent = state === 'live' ? 'live from Kafka' : 'reconnecting to the observer';
}

// ---- HUD, labels and controls, refreshed on every stats message ------------------------------------------------

function count(n, noun) {
  return `${n.toLocaleString()} ${noun}${n === 1 ? '' : 's'}`;
}

function seconds(iso) {
  return Math.max(0, (Date.now() - Date.parse(iso)) / 1000);
}

function refresh() {
  const s = model.stats;
  if (!s) return;
  const sim = s.sim;

  const stuck = s.stuck.events > 0;
  $('.hud').classList.toggle('alarm', s.atRisk.value > 0 && stuck);
  $('[data-testid=at-risk-sub]').textContent = s.atRisk.orders
    ? `${count(s.atRisk.orders, 'order')} · ${count(s.atRisk.customers, 'customer')} waiting`
    : 'nothing held up';
  const stuckEl = $('[data-testid=stuck]');
  stuckEl.textContent = stuck ? `stuck ${money.format(s.stuck.value)} · ${seconds(s.stuck.since).toFixed(0)} s` : 'stuck $0';
  stuckEl.classList.toggle('hot', stuck);
  const lagEl = $('[data-testid=lag]');
  lagEl.textContent = s.lag.events ? `in lag ${money.format(s.lag.value)} · ${count(s.lag.events, 'event')}` : 'in lag $0';
  lagEl.classList.toggle('hot', s.lag.events > 0);

  const banner = $('[data-testid=recovered]');
  const ep = s.lastEpisode;
  banner.hidden = !(ep && !stuck);
  if (ep) banner.textContent = `Recovered ${money.format(ep.value)} · ${count(ep.events, 'order')} · stuck ${(ep.tookMs / 1000).toFixed(1)} s`;

  labels = labelsFor(s, sim);
  view?.setLabels(labels);
  controlsFor(s, sim);
}

function lagOf(s, group) {
  const l = s.lag.byGroup[group];
  return l && l.events ? [{ text: `lag ${l.events} · ${moneyShort.format(l.value)}`, tone: 'warn' }] : [];
}

function labelsFor(s, sim) {
  const stuckAt = s.stuck.byDistrict;
  const districts = {
    orders: {
      name: 'Orders',
      lines: sim ? [
        { text: `OrderPlaced v${sim.orderVersion}`, tone: sim.orderVersion > 1 ? 'warn' : '' },
        { text: `${sim.rate} orders/s` },
      ] : [{ text: 'simulator not running', tone: 'bad' }],
    },
    payments: {
      name: 'Payments',
      alarm: !!stuckAt.payments,
      lines: [
        ...(sim ? [{ text: sim.patched ? 'reads v1 and v2' : 'reads v1 only', tone: sim.patched ? 'good' : '' }] : []),
        ...(stuckAt.payments ? [{ text: `${moneyShort.format(stuckAt.payments)} stuck`, tone: 'bad' }] : []),
        ...lagOf(s, 'payments-service'),
      ],
    },
    inventory: {
      name: 'Inventory',
      alarm: !!stuckAt.inventory,
      lines: [{ text: 'reads items only' }, ...lagOf(s, 'inventory-service')],
    },
    shipping: {
      name: 'Shipping',
      alarm: !!stuckAt.shipping,
      lines: [
        ...(sim ? [{ text: `${sim.shippingWaiting} waiting for payment`, tone: sim.shippingWaiting > 40 ? 'warn' : '' }] : []),
        ...lagOf(s, 'shipping-service'),
      ],
    },
    customers: {
      name: 'Customers',
      lines: [{ text: `${moneyShort.format(model.delivered.value)} dispatched`, tone: 'good' }, { text: 'since you opened this' }],
    },
  };
  let pile = null;
  if (model.pile.length) {
    const district = model.pile[0].district;
    pile = {
      district,
      count: model.pile.length,
      name: stuck(s) ? `${model.flow?.dlqs.find(q => q.district === district)?.dlq ?? 'dead letters'} · ${s.stuck.events.toLocaleString()} · ${money.format(s.stuck.value)}` : 'redriving',
      lines: [],
      error: stuck(s) && model.lastError ? `${model.lastError.event}: ${model.lastError.error}` : null,
    };
  }
  return { districts, pile };
}

const stuck = s => s.stuck.events > 0;

let pending = null;

function controlsFor(s, sim) {
  const up = !!sim && connected;
  const set = (action, enabled) => { $(`[data-action=${action}]`).disabled = !up || !enabled || pending === action; };
  const redriving = sim?.redrive?.running;
  set('break', sim?.orderVersion === 1);
  set('restore', sim?.orderVersion !== 1);
  set('patch', !sim?.patched);
  set('unpatch', !!sim?.patched);
  set('redrive', !redriving);
  $('#rate').disabled = !up;
  if (sim && document.activeElement !== $('#rate')) {
    $('#rate').value = sim.rate;
    $('[data-testid=rate]').textContent = `${sim.rate}/s`;
  }

  let hint = '', next = null;
  if (!connected) hint = 'Waiting for the observer…';
  else if (!sim) hint = 'The simulator is not running. Start it and orders will flow.';
  else if (redriving) hint = `Redriving payments.dlq… ${sim.redrive.redriven} done, ${sim.redrive.remaining} to go`;
  else if (stuck(s) && !sim.patched) { hint = 'Payments cannot read OrderPlaced v2, so every new order is dead-lettered. Patch payments.'; next = 'patch'; }
  else if (stuck(s) && sim.patched) { hint = 'New orders flow again, but the dead letters are still stuck. Redrive the DLQ.'; next = 'redrive'; }
  else if (sim.orderVersion === 1 && !sim.patched) { hint = 'Orders are flowing. The orders team is about to ship OrderPlaced v2.'; next = 'break'; }
  else if (s.lastEpisode) hint = 'Recovered. Go back to v1 and unpatch to run it again.';
  $('[data-testid=hint]').textContent = hint;
  document.querySelectorAll('[data-action]').forEach(b => b.classList.toggle('suggested', b.dataset.action === next && !b.disabled));

  $('[data-testid=sim-state]').textContent = sim
    ? `OrderPlaced v${sim.orderVersion} · payments ${sim.patched ? 'patched' : 'strict'} · ${sim.placed.toLocaleString()} orders placed` +
      (sim.redrive.redriven || sim.redrive.error ? ` · last redrive: ${sim.redrive.redriven} redriven${sim.redrive.error ? `, stopped at ${sim.redrive.error}` : ''}` : '')
    : '';
}

async function act(action, query = '') {
  pending = action;
  if (model.stats) controlsFor(model.stats, model.stats.sim);
  try {
    const res = await fetch(`/api/sim/${action}${query}`, { method: 'POST' });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) toast(body.error ?? `${action} failed (${res.status})`);
  } catch (e) {
    toast(`${action} failed: ${e.message}`);
  } finally {
    pending = null;
  }
}

document.querySelectorAll('[data-action]').forEach(b => b.addEventListener('click', () => act(b.dataset.action)));
$('#rate').addEventListener('input', e => { $('[data-testid=rate]').textContent = `${e.target.value}/s`; });
$('#rate').addEventListener('change', e => act('rate', `?perSecond=${e.target.value}`));

let toastTimer = null;
function toast(text) {
  const el = $('.toast');
  el.textContent = text;
  el.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { el.hidden = true; }, 5000);
}

function legend() {
  const row = $('.legend-row');
  for (const v of [10, 100, 1000, 10000]) {
    const d = Math.round(size(v) * 22);
    const [r, g, b] = heat(v).map(x => Math.round(x * 255));
    const span = document.createElement('span');
    span.innerHTML = `<b style="width:${d}px;height:${d}px;background:rgb(${r},${g},${b});box-shadow:0 0 ${d / 2}px rgb(${r},${g},${b})"></b>${moneyShort.format(v)}`;
    row.appendChild(span);
  }
}

// ---- The frame loop ---------------------------------------------------------------------------------------------

let frames = 0;
let last = performance.now();
let wasRecovering = false;
function loop(now) {
  model.tick(now);
  view?.frame(now);
  const pending = model.pendingRecovery(now);
  const target = (model.stats?.atRisk.value ?? 0) + pending.value;
  if (pending.orders) $('[data-testid=at-risk-sub]').textContent = `recovering ${count(pending.orders, 'order')}`;
  else if (wasRecovering) refresh();
  wasRecovering = pending.orders > 0;
  const dt = Math.min(0.1, (now - last) / 1000);
  last = now;
  shownRisk += (target - shownRisk) * Math.min(1, dt * 6);
  if (Math.abs(target - shownRisk) < 0.005) shownRisk = target;
  $('[data-testid=at-risk]').textContent = money.format(shownRisk);
  frames++;
  requestAnimationFrame(loop);
}

// For the browser test and for poking at it from the console.
window.rif = {
  model,
  get view() { return view?.kind; },
  get frames() { return frames; },
  get particles() { return model.particles.length; },
  get blocks() { return model.pile.length; },
};

legend();
await useView(preferred());
connect();
requestAnimationFrame(loop);
