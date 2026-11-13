/* global window, document, requestAnimationFrame, innerWidth, innerHeight */
// Annotations drawn over whatever page is being recorded: a step chip, callout bubbles with curved arrows to the thing
// they talk about, a ring around it, and a cursor that shows clicks. Installed with page.evaluate after each load.
(() => {
  if (document.getElementById('v-style')) return;
  const css = document.createElement('style'); css.id = 'v-style';
  css.textContent = `
    .v-layer{position:fixed;inset:0;pointer-events:none;z-index:2147483640}
    .v-step{position:fixed;top:18px;left:50%;transform:translateX(-50%);z-index:2147483646;background:#111827;color:#facc15;
      border:2px solid #facc15;border-radius:999px;padding:7px 18px;font:700 17px/1 ui-sans-serif,system-ui,-apple-system,sans-serif;
      letter-spacing:.02em;box-shadow:0 8px 24px rgba(0,0,0,.45);transition:opacity .3s}
    .v-ring{position:fixed;border:3px solid #facc15;border-radius:12px;z-index:2147483641;
      box-shadow:0 0 0 5px rgba(250,204,21,.22);animation:v-pulse 1.4s ease-in-out infinite;transition:opacity .3s}
    .v-ring.dim{box-shadow:0 0 0 5px rgba(250,204,21,.22),0 0 0 9999px rgba(2,6,15,.55)}
    @keyframes v-pulse{50%{box-shadow:0 0 0 11px rgba(250,204,21,.10)}}
    .v-ring.dim{animation:none}
    .v-bubble{position:fixed;z-index:2147483645;max-width:430px;background:#facc15;color:#0b0f17;border-radius:16px;
      padding:14px 18px;font:600 21px/1.35 ui-sans-serif,system-ui,-apple-system,sans-serif;box-shadow:0 14px 34px rgba(0,0,0,.5);
      opacity:0;transform:scale(.92);transition:opacity .3s,transform .3s}
    .v-bubble.in{opacity:1;transform:none}
    .v-bubble small{display:block;font-weight:500;font-size:16px;margin-top:6px;color:#3b3200}
    .v-bubble code{font:600 18px ui-monospace,SFMono-Regular,Menlo,monospace;background:rgba(0,0,0,.1);padding:1px 5px;border-radius:5px}
    .v-svg{position:fixed;inset:0;width:100vw;height:100vh;z-index:2147483644;pointer-events:none;overflow:visible}
    .v-cursor{position:fixed;left:0;top:0;z-index:2147483647;width:26px;height:26px;pointer-events:none;
      transition:transform .7s cubic-bezier(.4,0,.2,1),opacity .3s}
    .v-click{position:fixed;z-index:2147483646;width:16px;height:16px;margin:-8px 0 0 -8px;border-radius:50%;border:3px solid #facc15;
      pointer-events:none;animation:v-ripple .6s ease-out forwards}
    @keyframes v-ripple{to{transform:scale(4.5);opacity:0}}`;
  document.head.appendChild(css);
  const svgNS = 'http://www.w3.org/2000/svg';
  const svg = document.createElementNS(svgNS, 'svg'); svg.classList.add('v-svg');
  svg.innerHTML = `<defs><marker id="v-head" viewBox="0 0 10 10" refX="7" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse">
    <path d="M0,0 L10,5 L0,10 z" fill="#facc15"/></marker></defs>`;
  document.body.appendChild(svg);
  const step = document.createElement('div'); step.className = 'v-step'; step.style.opacity = '0'; document.body.appendChild(step);
  const cursor = document.createElement('div'); cursor.className = 'v-cursor';
  cursor.innerHTML = `<svg viewBox="0 0 24 24" width="26" height="26"><path d="M3 2l7 19 2.6-7.6L20 11z" fill="#fff" stroke="#000" stroke-width="1.6" stroke-linejoin="round"/></svg>`;
  cursor.style.transform = 'translate(1200px,700px)';
  document.body.appendChild(cursor);

  const find = sel => {
    if (!sel) return null;
    if (sel.startsWith('text:')) {
      const want = sel.slice(5);
      const all = [...document.querySelectorAll('body *')].filter(e => !e.closest('.v-bubble,.v-step') && e.textContent.includes(want));
      return all.length ? all[all.length - 1] : null;
    }
    const [inner, outer] = sel.split(' >> up ');
    const el = document.querySelector(inner);
    return el && outer ? el.closest(outer) : el;
  };
  const notes = [];
  function place(n) {
    const el = find(n.sel);
    const b = n.bubble, W = innerWidth, H = innerHeight, gap = n.gap ?? 70, pad = n.pad ?? 6;
    const bw = b.offsetWidth, bh = b.offsetHeight;
    if (!el) { b.style.left = `${(W - bw) / 2}px`; b.style.top = `${(H - bh) / 2}px`; if (n.ring) n.ring.style.opacity = '0'; n.path.setAttribute('d', ''); return; }
    const r = el.getBoundingClientRect();
    const R = { l: r.left - pad, t: r.top - pad, r: r.right + pad, b: r.bottom + pad };
    if (n.ring) Object.assign(n.ring.style, { left: `${R.l}px`, top: `${R.t}px`, width: `${R.r - R.l}px`, height: `${R.b - R.t}px`, opacity: '1' });
    const cx = (R.l + R.r) / 2, cy = (R.t + R.b) / 2, clampX = x => Math.max(16, Math.min(W - bw - 16, x)), clampY = y => Math.max(64, Math.min(H - bh - 16, y));
    let x, y;
    if (n.side === 'left') { x = clampX(R.l - gap - bw); y = clampY(cy - bh / 2 + (n.dy ?? 0)); }
    else if (n.side === 'right') { x = clampX(R.r + gap); y = clampY(cy - bh / 2 + (n.dy ?? 0)); }
    else if (n.side === 'top') { x = clampX(cx - bw / 2 + (n.dx ?? 0)); y = clampY(R.t - gap - bh); }
    else { x = clampX(cx - bw / 2 + (n.dx ?? 0)); y = clampY(R.b + gap); }
    b.style.left = `${x}px`; b.style.top = `${y}px`;
    // The arrow: from the bubble's edge facing the target to the ring's facing edge.
    const bx = Math.max(x, Math.min(x + bw, cx)), by = Math.max(y, Math.min(y + bh, cy));
    const tx = Math.max(R.l, Math.min(R.r, bx)), ty = Math.max(R.t, Math.min(R.b, by));
    let sx = bx, sy = by;
    if (n.side === 'left') sx = x + bw; else if (n.side === 'right') sx = x; else if (n.side === 'top') sy = y + bh; else sy = y;
    const ex = n.side === 'left' ? R.l : n.side === 'right' ? R.r : tx, ey = n.side === 'top' ? R.t : n.side === 'bottom' ? R.b : ty;
    const mx = (sx + ex) / 2 + (n.side === 'left' || n.side === 'right' ? 0 : 40), my = (sy + ey) / 2 + (n.side === 'left' || n.side === 'right' ? -40 : 0);
    n.path.setAttribute('d', `M${sx},${sy} Q${mx},${my} ${ex},${ey}`);
  }
  (function loop() { notes.forEach(place); requestAnimationFrame(loop); })();

  window.__v = {
    step(text) { step.textContent = text; step.style.opacity = text ? '1' : '0'; },
    note(o) {
      const n = { ...o };
      n.bubble = document.createElement('div'); n.bubble.className = 'v-bubble'; n.bubble.innerHTML = o.html;
      if (o.width) n.bubble.style.maxWidth = `${o.width}px`;
      document.body.appendChild(n.bubble);
      if (o.ring !== false && o.sel) { n.ring = document.createElement('div'); n.ring.className = 'v-ring' + (o.dim ? ' dim' : ''); document.body.appendChild(n.ring); }
      n.path = document.createElementNS(svgNS, 'path');
      Object.entries({ fill: 'none', stroke: '#facc15', 'stroke-width': '4', 'stroke-linecap': 'round', 'marker-end': 'url(#v-head)' }).forEach(([k, v]) => n.path.setAttribute(k, v));
      if (!o.sel) n.path.style.display = 'none';
      svg.appendChild(n.path);
      notes.push(n); place(n);
      requestAnimationFrame(() => n.bubble.classList.add('in'));
    },
    clear() {
      for (const n of notes.splice(0)) { n.bubble.remove(); n.ring?.remove(); n.path.remove(); }
    },
    cursor(x, y) { cursor.style.transform = `translate(${x - 4}px,${y - 2}px)`; },
    hideCursor(h) { cursor.style.opacity = h ? '0' : '1'; },
    click(x, y) { const c = document.createElement('div'); c.className = 'v-click'; c.style.left = `${x}px`; c.style.top = `${y}px`; document.body.appendChild(c); setTimeout(() => c.remove(), 700); },
  };
})();
