// The same scene on a 2D canvas, for browsers without WebGL or on request. An oblique projection keeps the arcs.
import type { Labels, Label, Tone } from './labels';
import { PLACES, PILE, Model, arcPoint, particlePoint, pileSlot, size, heat, worth } from './model';
import type { Flow } from './protocol';

/** What both views do: take the flow and labels, draw a frame, and clean up. */
export interface SceneView {
  readonly kind: '2d' | '3d';
  setFlow(flow: Flow): void;
  setLabels(labels: Labels): void;
  frame(now: number): void;
  dispose(): void;
}

const BOUNDS = { minX: -21, maxX: 33, minZ: -22, maxZ: 15 };
const LIFT = 0.45; // how far up the screen one unit of height goes
const TONES: Record<Tone, string> = { bad: '#ff4d5e', warn: '#ff9f43', good: '#3ee08f', '': '#8a97b3' };

export class View2D implements SceneView {
  readonly kind = '2d' as const;
  private readonly canvas: HTMLCanvasElement;
  private readonly ctx: CanvasRenderingContext2D;
  private labels: Labels = { districts: {}, pile: null };
  private scale = 1;
  private ox = 0;
  private oy = 0;
  private readonly onResize = () => this.resize();

  constructor(private readonly container: HTMLElement, private readonly model: Model) {
    this.canvas = document.createElement('canvas');
    this.canvas.setAttribute('role', 'img');
    this.canvas.setAttribute('aria-label', 'Order-to-cash flow, drawn top-down');
    container.appendChild(this.canvas);
    this.ctx = this.canvas.getContext('2d')!;
    window.addEventListener('resize', this.onResize);
    this.resize();
  }

  setFlow(): void {}

  setLabels(labels: Labels): void {
    this.labels = labels;
  }

  resize(): void {
    const dpr = Math.min(window.devicePixelRatio, 2);
    const w = this.container.clientWidth, h = this.container.clientHeight;
    this.canvas.width = w * dpr;
    this.canvas.height = h * dpr;
    this.canvas.style.width = `${w}px`;
    this.canvas.style.height = `${h}px`;
    this.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    // Leave room for the HUD above and the controls below.
    const top = 150, bottom = 150;
    const sx = (w - 40) / (BOUNDS.maxX - BOUNDS.minX);
    const sz = Math.max(40, h - top - bottom) / (BOUNDS.maxZ - BOUNDS.minZ);
    this.scale = Math.min(sx, sz);
    this.ox = w / 2 - ((BOUNDS.minX + BOUNDS.maxX) / 2) * this.scale;
    this.oy = top + (h - top - bottom) / 2 - ((BOUNDS.minZ + BOUNDS.maxZ) / 2) * this.scale;
  }

  private at(x: number, y: number, z: number): [number, number] {
    return [this.ox + x * this.scale, this.oy + (z - y * LIFT) * this.scale];
  }

  frame(now: number): void {
    const { ctx, scale } = this;
    const w = this.container.clientWidth, h = this.container.clientHeight;
    ctx.fillStyle = '#070b16';
    ctx.fillRect(0, 0, w, h);

    // Arcs.
    ctx.lineWidth = 1.2;
    ctx.strokeStyle = 'rgba(44, 65, 112, 0.9)';
    ctx.font = '10.5px ui-monospace, Menlo, monospace';
    ctx.textAlign = 'center';
    for (const a of this.model.arcs) {
      const from = PLACES[a.from], to = PLACES[a.to];
      if (!from || !to) continue;
      ctx.beginPath();
      for (let i = 0; i <= 32; i++) {
        const p = arcPoint(from, to, i / 32);
        const [x, y] = this.at(p.x, p.y, p.z);
        if (i) ctx.lineTo(x, y);
        else ctx.moveTo(x, y);
      }
      ctx.stroke();
      const mid = arcPoint(from, to, 0.5);
      const [mx, my] = this.at(mid.x, mid.y + 0.6, mid.z);
      ctx.fillStyle = '#56627d';
      ctx.fillText(a.topic, mx, my);
    }

    // Districts.
    for (const d of this.model.districts) {
      const at = PLACES[d.id];
      if (!at) continue;
      const [x, y] = this.at(at.x, 0, at.z);
      const alarm = this.labels.districts[d.id]?.alarm;
      ctx.beginPath();
      if (d.sink) ctx.ellipse(x, y, 3.2 * scale, 3.2 * scale * (1 - LIFT * 0.5), 0, 0, Math.PI * 2);
      else for (let i = 0; i <= 6; i++) {
        const a = (Math.PI / 3) * i + Math.PI / 6;
        const px = x + Math.cos(a) * 3.4 * scale, py = y + Math.sin(a) * 3.4 * scale * (1 - LIFT * 0.5);
        if (i) ctx.lineTo(px, py);
        else ctx.moveTo(px, py);
      }
      ctx.fillStyle = d.sink ? '#0f1a33' : '#18223d';
      ctx.fill();
      ctx.lineWidth = d.sink ? 2.5 : 1.5;
      ctx.strokeStyle = alarm ? '#ff4d5e' : d.sink ? '#6ea8ff' : '#3d5a99';
      ctx.stroke();
    }

    // The pile.
    const index = new Map<string, number>();
    for (const b of this.model.pile) {
      const i = index.get(b.district) ?? 0;
      index.set(b.district, i + 1);
      const slot = pileSlot(b.district, i);
      const age = now - b.born;
      const g = Model.greenness(b, now);
      let y = slot.y + (age < 600 ? 7 * Math.pow(1 - age / 600, 2) : 0);
      if (g > 0) y += 3 * Math.sin(g * Math.PI);
      const [x, py] = this.at(slot.x, y, slot.z);
      const side = 0.44 * scale * (1 - g * 0.85);
      ctx.globalAlpha = 1 - g * 0.6;
      ctx.fillStyle = b.greenAt && now >= b.greenAt ? '#3ee08f' : age < 700 ? '#ff8a95' : '#e0303f';
      ctx.fillRect(x - side / 2, py - side / 2, side, side);
      ctx.strokeStyle = '#070b16';
      ctx.lineWidth = 1;
      ctx.strokeRect(x - side / 2, py - side / 2, side, side);
    }
    ctx.globalAlpha = 1;

    // Particles, with a glow that grows with value.
    for (const part of this.model.particles) {
      const p = particlePoint(part, now);
      if (!p) continue;
      let r = size(part.value) * scale;
      if (p.t > 0.9) r *= (1 - p.t) * 10;
      const [x, y] = this.at(p.x, p.y, p.z);
      const rgb = part.kind === 'redrive' ? [0.24, 1, 0.56] : part.kind === 'declined' ? [1, 0.55, 0.15] : heat(part.value);
      const col = `rgb(${rgb.map(v => Math.round(v * 255)).join(',')})`;
      ctx.shadowColor = col;
      ctx.shadowBlur = 4 + 14 * (part.kind === 'flow' ? worth(part.value) : 0.6);
      ctx.globalAlpha = part.kind === 'declined' ? 1 - p.t : 1;
      ctx.fillStyle = col;
      ctx.beginPath();
      ctx.arc(x, y, Math.max(r, 0.5), 0, Math.PI * 2);
      ctx.fill();
    }
    ctx.shadowBlur = 0;
    ctx.globalAlpha = 1;

    // Labels.
    for (const d of this.model.districts) {
      const at = PLACES[d.id], l = this.labels.districts[d.id];
      if (!at || !l) continue;
      const [x, y] = this.at(at.x, 0, at.z + 4.4);
      this.text(l, x, y);
    }
    const pile = this.labels.pile;
    if (pile) {
      // Above the back row of the pile, growing upward so it never covers the blocks.
      const back = pileSlot(pile.district, (PILE.rows - 1) * PILE.cols);
      const top = pileSlot(pile.district, Math.max(0, pile.count - 1));
      const [x, y] = this.at((PLACES[pile.district] ?? PLACES.payments!).x + PILE.dx, top.y + 1, back.z - 0.6);
      const height = 14 * ((pile.lines?.length ?? 0) + (pile.error ? 1 : 0));
      this.text(pile, x, y - height, true);
    }
  }

  private text(l: Label, x: number, y: number, pile = false): void {
    const { ctx } = this;
    ctx.textAlign = 'center';
    ctx.shadowColor = '#000';
    ctx.shadowBlur = 6;
    ctx.font = pile ? '600 12.5px ui-monospace, Menlo, monospace' : '650 14px -apple-system, Inter, sans-serif';
    ctx.fillStyle = pile ? '#ff4d5e' : '#e6ecf8';
    ctx.fillText(l.name, x, y);
    ctx.font = '11.5px ui-monospace, Menlo, monospace';
    let dy = 15;
    for (const line of l.lines ?? []) {
      ctx.fillStyle = TONES[line.tone ?? ''];
      ctx.fillText(line.text, x, y + dy);
      dy += 14;
    }
    if (l.error) {
      ctx.fillStyle = '#ffb3bb';
      ctx.font = '11px ui-monospace, Menlo, monospace';
      ctx.fillText(l.error, x, y + dy + 2);
    }
    ctx.shadowBlur = 0;
  }

  dispose(): void {
    window.removeEventListener('resize', this.onResize);
    this.canvas.remove();
  }
}
