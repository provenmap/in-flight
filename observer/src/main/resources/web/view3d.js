// The landscape in three.js: platforms, arcs, value-lit particles, and a dead-letter pile that drains green.
import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { CSS2DRenderer, CSS2DObject } from 'three/addons/renderers/CSS2DRenderer.js';
import { EffectComposer } from 'three/addons/postprocessing/EffectComposer.js';
import { RenderPass } from 'three/addons/postprocessing/RenderPass.js';
import { UnrealBloomPass } from 'three/addons/postprocessing/UnrealBloomPass.js';
import { OutputPass } from 'three/addons/postprocessing/OutputPass.js';
import {
  PLACES, PILE, MAX_PARTICLES, MAX_BLOCKS, Model, arcPoint, particlePoint, pileSlot, size, heat, worth,
} from './model.js';
import { labelHtml } from './labels.js';

const GREEN = [0.24, 1.0, 0.56];

/** A box lit by its faces instead of by lights, so unlit instanced blocks still read as blocks on a pile. */
function shadedBox(w, h, d) {
  const geo = new THREE.BoxGeometry(w, h, d);
  // Faces in BoxGeometry order: +x, -x, +y, -y, +z, -z; four vertices each.
  const shade = [0.62, 0.5, 1.0, 0.3, 0.78, 0.45];
  const colors = new Float32Array(geo.attributes.position.count * 3);
  for (let face = 0; face < 6; face++) for (let v = 0; v < 4; v++) colors.fill(shade[face], (face * 4 + v) * 3, (face * 4 + v) * 3 + 3);
  geo.setAttribute('color', new THREE.BufferAttribute(colors, 3));
  return geo;
}
const RED = [0.92, 0.16, 0.22];
const AMBER = [1.0, 0.55, 0.15];

export class View3D {
  constructor(container, model) {
    this.container = container;
    this.model = model;
    this.kind = '3d';

    const w = container.clientWidth, h = container.clientHeight;
    this.renderer = new THREE.WebGLRenderer({ antialias: true, powerPreference: 'high-performance' });
    this.renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    this.renderer.setSize(w, h);
    this.renderer.toneMapping = THREE.ACESFilmicToneMapping;
    container.appendChild(this.renderer.domElement);

    this.labels = new CSS2DRenderer();
    this.labels.setSize(w, h);
    Object.assign(this.labels.domElement.style, { position: 'absolute', inset: '0', pointerEvents: 'none' });
    container.appendChild(this.labels.domElement);

    const scene = this.scene = new THREE.Scene();
    scene.background = new THREE.Color(0x070b16);
    scene.fog = new THREE.Fog(0x070b16, 55, 120);

    this.camera = new THREE.PerspectiveCamera(42, w / h, 0.5, 400);
    this.camera.position.set(2, 31, 38);
    this.controls = new OrbitControls(this.camera, this.renderer.domElement);
    this.controls.target.set(5, 0, 2.5);
    this.controls.enableDamping = true;
    this.controls.maxPolarAngle = 1.38;
    this.controls.minDistance = 14;
    this.controls.maxDistance = 220;
    this.frameScene(w / h);

    scene.add(new THREE.HemisphereLight(0x9fb6ff, 0x0a0f1e, 1.1));
    const sun = new THREE.DirectionalLight(0xffffff, 1.4);
    sun.position.set(-10, 30, 20);
    scene.add(sun);

    const grid = new THREE.GridHelper(140, 70, 0x1a2440, 0x111a30);
    grid.position.y = -0.31;
    scene.add(grid);

    this.composer = new EffectComposer(this.renderer);
    this.composer.addPass(new RenderPass(scene, this.camera));
    this.bloom = new UnrealBloomPass(new THREE.Vector2(w, h), 0.6, 0.4, 1.0);
    this.composer.addPass(this.bloom);
    this.composer.addPass(new OutputPass());

    // Particles: one instanced sphere per event in flight, coloured and sized by value.
    this.particles = new THREE.InstancedMesh(
      new THREE.IcosahedronGeometry(1, 2),
      new THREE.MeshBasicMaterial({ toneMapped: false }),
      MAX_PARTICLES,
    );
    this.particles.instanceMatrix.setUsage(THREE.DynamicDrawUsage);
    this.particles.setColorAt(0, new THREE.Color());
    this.particles.frustumCulled = false;
    scene.add(this.particles);

    // Dead letters: instanced blocks on a pile.
    this.blocks = new THREE.InstancedMesh(
      shadedBox(0.44, 0.4, 0.44),
      new THREE.MeshBasicMaterial({ toneMapped: false, vertexColors: true }),
      MAX_BLOCKS,
    );
    this.blocks.instanceMatrix.setUsage(THREE.DynamicDrawUsage);
    this.blocks.setColorAt(0, new THREE.Color());
    this.blocks.frustumCulled = false;
    scene.add(this.blocks);

    this.flowGroup = new THREE.Group();
    scene.add(this.flowGroup);
    this.labelObjects = new Map();
    this.pileLabel = null;
    this.blockPos = new Map(); // id -> {x, y, z}, eased towards its slot as the pile compacts

    this.tmp = { m: new THREE.Matrix4(), q: new THREE.Quaternion(), s: new THREE.Vector3(), p: new THREE.Vector3(), c: new THREE.Color(), pt: {} };
    this.onResize = () => this.resize();
    window.addEventListener('resize', this.onResize);
  }

  /** Fit the whole flow, Orders to Customers, across the screen by backing the camera off on narrow screens. */
  frameScene(aspect) {
    const halfWidth = 30; // scene units either side of the target, labels included
    const halfFov = THREE.MathUtils.degToRad(this.camera.fov / 2);
    const needed = halfWidth / (Math.tan(halfFov) * aspect);
    const back = Math.max(1, needed / Math.hypot(-3, 36, 32.5));
    this.camera.position.set(5 - 3 * back, 36 * back, 2.5 + 32.5 * back);
    this.scene.fog.near = 55 * back;
    this.scene.fog.far = 120 * back;
    this.controls.update();
  }

  /** Builds the districts and arcs from the observer's flow. */
  setFlow(flow) {
    this.flowGroup.clear();
    for (const o of this.labelObjects.values()) o.removeFromParent();
    this.labelObjects.clear();

    for (const d of flow.districts) {
      const at = PLACES[d.id];
      if (!at) continue;
      const g = new THREE.Group();
      g.position.set(at.x, 0, at.z);
      if (d.sink) {
        const ring = new THREE.Mesh(new THREE.TorusGeometry(3.2, 0.14, 12, 64), new THREE.MeshBasicMaterial({ color: 0x6ea8ff, toneMapped: false }));
        ring.rotation.x = Math.PI / 2;
        g.add(ring);
        const disc = new THREE.Mesh(new THREE.CircleGeometry(3.1, 64), new THREE.MeshStandardMaterial({ color: 0x0f1a33, roughness: 0.9 }));
        disc.rotation.x = -Math.PI / 2;
        disc.position.y = -0.2;
        g.add(disc);
      } else {
        const geo = new THREE.CylinderGeometry(3.3, 3.5, 0.6, 6);
        const slab = new THREE.Mesh(geo, new THREE.MeshStandardMaterial({ color: 0x1d2746, roughness: 0.6, metalness: 0.2 }));
        g.add(slab);
        const edges = new THREE.LineSegments(new THREE.EdgesGeometry(geo), new THREE.LineBasicMaterial({ color: 0x5b80d0 }));
        g.add(edges);
        g.userData.edges = edges;
      }
      this.flowGroup.add(g);

      const div = document.createElement('div');
      div.className = 'label';
      const label = new CSS2DObject(div);
      // Below the platform, towards the viewer, so nothing behind it (such as a pile) is covered.
      label.position.set(at.x, 0, at.z + (d.sink ? 3.6 : 3.9));
      label.center.set(0.5, 0);
      this.scene.add(label);
      this.labelObjects.set(d.id, label);
      g.userData.id = d.id;
    }

    const lineMat = new THREE.LineBasicMaterial({ color: 0x2c4170, transparent: true, opacity: 0.75 });
    for (const a of this.model.arcs) {
      const from = PLACES[a.from], to = PLACES[a.to];
      if (!from || !to) continue;
      const pts = [];
      for (let i = 0; i <= 48; i++) {
        const p = arcPoint(from, to, i / 48);
        pts.push(new THREE.Vector3(p.x, p.y, p.z));
      }
      this.flowGroup.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints(pts), lineMat));
      const mid = arcPoint(from, to, 0.5);
      const div = document.createElement('div');
      div.className = 'label topic';
      div.textContent = a.topic;
      const t = new CSS2DObject(div);
      t.position.set(mid.x, mid.y + 0.5, mid.z);
      this.flowGroup.add(t);
    }
  }

  /** Writes the labels the app worked out: one per district, and one over each pile. */
  setLabels(labels) {
    for (const [id, l] of Object.entries(labels.districts)) {
      const o = this.labelObjects.get(id);
      if (o) o.element.innerHTML = labelHtml(l);
    }
    for (const g of this.flowGroup.children) {
      const edges = g.userData.edges;
      if (edges) edges.material.color.set(labels.districts[g.userData.id]?.alarm ? 0xff4d5e : 0x5b80d0);
    }
    if (labels.pile) {
      if (!this.pileLabel) {
        const div = document.createElement('div');
        div.className = 'label pile';
        this.pileLabel = new CSS2DObject(div);
        this.pileLabel.center.set(0.5, 1);
        this.scene.add(this.pileLabel);
      }
      const home = PLACES[labels.pile.district] ?? PLACES.payments;
      const top = pileSlot(labels.pile.district, Math.max(0, labels.pile.count - 1));
      const back = pileSlot(labels.pile.district, (PILE.rows - 1) * PILE.cols);
      this.pileLabel.position.set(home.x + PILE.dx, top.y + 1.2, back.z);
      this.pileLabel.element.innerHTML = labelHtml(labels.pile);
      this.pileLabel.visible = true;
    } else if (this.pileLabel) {
      this.pileLabel.visible = false;
    }
  }

  frame(now) {
    const { m, q, s, p, c, pt } = this.tmp;

    // Particles in flight.
    let n = 0;
    for (const part of this.model.particles) {
      const at = particlePoint(part, now, pt);
      if (!at) continue;
      let r = size(part.value);
      let rgb, glow;
      if (part.kind === 'redrive') { rgb = GREEN; glow = 1.5; }
      else if (part.kind === 'declined') { rgb = AMBER; glow = 1.6 * (1 - at.t); r *= 1 - at.t * 0.7; }
      else { const v = worth(part.value); rgb = heat(part.value); glow = 0.55 + 1.9 * v * v; }
      // Land softly: shrink over the last tenth of the flight.
      if (at.t > 0.9) r *= (1 - at.t) * 10;
      p.set(at.x, at.y, at.z);
      s.setScalar(Math.max(r, 0.001));
      m.compose(p, q, s);
      this.particles.setMatrixAt(n, m);
      c.setRGB(rgb[0], rgb[1], rgb[2], THREE.SRGBColorSpace).multiplyScalar(glow);
      this.particles.setColorAt(n, c);
      n++;
    }
    this.particles.count = n;
    this.particles.instanceMatrix.needsUpdate = true;
    if (this.particles.instanceColor) this.particles.instanceColor.needsUpdate = true;

    // The pile: blocks ease into their slot, drop in when new, and go green and lift away when recovered.
    const seen = new Set();
    const index = new Map();
    let k = 0;
    for (const b of this.model.pile) {
      const i = index.get(b.district) ?? 0;
      index.set(b.district, i + 1);
      const slot = pileSlot(b.district, i, pt);
      let cur = this.blockPos.get(b.id);
      const age = now - b.born;
      if (!cur) { cur = { x: slot.x, y: slot.y + 7, z: slot.z }; this.blockPos.set(b.id, cur); }
      cur.x += (slot.x - cur.x) * 0.12;
      cur.z += (slot.z - cur.z) * 0.12;
      cur.y = age < 600 ? slot.y + 7 * Math.pow(1 - age / 600, 2) : cur.y + (slot.y - cur.y) * 0.12;
      seen.add(b.id);

      const g = Model.greenness(b, now);
      let x = cur.x, y = cur.y, z = cur.z, scale = 1;
      let rgb = RED, glow = age < 700 ? 1.6 - 0.75 * age / 700 : 0.85;
      if (b.greenAt && now >= b.greenAt) {
        const home = PLACES[b.district] ?? PLACES.payments;
        const lift = g * g;
        x += (home.x - x) * lift;
        z += (home.z - z) * lift;
        y += 3 * Math.sin(g * Math.PI) + (1.2 - y) * lift;
        scale = 1 - lift * 0.85;
        rgb = GREEN;
        glow = 1.4;
      }
      p.set(x, y, z);
      s.setScalar(scale);
      m.compose(p, q, s);
      this.blocks.setMatrixAt(k, m);
      c.setRGB(rgb[0], rgb[1], rgb[2], THREE.SRGBColorSpace).multiplyScalar(glow);
      this.blocks.setColorAt(k, c);
      k++;
    }
    for (const id of this.blockPos.keys()) if (!seen.has(id)) this.blockPos.delete(id);
    this.blocks.count = k;
    this.blocks.instanceMatrix.needsUpdate = true;
    if (this.blocks.instanceColor) this.blocks.instanceColor.needsUpdate = true;

    this.controls.update();
    this.composer.render();
    this.labels.render(this.scene, this.camera);
  }

  resize() {
    const w = this.container.clientWidth, h = this.container.clientHeight;
    this.camera.aspect = w / h;
    this.camera.updateProjectionMatrix();
    this.frameScene(w / h);
    this.renderer.setSize(w, h);
    this.composer.setSize(w, h);
    this.labels.setSize(w, h);
  }

  dispose() {
    window.removeEventListener('resize', this.onResize);
    this.controls.dispose();
    this.renderer.dispose();
    this.renderer.domElement.remove();
    this.labels.domElement.remove();
  }
}
