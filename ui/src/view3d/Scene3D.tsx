// The landscape in react-three-fiber: platforms, arcs, value-lit particles, and a dead-letter pile that drains green.
import { OrbitControls } from '@react-three/drei';
import { Canvas, useFrame, useThree } from '@react-three/fiber';
import { useEffect, useLayoutEffect, useMemo, useRef, type ReactNode } from 'react';
import * as THREE from 'three';
import { EffectComposer } from 'three/addons/postprocessing/EffectComposer.js';
import { OutputPass } from 'three/addons/postprocessing/OutputPass.js';
import { RenderPass } from 'three/addons/postprocessing/RenderPass.js';
import { UnrealBloomPass } from 'three/addons/postprocessing/UnrealBloomPass.js';
import type { OrbitControls as OrbitControlsImpl } from 'three-stdlib';
import type { Label, Labels } from '../labels';
import { MAX_BLOCKS, MAX_PARTICLES, Model, PILE, PLACES, arcPoint, heat, particlePoint, pileSlot, size, worth, type Arc, type Point, type Rgb } from '../model';
import type { Flow } from '../protocol';
import type { StageProps } from '../Stage';

const GREEN: Rgb = [0.24, 1.0, 0.56];
const RED: Rgb = [0.92, 0.16, 0.22];
const AMBER: Rgb = [1.0, 0.55, 0.15];
const BG = 0x070b16;
const TARGET = new THREE.Vector3(5, 0, 2.5);

/** A box lit by its faces instead of by lights, so unlit instanced blocks still read as blocks on a pile. */
function shadedBox(w: number, h: number, d: number): THREE.BoxGeometry {
  const geo = new THREE.BoxGeometry(w, h, d);
  // Faces in BoxGeometry order: +x, -x, +y, -y, +z, -z; four vertices each.
  const shade = [0.62, 0.5, 1.0, 0.3, 0.78, 0.45];
  const colors = new Float32Array(geo.attributes.position!.count * 3);
  for (let face = 0; face < 6; face++) for (let v = 0; v < 4; v++) colors.fill(shade[face]!, (face * 4 + v) * 3, (face * 4 + v) * 3 + 3);
  geo.setAttribute('color', new THREE.BufferAttribute(colors, 3));
  return geo;
}

type Anchor = 'top' | 'bottom' | 'middle';

/** A label over the scene: where it is, and where that point sits on the label (its top, bottom or middle). */
interface Tag {
  id: string;
  at: [number, number, number];
  anchor: Anchor;
  className: string;
  content: ReactNode;
}

const SHIFT: Record<Anchor, string> = { top: '-50%, 0', bottom: '-50%, -100%', middle: '-50%, -50%' };

/** The label elements, by id, and where each one goes. The overlay fills it; the projector moves them every frame. */
class Overlay {
  readonly els = new Map<string, HTMLElement>();
  tags: Tag[] = [];

  ref(id: string) {
    return (el: HTMLElement | null) => {
      if (el) this.els.set(id, el);
      else this.els.delete(id);
    };
  }
}

/** Labels are plain DOM over the canvas, as CSS2DRenderer draws them: projected each frame, nearer ones on top. */
function Project({ overlay }: { overlay: Overlay }) {
  const v = useMemo(() => new THREE.Vector3(), []);
  useFrame(({ camera, size }) => {
    for (const t of overlay.tags) {
      const el = overlay.els.get(t.id);
      if (!el) continue;
      v.set(...t.at);
      const distance = v.distanceTo(camera.position);
      v.project(camera);
      if (v.z < -1 || v.z > 1) {
        el.style.visibility = 'hidden';
        continue;
      }
      const x = (v.x * 0.5 + 0.5) * size.width, y = (-v.y * 0.5 + 0.5) * size.height;
      el.style.transform = `translate(${x}px, ${y}px) translate(${SHIFT[t.anchor]})`;
      el.style.zIndex = String(Math.round(1000 - distance));
      el.style.visibility = 'visible';
    }
  });
  return null;
}

function LabelBody({ label }: { label: Label }) {
  return (
    <>
      <div className="name">{label.name}</div>
      {label.lines.map((l, i) => <div key={i} className={`line ${l.tone ?? ''}`}>{l.text}</div>)}
      {label.error && <div className="err">{label.error}</div>}
    </>
  );
}

/** Fit the whole flow, Orders to Customers, across the screen by backing the camera off on narrow screens. */
function Rig() {
  const camera = useThree(s => s.camera) as THREE.PerspectiveCamera;
  const scene = useThree(s => s.scene);
  const { width, height } = useThree(s => s.size);
  const controls = useRef<OrbitControlsImpl>(null);

  useLayoutEffect(() => {
    const aspect = width / height;
    const halfWidth = 30; // scene units either side of the target, labels included
    const halfFov = THREE.MathUtils.degToRad(camera.fov / 2);
    const needed = halfWidth / (Math.tan(halfFov) * aspect);
    const back = Math.max(1, needed / Math.hypot(-3, 36, 32.5));
    camera.position.set(5 - 3 * back, 36 * back, 2.5 + 32.5 * back);
    const fog = scene.fog as THREE.Fog;
    fog.near = 55 * back;
    fog.far = 120 * back;
    controls.current?.update();
  }, [camera, scene, width, height]);

  return <OrbitControls ref={controls} makeDefault target={TARGET} enableDamping maxPolarAngle={1.38} minDistance={14} maxDistance={220} />;
}

function District({ d, alarm }: { d: Flow['districts'][number]; alarm: boolean }) {
  const at = PLACES[d.id]!;
  const slab = useMemo(() => new THREE.CylinderGeometry(3.3, 3.5, 0.6, 6), []);
  const edges = useMemo(() => new THREE.EdgesGeometry(slab), [slab]);
  return (
    <group position={[at.x, 0, at.z]}>
        {d.sink ? (
          <>
            <mesh rotation-x={Math.PI / 2}>
              <torusGeometry args={[3.2, 0.14, 12, 64]} />
              <meshBasicMaterial color={0x6ea8ff} toneMapped={false} />
            </mesh>
            <mesh rotation-x={-Math.PI / 2} position-y={-0.2}>
              <circleGeometry args={[3.1, 64]} />
              <meshStandardMaterial color={0x0f1a33} roughness={0.9} />
            </mesh>
          </>
        ) : (
          <>
            <mesh geometry={slab}>
              <meshStandardMaterial color={0x1d2746} roughness={0.6} metalness={0.2} />
            </mesh>
            <lineSegments geometry={edges}>
              <lineBasicMaterial color={alarm ? 0xff4d5e : 0x5b80d0} />
            </lineSegments>
          </>
        )}
    </group>
  );
}

function arcLines(arcs: Arc[]) {
  const mat = new THREE.LineBasicMaterial({ color: 0x2c4170, transparent: true, opacity: 0.75 });
  return arcs.flatMap(a => {
    const from = PLACES[a.from], to = PLACES[a.to];
    if (!from || !to) return [];
    const pts = Array.from({ length: 49 }, (_, i) => {
      const p = arcPoint(from, to, i / 48);
      return new THREE.Vector3(p.x, p.y, p.z);
    });
    const mid = arcPoint(from, to, 0.5);
    return [{ key: `${a.topic}>${a.to}`, topic: a.topic, line: new THREE.Line(new THREE.BufferGeometry().setFromPoints(pts), mat), mid }];
  });
}

const scratch = () => ({ m: new THREE.Matrix4(), q: new THREE.Quaternion(), s: new THREE.Vector3(), p: new THREE.Vector3(), c: new THREE.Color(), pt: { x: 0, y: 0, z: 0 } as Point });

/** One instanced sphere per event in flight, coloured and sized by value. */
function Particles({ model }: { model: Model }) {
  const mesh = useRef<THREE.InstancedMesh>(null);
  const tmp = useMemo(() => scratch(), []);
  useLayoutEffect(() => {
    mesh.current!.instanceMatrix.setUsage(THREE.DynamicDrawUsage);
    mesh.current!.setColorAt(0, new THREE.Color());
  }, []);

  useFrame(() => {
    const now = performance.now();
    const { m, q, s, p, c, pt } = tmp;
    const particles = mesh.current!;
    let n = 0;
    for (const part of model.particles) {
      const at = particlePoint(part, now, pt);
      if (!at) continue;
      let r = size(part.value);
      let rgb: Rgb, glow: number;
      if (part.kind === 'redrive') { rgb = GREEN; glow = 1.5; }
      else if (part.kind === 'declined') { rgb = AMBER; glow = 1.6 * (1 - at.t); r *= 1 - at.t * 0.7; }
      else { const v = worth(part.value); rgb = heat(part.value); glow = 0.55 + 1.9 * v * v; }
      // Land softly: shrink over the last tenth of the flight.
      if (at.t > 0.9) r *= (1 - at.t) * 10;
      p.set(at.x, at.y, at.z);
      s.setScalar(Math.max(r, 0.001));
      m.compose(p, q, s);
      particles.setMatrixAt(n, m);
      c.setRGB(rgb[0], rgb[1], rgb[2], THREE.SRGBColorSpace).multiplyScalar(glow);
      particles.setColorAt(n, c);
      n++;
    }
    particles.count = n;
    particles.instanceMatrix.needsUpdate = true;
    if (particles.instanceColor) particles.instanceColor.needsUpdate = true;
  });

  return (
    <instancedMesh ref={mesh} args={[undefined, undefined, MAX_PARTICLES]} frustumCulled={false}>
      <icosahedronGeometry args={[1, 2]} />
      <meshBasicMaterial toneMapped={false} />
    </instancedMesh>
  );
}

/** Dead letters: blocks ease into their slot, drop in when new, and go green and lift away when recovered. */
function Pile({ model }: { model: Model }) {
  const mesh = useRef<THREE.InstancedMesh>(null);
  const tmp = useMemo(() => scratch(), []);
  const geo = useMemo(() => shadedBox(0.44, 0.4, 0.44), []);
  const positions = useRef(new Map<string, Point>()); // id -> where it is, eased towards its slot as the pile compacts
  useLayoutEffect(() => {
    mesh.current!.instanceMatrix.setUsage(THREE.DynamicDrawUsage);
    mesh.current!.setColorAt(0, new THREE.Color());
  }, []);

  useFrame(() => {
    const now = performance.now();
    const { m, q, s, p, c, pt } = tmp;
    const blocks = mesh.current!;
    const blockPos = positions.current;
    const seen = new Set<string>();
    const index = new Map<string, number>();
    let k = 0;
    for (const b of model.pile) {
      const i = index.get(b.district) ?? 0;
      index.set(b.district, i + 1);
      const slot = pileSlot(b.district, i, pt);
      let cur = blockPos.get(b.id);
      const age = now - b.born;
      if (!cur) { cur = { x: slot.x, y: slot.y + 7, z: slot.z }; blockPos.set(b.id, cur); }
      cur.x += (slot.x - cur.x) * 0.12;
      cur.z += (slot.z - cur.z) * 0.12;
      cur.y = age < 600 ? slot.y + 7 * Math.pow(1 - age / 600, 2) : cur.y + (slot.y - cur.y) * 0.12;
      seen.add(b.id);

      const g = Model.greenness(b, now);
      let x = cur.x, y = cur.y, z = cur.z, scale = 1;
      let rgb = RED, glow = age < 700 ? 1.6 - (0.75 * age) / 700 : 0.85;
      if (b.greenAt && now >= b.greenAt) {
        const home = PLACES[b.district] ?? PLACES.payments!;
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
      blocks.setMatrixAt(k, m);
      c.setRGB(rgb[0], rgb[1], rgb[2], THREE.SRGBColorSpace).multiplyScalar(glow);
      blocks.setColorAt(k, c);
      k++;
    }
    for (const id of blockPos.keys()) if (!seen.has(id)) blockPos.delete(id);
    blocks.count = k;
    blocks.instanceMatrix.needsUpdate = true;
    if (blocks.instanceColor) blocks.instanceColor.needsUpdate = true;
  });

  return (
    <>
      <instancedMesh ref={mesh} args={[geo, undefined, MAX_BLOCKS]} frustumCulled={false}>
        <meshBasicMaterial toneMapped={false} vertexColors />
      </instancedMesh>
    </>
  );
}

/** Bloom as the plain-module scene had it: three's UnrealBloomPass, then the output pass for tone mapping. */
function Bloom() {
  const gl = useThree(s => s.gl);
  const scene = useThree(s => s.scene);
  const camera = useThree(s => s.camera);
  const { width, height } = useThree(s => s.size);
  const composer = useMemo(() => {
    const c = new EffectComposer(gl);
    c.addPass(new RenderPass(scene, camera));
    c.addPass(new UnrealBloomPass(new THREE.Vector2(256, 256), 0.6, 0.4, 1.0));
    c.addPass(new OutputPass());
    return c;
  }, [gl, scene, camera]);
  useEffect(() => composer.setSize(width, height), [composer, width, height]);
  useEffect(() => () => composer.dispose(), [composer]);
  // A positive priority takes over rendering from the default loop.
  useFrame(() => composer.render(), 1);
  return null;
}

/** Every label: one under each district, one on each arc, and one over the pile. */
function tagsFor(flow: Flow | null, labels: Labels, arcs: ReturnType<typeof arcLines>): Tag[] {
  const tags: Tag[] = [];
  for (const d of flow?.districts ?? []) {
    const at = PLACES[d.id], label = labels.districts[d.id];
    if (!at || !label) continue;
    // Below the platform, towards the viewer, so nothing behind it (such as a pile) is covered.
    tags.push({ id: `d:${d.id}`, at: [at.x, 0, at.z + (d.sink ? 3.6 : 3.9)], anchor: 'top', className: 'label', content: <LabelBody label={label} /> });
  }
  for (const a of arcs) {
    tags.push({ id: `t:${a.key}`, at: [a.mid.x, a.mid.y + 0.5, a.mid.z], anchor: 'middle', className: 'label topic', content: a.topic });
  }
  const pile = labels.pile;
  if (pile) {
    const home = PLACES[pile.district] ?? PLACES.payments!;
    const top = pileSlot(pile.district, Math.max(0, pile.count - 1));
    const back = pileSlot(pile.district, (PILE.rows - 1) * PILE.cols);
    tags.push({ id: 'pile', at: [home.x + PILE.dx, top.y + 1.2, back.z], anchor: 'bottom', className: 'label pile', content: <LabelBody label={pile} /> });
  }
  return tags;
}

export default function Scene3D({ model, flow, labels, viewRef }: StageProps) {
  // The scene draws itself in its own loop; the frame loop only needs to know which view is up.
  useEffect(() => {
    viewRef.current = { kind: '3d', setFlow() {}, setLabels() {}, frame() {}, dispose() {} };
    return () => { viewRef.current = null; };
  }, [viewRef]);

  const arcs = useMemo(() => arcLines(model.arcs), [model.arcs]);
  const tags = useMemo(() => tagsFor(flow, labels, arcs), [flow, labels, arcs]);
  const overlayRef = useRef(new Overlay());
  useLayoutEffect(() => {
    overlayRef.current.tags = tags;
  }, [tags]);
  const overlay = overlayRef.current;

  return (
    <div className="fixed inset-0" aria-label="The order-to-cash flow, live">
      <Canvas
        dpr={[1, 2]}
        gl={{ antialias: true, powerPreference: 'high-performance', toneMapping: THREE.ACESFilmicToneMapping }}
        camera={{ fov: 42, near: 0.5, far: 400, position: [2, 31, 38] }}
      >
        <color attach="background" args={[BG]} />
        <fog attach="fog" args={[BG, 55, 120]} />
        <Rig />
        <hemisphereLight args={[0x9fb6ff, 0x0a0f1e, 1.1]} />
        <directionalLight color={0xffffff} intensity={1.4} position={[-10, 30, 20]} />
        <gridHelper args={[140, 70, 0x1a2440, 0x111a30]} position-y={-0.31} />
        {flow?.districts.filter(d => PLACES[d.id]).map(d => <District key={d.id} d={d} alarm={!!labels.districts[d.id]?.alarm} />)}
        {arcs.map(a => <primitive key={a.key} object={a.line} />)}
        <Particles model={model} />
        <Pile model={model} />
        <Bloom />
        <Project overlay={overlay} />
      </Canvas>
      <div className="pointer-events-none absolute inset-0 overflow-hidden">
        {tags.map(t => (
          <div key={t.id} ref={overlay.ref(t.id)} className={`${t.className} absolute top-0 left-0`} style={{ visibility: 'hidden' }}>
            {t.content}
          </div>
        ))}
      </div>
    </div>
  );
}
