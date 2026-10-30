// The page: the stage, the counter, the controls, and the view choice. The model lives outside React; the frame loop
// draws it and eases the counter every frame, and React re-renders only on snapshots and stats.
import { useMutation } from '@tanstack/react-query';
import { lazy, Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Controls, type Action } from './hud/Controls';
import { Hud } from './hud/Hud';
import { Brand, Side, Toast } from './hud/Side';
import { labelsFor, noLabels } from './labels';
import { Model, money } from './model';
import { ClassStage, Fallback } from './Stage';
import { useStream } from './useStream';
import { View2D, type SceneView } from './view2d';

const model = new Model();
const Scene3D = lazy(() => import('./view3d/Scene3D'));

type Kind = '2d' | '3d';

function webglWorks(): boolean {
  try {
    const c = document.createElement('canvas');
    return !!(c.getContext('webgl2') || c.getContext('webgl'));
  } catch {
    return false;
  }
}

function preferred(): Kind {
  const asked = new URLSearchParams(location.search).get('view');
  if (asked === '2d' || asked === '3d') return asked;
  try {
    return localStorage.getItem('rif.view') === '2d' ? '2d' : '3d';
  } catch {
    return '3d';
  }
}

const make2d = (el: HTMLElement, m: Model) => new View2D(el, m);

declare global {
  interface Window {
    rif: { model: Model; readonly view: string | undefined; readonly frames: number; readonly particles: number; readonly blocks: number };
  }
}

function useToast(): [string | null, (text: string) => void] {
  const [text, setText] = useState<string | null>(null);
  const timer = useRef<ReturnType<typeof setTimeout>>(undefined);
  const show = useCallback((t: string) => {
    setText(t);
    clearTimeout(timer.current);
    timer.current = setTimeout(() => setText(null), 5000);
  }, []);
  return [text, show];
}

export function App() {
  const { connection, stats } = useStream(model);
  const connected = connection === 'live';
  const [toast, showToast] = useToast();
  const [view, setView] = useState<Kind>(() => (preferred() === '3d' && webglWorks() ? '3d' : '2d'));
  const [recovering, setRecovering] = useState(0);
  const viewRef = useRef<SceneView | null>(null);
  const counter = useRef<HTMLDivElement>(null);
  const frames = useRef(0);

  const fallBack = useCallback((e?: unknown) => {
    if (e) console.warn('3D view unavailable, drawing in 2D:', e);
    showToast('3D is unavailable here, so the view is drawn in 2D');
    setView('2d');
  }, [showToast]);

  useEffect(() => {
    if (preferred() === '3d' && !webglWorks()) fallBack();
  }, [fallBack]);

  const choose = (v: Kind) => {
    try {
      localStorage.setItem('rif.view', v);
    } catch {
      // private mode: the choice just isn't kept
    }
    if (v === '3d' && !webglWorks()) fallBack();
    else setView(v);
  };

  useEffect(() => {
    document.body.dataset.view = view;
  }, [view]);

  // Labels change with stats, and when a recovery wave finishes and its blocks leave the pile.
  const labels = useMemo(() => (stats ? labelsFor(stats, stats.sim, model) : noLabels), [stats, recovering]); // eslint-disable-line react-hooks/exhaustive-deps

  const control = useMutation({
    mutationFn: async ({ action, query = '' }: { action: Action; query?: string }) => {
      let res: Response;
      try {
        res = await fetch(`/api/sim/${action}${query}`, { method: 'POST' });
      } catch (e) {
        throw new Error(`${action} failed: ${(e as Error).message}`, { cause: e });
      }
      const body = (await res.json().catch(() => ({}))) as { error?: string };
      if (!res.ok) throw new Error(body.error ?? `${action} failed (${res.status})`);
      return body;
    },
    onError: e => showToast(e.message),
  });
  const pending = control.isPending ? control.variables.action : null;
  const act = useCallback((action: Action, query?: string) => control.mutate({ action, query }), [control]);

  // The frame loop: tick the model, draw a view that draws on demand, and ease the counter toward money at risk
  // plus the recoveries the wave has not played yet, so it drains in step with the pile.
  useEffect(() => {
    let raf = 0;
    let last = performance.now();
    let shown = 0;
    let wasRecovering = 0;
    const loop = (now: number) => {
      model.tick(now);
      viewRef.current?.frame(now);
      const pending = model.pendingRecovery(now);
      if (pending.orders !== wasRecovering) setRecovering(pending.orders);
      wasRecovering = pending.orders;
      const target = (model.stats?.atRisk.value ?? 0) + pending.value;
      const dt = Math.min(0.1, (now - last) / 1000);
      last = now;
      shown += (target - shown) * Math.min(1, dt * 6);
      if (Math.abs(target - shown) < 0.005) shown = target;
      if (counter.current) counter.current.textContent = money.format(shown);
      frames.current++;
      raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(raf);
  }, []);

  // For the browser test and for poking at it from the console.
  useEffect(() => {
    window.rif = {
      model,
      get view() { return viewRef.current?.kind; },
      get frames() { return frames.current; },
      get particles() { return model.particles.length; },
      get blocks() { return model.pile.length; },
    };
  }, []);

  const stage = { model, flow: model.flow, labels, viewRef };
  return (
    <>
      {view === '2d' ? (
        <ClassStage key="2d" make={make2d} {...stage} />
      ) : (
        <Fallback key="3d" onError={fallBack}>
          <Suspense fallback={null}>
            <Scene3D {...stage} />
          </Suspense>
        </Fallback>
      )}
      <Brand connection={connection} />
      <Hud stats={stats} recovering={recovering} value={counter} />
      <Side view={view} choose={choose} />
      <Controls stats={stats} connected={connected} pending={pending} act={act} />
      <Toast text={toast} />
    </>
  );
}
