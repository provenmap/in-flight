// The console: money at risk, the flow map, the details behind them, and the runbook. The model lives outside React;
// the map draws it every frame, the counter eases every frame, and React re-renders on snapshots and stats.
import { useMutation } from '@tanstack/react-query';
import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { TooltipProvider } from '@/components/ui/tooltip';
import { FlowMap } from './map/FlowMap';
import { Model, money } from './model';
import { Activity } from './panels/Activity';
import { DataTabs } from './panels/DataTabs';
import { DistrictSheet } from './panels/DistrictSheet';
import { Headline } from './panels/Headline';
import { Runbook, type Action } from './panels/Runbook';
import { TopBar } from './panels/TopBar';
import { Watch } from './panels/Watch';
import { useTimeline } from './timeline';
import { useStream } from './useStream';

const model = new Model();

declare global {
  interface Window {
    rif: { model: Model; readonly view: string; readonly frames: number; readonly particles: number; readonly blocks: number };
  }
}

function initialTheme(): 'light' | 'dark' {
  try {
    const saved = localStorage.getItem('rif.theme');
    if (saved === 'light' || saved === 'dark') return saved;
  } catch {
    // storage unavailable: follow the system
  }
  return matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

/** Whether the page is wide enough for the runbook to sit beside the map rather than above it. */
function useWide(): boolean {
  const query = '(min-width: 1280px)';
  return useSyncExternalStore(
    notify => {
      const m = matchMedia(query);
      m.addEventListener('change', notify);
      return () => m.removeEventListener('change', notify);
    },
    () => matchMedia(query).matches,
  );
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

function Legend({ demo, unread }: { demo: boolean; unread: boolean }) {
  const item = (cls: string, label: string) => <span className="flex items-center gap-1.5"><span className={`size-2 rounded-full ${cls}`} />{label}</span>;
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
      {item('bg-flow', demo ? 'Order money moving' : 'Money moving')}
      {item('bg-recovered', 'Redriven')}
      {unread && item('bg-declined', demo ? 'Declined' : 'Read by nobody')}
      <span className="flex items-center gap-1.5"><span className="size-2 rounded-[2px] bg-stuck" />Stuck order</span>
    </div>
  );
}

/** `order-to-cash` is Order to cash. */
const flowTitle = (name: string) => (name.charAt(0).toUpperCase() + name.slice(1)).replaceAll('-', ' ');

export function App() {
  const { connection, stats } = useStream(model);
  const connected = connection === 'live';
  const [toast, showToast] = useToast();
  const [theme, setTheme] = useState(initialTheme);
  const [recovering, setRecovering] = useState(0);
  const [selected, setSelected] = useState<string | null>(null);
  const counter = useRef<HTMLDivElement>(null);
  const frames = useRef(0);
  const entries = useTimeline(stats, model);
  const wide = useWide();

  useEffect(() => {
    document.documentElement.classList.toggle('dark', theme === 'dark');
  }, [theme]);
  const toggleTheme = () => {
    const t = theme === 'dark' ? 'light' : 'dark';
    setTheme(t);
    try {
      localStorage.setItem('rif.theme', t);
    } catch {
      // the choice just isn't kept
    }
  };

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

  // Every frame: tick the model, and ease the counter toward money at risk plus the recoveries the wave has not
  // played yet, so it drains in step with the tank.
  useEffect(() => {
    let raf = 0;
    let last = performance.now();
    let shown = 0;
    let wasRecovering = 0;
    const loop = (now: number) => {
      model.tick(now);
      const pendingWave = model.pendingRecovery(now);
      if (pendingWave.orders !== wasRecovering) setRecovering(pendingWave.orders);
      wasRecovering = pendingWave.orders;
      const target = (model.stats?.atRisk.value ?? 0) + pendingWave.value;
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
      view: 'map',
      get frames() { return frames.current; },
      get particles() { return model.particles.length; },
      get blocks() { return model.pile.length; },
    };
  }, []);

  const demo = model.mode === 'demo';
  const runbook = demo
    ? <Runbook stats={stats} connected={connected} pending={pending} act={act} />
    : <Watch flow={model.flow} stats={stats} model={model} connected={connected} />;
  return (
    <TooltipProvider>
      <TopBar flow={model.flow} connection={connection} theme={theme} toggleTheme={toggleTheme} />
      <main className="mx-auto grid max-w-[1600px] gap-4 p-4 sm:p-6 xl:grid-cols-[minmax(0,1fr)_380px]">
        <div className="flex min-w-0 flex-col gap-4">
          <Headline stats={stats} recovering={recovering} counter={counter} history={model.history} />
          {/* On narrow screens the runbook comes straight after the counter: it is what you act on. */}
          {!wide && runbook}
          <Card className="gap-3">
            <CardHeader>
              <CardTitle>{model.flow ? `${flowTitle(model.flow.name)}, live` : 'Live'}</CardTitle>
              <CardDescription>Each edge is as thick as the money crossing it a minute. Select a district for its details.</CardDescription>
            </CardHeader>
            <CardContent className="flex flex-col gap-3">
              <div className="-mx-2 overflow-x-auto px-2">
                <div className="min-w-[720px]">
                  <FlowMap model={model} flow={model.flow} stats={stats} theme={theme} onSelect={setSelected} />
                </div>
              </div>
              <Legend demo={demo} unread={!!model.flow?.topics.some(t => !t.to.length)} />
            </CardContent>
          </Card>
          <DataTabs model={model} flow={model.flow} stats={stats} />
        </div>
        <aside className="flex min-w-0 flex-col gap-4">
          {wide && runbook}
          <Activity entries={entries} demo={demo} />
        </aside>
      </main>
      <DistrictSheet id={selected} onClose={() => setSelected(null)} flow={model.flow} stats={stats} model={model} />
      <div
        role="status"
        hidden={!toast}
        className="fixed right-4 bottom-4 z-50 max-w-[calc(100vw-32px)] rounded-lg border border-stuck/40 bg-card px-4 py-3 text-sm text-stuck shadow-lg"
      >
        {toast}
      </div>
    </TooltipProvider>
  );
}
