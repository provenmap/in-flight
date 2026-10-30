// The view toggle and the value legend, top right; the brand and connection, top left.
import { Button } from '../components/button';
import { heat, moneyShort, size } from '../model';
import type { Connection } from '../useStream';

export function Brand({ connection }: { connection: Connection }) {
  const dot = connection === 'live' ? 'bg-green shadow-[0_0_8px_var(--color-green)]' : connection === 'down' ? 'bg-red' : 'bg-faint';
  return (
    <header className="pointer-events-none fixed top-4 left-[18px]">
      <h1 className="m-0 text-lg font-[650] tracking-[0.01em]">Revenue in Flight</h1>
      <p className="mt-0.5 hidden text-[12.5px] text-muted wide:block">Order-to-cash, live from Kafka</p>
      <p data-testid="connection" className="mt-0.5 flex items-center gap-1.5 text-[12.5px] text-muted" data-state={connection}>
        <span className={`size-[7px] rounded-full ${dot}`} />
        <span>{connection === 'live' ? 'live from Kafka' : connection === 'down' ? 'reconnecting to the observer' : 'connecting'}</span>
      </p>
    </header>
  );
}

export function Side({ view, choose }: { view: '2d' | '3d'; choose: (v: '2d' | '3d') => void }) {
  return (
    <aside className="fixed top-4 right-[18px] flex flex-col items-end gap-2.5">
      <div className="flex gap-1" role="group" aria-label="View">
        {(['3d', '2d'] as const).map(v => (
          <Button key={v} size="sm" data-view={v} aria-pressed={view === v} variant={view === v ? 'on' : 'default'} onClick={() => choose(v)}>
            {v.toUpperCase()}
          </Button>
        ))}
      </div>
      <div className="hidden rounded-[10px] border border-panel-edge bg-panel px-2.5 py-2 text-[11.5px] text-muted wide:block" aria-label="Particle size and brightness by value">
        <div className="mb-1.5">Each event, by value</div>
        <div className="flex items-center gap-2.5">
          {[10, 100, 1000, 10000].map(v => {
            const d = Math.round(size(v) * 22);
            const rgb = `rgb(${heat(v).map(x => Math.round(x * 255)).join(',')})`;
            return (
              <span key={v} className="flex flex-col items-center gap-[3px] font-mono">
                <b className="block rounded-full" style={{ width: d, height: d, background: rgb, boxShadow: `0 0 ${d / 2}px ${rgb}` }} />
                {moneyShort.format(v)}
              </span>
            );
          })}
        </div>
        <div className="mt-2 flex gap-2.5">
          <span className="flex items-center gap-1"><i className="inline-block size-[9px] rounded-full bg-green" />redriven</span>
          <span className="flex items-center gap-1"><i className="inline-block size-[9px] rounded-[2px] bg-red" />dead letter</span>
          <span className="flex items-center gap-1"><i className="inline-block size-[9px] rounded-full bg-amber" />declined</span>
        </div>
      </div>
    </aside>
  );
}

export function Toast({ text }: { text: string | null }) {
  return (
    <div
      role="status"
      hidden={!text}
      className="fixed bottom-[150px] left-1/2 max-w-[calc(100vw-32px)] -translate-x-1/2 rounded-lg border border-red/40 bg-[rgb(40_14_20/0.92)] px-3.5 py-2 text-[13px] text-[#ffd0d5]"
    >
      {text}
    </div>
  );
}
