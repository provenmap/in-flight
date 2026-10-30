// The observer's stream, in one hook: it feeds the model and re-renders on every snapshot and stats message.
import { useEffect, useState } from 'react';
import type { Model } from './model';
import type { Msg, Stats } from './protocol';

export type Connection = 'connecting' | 'live' | 'down';

export function useStream(model: Model): { connection: Connection; stats: Stats | null } {
  const [connection, setConnection] = useState<Connection>('connecting');
  const [stats, setStats] = useState<Stats | null>(null);

  useEffect(() => {
    let ws: WebSocket | null = null;
    let retry: ReturnType<typeof setTimeout> | undefined;
    let stopped = false;

    const connect = (attempt: number) => {
      ws = new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws`);
      ws.onopen = () => { attempt = 0; };
      ws.onmessage = m => {
        const now = performance.now();
        for (const msg of JSON.parse(m.data as string) as Msg[]) {
          model.apply(msg, now);
          if (msg.type === 'snapshot') {
            setConnection('live');
            setStats(msg.stats);
          } else if (msg.type === 'stats') {
            setStats(msg);
          }
        }
      };
      ws.onclose = () => {
        if (stopped) return;
        setConnection('down');
        retry = setTimeout(() => connect(attempt + 1), Math.min(5000, 500 * 2 ** attempt));
      };
    };
    connect(0);
    return () => {
      stopped = true;
      clearTimeout(retry);
      ws?.close();
    };
  }, [model]);

  return { connection, stats };
}
