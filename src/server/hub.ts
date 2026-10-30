import type { WebSocket } from 'ws';
import type { Frame, Reader } from '../observer/reader.js';

/**
 * Sends frames to viewers, in order. A new viewer gets its snapshot from the reader between two batches and joins
 * the broadcast at once, so it sees every message after the snapshot exactly once.
 */
export class Hub {
  private readonly viewers = new Set<WebSocket>();
  private readonly alive = new WeakSet<WebSocket>();
  private readonly pinger: NodeJS.Timeout;

  constructor(private readonly reader: Reader) {
    // Keeps idle connections open through proxies, and drops viewers that stopped answering.
    this.pinger = setInterval(() => {
      for (const ws of this.viewers) {
        if (!this.alive.has(ws)) {
          ws.terminate();
          this.viewers.delete(ws);
          continue;
        }
        this.alive.delete(ws);
        ws.ping();
      }
    }, 15_000);
  }

  /** Every frame is a JSON array of messages. */
  publish(frame: Frame): void {
    if (!this.viewers.size) return;
    const text = JSON.stringify(frame);
    for (const ws of this.viewers) this.send(ws, text);
  }

  join(ws: WebSocket): void {
    this.alive.add(ws);
    ws.on('pong', () => this.alive.add(ws));
    ws.on('close', () => this.viewers.delete(ws));
    ws.on('error', () => this.viewers.delete(ws));
    this.reader.join(snapshot => {
      if (this.send(ws, JSON.stringify([snapshot]))) this.viewers.add(ws);
    });
  }

  get count(): number {
    return this.viewers.size;
  }

  private send(ws: WebSocket, text: string): boolean {
    if (ws.readyState !== ws.OPEN) {
      this.viewers.delete(ws);
      return false;
    }
    ws.send(text);
    return true;
  }

  close(): void {
    clearInterval(this.pinger);
    for (const ws of this.viewers) ws.close();
    this.viewers.clear();
  }
}
