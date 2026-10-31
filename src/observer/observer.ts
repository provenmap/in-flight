import { defaultCatalog, type Catalog } from '../pricing/catalog.js';
import { Poller, SimulatorClient } from './poller.js';
import { Reader, flowJson, type Frame } from './reader.js';

export interface ObserverOptions {
  bootstrap?: string;
  /** Where the simulator's control API is, or null to watch a flow with no simulator: no controls. */
  simulatorUrl?: string | null;
  catalog?: Catalog;
}

/** `SIMULATOR_URL`, where `none` means there is no simulator. */
export function simulatorUrlFrom(env: NodeJS.ProcessEnv): string | null {
  const url = env.SIMULATOR_URL ?? 'http://localhost:8090';
  return url === 'none' ? null : url;
}

/**
 * The observer: a reader that prices every event into the ledger, a poller for offsets and the simulator, and the
 * simulator's control API. The web view and the MCP server both work from one of these. Without a simulator it
 * watches: the same figures, from whatever produces the catalog's topics, and no controls.
 */
export class Observer {
  readonly bootstrap: string;
  readonly catalog: Catalog;
  readonly simulator: SimulatorClient | null;
  readonly poller: Poller;
  readonly reader: Reader;
  private readonly listeners = new Set<(frame: Frame) => void>();

  constructor(opts: ObserverOptions = {}) {
    const bootstrap = (this.bootstrap = opts.bootstrap ?? process.env.BOOTSTRAP_SERVERS ?? 'localhost:9092');
    this.catalog = opts.catalog ?? defaultCatalog();
    const url = opts.simulatorUrl === undefined ? simulatorUrlFrom(process.env) : opts.simulatorUrl;
    this.simulator = url === null ? null : new SimulatorClient(url);
    this.poller = new Poller(bootstrap, this.catalog, this.simulator);
    this.reader = new Reader(bootstrap, this.catalog, frame => this.listeners.forEach(l => l(frame)), this.poller, this.simulator === null);
  }

  start(): this {
    this.poller.start();
    this.reader.start();
    return this;
  }

  /** Every frame the reader publishes from now on. */
  onFrame(listener: (frame: Frame) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  get flow() {
    return flowJson(this.catalog);
  }

  get watching(): boolean {
    return this.simulator === null;
  }

  async close(): Promise<void> {
    await this.reader.stop();
    await this.poller.stop();
  }
}
