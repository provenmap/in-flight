import { defaultCatalog, type Catalog } from '../pricing/catalog.js';
import { Poller, SimulatorClient } from './poller.js';
import { Reader, flowJson, type Frame } from './reader.js';

export interface ObserverOptions {
  bootstrap?: string;
  simulatorUrl?: string;
  catalog?: Catalog;
}

/**
 * The observer: a reader that prices every event into the ledger, a poller for offsets and the simulator, and the
 * simulator's control API. The web view and the MCP server both work from one of these.
 */
export class Observer {
  readonly bootstrap: string;
  readonly catalog: Catalog;
  readonly simulator: SimulatorClient;
  readonly poller: Poller;
  readonly reader: Reader;
  private readonly listeners = new Set<(frame: Frame) => void>();

  constructor(opts: ObserverOptions = {}) {
    const bootstrap = (this.bootstrap = opts.bootstrap ?? process.env.BOOTSTRAP_SERVERS ?? 'localhost:9092');
    this.catalog = opts.catalog ?? defaultCatalog();
    this.simulator = new SimulatorClient(opts.simulatorUrl ?? process.env.SIMULATOR_URL ?? 'http://localhost:8090');
    this.poller = new Poller(bootstrap, this.catalog, this.simulator);
    this.reader = new Reader(bootstrap, this.catalog, frame => this.listeners.forEach(l => l(frame)), this.poller);
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

  async close(): Promise<void> {
    await this.reader.stop();
    await this.poller.stop();
  }
}
