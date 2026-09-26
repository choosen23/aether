import {
  assertSimulationStreamMessage,
  parseSimulationSnapshot,
  type SimulationSnapshotResponse,
} from "./simulation-contracts";
import { SimulationStore } from "./simulation-store";

type FetchSnapshot = (signal: AbortSignal) => Promise<SimulationSnapshotResponse>;

type WebSocketLike = {
  onopen?: ((event: any) => void) | null;
  onmessage: ((event: any) => void) | null;
  onclose: ((event: any) => void) | null;
  close: () => void;
};

type OpenSocket = (url: string) => WebSocketLike;

type TimerApi = {
  setTimeout: (callback: () => void, delayMs: number) => number;
  clearTimeout: (handle: number) => void;
};

const defaultTimerApi: TimerApi = {
  setTimeout: (callback, delayMs) => window.setTimeout(callback, delayMs),
  clearTimeout: (handle) => window.clearTimeout(handle),
};

const jitter = (baseMs: number): number => {
  const variation = baseMs * 0.2;
  return Math.max(0, Math.round(baseMs + (Math.random() * 2 - 1) * variation));
};

export type SimulationClient = {
  start: () => void | Promise<void>;
  stop: () => void;
  subscribe: (listener: () => void) => () => void;
  getSnapshot: () => ReturnType<SimulationStore["getSnapshot"]>;
};

export class WebSimulationClient implements SimulationClient {
  private readonly store: SimulationStore;
  private readonly wsUrl: string;
  private readonly fetchSnapshot: FetchSnapshot;
  private readonly openSocket: OpenSocket;
  private readonly timerApi: TimerApi;
  private readonly retryBaseMs: number;
  private readonly retryMaxMs: number;

  private listeners = new Set<() => void>();
  private retryAttempt = 0;
  private retryHandle: number | null = null;
  private socket: WebSocketLike | null = null;
  private abortController: AbortController | null = null;
  private started = false;

  constructor(options: {
    store: SimulationStore;
    wsUrl: string;
    fetchSnapshot: FetchSnapshot;
    openSocket: OpenSocket;
    timerApi?: TimerApi;
    retryBaseMs?: number;
    retryMaxMs?: number;
  }) {
    this.store = options.store;
    this.wsUrl = options.wsUrl;
    this.fetchSnapshot = options.fetchSnapshot;
    this.openSocket = options.openSocket;
    this.timerApi = options.timerApi ?? defaultTimerApi;
    this.retryBaseMs = options.retryBaseMs ?? 1000;
    this.retryMaxMs = options.retryMaxMs ?? 30_000;
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    const unsubscribeStore = this.store.subscribe(listener);
    return () => {
      this.listeners.delete(listener);
      unsubscribeStore();
    };
  }

  getSnapshot(): ReturnType<SimulationStore["getSnapshot"]> {
    return this.store.getSnapshot();
  }

  async start(): Promise<void> {
    if (this.started) {
      return;
    }
    this.started = true;
    this.abortController = new AbortController();
    await this.connect();
  }

  stop(): void {
    this.started = false;
    this.retryAttempt = 0;

    if (this.retryHandle !== null) {
      this.timerApi.clearTimeout(this.retryHandle);
      this.retryHandle = null;
    }

    this.abortController?.abort();
    this.abortController = null;

    this.socket?.close();
    this.socket = null;
    this.store.setStreamConnected(false);
  }

  private async connect(): Promise<void> {
    if (!this.started || !this.abortController) {
      return;
    }

    const socket = this.openSocket(this.wsUrl);
    this.socket = socket;
    let snapshotInstalled = false;
    const pendingMessages: ReturnType<typeof assertSimulationStreamMessage>[] = [];

    socket.onopen = () => {
      this.store.setStreamConnected(true);
    };

    socket.onmessage = (event) => {
      let message: ReturnType<typeof assertSimulationStreamMessage>;
      try {
        const parsed = JSON.parse(event.data) as unknown;
        message = assertSimulationStreamMessage(parsed);
      } catch {
        return;
      }
      if (snapshotInstalled) {
        this.store.apply(message);
      } else {
        pendingMessages.push(message);
      }
    };

    socket.onclose = () => {
      this.socket = null;
      this.store.setStreamConnected(false);
      this.scheduleRetry();
    };

    try {
      const snapshot = parseSimulationSnapshot(await this.fetchSnapshot(this.abortController.signal));
      if (!this.started || this.socket !== socket) {
        return;
      }
      this.store.replaceSnapshot(snapshot);
      snapshotInstalled = true;
      for (const message of pendingMessages) {
        this.store.apply(message);
      }
    } catch {
      socket.onclose = null;
      socket.close();
      if (this.socket === socket) {
        this.socket = null;
      }
      this.store.setStreamConnected(false);
      this.scheduleRetry();
      return;
    }

    this.retryAttempt = 0;
  }

  private scheduleRetry(): void {
    if (!this.started || this.retryHandle !== null) {
      return;
    }
    const uncapped = this.retryBaseMs * 2 ** this.retryAttempt;
    const delay = jitter(Math.min(uncapped, this.retryMaxMs));
    this.retryAttempt += 1;
    this.retryHandle = this.timerApi.setTimeout(() => {
      this.retryHandle = null;
      void this.connect();
    }, delay);
  }
}

export type { OpenSocket, TimerApi, WebSocketLike };
