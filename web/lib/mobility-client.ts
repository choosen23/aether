import { assertMobilityStreamMessage, parseMobilitySnapshot, type MobilitySnapshot } from "./mobility-contracts";
import { MobilityStore } from "./mobility-store";

type FetchSnapshot = (signal: AbortSignal) => Promise<MobilitySnapshot>;

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

export class MobilityClient {
  private readonly store: MobilityStore;
  private readonly wsUrl: string;
  private readonly fetchSnapshot: FetchSnapshot;
  private readonly openSocket: OpenSocket;
  private readonly timerApi: TimerApi;
  private readonly retryBaseMs: number;
  private readonly onConnected?: () => void;
  private readonly onDisconnected?: () => void;

  private retryAttempt = 0;
  private retryHandle: number | null = null;
  private socket: WebSocketLike | null = null;
  private abortController: AbortController | null = null;
  private started = false;

  constructor(options: {
    store: MobilityStore;
    wsUrl: string;
    fetchSnapshot: FetchSnapshot;
    openSocket: OpenSocket;
    timerApi?: TimerApi;
    retryBaseMs?: number;
    onConnected?: () => void;
    onDisconnected?: () => void;
  }) {
    this.store = options.store;
    this.wsUrl = options.wsUrl;
    this.fetchSnapshot = options.fetchSnapshot;
    this.openSocket = options.openSocket;
    this.timerApi = options.timerApi ?? defaultTimerApi;
    this.retryBaseMs = options.retryBaseMs ?? 1000;
    this.onConnected = options.onConnected;
    this.onDisconnected = options.onDisconnected;
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
    this.onDisconnected?.();
  }

  private async connect(): Promise<void> {
    if (!this.started || !this.abortController) {
      return;
    }

    const socket = this.openSocket(this.wsUrl);
    this.socket = socket;
    let snapshotInstalled = false;
    const pendingMessages: ReturnType<typeof assertMobilityStreamMessage>[] = [];
    socket.onopen = () => {
      this.onConnected?.();
    };
    socket.onmessage = (event) => {
      const parsed = JSON.parse(event.data) as unknown;
      const message = assertMobilityStreamMessage(parsed);
      if (snapshotInstalled) {
        this.store.apply(message);
      } else {
        pendingMessages.push(message);
      }
    };
    socket.onclose = () => {
      this.socket = null;
      this.onDisconnected?.();
      this.scheduleRetry();
    };

    try {
      const snapshot = parseMobilitySnapshot(
        await this.fetchSnapshot(this.abortController.signal),
      );
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
      this.onDisconnected?.();
      this.scheduleRetry();
      return;
    }
    this.retryAttempt = 0;
  }

  private scheduleRetry(): void {
    if (!this.started || this.retryHandle !== null) {
      return;
    }
    const delay = jitter(this.retryBaseMs * 2 ** this.retryAttempt);
    this.retryAttempt += 1;
    this.retryHandle = this.timerApi.setTimeout(() => {
      this.retryHandle = null;
      void this.connect();
    }, delay);
  }
}

export type { OpenSocket, TimerApi, WebSocketLike };
