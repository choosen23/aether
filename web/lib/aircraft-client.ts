import { AircraftStore } from "./aircraft-store";
import { assertStreamMessage, type AircraftSnapshot } from "./contracts";

type FetchSnapshot = (signal: AbortSignal) => Promise<AircraftSnapshot>;

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

export class AircraftClient {
  private readonly store: AircraftStore;
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
    store: AircraftStore;
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

    const snapshot = await this.fetchSnapshot(this.abortController.signal);
    if (!this.started) {
      return;
    }
    this.store.replaceSnapshot(snapshot);

    const socket = this.openSocket(this.wsUrl);
    this.socket = socket;
    socket.onopen = () => {
      this.onConnected?.();
    };
    socket.onmessage = (event) => {
      const payload = typeof event.data === "string" ? event.data : String(event.data);
      const parsed = JSON.parse(payload) as unknown;
      this.store.apply(assertStreamMessage(parsed));
    };
    socket.onclose = () => {
      this.socket = null;
      this.onDisconnected?.();
      if (!this.started) {
        return;
      }

      const delay = jitter(this.retryBaseMs * 2 ** this.retryAttempt);
      this.retryAttempt += 1;
      this.retryHandle = this.timerApi.setTimeout(() => {
        this.retryHandle = null;
        void this.connect();
      }, delay);
    };
    this.retryAttempt = 0;
  }
}

export type { OpenSocket, TimerApi, WebSocketLike };
