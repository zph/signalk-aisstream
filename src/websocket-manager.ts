/**
 * WebSocket connection manager for aisstream.io.
 * Handles connection lifecycle, subscription updates, reconnection with
 * exponential backoff, and watchdog timeout.
 */

import WebSocket, { ClientOptions } from 'ws';
import { AisStreamMessage, AisMessageType, SubscriptionMessage } from './types/aisstream';

const AISSTREAM_URL = 'wss://stream.aisstream.io/v0/stream';
const HANDSHAKE_TIMEOUT = 30000;
const CONNECT_TIMEOUT_FALLBACK = 32000;
const INITIAL_RECONNECT_DELAY = 5000;
const MAX_RECONNECT_DELAY = 300000;
const INITIAL_RATE_LIMIT_DELAY = 60000;
const MAX_RATE_LIMIT_DELAY = 900000;
const MAX_JITTER_DELAY = 60000;
const SUBSCRIPTION_MIN_INTERVAL = 1100;

export function parseRetryAfterMs(
  value: string | string[] | undefined,
  now: number = Date.now(),
): number | undefined {
  const candidate = Array.isArray(value) ? value[0] : value;
  if (!candidate) return undefined;
  const seconds = Number(candidate);
  if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1000;
  const at = Date.parse(candidate);
  return Number.isFinite(at) ? Math.max(0, at - now) : undefined;
}

export function addReconnectJitter(delay: number, random: () => number = Math.random): number {
  const jitter = Math.min(delay * 0.25, MAX_JITTER_DELAY) * Math.max(0, Math.min(1, random()));
  return Math.round(delay + jitter);
}

export function createSubscriptionMessage(
  apiKey: string,
  boundingBoxes: BoundingBox[],
  messageTypes: AisMessageType[],
): SubscriptionMessage {
  return {
    APIKey: apiKey,
    BoundingBoxes: boundingBoxes.map((boundingBox) => [
      [boundingBox[0].latitude, boundingBox[0].longitude],
      [boundingBox[1].latitude, boundingBox[1].longitude],
    ]),
    FilterMessageTypes: messageTypes,
  };
}

export interface BoundingBoxCorner {
  latitude: number;
  longitude: number;
}

export type BoundingBox = [BoundingBoxCorner, BoundingBoxCorner];

export interface WebSocketManagerCallbacks {
  onMessage: (message: AisStreamMessage) => void;
  onStatus: (status: string) => void;
  onDebug: (message: string) => void;
  onError: (message: string) => void;
  onSubscriptionConfirmed?: (boundingBoxes: BoundingBox[]) => void;
}

export type WebSocketFactory = (url: string, options: ClientOptions) => WebSocket;

export class WebSocketManager {
  private socket: WebSocket | null = null;
  private watchdogTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private subscriptionTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectDelay: number = INITIAL_RECONNECT_DELAY;
  private rateLimitDelay: number = INITIAL_RATE_LIMIT_DELAY;
  private nextReconnectDelay: number | null = null;
  private suppressNextError = false;
  private lastSubscriptionAt = 0;
  private readonly apiKey: string;
  private readonly messageTypes: AisMessageType[];
  private readonly watchdogTimeout: number;
  private readonly callbacks: WebSocketManagerCallbacks;
  private readonly createSocket: WebSocketFactory;
  private boundingBoxes: BoundingBox[] = [];
  private pendingConfirmations: BoundingBox[][] = [];
  private stopped = false;

  constructor(
    apiKey: string,
    messageTypes: AisMessageType[],
    watchdogTimeoutMs: number,
    callbacks: WebSocketManagerCallbacks,
    createSocket: WebSocketFactory = (url, options) => new WebSocket(url, options),
  ) {
    this.apiKey = apiKey;
    this.messageTypes = messageTypes;
    this.watchdogTimeout = watchdogTimeoutMs;
    this.callbacks = callbacks;
    this.createSocket = createSocket;
  }

  get isConnected(): boolean {
    return this.socket !== null;
  }

  get isReconnecting(): boolean {
    return this.reconnectTimer !== null;
  }

  start(boundingBox: BoundingBox): void {
    this.startBoundingBoxes([boundingBox]);
  }

  startBoundingBoxes(boundingBoxes: BoundingBox[]): void {
    this.stopped = false;
    this.boundingBoxes = boundingBoxes;
    if (this.socket || this.reconnectTimer) return;
    this.connect();
  }

  updateBoundingBox(boundingBox: BoundingBox): void {
    this.updateBoundingBoxes([boundingBox]);
  }

  updateBoundingBoxes(boundingBoxes: BoundingBox[]): void {
    this.boundingBoxes = boundingBoxes;
    if (this.socket && this.socket.readyState === WebSocket.OPEN) {
      this.scheduleSubscription();
    }
  }

  stop(): void {
    this.stopped = true;
    this.clearWatchdog();
    this.clearReconnectTimer();
    this.clearSubscriptionTimer();
    this.reconnectDelay = INITIAL_RECONNECT_DELAY;
    this.rateLimitDelay = INITIAL_RATE_LIMIT_DELAY;
    this.nextReconnectDelay = null;
    this.suppressNextError = false;
    this.lastSubscriptionAt = 0;
    this.pendingConfirmations = [];
    if (this.socket) {
      this.socket.close();
    }
    this.socket = null;
  }

  private connect(): void {
    if (
      this.stopped ||
      this.socket ||
      this.boundingBoxes.length === 0 ||
      this.messageTypes.length === 0
    ) {
      return;
    }

    this.socket = this.createSocket(AISSTREAM_URL, {
      handshakeTimeout: HANDSHAKE_TIMEOUT,
      perMessageDeflate: true,
    });

    this.callbacks.onStatus('Connecting...');

    const connectTimeout = setTimeout(() => {
      if (this.socket && this.socket.readyState === WebSocket.CONNECTING) {
        this.callbacks.onDebug('WebSocket connection timeout (fallback), retrying...');
        this.socket.terminate();
      }
    }, CONNECT_TIMEOUT_FALLBACK);

    this.socket.on('unexpected-response', (_request, response) => {
      clearTimeout(connectTimeout);
      const statusCode = response.statusCode ?? 0;
      if (statusCode === 429) {
        const retryAfter = parseRetryAfterMs(response.headers['retry-after']);
        const baseDelay = Math.max(this.rateLimitDelay, retryAfter ?? 0);
        this.nextReconnectDelay = addReconnectJitter(baseDelay);
        this.rateLimitDelay = Math.min(this.rateLimitDelay * 2, MAX_RATE_LIMIT_DELAY);
      } else {
        this.callbacks.onError(`WebSocket upgrade rejected with HTTP ${statusCode}`);
      }
      this.suppressNextError = true;
      response.resume();
      this.socket?.terminate();
    });

    this.socket.addEventListener('open', () => {
      clearTimeout(connectTimeout);
      this.callbacks.onStatus('Connected - waiting for AIS data');
      this.resetWatchdog();
      this.sendSubscription();
    });

    this.socket.addEventListener('error', (event) => {
      if (this.suppressNextError) {
        this.suppressNextError = false;
        return;
      }
      this.callbacks.onError('WebSocket error: ' + event.message);
    });

    this.socket.addEventListener('close', (event) => {
      clearTimeout(connectTimeout);
      this.callbacks.onDebug(
        `WebSocket closed: code=${event.code} wasClean=${event.wasClean} reason=${event.reason || 'none'}`,
      );
      this.socket = null;
      this.clearSubscriptionTimer();
      this.pendingConfirmations = [];
      if (!this.stopped) this.scheduleReconnect();
    });

    this.socket.addEventListener('message', (event) => {
      try {
        const message = JSON.parse(String(event.data)) as unknown;
        if (
          typeof message === 'object' &&
          message !== null &&
          (message as { MessageType?: unknown }).MessageType === 'SubscriptionConfirmation'
        ) {
          const confirmed = this.pendingConfirmations.shift();
          if (confirmed) this.callbacks.onSubscriptionConfirmed?.(confirmed);
          this.resetWatchdog();
          this.reconnectDelay = INITIAL_RECONNECT_DELAY;
          this.rateLimitDelay = INITIAL_RATE_LIMIT_DELAY;
          this.callbacks.onStatus('Connected');
          return;
        }
        const aisMessage = message as AisStreamMessage;
        this.callbacks.onMessage(aisMessage);
        this.resetWatchdog();
        this.reconnectDelay = INITIAL_RECONNECT_DELAY;
        this.rateLimitDelay = INITIAL_RATE_LIMIT_DELAY;
        this.callbacks.onStatus('Connected');
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        this.callbacks.onError('Error parsing message: ' + errorMessage);
      }
    });
  }

  private sendSubscription(): void {
    if (!this.socket || this.boundingBoxes.length === 0) return;

    const subscription = createSubscriptionMessage(
      this.apiKey,
      this.boundingBoxes,
      this.messageTypes,
    );

    this.callbacks.onDebug('Subscription Message: ' + JSON.stringify(subscription));
    this.socket.send(JSON.stringify(subscription));
    this.pendingConfirmations.push(
      this.boundingBoxes.map((box): BoundingBox => [{ ...box[0] }, { ...box[1] }]),
    );
    this.lastSubscriptionAt = Date.now();
  }

  private scheduleSubscription(): void {
    if (this.subscriptionTimer) return;
    const delay = Math.max(0, SUBSCRIPTION_MIN_INTERVAL - (Date.now() - this.lastSubscriptionAt));
    this.subscriptionTimer = setTimeout(() => {
      this.subscriptionTimer = null;
      if (this.socket?.readyState === WebSocket.OPEN) this.sendSubscription();
    }, delay);
  }

  private scheduleReconnect(): void {
    if (
      this.reconnectTimer ||
      this.stopped ||
      this.boundingBoxes.length === 0 ||
      this.messageTypes.length === 0
    ) {
      return;
    }

    const rateLimited = this.nextReconnectDelay !== null;
    const delay = this.nextReconnectDelay ?? this.reconnectDelay;
    this.nextReconnectDelay = null;
    const delaySec = Math.ceil(delay / 1000);
    const reason = rateLimited ? 'Rate limited' : 'Disconnected';
    this.callbacks.onDebug(`${reason}; reconnecting in ${delaySec}s...`);
    this.callbacks.onStatus(`${reason} - reconnecting in ${delaySec}s`);

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (
        !this.socket &&
        this.boundingBoxes.length > 0 &&
        this.messageTypes.length > 0 &&
        !this.stopped
      ) {
        this.connect();
      }
    }, delay);

    if (!rateLimited) {
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, MAX_RECONNECT_DELAY);
    }
  }

  private resetWatchdog(): void {
    this.clearWatchdog();
    this.watchdogTimer = setTimeout(() => {
      if (this.socket) {
        this.clearReconnectTimer();
        this.reconnectDelay = INITIAL_RECONNECT_DELAY;
        this.socket.terminate();
        this.socket = null;
        this.callbacks.onDebug(
          'Watchdog event, websocket connection closed and reconnection will be tried',
        );
        if (!this.stopped) {
          this.scheduleReconnect();
        }
      }
    }, this.watchdogTimeout);
  }

  private clearWatchdog(): void {
    if (this.watchdogTimer) {
      clearTimeout(this.watchdogTimer);
      this.watchdogTimer = null;
    }
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  private clearSubscriptionTimer(): void {
    if (this.subscriptionTimer) {
      clearTimeout(this.subscriptionTimer);
      this.subscriptionTimer = null;
    }
  }
}
