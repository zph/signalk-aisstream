import { EventEmitter } from 'node:events';
import { describe, expect, it, vi } from 'vitest';
import WebSocket from 'ws';
import {
  addReconnectJitter,
  createSubscriptionMessage,
  parseRetryAfterMs,
  WebSocketManager,
} from '../src/websocket-manager';

class FakeSocket extends EventEmitter {
  readyState = WebSocket.CONNECTING;
  send = vi.fn();
  close = vi.fn();

  addEventListener(type: string, listener: (...args: unknown[]) => void): void {
    this.on(type, listener);
  }

  terminate(): void {
    this.emit('error', { message: 'closed before connection established' });
    this.emit('close', { code: 1006, wasClean: false, reason: '' });
  }
}

describe('WebSocket reconnect backoff', () => {
  it('parses Retry-After seconds and HTTP dates', () => {
    const now = Date.parse('2026-09-02T12:00:00Z');
    expect(parseRetryAfterMs('120', now)).toBe(120_000);
    expect(parseRetryAfterMs('Wed, 02 Sep 2026 12:03:00 GMT', now)).toBe(180_000);
    expect(parseRetryAfterMs(['45'], now)).toBe(45_000);
    expect(parseRetryAfterMs('not-a-delay', now)).toBeUndefined();
  });

  it('adds bounded positive jitter so clients do not retry together', () => {
    expect(addReconnectJitter(60_000, () => 0)).toBe(60_000);
    expect(addReconnectJitter(60_000, () => 1)).toBe(75_000);
    expect(addReconnectJitter(900_000, () => 1)).toBe(960_000);
  });

  it('honors Retry-After on a 429 without reconnecting early or reporting a noisy error', () => {
    vi.useFakeTimers();
    vi.spyOn(Math, 'random').mockReturnValue(0);
    const sockets: FakeSocket[] = [];
    const createSocket = vi.fn(() => {
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket as unknown as WebSocket;
    });
    const onError = vi.fn();
    const onStatus = vi.fn();
    const manager = new WebSocketManager(
      'test-key',
      ['PositionReport'],
      120_000,
      { onMessage: vi.fn(), onStatus, onDebug: vi.fn(), onError },
      createSocket,
    );
    manager.start([
      { latitude: 42, longitude: -72 },
      { latitude: 41, longitude: -71 },
    ]);

    const response = new EventEmitter() as EventEmitter & {
      statusCode: number;
      headers: Record<string, string>;
      resume: ReturnType<typeof vi.fn>;
    };
    response.statusCode = 429;
    response.headers = { 'retry-after': '120' };
    response.resume = vi.fn();
    sockets[0].emit('unexpected-response', {}, response);

    expect(response.resume).toHaveBeenCalled();
    expect(onError).not.toHaveBeenCalled();
    expect(onStatus).toHaveBeenLastCalledWith('Rate limited - reconnecting in 120s');
    vi.advanceTimersByTime(119_999);
    expect(createSocket).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(1);
    expect(createSocket).toHaveBeenCalledTimes(2);

    manager.stop();
    vi.restoreAllMocks();
    vi.useRealTimers();
  });
});

describe('WebSocket subscription messages', () => {
  it('serializes every requested box into one subscription message', () => {
    expect(
      createSubscriptionMessage(
        'test-key',
        [
          [
            { latitude: 38, longitude: -123 },
            { latitude: 37, longitude: -122 },
          ],
          [
            { latitude: 42, longitude: -72 },
            { latitude: 41, longitude: -71 },
          ],
        ],
        ['PositionReport', 'StandardClassBPositionReport'],
      ),
    ).toEqual({
      APIKey: 'test-key',
      BoundingBoxes: [
        [
          [38, -123],
          [37, -122],
        ],
        [
          [42, -72],
          [41, -71],
        ],
      ],
      FilterMessageTypes: ['PositionReport', 'StandardClassBPositionReport'],
    });
  });

  it('reports which bounding boxes a subscription confirmation activates', () => {
    const socket = new FakeSocket();
    const onMessage = vi.fn();
    const onSubscriptionConfirmed = vi.fn();
    const manager = new WebSocketManager(
      'test-key',
      ['PositionReport'],
      120_000,
      {
        onMessage,
        onStatus: vi.fn(),
        onDebug: vi.fn(),
        onError: vi.fn(),
        onSubscriptionConfirmed,
      },
      () => socket as unknown as WebSocket,
    );
    const box = [
      { latitude: 42, longitude: -72 },
      { latitude: 41, longitude: -71 },
    ] as const;
    manager.start([box[0], box[1]]);
    socket.readyState = WebSocket.OPEN;
    socket.emit('open');
    socket.emit(
      'message',
      { data: Buffer.from(JSON.stringify({ MessageType: 'SubscriptionConfirmation', Message: {} })) },
    );

    expect(onSubscriptionConfirmed).toHaveBeenCalledWith([[box[0], box[1]]]);
    expect(onMessage).not.toHaveBeenCalled();
    manager.stop();
  });
});
