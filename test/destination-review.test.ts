import { describe, expect, it, vi } from 'vitest';
import {
  DestinationAisReview,
  DestinationManagerFactory,
  parseDestinationBbox,
} from '../src/destination-review';
import { AisStreamMessage } from '../src/types/aisstream';
import { BoundingBox, WebSocketManagerCallbacks } from '../src/websocket-manager';
import { positionReportMessage, standardClassBMessage } from './fixtures/messages';

const BOX: BoundingBox = [
  { latitude: 42, longitude: -71 },
  { latitude: 41, longitude: -70 },
];

function harness() {
  let connected = false;
  let now = 1_000_000;
  let callbacks: WebSocketManagerCallbacks | undefined;
  const start = vi.fn(() => {
    connected = true;
  });
  const stop = vi.fn(() => {
    connected = false;
  });
  const updateBoundingBox = vi.fn();
  const managerFactory: DestinationManagerFactory = (
    _apiKey,
    _messageTypes,
    _watchdog,
    nextCallbacks,
  ) => {
    callbacks = nextCallbacks;
    return {
      get isConnected() {
        return connected;
      },
      start,
      stop,
      updateBoundingBox,
    };
  };
  const review = new DestinationAisReview(
    'test-key',
    { onDebug: vi.fn(), onError: vi.fn() },
    managerFactory,
    () => now,
  );
  return {
    review,
    start,
    stop,
    updateBoundingBox,
    callbacks: () => {
      if (!callbacks) throw new Error('manager callbacks were not registered');
      return callbacks;
    },
    advance(milliseconds: number) {
      now += milliseconds;
    },
  };
}

describe('destination AIS review', () => {
  it('validates and converts a bounded GeoJSON bbox', () => {
    expect(parseDestinationBbox('[-71,41,-70,42]')).toEqual(BOX);
    expect(parseDestinationBbox('[-180,-90,180,90]')).toBeUndefined();
    expect(parseDestinationBbox('[0,0,0,1]')).toBeUndefined();
    expect(parseDestinationBbox('not-json')).toBeUndefined();
  });

  it('uses a separate connection and returns bounded history summaries', () => {
    const test = harness();
    expect(test.review.request(BOX)).toMatchObject({ state: 'connecting', targets: [] });
    expect(test.start).toHaveBeenCalledWith(BOX);

    test.callbacks().onStatus('Connected');
    test.callbacks().onMessage(positionReportMessage);
    test.advance(30_000);
    const moved = structuredClone(positionReportMessage);
    moved.MetaData.latitude += 0.0001;
    moved.MetaData.longitude += 0.0001;
    moved.Message.PositionReport!.Sog = 0.2;
    test.callbacks().onMessage(moved);

    const snapshot = test.review.request(BOX);
    expect(snapshot.state).toBe('live');
    expect(snapshot.targets).toHaveLength(1);
    expect(snapshot.targets[0]).toMatchObject({
      mmsi: '211234560',
      name: 'TEST VESSEL',
      history: { sampleCount: 2, firstSeenAtMs: 1_000_000 },
    });
    expect(snapshot.targets[0].history.maxRadiusMeters).toBeGreaterThan(0);
    test.review.stop();
  });

  it('accepts Class B positions without inventing a navigation state', () => {
    const test = harness();
    test.review.request(BOX);
    test.callbacks().onMessage(standardClassBMessage);
    const target = test.review.request(BOX).targets[0];
    expect(target.mmsi).toBe('261000001');
    expect(target.navigationState).toBeUndefined();
    expect(target.sogMps).toBeCloseTo(3.19, 2);
    test.review.stop();
  });

  it('rate-limits replacement subscriptions and clears the prior area', () => {
    vi.useFakeTimers();
    const test = harness();
    test.review.request(BOX);
    test.callbacks().onMessage(positionReportMessage);
    const next: BoundingBox = [
      { latitude: 43, longitude: -72 },
      { latitude: 42, longitude: -71 },
    ];
    expect(test.review.request(next)).toMatchObject({ state: 'connecting', targets: [] });
    expect(test.updateBoundingBox).not.toHaveBeenCalled();
    test.advance(1_100);
    vi.advanceTimersByTime(1_100);
    expect(test.updateBoundingBox).toHaveBeenCalledWith(next);
    expect(test.review.request(next).targets).toEqual([]);
    test.review.stop();
    vi.useRealTimers();
  });

  it('drops stale destination targets', () => {
    const test = harness();
    test.review.request(BOX);
    test.callbacks().onMessage(positionReportMessage);
    test.advance(5 * 60 * 1000 + 1);
    expect(test.review.request(BOX).targets).toEqual([]);
    test.review.stop();
  });

  it('ignores messages without a vessel position report', () => {
    const test = harness();
    test.review.request(BOX);
    test.callbacks().onMessage({
      ...positionReportMessage,
      MessageType: 'ShipStaticData',
      Message: {},
    } as AisStreamMessage);
    expect(test.review.request(BOX).targets).toEqual([]);
    test.review.stop();
  });
});
