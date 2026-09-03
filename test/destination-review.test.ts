import { describe, expect, it, vi } from 'vitest';
import {
  DestinationAisReview,
  DestinationManagerFactory,
  parseDestinationBbox,
} from '../src/destination-review';
import { AisStreamMessage } from '../src/types/aisstream';
import { BoundingBox, WebSocketManagerCallbacks } from '../src/websocket-manager';
import {
  extendedClassBMessage,
  positionReportMessage,
  standardClassBMessage,
} from './fixtures/messages';

const BOX: BoundingBox = [
  { latitude: 58, longitude: 11 },
  { latitude: 57, longitude: 12 },
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
    expect(parseDestinationBbox('[11,57,12,58]')).toEqual(BOX);
    expect(parseDestinationBbox('[0,0,10,10]')).toEqual([
      { latitude: 10, longitude: 0 },
      { latitude: 0, longitude: 10 },
    ]);
    expect(parseDestinationBbox('[0,0,10.1,10]')).toBeUndefined();
    expect(parseDestinationBbox('[-180,-90,180,90]')).toBeUndefined();
    expect(parseDestinationBbox('[0,0,0,1]')).toBeUndefined();
    expect(parseDestinationBbox('not-json')).toBeUndefined();
  });

  it('uses its manager and returns bounded history summaries', () => {
    const test = harness();
    expect(test.review.request(BOX)).toMatchObject({ state: 'connecting', targets: [] });
    expect(test.start).toHaveBeenCalledWith(BOX);

    test.callbacks().onStatus('Connected');
    test.callbacks().onSubscriptionConfirmed?.([BOX]);
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
      cogRad: Math.PI / 4,
      headingRad: (47 * Math.PI) / 180,
      history: { sampleCount: 2, firstSeenAtMs: 1_000_000 },
    });
    expect(snapshot.targets[0].history.maxRadiusMeters).toBeGreaterThan(0);
    test.review.stop();
  });

  it('accepts Class B positions without inventing a navigation state', () => {
    const test = harness();
    const message = structuredClone(standardClassBMessage);
    message.MetaData.latitude = 57.5;
    message.MetaData.longitude = 11.5;
    test.review.request(BOX);
    test.callbacks().onMessage(message);
    const target = test.review.request(BOX).targets[0];
    expect(target.mmsi).toBe('261000001');
    expect(target.navigationState).toBeUndefined();
    expect(target.sogMps).toBeCloseTo(3.19, 2);
    test.review.stop();
  });

  it('includes ship type and dimensions from extended Class B positions', () => {
    const test = harness();
    const message = structuredClone(extendedClassBMessage);
    message.MetaData.latitude = 57.5;
    message.MetaData.longitude = 11.5;
    test.review.request(BOX);
    test.callbacks().onMessage(message);

    const target = test.review.request(BOX).targets[0];
    expect(target).toMatchObject({
      shipTypeId: 37,
      lengthMeters: 15,
    });
    expect(target.cogRad).toBeCloseTo((270 * Math.PI) / 180);
    expect(target.headingRad).toBeCloseTo((268 * Math.PI) / 180);
    test.review.stop();
  });

  it('accepts the current uppercase MetaData position fields', () => {
    const test = harness();
    const message = structuredClone(positionReportMessage);
    message.MetaData.Latitude = message.MetaData.latitude;
    message.MetaData.Longitude = message.MetaData.longitude;
    delete message.MetaData.latitude;
    delete message.MetaData.longitude;
    test.review.request(BOX);
    test.callbacks().onMessage(message);
    expect(test.review.request(BOX).targets[0]?.position).toEqual({
      latitude: 57.6721,
      longitude: 11.8365,
    });
    test.review.stop();
  });

  it('rate-limits replacement subscriptions and bounds retained targets to the requested area', () => {
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
    expect(test.review.request(BOX).targets).toHaveLength(1);
    test.review.stop();
    vi.useRealTimers();
  });

  it('keeps a replacement connecting until its own subscription is confirmed', () => {
    vi.useFakeTimers();
    const test = harness();
    test.review.request(BOX);
    test.callbacks().onSubscriptionConfirmed?.([BOX]);
    expect(test.review.request(BOX).state).toBe('live');

    const next: BoundingBox = [
      { latitude: 43, longitude: -72 },
      { latitude: 42, longitude: -71 },
    ];
    expect(test.review.request(next).state).toBe('connecting');
    test.callbacks().onStatus('Connected');
    test.callbacks().onMessage(positionReportMessage);
    expect(test.review.request(next).state).toBe('connecting');

    test.advance(1_100);
    vi.advanceTimersByTime(1_100);
    test.callbacks().onSubscriptionConfirmed?.([next]);
    expect(test.review.request(next).state).toBe('live');
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

  it('retains at most one history sample every thirty seconds', () => {
    const test = harness();
    test.review.request(BOX);
    for (let index = 0; index <= 30; index += 1) {
      test.callbacks().onMessage(positionReportMessage);
      test.advance(1_000);
    }
    expect(test.review.request(BOX).targets[0].history.sampleCount).toBe(2);
    test.review.stop();
  });

  it('keeps ten thousand targets nearest the active box center', () => {
    const test = harness();
    test.review.request(BOX);
    const message = structuredClone(positionReportMessage);
    message.MetaData.MMSI = 200000000;
    message.MetaData.latitude = 57.99;
    message.MetaData.longitude = 11.99;
    test.callbacks().onMessage(message);
    for (let index = 0; index < 10_000; index += 1) {
      const nearby = structuredClone(positionReportMessage);
      nearby.MetaData.MMSI = 300000000 + index;
      nearby.MetaData.latitude = 57.5 + (index % 10) * 0.00001;
      nearby.MetaData.longitude = 11.5 + (index % 10) * 0.00001;
      test.callbacks().onMessage(nearby);
    }
    const targets = test.review.request(BOX).targets;
    expect(targets).toHaveLength(10_000);
    expect(targets.some((target) => target.mmsi === '200000000')).toBe(false);
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
