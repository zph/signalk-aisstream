import { describe, expect, it, vi } from 'vitest';
import type {
  PluginOptions,
  SignalKApp,
  SignalKPositionDelta,
} from '../src/types/signalk';
import createPlugin from '../src/index';
import type { AisMessageType } from '../src/types/aisstream';
import type { WebSocketManager, WebSocketManagerCallbacks } from '../src/websocket-manager';
import { positionReportMessage } from './fixtures/messages';

type WebSocketManagerFactory = (
  apiKey: string,
  messageTypes: AisMessageType[],
  watchdogTimeoutMs: number,
  callbacks: WebSocketManagerCallbacks,
) => WebSocketManager;

function baseOptions(overrides: Partial<PluginOptions> = {}): PluginOptions {
  return {
    apiKey: 'test-api-key',
    boundingBoxSize: 1,
    moveRelatedBoundingBox: 10,
    refreshRate: 60,
    positionReport: true,
    shipStaticData: false,
    staticDataReport: false,
    standardClassBPositionReport: false,
    extendedClassBPositionReport: false,
    aidsToNavigationReport: false,
    baseStationReport: false,
    ...overrides,
  };
}

function createApp(): {
  app: SignalKApp;
  deltaCallback: () => ((delta: SignalKPositionDelta) => void);
  debug: ReturnType<typeof vi.fn>;
  error: ReturnType<typeof vi.fn>;
} {
  const debug = vi.fn();
  const error = vi.fn();
  let callback: ((delta: SignalKPositionDelta) => void) | undefined;

  const app: SignalKApp = {
    debug,
    error,
    setPluginStatus: vi.fn(),
    handleMessage: vi.fn(),
    subscriptionmanager: {
      subscribe: vi.fn((_subscription, _unsubscribes, _errorCallback, nextCallback) => {
        callback = nextCallback;
      }),
    },
  };

  return {
    app,
    deltaCallback: () => {
      if (!callback) throw new Error('position subscription was not registered');
      return callback;
    },
    debug,
    error,
  };
}

describe('plugin position subscription', () => {
  it('ignores updates without values instead of throwing', () => {
    const { app, deltaCallback, error } = createApp();
    const plugin = createPlugin(app);

    plugin.start(baseOptions());

    expect(() => deltaCallback()({ updates: [{}] })).not.toThrow();
    expect(error).not.toHaveBeenCalledWith('Invalid delta received.');
  });

  it('finds a valid navigation position without assuming it is the first value', () => {
    const { app, deltaCallback, debug } = createApp();
    const plugin = createPlugin(app);

    plugin.start(baseOptions({ positionReport: false }));

    expect(() => deltaCallback()({
      updates: [
        {
          values: [
            { path: 'navigation.state', value: 'motoring' },
            { path: 'navigation.position', value: { longitude: 24.9, latitude: 60.2 } },
          ],
        },
      ],
    })).not.toThrow();
    expect(debug).toHaveBeenCalledWith('No need to update AIS stream');
  });
});

describe('destination subscription routing', () => {
  it('uses a dedicated second upstream socket without publishing remote targets as local traffic', () => {
    vi.useFakeTimers();
    const { app } = createApp();
    app.getSelfPath = () => ({ longitude: -122.4, latitude: 37.8 });
    const starts = [vi.fn(), vi.fn()];
    const stops = [vi.fn(), vi.fn()];
    const callbacks: Array<Parameters<WebSocketManagerFactory>[3]> = [];
    const managerFactory = vi.fn<WebSocketManagerFactory>(
      (_apiKey, _messageTypes, _watchdogTimeoutMs, nextCallbacks) => {
        const index = callbacks.length;
        callbacks.push(nextCallbacks);
        return {
          isConnected: index === 0,
          isReconnecting: false,
          start: starts[index],
          updateBoundingBox: vi.fn(),
          stop: stops[index],
        } as never;
      },
    );
    const plugin = createPlugin(app, managerFactory);
    let routeHandler:
      | ((request: { query?: Record<string, unknown> }, response: {
          json: ReturnType<typeof vi.fn>;
          set: ReturnType<typeof vi.fn>;
          status: ReturnType<typeof vi.fn>;
        }) => void)
      | undefined;
    plugin.registerWithRouter?.({
      access: () => ({
        get: (_path, handler) => {
          routeHandler = handler as typeof routeHandler;
        },
      }),
    });
    plugin.start(baseOptions());

    const response = {
      json: vi.fn(),
      set: vi.fn().mockReturnThis(),
      status: vi.fn().mockReturnThis(),
    };
    routeHandler?.({ query: { bbox: '[-71.4,41.4,-71.2,41.6]' } }, response);
    vi.advanceTimersByTime(0);

    expect(managerFactory).toHaveBeenCalledTimes(2);
    expect(starts[0]).toHaveBeenCalledWith(expect.any(Array));
    expect(starts[1]).toHaveBeenCalledWith(
      [
        { latitude: 41.6, longitude: -71.4 },
        { latitude: 41.4, longitude: -71.2 },
      ],
    );

    const remote = structuredClone(positionReportMessage);
    remote.MetaData.Latitude = 41.5;
    remote.MetaData.Longitude = -71.3;
    delete remote.MetaData.latitude;
    delete remote.MetaData.longitude;
    callbacks[1]?.onMessage(remote);
    expect(app.handleMessage).not.toHaveBeenCalled();

    routeHandler?.({ query: { bbox: '[-71.4,41.4,-71.2,41.6]' } }, response);
    expect(response.json).toHaveBeenLastCalledWith(
      expect.objectContaining({
        targets: [expect.objectContaining({ mmsi: '211234560' })],
      }),
    );

    const local = structuredClone(positionReportMessage);
    local.MetaData.Latitude = 37.8;
    local.MetaData.Longitude = -122.4;
    delete local.MetaData.latitude;
    delete local.MetaData.longitude;
    callbacks[0]?.onMessage(local);
    expect(app.handleMessage).toHaveBeenCalledTimes(1);
    plugin.stop();
    expect(stops[0]).toHaveBeenCalledOnce();
    expect(stops[1]).toHaveBeenCalledOnce();
    vi.useRealTimers();
  });
});
