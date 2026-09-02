import { AisStreamMessage, AisMessageType } from './types/aisstream';
import { BoundingBox, WebSocketManager, WebSocketManagerCallbacks } from './websocket-manager';

const HISTORY_MS = 30 * 60 * 1000;
const TARGET_STALE_MS = 5 * 60 * 1000;
const IDLE_MS = 5 * 60 * 1000;
const UPDATE_MIN_MS = 1100;
const MAX_TARGETS = 1000;
const KNOTS_TO_MPS = 0.514444;

export type DestinationReviewState =
  | 'idle'
  | 'connecting'
  | 'live'
  | 'disconnected'
  | 'error';

export interface DestinationTargetHistory {
  firstSeenAtMs: number;
  sampleCount: number;
  medianSogMps?: number;
  center: { latitude: number; longitude: number };
  maxRadiusMeters: number;
}

export interface DestinationTarget {
  id: string;
  mmsi: string;
  name?: string;
  position: { latitude: number; longitude: number };
  sogMps?: number;
  navigationState?: 'anchored' | 'moored';
  lastReportAtMs: number;
  history: DestinationTargetHistory;
}

export interface DestinationSnapshot {
  state: DestinationReviewState;
  error?: string;
  targets: DestinationTarget[];
}

interface PositionSample {
  at: number;
  position: { latitude: number; longitude: number };
  sogMps?: number;
}

interface TrackedTarget extends Omit<DestinationTarget, 'history'> {
  samples: PositionSample[];
}

export type DestinationManagerFactory = (
  apiKey: string,
  messageTypes: AisMessageType[],
  watchdogTimeoutMs: number,
  callbacks: WebSocketManagerCallbacks,
) => Pick<WebSocketManager, 'isConnected' | 'start' | 'stop' | 'updateBoundingBox'>;

function finiteInRange(value: unknown, min: number, max: number): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= min && value <= max;
}

function cleanName(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined;
  const name = value.trim().replace(/\s+/g, ' ');
  return name.length > 0 && name.length <= 256 ? name : undefined;
}

function navigationState(code: unknown): 'anchored' | 'moored' | undefined {
  if (code === 1) return 'anchored';
  if (code === 5) return 'moored';
  return undefined;
}

function positionReport(message: AisStreamMessage): {
  Sog?: number;
  NavigationalStatus?: number;
  Latitude?: number;
  Longitude?: number;
} | undefined {
  return (
    message.Message.PositionReport ??
    message.Message.StandardClassBPositionReport ??
    message.Message.ExtendedClassBPositionReport
  );
}

function parseTarget(message: AisStreamMessage, now: number): Omit<TrackedTarget, 'samples'> | undefined {
  const report = positionReport(message);
  if (!report) return undefined;
  const mmsi = String(message.MetaData?.MMSI ?? '');
  const latitude = message.MetaData?.latitude ?? message.MetaData?.Latitude ?? report.Latitude;
  const longitude = message.MetaData?.longitude ?? message.MetaData?.Longitude ?? report.Longitude;
  if (
    !/^\d{9}$/u.test(mmsi) ||
    !finiteInRange(latitude, -90, 90) ||
    !finiteInRange(longitude, -180, 180)
  ) {
    return undefined;
  }
  return {
    id: `aisstream:${mmsi}`,
    mmsi,
    name: cleanName(message.MetaData.ShipName),
    position: { latitude, longitude },
    sogMps: finiteInRange(report.Sog, 0, 200) ? report.Sog * KNOTS_TO_MPS : undefined,
    navigationState: navigationState(report.NavigationalStatus),
    lastReportAtMs: now,
  };
}

function metersBetween(
  left: { latitude: number; longitude: number },
  right: { latitude: number; longitude: number },
): number {
  const latitude = ((left.latitude + right.latitude) / 2) * (Math.PI / 180);
  const north = (right.latitude - left.latitude) * 111_320;
  const east = (right.longitude - left.longitude) * 111_320 * Math.cos(latitude);
  return Math.hypot(east, north);
}

function median(values: number[]): number | undefined {
  if (values.length === 0) return undefined;
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[middle - 1] + sorted[middle]) / 2
    : sorted[middle];
}

function summarize(target: TrackedTarget): DestinationTarget {
  const latitude =
    target.samples.reduce((sum, sample) => sum + sample.position.latitude, 0) /
    target.samples.length;
  const longitude =
    target.samples.reduce((sum, sample) => sum + sample.position.longitude, 0) /
    target.samples.length;
  const center = { latitude, longitude };
  return {
    id: target.id,
    mmsi: target.mmsi,
    name: target.name,
    position: target.position,
    sogMps: target.sogMps,
    navigationState: target.navigationState,
    lastReportAtMs: target.lastReportAtMs,
    history: {
      firstSeenAtMs: target.samples[0].at,
      sampleCount: target.samples.length,
      medianSogMps: median(
        target.samples.flatMap((sample) =>
          sample.sogMps === undefined ? [] : [sample.sogMps],
        ),
      ),
      center,
      maxRadiusMeters: target.samples.reduce(
        (largest, sample) => Math.max(largest, metersBetween(center, sample.position)),
        0,
      ),
    },
  };
}

export function parseDestinationBbox(value: unknown): BoundingBox | undefined {
  if (typeof value !== 'string' || value.length > 160) return undefined;
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    return undefined;
  }
  if (!Array.isArray(parsed) || parsed.length !== 4) return undefined;
  const [west, south, east, north] = parsed;
  if (
    !finiteInRange(west, -180, 180) ||
    !finiteInRange(east, -180, 180) ||
    !finiteInRange(south, -90, 90) ||
    !finiteInRange(north, -90, 90) ||
    west >= east ||
    south >= north ||
    east - west > 5 ||
    north - south > 5
  ) {
    return undefined;
  }
  return [
    { latitude: north, longitude: west },
    { latitude: south, longitude: east },
  ];
}

function sameBbox(left: BoundingBox | null, right: BoundingBox): boolean {
  return (
    left !== null &&
    left[0].latitude === right[0].latitude &&
    left[0].longitude === right[0].longitude &&
    left[1].latitude === right[1].latitude &&
    left[1].longitude === right[1].longitude
  );
}

export class DestinationAisReview {
  private readonly manager: ReturnType<DestinationManagerFactory>;
  private readonly targets = new Map<string, TrackedTarget>();
  private readonly now: () => number;
  private state: DestinationReviewState = 'idle';
  private error: string | undefined;
  private bbox: BoundingBox | null = null;
  private pendingBbox: BoundingBox | null = null;
  private lastUpdateAt = 0;
  private updateTimer: ReturnType<typeof setTimeout> | null = null;
  private idleTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    apiKey: string,
    callbacks: Pick<WebSocketManagerCallbacks, 'onDebug' | 'onError'>,
    managerFactory: DestinationManagerFactory = (...args) => new WebSocketManager(...args),
    now: () => number = Date.now,
  ) {
    this.now = now;
    this.manager = managerFactory(
      apiKey,
      ['PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport'],
      120_000,
      {
        onMessage: (message) => this.observe(message),
        onStatus: (status) => {
          this.state = status.startsWith('Connected')
            ? 'live'
            : status.startsWith('Disconnected')
              ? 'disconnected'
              : 'connecting';
          this.error = undefined;
        },
        onDebug: callbacks.onDebug,
        onError: (message) => {
          this.state = 'error';
          this.error = message.slice(0, 512);
          callbacks.onError(message);
        },
      },
    );
  }

  request(bbox: BoundingBox): DestinationSnapshot {
    this.resetIdleTimer();
    if (!this.manager.isConnected) {
      this.bbox = bbox;
      this.pendingBbox = null;
      this.state = 'connecting';
      this.manager.start(bbox);
      this.lastUpdateAt = this.now();
    } else if (!sameBbox(this.bbox, bbox)) {
      this.pendingBbox = bbox;
      this.state = 'connecting';
      this.scheduleUpdate();
    }
    return this.snapshot();
  }

  stop(): void {
    if (this.updateTimer) clearTimeout(this.updateTimer);
    if (this.idleTimer) clearTimeout(this.idleTimer);
    this.updateTimer = null;
    this.idleTimer = null;
    this.pendingBbox = null;
    this.bbox = null;
    this.targets.clear();
    this.manager.stop();
    this.state = 'idle';
    this.error = undefined;
  }

  private observe(message: AisStreamMessage): void {
    const now = this.now();
    const report = parseTarget(message, now);
    if (!report) return;
    const prior = this.targets.get(report.mmsi);
    const samples = prior?.samples ?? [];
    samples.push({ at: now, position: report.position, sogMps: report.sogMps });
    while (samples[0] && now - samples[0].at > HISTORY_MS) samples.shift();
    this.targets.delete(report.mmsi);
    this.targets.set(report.mmsi, { ...prior, ...report, samples });
    while (this.targets.size > MAX_TARGETS) {
      const oldest = this.targets.keys().next().value;
      if (oldest === undefined) break;
      this.targets.delete(oldest);
    }
  }

  private snapshot(): DestinationSnapshot {
    const now = this.now();
    for (const [mmsi, target] of this.targets) {
      while (target.samples[0] && now - target.samples[0].at > HISTORY_MS) {
        target.samples.shift();
      }
      if (now - target.lastReportAtMs > TARGET_STALE_MS || target.samples.length === 0) {
        this.targets.delete(mmsi);
      }
    }
    return {
      state: this.state,
      error: this.error,
      targets: [...this.targets.values()].map(summarize),
    };
  }

  private scheduleUpdate(): void {
    if (this.updateTimer) return;
    const delay = Math.max(0, UPDATE_MIN_MS - (this.now() - this.lastUpdateAt));
    this.updateTimer = setTimeout(() => {
      this.updateTimer = null;
      const bbox = this.pendingBbox;
      this.pendingBbox = null;
      if (!bbox || sameBbox(this.bbox, bbox)) return;
      this.bbox = bbox;
      this.state = 'connecting';
      this.manager.updateBoundingBox(bbox);
      this.lastUpdateAt = this.now();
    }, delay);
  }

  private resetIdleTimer(): void {
    if (this.idleTimer) clearTimeout(this.idleTimer);
    this.idleTimer = setTimeout(() => this.stop(), IDLE_MS);
  }
}
