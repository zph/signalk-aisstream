import { AisStreamMessage, AisMessageType } from './types/aisstream';
import { BoundingBox, WebSocketManager, WebSocketManagerCallbacks } from './websocket-manager';

const HISTORY_MS = 30 * 60 * 1000;
const HISTORY_SAMPLE_INTERVAL_MS = 30 * 1000;
const TARGET_STALE_MS = 5 * 60 * 1000;
const IDLE_MS = 5 * 60 * 1000;
const UPDATE_MIN_MS = 1100;
const HANDOFF_OVERLAP_MS = 60 * 1000;
const MAX_TARGETS = 10_000;
const MAX_BBOX_SPAN_DEGREES = 10;
// A client can serialize two valid endpoints whose subtraction lands a few ulps above ten. Keep
// the public limit exact in practical terms without rejecting that ordinary floating-point noise.
const BBOX_SPAN_EPSILON = 1e-9;
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
  cogRad?: number;
  headingRad?: number;
  sogMps?: number;
  shipTypeId?: number;
  lengthMeters?: number;
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

type DestinationManager = ReturnType<DestinationManagerFactory>;

interface ManagerSlot {
  manager: DestinationManager;
  bbox: BoundingBox | null;
  confirmed: boolean;
}

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

function positionReport(message: AisStreamMessage):
  | {
      Sog?: number;
      Cog?: number;
      TrueHeading?: number;
      Type?: number;
      Dimension?: { A?: number; B?: number };
      NavigationalStatus?: number;
      Latitude?: number;
      Longitude?: number;
    }
  | undefined {
  return (
    message.Message.PositionReport ??
    message.Message.StandardClassBPositionReport ??
    message.Message.ExtendedClassBPositionReport
  );
}

function radians(value: unknown, maximum: number): number | undefined {
  return finiteInRange(value, 0, maximum) ? value * (Math.PI / 180) : undefined;
}

function vesselLength(dimension: { A?: number; B?: number } | undefined): number | undefined {
  const bow = dimension?.A;
  const stern = dimension?.B;
  if (!finiteInRange(bow, 0, 1_000) || !finiteInRange(stern, 0, 1_000)) return undefined;
  const length = bow + stern;
  return length > 0 && length <= 1_000 ? length : undefined;
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
    cogRad: radians(report.Cog, 359.9),
    headingRad: radians(report.TrueHeading, 359),
    sogMps: finiteInRange(report.Sog, 0, 200) ? report.Sog * KNOTS_TO_MPS : undefined,
    shipTypeId: finiteInRange(report.Type, 0, 99) ? report.Type : undefined,
    lengthMeters: vesselLength(report.Dimension),
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
    cogRad: target.cogRad,
    headingRad: target.headingRad,
    sogMps: target.sogMps,
    shipTypeId: target.shipTypeId,
    lengthMeters: target.lengthMeters,
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
    east - west > MAX_BBOX_SPAN_DEGREES + BBOX_SPAN_EPSILON ||
    north - south > MAX_BBOX_SPAN_DEGREES + BBOX_SPAN_EPSILON
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

function bboxContains(
  bbox: BoundingBox,
  position: { latitude: number; longitude: number },
): boolean {
  const north = Math.max(bbox[0].latitude, bbox[1].latitude);
  const south = Math.min(bbox[0].latitude, bbox[1].latitude);
  const east = Math.max(bbox[0].longitude, bbox[1].longitude);
  const west = Math.min(bbox[0].longitude, bbox[1].longitude);
  return (
    position.latitude >= south &&
    position.latitude <= north &&
    position.longitude >= west &&
    position.longitude <= east
  );
}

function bboxCenter(bbox: BoundingBox): { latitude: number; longitude: number } {
  return {
    latitude: (bbox[0].latitude + bbox[1].latitude) / 2,
    longitude: (bbox[0].longitude + bbox[1].longitude) / 2,
  };
}

export class DestinationAisReview {
  private readonly apiKey: string;
  private readonly callbacks: Pick<WebSocketManagerCallbacks, 'onDebug' | 'onError'>;
  private readonly managerFactory: DestinationManagerFactory;
  private readonly targets = new Map<string, TrackedTarget>();
  private readonly now: () => number;
  private state: DestinationReviewState = 'idle';
  private error: string | undefined;
  private active: ManagerSlot;
  private replacement: ManagerSlot | null = null;
  private desiredBbox: BoundingBox | null = null;
  private pendingBbox: BoundingBox | null = null;
  private lastUpdateAt = 0;
  private updateTimer: ReturnType<typeof setTimeout> | null = null;
  private idleTimer: ReturnType<typeof setTimeout> | null = null;
  private overlapTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    apiKey: string,
    callbacks: Pick<WebSocketManagerCallbacks, 'onDebug' | 'onError'>,
    managerFactory: DestinationManagerFactory = (...args) => new WebSocketManager(...args),
    now: () => number = Date.now,
  ) {
    this.apiKey = apiKey;
    this.callbacks = callbacks;
    this.managerFactory = managerFactory;
    this.now = now;
    this.active = this.createManager();
  }

  private createManager(): ManagerSlot {
    const slot = {} as ManagerSlot;
    slot.bbox = null;
    slot.confirmed = false;
    slot.manager = this.managerFactory(
      this.apiKey,
      ['PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport'],
      120_000,
      {
        onMessage: (message) => this.observe(message),
        onStatus: (status) => {
          if (status.startsWith('Disconnected') || status.startsWith('Rate limited')) {
            slot.confirmed = false;
            if (slot === this.replacement) this.clearOverlapTimer();
            if (this.isRelevant(slot)) this.state = 'disconnected';
          } else if (!status.startsWith('Connected') && this.isRelevant(slot)) {
            this.state = 'connecting';
          }
          if (this.isRelevant(slot)) this.error = undefined;
        },
        onSubscriptionConfirmed: (boundingBoxes) => {
          const confirmed = boundingBoxes[0];
          if (!confirmed || !sameBbox(slot.bbox, confirmed)) return;
          slot.confirmed = true;
          if (slot === this.replacement && sameBbox(this.desiredBbox, confirmed)) {
            this.state = 'live';
            this.error = undefined;
            this.beginOverlap(slot);
          } else if (
            slot === this.active &&
            this.replacement === null &&
            sameBbox(this.desiredBbox, confirmed)
          ) {
            this.state = 'live';
            this.error = undefined;
          }
        },
        onDebug: this.callbacks.onDebug,
        onError: (message) => {
          if (this.isRelevant(slot)) {
            this.state = 'error';
            this.error = message.slice(0, 512);
          }
          this.callbacks.onError(message);
        },
      },
    );
    return slot;
  }

  request(bbox: BoundingBox): DestinationSnapshot {
    this.resetIdleTimer();
    this.desiredBbox = bbox;
    if (this.active.bbox === null) {
      this.active.bbox = bbox;
      this.active.confirmed = false;
      this.pendingBbox = null;
      this.state = 'connecting';
      this.active.manager.start(bbox);
      this.lastUpdateAt = this.now();
    } else if (sameBbox(this.active.bbox, bbox)) {
      if (this.replacement) this.cancelReplacement();
      this.pendingBbox = null;
      if (this.active.confirmed) {
        this.state = 'live';
        this.error = undefined;
      }
    } else if (!this.replacement || !sameBbox(this.replacement.bbox, bbox)) {
      this.pendingBbox = bbox;
      this.state = 'connecting';
      this.scheduleUpdate();
    }
    return this.snapshot(bbox);
  }

  stop(): void {
    if (this.updateTimer) clearTimeout(this.updateTimer);
    if (this.idleTimer) clearTimeout(this.idleTimer);
    this.updateTimer = null;
    this.idleTimer = null;
    this.clearOverlapTimer();
    this.pendingBbox = null;
    this.desiredBbox = null;
    this.targets.clear();
    this.replacement?.manager.stop();
    this.replacement = null;
    this.active.manager.stop();
    this.active.bbox = null;
    this.active.confirmed = false;
    this.state = 'idle';
    this.error = undefined;
  }

  private observe(message: AisStreamMessage): void {
    const now = this.now();
    const report = parseTarget(message, now);
    if (!report) return;
    const prior = this.targets.get(report.mmsi);
    const samples = prior?.samples ?? [];
    const sample = { at: now, position: report.position, sogMps: report.sogMps };
    const latest = samples.at(-1);
    if (!latest || now - latest.at >= HISTORY_SAMPLE_INTERVAL_MS) samples.push(sample);
    while (samples[0] && now - samples[0].at > HISTORY_MS) samples.shift();
    this.targets.delete(report.mmsi);
    this.targets.set(report.mmsi, { ...prior, ...report, samples });
    while (this.targets.size > MAX_TARGETS) {
      const center = this.desiredBbox ? bboxCenter(this.desiredBbox) : undefined;
      let discard: string | undefined;
      let greatestDistance = -1;
      for (const [mmsi, target] of this.targets) {
        const distance = center ? metersBetween(center, target.position) : now - target.lastReportAtMs;
        if (distance > greatestDistance) {
          greatestDistance = distance;
          discard = mmsi;
        }
      }
      if (discard === undefined) break;
      this.targets.delete(discard);
    }
  }

  private snapshot(bbox: BoundingBox): DestinationSnapshot {
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
      targets: [...this.targets.values()]
        .filter((target) => bboxContains(bbox, target.position))
        .map(summarize),
    };
  }

  private scheduleUpdate(): void {
    if (this.updateTimer) return;
    const delay = Math.max(0, UPDATE_MIN_MS - (this.now() - this.lastUpdateAt));
    this.updateTimer = setTimeout(() => {
      this.updateTimer = null;
      const bbox = this.pendingBbox;
      this.pendingBbox = null;
      if (!bbox || sameBbox(this.active.bbox, bbox)) return;
      this.state = 'connecting';
      this.startOrRetargetReplacement(bbox);
      this.lastUpdateAt = this.now();
    }, delay);
  }

  private startOrRetargetReplacement(bbox: BoundingBox): void {
    this.clearOverlapTimer();
    if (this.replacement) {
      this.replacement.bbox = bbox;
      this.replacement.confirmed = false;
      this.replacement.manager.updateBoundingBox(bbox);
      this.callbacks.onDebug('Retargeting pending AIS viewport replacement');
      return;
    }
    const replacement = this.createManager();
    replacement.bbox = bbox;
    this.replacement = replacement;
    this.callbacks.onDebug('Starting replacement AIS viewport connection');
    replacement.manager.start(bbox);
  }

  private beginOverlap(slot: ManagerSlot): void {
    this.clearOverlapTimer();
    this.callbacks.onDebug('Replacement AIS viewport confirmed; overlapping connections for 60s');
    this.overlapTimer = setTimeout(() => {
      this.overlapTimer = null;
      if (
        this.replacement !== slot ||
        !slot.confirmed ||
        !slot.manager.isConnected ||
        slot.bbox === null ||
        !sameBbox(this.desiredBbox, slot.bbox)
      ) {
        return;
      }
      const previous = this.active;
      this.active = slot;
      this.replacement = null;
      previous.manager.stop();
      this.state = 'live';
      this.error = undefined;
      this.callbacks.onDebug('Replacement AIS viewport promoted; previous connection stopped');
    }, HANDOFF_OVERLAP_MS);
  }

  private cancelReplacement(): void {
    this.clearOverlapTimer();
    this.replacement?.manager.stop();
    this.replacement = null;
  }

  private clearOverlapTimer(): void {
    if (this.overlapTimer) clearTimeout(this.overlapTimer);
    this.overlapTimer = null;
  }

  private isRelevant(slot: ManagerSlot): boolean {
    if (slot === this.replacement) return true;
    return slot === this.active && this.replacement === null;
  }

  private resetIdleTimer(): void {
    if (this.idleTimer) clearTimeout(this.idleTimer);
    this.idleTimer = setTimeout(() => this.stop(), IDLE_MS);
  }
}
