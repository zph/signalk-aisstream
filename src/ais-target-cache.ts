import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { DatabaseSync, type StatementSync } from 'node:sqlite';
import type { DestinationTarget } from './destination-review';
import type { BoundingBox } from './websocket-manager';

export const AIS_TARGET_RETENTION_MS = 60 * 60 * 1000;
const FLUSH_INTERVAL_MS = 1000;
const CLEANUP_INTERVAL_MS = 60 * 1000;
const MAX_QUERY_TARGETS = 10_000;

interface CacheRow {
  target_json: string;
}

export interface DestinationTargetCache {
  remember(target: DestinationTarget): void;
  within(bbox: BoundingBox, now: number): DestinationTarget[];
}

function contains(bbox: BoundingBox, target: DestinationTarget): boolean {
  const north = Math.max(bbox[0].latitude, bbox[1].latitude);
  const south = Math.min(bbox[0].latitude, bbox[1].latitude);
  const east = Math.max(bbox[0].longitude, bbox[1].longitude);
  const west = Math.min(bbox[0].longitude, bbox[1].longitude);
  return (
    target.position.latitude >= south &&
    target.position.latitude <= north &&
    target.position.longitude >= west &&
    target.position.longitude <= east
  );
}

function isDestinationTarget(value: unknown): value is DestinationTarget {
  if (typeof value !== 'object' || value === null) return false;
  const target = value as Partial<DestinationTarget>;
  return (
    typeof target.mmsi === 'string' &&
    typeof target.lastReportAtMs === 'number' &&
    typeof target.position?.latitude === 'number' &&
    typeof target.position.longitude === 'number'
  );
}

/** Durable, bounded latest-position cache shared by every AISStream connection. */
export class AisTargetCache implements DestinationTargetCache {
  private readonly db: DatabaseSync;
  private readonly pending = new Map<string, DestinationTarget>();
  private readonly upsertTarget: StatementSync;
  private readonly selectId: StatementSync;
  private readonly upsertBounds: StatementSync;
  private readonly selectWithin: StatementSync;
  private readonly deleteExpired: StatementSync;
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private cleanupTimer: ReturnType<typeof setInterval> | null = null;
  private closed = false;

  constructor(
    databasePath: string,
    private readonly onError: (message: string) => void = () => {},
  ) {
    mkdirSync(dirname(databasePath), { recursive: true });
    this.db = new DatabaseSync(databasePath, { timeout: 5000 });
    this.db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON;');
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS ais_targets (
        id INTEGER PRIMARY KEY,
        mmsi TEXT NOT NULL UNIQUE,
        latitude REAL NOT NULL,
        longitude REAL NOT NULL,
        received_at_ms INTEGER NOT NULL,
        target_json TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS ais_targets_received_at
        ON ais_targets(received_at_ms);
      CREATE VIRTUAL TABLE IF NOT EXISTS ais_target_bounds USING rtree(
        id,
        min_lon, max_lon,
        min_lat, max_lat
      );
      CREATE TRIGGER IF NOT EXISTS ais_targets_delete_bounds
      AFTER DELETE ON ais_targets
      BEGIN
        DELETE FROM ais_target_bounds WHERE id = OLD.id;
      END;
    `);
    this.upsertTarget = this.db.prepare(`
      INSERT INTO ais_targets (
        mmsi, latitude, longitude, received_at_ms, target_json
      ) VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(mmsi) DO UPDATE SET
        latitude=excluded.latitude,
        longitude=excluded.longitude,
        received_at_ms=excluded.received_at_ms,
        target_json=excluded.target_json
      WHERE excluded.received_at_ms >= ais_targets.received_at_ms
    `);
    this.selectId = this.db.prepare('SELECT id FROM ais_targets WHERE mmsi=?');
    this.upsertBounds = this.db.prepare(`
      INSERT OR REPLACE INTO ais_target_bounds (
        id, min_lon, max_lon, min_lat, max_lat
      ) VALUES (?, ?, ?, ?, ?)
    `);
    this.selectWithin = this.db.prepare(`
      SELECT target_json
      FROM ais_target_bounds AS bounds
      JOIN ais_targets AS targets ON targets.id = bounds.id
      WHERE bounds.max_lon >= ? AND bounds.min_lon <= ?
        AND bounds.max_lat >= ? AND bounds.min_lat <= ?
        AND targets.received_at_ms >= ?
      ORDER BY targets.received_at_ms DESC
      LIMIT ${MAX_QUERY_TARGETS}
    `);
    this.deleteExpired = this.db.prepare('DELETE FROM ais_targets WHERE received_at_ms < ?');
    this.cleanup(Date.now());
    this.cleanupTimer = setInterval(() => this.cleanup(Date.now()), CLEANUP_INTERVAL_MS);
    this.cleanupTimer.unref?.();
  }

  remember(target: DestinationTarget): void {
    if (this.closed) return;
    const prior = this.pending.get(target.mmsi);
    if (!prior || target.lastReportAtMs >= prior.lastReportAtMs) {
      this.pending.set(target.mmsi, target);
    }
    if (!this.flushTimer) {
      this.flushTimer = setTimeout(() => {
        this.flushTimer = null;
        this.flush();
      }, FLUSH_INTERVAL_MS);
      this.flushTimer.unref?.();
    }
  }

  within(bbox: BoundingBox, now: number): DestinationTarget[] {
    if (this.closed) return [];
    const north = Math.max(bbox[0].latitude, bbox[1].latitude);
    const south = Math.min(bbox[0].latitude, bbox[1].latitude);
    const east = Math.max(bbox[0].longitude, bbox[1].longitude);
    const west = Math.min(bbox[0].longitude, bbox[1].longitude);
    const cutoff = now - AIS_TARGET_RETENTION_MS;
    const targets = new Map<string, DestinationTarget>();
    try {
      const rows = this.selectWithin.all(west, east, south, north, cutoff) as unknown as CacheRow[];
      for (const row of rows) {
        try {
          const target: unknown = JSON.parse(row.target_json);
          if (isDestinationTarget(target)) targets.set(target.mmsi, target);
        } catch {
          // Ignore an individually damaged cache row; live AIS remains available.
        }
      }
    } catch (error) {
      this.report('read', error);
    }
    for (const target of this.pending.values()) {
      if (target.lastReportAtMs >= cutoff && contains(bbox, target)) {
        const prior = targets.get(target.mmsi);
        if (!prior || target.lastReportAtMs >= prior.lastReportAtMs) {
          targets.set(target.mmsi, target);
        }
      }
    }
    return [...targets.values()];
  }

  flush(): void {
    if (this.closed || this.pending.size === 0) return;
    const batch = [...this.pending.values()];
    this.pending.clear();
    try {
      this.db.exec('BEGIN IMMEDIATE');
      for (const target of batch) {
        const result = this.upsertTarget.run(
          target.mmsi,
          target.position.latitude,
          target.position.longitude,
          target.lastReportAtMs,
          JSON.stringify(target),
        );
        if (Number(result.changes) === 0) continue;
        const row = this.selectId.get(target.mmsi) as { id: number } | undefined;
        if (row) {
          this.upsertBounds.run(
            row.id,
            target.position.longitude,
            target.position.longitude,
            target.position.latitude,
            target.position.latitude,
          );
        }
      }
      this.db.exec('COMMIT');
    } catch (error) {
      try {
        this.db.exec('ROLLBACK');
      } catch {
        // The original write error is the useful one.
      }
      for (const target of batch) {
        const prior = this.pending.get(target.mmsi);
        if (!prior || target.lastReportAtMs > prior.lastReportAtMs) {
          this.pending.set(target.mmsi, target);
        }
      }
      this.report('write', error);
    }
  }

  close(): void {
    if (this.closed) return;
    if (this.flushTimer) clearTimeout(this.flushTimer);
    if (this.cleanupTimer) clearInterval(this.cleanupTimer);
    this.flushTimer = null;
    this.cleanupTimer = null;
    this.flush();
    this.closed = true;
    this.db.close();
  }

  private cleanup(now: number): void {
    if (this.closed) return;
    try {
      this.deleteExpired.run(now - AIS_TARGET_RETENTION_MS);
    } catch (error) {
      this.report('expiry', error);
    }
  }

  private report(operation: string, error: unknown): void {
    const detail = error instanceof Error ? error.message : String(error);
    this.onError(`AIS target cache ${operation} failed: ${detail}`);
  }
}
