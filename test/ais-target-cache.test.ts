import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  AIS_TARGET_RETENTION_MS,
  AisTargetCache,
} from '../src/ais-target-cache';
import type { DestinationTarget } from '../src/destination-review';
import type { BoundingBox } from '../src/websocket-manager';

const BAY: BoundingBox = [
  { latitude: 38, longitude: -123 },
  { latitude: 37, longitude: -122 },
];

const opened: AisTargetCache[] = [];
const directories: string[] = [];

function openCache(onError = vi.fn()): { cache: AisTargetCache; path: string } {
  const directory = mkdtempSync(join(tmpdir(), 'ais-target-cache-'));
  const path = join(directory, 'targets.sqlite');
  const cache = new AisTargetCache(path, onError);
  directories.push(directory);
  opened.push(cache);
  return { cache, path };
}

function target(
  mmsi: string,
  latitude: number,
  longitude: number,
  lastReportAtMs: number,
): DestinationTarget {
  return {
    id: `aisstream:${mmsi}`,
    mmsi,
    position: { latitude, longitude },
    lastReportAtMs,
    history: {
      firstSeenAtMs: lastReportAtMs,
      sampleCount: 1,
      center: { latitude, longitude },
      maxRadiusMeters: 0,
    },
  };
}

afterEach(() => {
  while (opened.length > 0) opened.pop()?.close();
  while (directories.length > 0) rmSync(directories.pop()!, { recursive: true, force: true });
});

describe('AIS target cache', () => {
  it('returns pending targets immediately and persists them across restarts', () => {
    const now = Date.now();
    const { cache, path } = openCache();
    cache.remember(target('211234560', 37.8, -122.4, now));
    expect(cache.within(BAY, now).map((entry) => entry.mmsi)).toEqual(['211234560']);
    cache.close();
    opened.pop();

    const reopened = new AisTargetCache(path);
    opened.push(reopened);
    expect(reopened.within(BAY, now).map((entry) => entry.mmsi)).toEqual(['211234560']);
  });

  it('uses the spatial index to exclude targets outside the requested bounds', () => {
    const now = Date.now();
    const { cache } = openCache();
    cache.remember(target('211234560', 37.8, -122.4, now));
    cache.remember(target('211234561', 47.6, -122.3, now));
    cache.flush();

    expect(cache.within(BAY, now).map((entry) => entry.mmsi)).toEqual(['211234560']);
  });

  it('does not return targets after the one-hour retention period', () => {
    const now = Date.now();
    const { cache } = openCache();
    cache.remember(target('211234560', 37.8, -122.4, now - AIS_TARGET_RETENTION_MS - 1));
    cache.flush();

    expect(cache.within(BAY, now)).toEqual([]);
  });

  it('does not let an older report move a newer cached target', () => {
    const now = Date.now();
    const { cache } = openCache();
    cache.remember(target('211234560', 37.8, -122.4, now));
    cache.flush();
    cache.remember(target('211234560', 47.6, -122.3, now - 1));
    cache.flush();

    expect(cache.within(BAY, now)[0]?.position).toEqual({ latitude: 37.8, longitude: -122.4 });
  });
});
