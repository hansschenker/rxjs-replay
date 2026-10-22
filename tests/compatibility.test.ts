import { describe, expect, it } from 'vitest';
import { of, queueScheduler } from 'rxjs';
import { inspectAt } from '../src/history.js';
import { replaySubscription } from '../src/playback.js';
import { createRecorder } from '../src/recorder.js';
import { parseRecording, serializeRecording } from '../src/serialization.js';
import type { Recording } from '../src/types.js';

function versionOne(): Recording {
  const common = { streamId: 'input', subscriptionId: 's1', at: 0 };
  return {
    format: 'rxjs-replay', schemaVersion: 1, rxjsVersion: '7.8.2', name: 'Existing v1 recording',
    clock: { name: 'virtual', unit: 'ms' },
    streams: [{ id: 'input', label: 'Input', lane: 'source' }],
    events: [
      { ...common, seq: 1, kind: 'subscribe' },
      { ...common, seq: 2, kind: 'next', value: { status: 'captured', data: 42 } },
      { ...common, seq: 3, kind: 'complete' },
      { ...common, seq: 4, kind: 'unsubscribe', reason: 'complete' },
    ],
    capture: { maxEvents: 10, totalEvents: 4, droppedEvents: 0, clockErrors: 0, stopped: true },
  };
}

describe('schema compatibility during Inspector feature integration', () => {
  it('opens and exports v1 recordings unchanged without inventing execution metadata', () => {
    const original = versionOne();
    const parsed = parseRecording(JSON.stringify(original));
    expect(parsed).toEqual(original);
    expect(parsed.schemaVersion).toBe(1);
    expect(JSON.parse(serializeRecording(parsed))).toEqual(original);
    expect(parsed.events.every(event => !Object.hasOwn(event, 'executionId') && !Object.hasOwn(event, 'role') && !Object.hasOwn(event, 'port'))).toBe(true);
    expect(Object.isFrozen(parsed.events[0])).toBe(true);
  });

  it('keeps v1 history and subscription replay usable with the new engine', () => {
    const saved = parseRecording(JSON.stringify(versionOne()));
    expect(inspectAt(saved, 2).subscriptions[0]).toMatchObject({ outcome: 'active', values: 1, latestValue: { data: 42 } });
    const view = inspectAt(saved).subscriptions[0]!;
    expect(view.outcome).toBe('complete');
    expect(view.executionId).toBeUndefined();
    const values: unknown[] = [];
    let complete = false;
    replaySubscription(saved, 's1', { scheduler: queueScheduler }).subscribe({ next: value => values.push(value), complete: () => { complete = true; } });
    expect(values).toEqual([42]);
    expect(complete).toBe(true);
  });

  it('emits v2 metadata from ordinary tracing without changing notification replay', () => {
    const recorder = createRecorder({ streams: [{ id: 'input', label: 'Input', lane: 'source' }], clock: { now: () => 0 } });
    of(42).pipe(recorder.trace('input')).subscribe();
    const saved = parseRecording(serializeRecording(recorder.stop()));
    expect(saved.schemaVersion).toBe(2);
    expect(saved.events.every(event => event.role === 'boundary' && event.port === 'output' && typeof event.executionId === 'string')).toBe(true);
    expect(new Set(saved.events.map(event => event.executionId)).size).toBe(1);
    const values: unknown[] = [];
    replaySubscription(saved, saved.events[0]!.subscriptionId, { scheduler: queueScheduler }).subscribe(value => values.push(value));
    expect(values).toEqual([42]);
  });

  it('preserves incomplete v1 prefixes and does not fabricate endings or richer models', () => {
    const original = versionOne();
    const prefix: Recording = {
      ...original,
      events: original.events.slice(0, 2),
      capture: { ...original.capture, maxEvents: 2, droppedEvents: 2 },
    };
    const saved = parseRecording(JSON.stringify(prefix));
    expect(saved).toEqual(prefix);
    expect(inspectAt(saved)).toMatchObject({ hasOmittedEvents: true });
    expect(inspectAt(saved).subscriptions[0]?.outcome).toBe('active');
    const subscription = replaySubscription(saved, 's1', { scheduler: queueScheduler }).subscribe();
    expect(subscription.closed).toBe(false);
    subscription.unsubscribe();
  });

  it.each([
    ['missing inner owner', (events: Record<string, unknown>[]) => { for (const event of events) if (event.role === 'inner') delete event.ownerExecutionId; }],
    ['negative inner index', (events: Record<string, unknown>[]) => { for (const event of events) if (event.role === 'inner') event.innerIndex = -1; }],
    ['changing channel metadata', (events: Record<string, unknown>[]) => { events[1]!.executionId = 'different-execution'; }],
  ] as const)('rejects invalid v2 execution relationships: %s', (_name, mutate) => {
    const recorder = createRecorder({ streams: [{ id: 'latest', label: 'Latest', lane: 'internal', operator: 'switchMap' }], clock: { now: () => 0 } });
    of(1).pipe(recorder.switchMap('latest', value => of(value))).subscribe();
    const document = JSON.parse(serializeRecording(recorder.stop())) as { events: Record<string, unknown>[] };
    mutate(document.events);
    expect(() => parseRecording(JSON.stringify(document))).toThrow();
  });
});
