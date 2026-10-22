import { describe, expect, it } from 'vitest';
import { of } from 'rxjs';
import { createRecorder } from '../src/recorder.js';
import { evaluateLogpoint, selectEvents } from '../src/query.js';
import type { Recording, TraceEvent } from '../src/types.js';

const common = { streamId: 'queries', subscriptionId: 's1' };
const events: readonly TraceEvent[] = [
  { ...common, seq: 1, at: 0, kind: 'subscribe' },
  { ...common, seq: 2, at: 10, kind: 'next', value: { status: 'captured', data: 'one' } },
  { ...common, seq: 3, at: 10, kind: 'next', value: { status: 'captured', data: 'two' } },
  { ...common, seq: 4, at: 15, kind: 'complete' },
  { ...common, seq: 5, at: 15, kind: 'unsubscribe', reason: 'complete' },
];
const recording: Recording = {
  format: 'rxjs-replay', schemaVersion: 1, rxjsVersion: '7.8.2', name: 'Query test',
  clock: { name: 'virtual', unit: 'ms' }, streams: [{ id: 'queries', label: 'queries$', lane: 'source' }], events,
  capture: { maxEvents: 10, totalEvents: 5, droppedEvents: 0, clockErrors: 0, stopped: true },
};

describe('saved-history queries', () => {
  it('selects operator channels and explicitly owned inners independently', () => {
    const recorder = createRecorder({ streams: [{ id: 'latest', label: 'Latest', lane: 'internal', operator: 'switchMap' }] });
    of(1, 2).pipe(recorder.switchMap('latest', value => of(value * 10))).subscribe();
    const saved = recorder.getRecording();
    const operator = saved.events.find(event => event.role === 'operator')!;
    const input = selectEvents(saved, { executionId: operator.executionId, role: 'operator', port: 'input', kinds: ['next'] });
    const output = selectEvents(saved, { executionId: operator.executionId, port: 'output', kinds: ['next'] });
    const inners = selectEvents(saved, { ownerExecutionId: operator.executionId, role: 'inner', kinds: ['next'] });
    expect(input.map(event => event.kind === 'next' && event.value.status === 'captured' ? event.value.data : null)).toEqual([1, 2]);
    expect(output.map(event => event.kind === 'next' && event.value.status === 'captured' ? event.value.data : null)).toEqual([10, 20]);
    expect(inners.map(event => event.innerIndex)).toEqual([0, 1]);
    expect(new Set([...input, ...output].map(event => event.subscriptionId)).size).toBe(2);
    expect(selectEvents(recording, { role: 'operator' })).toEqual([]);
  });
  it('combines inclusive filters while preserving same-time delivery order', () => {
    const result = selectEvents(recording, { streamId: 'queries', subscriptionId: 's1', kinds: ['next'], fromTime: 10, toTime: 10, fromSeq: 2, toSeq: 3 });
    expect(result.map(event => event.seq)).toEqual([2, 3]);
    expect(selectEvents(recording, { streamId: 'missing' })).toEqual([]);
    expect(selectEvents(recording, { fromSeq: 4, toSeq: 2 })).toEqual([]);
    expect(selectEvents(recording, { kinds: [] })).toEqual([]);
    expect(Object.isFrozen(result)).toBe(true);
  });

  it('evaluates retroactive projections without mutating saved history', () => {
    const before = JSON.stringify(recording);
    const result = evaluateLogpoint(recording, { kinds: ['next'] }, event => event.kind === 'next' && event.value.status === 'captured' ? String(event.value.data).length : 0);
    expect(result.map(row => row.result)).toEqual([{ ok: true, value: 3 }, { ok: true, value: 3 }]);
    expect(JSON.stringify(recording)).toBe(before);
  });

  it('keeps projection failures local to one event and continues with later matches', () => {
    const result = evaluateLogpoint(recording, { kinds: ['next'] }, event => {
      if (event.seq === 2) throw new Error('missing field');
      return event.seq;
    });
    expect(result.map(row => row.result)).toEqual([{ ok: false, error: 'missing field' }, { ok: true, value: 3 }]);
  });

  it('supports predicates and isolates predicate failures without calling the projection', () => {
    const projected: number[] = [];
    const result = evaluateLogpoint(recording, { predicate: event => {
      if (event.seq === 2) throw new Error('invalid predicate input');
      return event.kind === 'next';
    } }, event => { projected.push(event.seq); return event.seq; });
    expect(projected).toEqual([3]);
    expect(result.map(row => row.result)).toEqual([{ ok: false, error: 'invalid predicate input' }, { ok: true, value: 3 }]);
  });

  it('rejects invalid numeric query bounds', () => {
    expect(() => selectEvents(recording, { fromSeq: 1.5 })).toThrow(RangeError);
    expect(() => selectEvents(recording, { toTime: Infinity })).toThrow(RangeError);
    expect(() => selectEvents(recording, { fromTime: -1 })).toThrow(RangeError);
  });
});
