import { describe, expect, it } from 'vitest';
import { Observable, Subject, Subscriber, defer, finalize, map, of, share, switchMap, take, tap } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import { createRecorder } from '../src/recorder.js';
import { captureSnapshot } from '../src/snapshot.js';
import { parseRecording, serializeRecording } from '../src/serialization.js';

const streams = [
  { id: 'source', label: 'source$', lane: 'source' as const },
  { id: 'inner', label: 'inner$', lane: 'internal' as const, inputs: ['source'] },
  { id: 'result', label: 'result$', lane: 'result' as const, inputs: ['inner'] },
];
const makeRecorder = () => createRecorder({ streams, clock: { name: 'test', now: () => 0 } });

describe('recording real subscription executions', () => {
  it('stays lazy and gives every actual cold subscription its own identity', () => {
    const recorder = makeRecorder();
    let runs = 0;
    const source$ = defer(() => { runs++; return of(1, 2); }).pipe(recorder.trace('source'));
    expect(runs).toBe(0);
    expect(recorder.getRecording().events).toEqual([]);
    source$.subscribe(); source$.subscribe();
    expect(runs).toBe(2);
    expect(recorder.getRecording().events.filter(event => event.kind === 'subscribe').map(event => event.subscriptionId)).toEqual(['s1', 's2']);
    expect(() => serializeRecording(recorder.getRecording())).not.toThrow();
  });
  it('preserves immediate synchronous take(1) disposal and cleanup', () => {
    const recorder = makeRecorder();
    const produced: number[] = [];
    let disposed = 0;
    const source$ = new Observable<number>(subscriber => {
      for (let value = 0; value < 10 && !subscriber.closed; value++) { produced.push(value); subscriber.next(value); }
      if (!subscriber.closed) subscriber.complete();
      return () => { disposed++; };
    });
    const received: number[] = [];
    source$.pipe(recorder.trace('source'), take(1)).subscribe(value => received.push(value));
    expect(produced).toEqual([0]); expect(received).toEqual([0]); expect(disposed).toBe(1);
    expect(recorder.getRecording().events.map(event => event.kind)).toEqual(['subscribe', 'next', 'unsubscribe']);
    expect(recorder.getRecording().events.at(-1)).toMatchObject({ kind: 'unsubscribe', reason: 'cancel' });
  });
  it('preserves unsubscribe inside synchronous next', () => {
    const recorder = makeRecorder();
    let emissions = 0;
    const sink = new Subscriber<number>({ next: () => sink.unsubscribe(), error: () => {}, complete: () => {} });
    of(1, 2, 3).pipe(tap(() => emissions++), recorder.trace('source')).subscribe(sink);
    expect(emissions).toBe(1);
  });
  it('preserves live value/error identity while detaching snapshots', () => {
    const recorder = makeRecorder();
    const value = { count: 1 };
    const error = new Error('domain failure');
    let delivered: unknown; let caught: unknown;
    new Observable<object>(subscriber => { subscriber.next(value); subscriber.error(error); })
      .pipe(recorder.trace('source')).subscribe({ next: item => { delivered = item; }, error: item => { caught = item; } });
    value.count = 99;
    expect(delivered).toBe(value); expect(caught).toBe(error); expect(Object.isFrozen(value)).toBe(false);
    const event = recorder.getRecording().events.find(event => event.kind === 'next');
    expect(event).toMatchObject({ value: { status: 'captured', data: { count: 1 } } });
    expect(recorder.getRecording().events.at(-1)).toMatchObject({ reason: 'error' });
  });
  it('captures only explicit state and tolerates projection failure', () => {
    const recorder = makeRecorder();
    const values: number[] = [];
    of(3).pipe(recorder.trace<number>('result', { captureState: () => { throw new Error('capture'); } })).subscribe(value => values.push(value));
    expect(values).toEqual([3]);
    expect(recorder.getRecording().events.find(event => event.kind === 'state')).toMatchObject({ state: { status: 'unavailable' } });
    expect(() => serializeRecording(recorder.getRecording())).not.toThrow();
  });
  it('records actual reentrant delivery order and synchronous causes', () => {
    const recorder = makeRecorder();
    const source = new Subject<number>();
    const values: number[] = [];
    const subscription = source.pipe(recorder.trace('source'), map(value => value * 10), recorder.trace('result')).subscribe(value => {
      values.push(value); if (value === 10) source.next(2);
    });
    source.next(1); subscription.unsubscribe();
    expect(values).toEqual([10, 20]);
    const events = recorder.getRecording().events;
    expect(events.map(event => event.seq)).toEqual(events.map((_, index) => index + 1));
    expect(events.filter(event => event.kind === 'next').map(event => event.streamId)).toEqual(['source', 'result', 'source', 'result']);
    expect(events.find(event => event.kind === 'next' && event.streamId === 'result')?.causeSeq).toBeDefined();
    expect(() => serializeRecording(recorder.getRecording())).not.toThrow();
  });
  it('records switchMap replacement as cancellation and waits for the last inner after outer completion', () => {
    const scheduler = new TestScheduler((actual, expected) => expect(actual).toEqual(expected));
    scheduler.run(({ cold, expectObservable }) => {
      const recorder = createRecorder({ streams, clock: { name: 'virtual', now: () => scheduler.now() } });
      const results$ = cold('a-b|').pipe(recorder.trace('source'),
        switchMap(value => cold('----x|', { x: value }).pipe(recorder.trace('inner'))), recorder.trace('result'));
      expectObservable(results$).toBe('------b|');
      scheduler.schedule(() => {
        const saved = recorder.getRecording();
        const innerDisposals = saved.events.filter(event => event.streamId === 'inner' && event.kind === 'unsubscribe');
        expect(innerDisposals).toMatchObject([{ reason: 'cancel' }, { reason: 'complete' }]);
        expect(saved.events.find(event => event.streamId === 'inner' && event.kind === 'subscribe')).toHaveProperty('parentSubscriptionId');
        expect(() => serializeRecording(saved)).not.toThrow();
      }, 8);
    });
  });
  it('keeps producer connection and shared consumers distinct without adding sharing', () => {
    const recorder = makeRecorder();
    const source = new Subject<number>();
    let upstream = 0;
    const shared$ = defer(() => { upstream++; return source; }).pipe(recorder.trace('source'), share());
    const first = shared$.pipe(recorder.trace('result')).subscribe();
    const second = shared$.pipe(recorder.trace('result')).subscribe();
    source.next(1); first.unsubscribe(); source.next(2); second.unsubscribe();
    expect(upstream).toBe(1);
    const starts = recorder.getRecording().events.filter(event => event.kind === 'subscribe');
    expect(starts.filter(event => event.streamId === 'source')).toHaveLength(1);
    expect(starts.filter(event => event.streamId === 'result')).toHaveLength(2);
    expect(() => serializeRecording(recorder.getRecording())).not.toThrow();
  });
  it('leaves source finalization and terminal behavior unchanged', () => {
    const recorder = makeRecorder();
    let finalized = 0; let completed = 0;
    of(1).pipe(finalize(() => finalized++), recorder.trace('source')).subscribe({ complete: () => completed++ });
    expect(finalized).toBe(1); expect(completed).toBe(1);
    expect(recorder.getRecording().events.at(-1)).toMatchObject({ reason: 'complete' });
  });
});

describe('capture boundaries and immutable snapshots', () => {
  it('does not invoke getters or toJSON and safely handles cycles', () => {
    let calls = 0;
    const value: Record<string, unknown> = { toJSON() { calls++; return 'wrong'; } };
    Object.defineProperty(value, 'secret', { get() { calls++; return 42; }, enumerable: true });
    value.self = value;
    const captured = captureSnapshot(value);
    expect(calls).toBe(0); expect(captured.status).toBe('truncated');
  });
  it('treats __proto__ as an inert payload key and bounds large data', () => {
    const captured = captureSnapshot(JSON.parse('{"__proto__":{"polluted":true}}'));
    expect(captured).toMatchObject({ status: 'captured' });
    expect(({} as Record<string, unknown>).polluted).toBeUndefined();
    expect(captureSnapshot(Array.from({ length: 1000 }, () => 1)).status).toBe('truncated');
  });
  it('bounds tagged bigint and symbol previews so they can be exported again', () => {
    const recorder = makeRecorder();
    of(Symbol('x'.repeat(70_000)), 10n ** 70_000n).pipe(recorder.trace('source')).subscribe();
    expect(recorder.getRecording().events.filter(event => event.kind === 'next')).toMatchObject([
      { value: { status: 'truncated' } }, { value: { status: 'truncated' } },
    ]);
    expect(parseRecording(serializeRecording(recorder.getRecording()))).toEqual(recorder.getRecording());
  });
  it('keeps a complete prefix and counts omitted events without capturing more payloads', () => {
    let captures = 0;
    const recorder = createRecorder({ streams, maxEvents: 2, captureValue: value => { captures++; return value; } });
    of(1, 2, 3).pipe(recorder.trace('source')).subscribe();
    const saved = recorder.getRecording();
    expect(saved.events).toHaveLength(2); expect(captures).toBe(1);
    expect(saved.capture).toMatchObject({ totalEvents: 6, droppedEvents: 4 });
    expect(() => serializeRecording(saved)).not.toThrow();
  });
  it('stops observation without stopping the source and keeps previous snapshots frozen', () => {
    const recorder = makeRecorder();
    const source = new Subject<number>();
    const values: number[] = [];
    const subscription = source.pipe(recorder.trace('source')).subscribe(value => values.push(value));
    const before = recorder.getRecording();
    source.next(1); const stopped = recorder.stop(); source.next(2);
    expect(values).toEqual([1, 2]); expect(subscription.closed).toBe(false);
    expect(before.events).toHaveLength(1); expect(Object.isFrozen(before.events)).toBe(true);
    expect(recorder.getRecording()).toEqual(stopped);
    subscription.unsubscribe();
  });
  it('clamps bad clocks while preserving sequence order', () => {
    const times = [100, 103, 102, NaN, 107, 110];
    const recorder = createRecorder({ streams, clock: { now: () => times.shift() ?? 110 } });
    of(1, 2).pipe(recorder.trace('source')).subscribe();
    expect(recorder.getRecording().events.map(event => event.at)).toEqual([3, 3, 3, 7, 10]);
    expect(recorder.getRecording().capture.clockErrors).toBe(2);
  });
});
