import { describe, expect, it } from 'vitest';
import { defer, of, queueScheduler, take } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import { inspectAt } from '../src/history.js';
import { createReplay, replayEvents, replaySubscription } from '../src/playback.js';
import { createRecorder } from '../src/recorder.js';
import type { Recording, TraceEvent } from '../src/types.js';

const base = (seq: number, at: number) => ({ seq, at, streamId: 'source', subscriptionId: 's1' });
function recording(events: readonly TraceEvent[], droppedEvents = 0): Recording {
  return {
    format: 'rxjs-replay', schemaVersion: 1, rxjsVersion: '7.8.2', name: 'Playback test',
    clock: { name: 'virtual', unit: 'ms' },
    streams: [{ id: 'source', label: 'source$', lane: 'source' }], events,
    capture: { maxEvents: events.length || 1, totalEvents: events.length + droppedEvents, droppedEvents, clockErrors: 0, stopped: true },
  };
}
function example(): Recording {
  return recording([
    { ...base(1, 0), kind: 'subscribe' },
    { ...base(2, 10), kind: 'next', value: { status: 'captured', data: 'a' } },
    { ...base(3, 10), kind: 'next', value: { status: 'captured', data: 'b' } },
    { ...base(4, 30), kind: 'next', value: { status: 'captured', data: 'c' } },
    { ...base(5, 40), kind: 'complete' },
    { ...base(6, 40), kind: 'unsubscribe', reason: 'complete' },
  ]);
}
const virtual = () => new TestScheduler((actual, expected) => expect(actual).toEqual(expected));

describe('history navigation', () => {
  it('distinguishes delivery order at equal times and reconstructs only the selected prefix', () => {
    const trace = example();
    expect(inspectAt(trace, 0).subscriptions).toEqual([]);
    const first = inspectAt(trace, 2);
    expect(first.event?.seq).toBe(2);
    expect(first.subscriptions[0]?.latestValue).toEqual({ status: 'captured', data: 'a' });
    expect(first.subscriptions[0]?.values).toBe(1);
    expect(first.subscriptions[0]?.outcome).toBe('active');
    expect(inspectAt(trace, 3).subscriptions[0]?.latestValue).toEqual({ status: 'captured', data: 'b' });
    expect(inspectAt(trace, 5).subscriptions[0]?.end?.kind).toBe('complete');
    expect(inspectAt(trace, 6).subscriptions[0]?.end?.kind).toBe('unsubscribe');
    expect(inspectAt(trace, 100).cursor).toBe(6);
    expect(() => inspectAt(trace, Number.NaN)).toThrow(RangeError);
  });

  it('records domain state and cancellation separately and reports omitted evidence', () => {
    const trace = recording([
      { ...base(1, 0), kind: 'subscribe', parentSubscriptionId: 'parent' },
      { ...base(2, 4), kind: 'state', label: 'total', state: { status: 'captured', data: 9 } },
      { ...base(3, 5), kind: 'unsubscribe', reason: 'cancel' },
    ], 2);
    const view = inspectAt(trace);
    expect(view.subscriptions[0]).toMatchObject({ outcome: 'cancel', latestState: { data: 9 }, parentSubscriptionId: 'parent' });
    expect(view.hasOmittedEvents).toBe(true);
    expect(Object.isFrozen(view.subscriptions[0])).toBe(true);
  });

  it('never repeats source effects during backward seeking or event playback', () => {
    let effects = 0;
    const recorder = createRecorder({ streams: [{ id: 'source', label: 'source$', lane: 'source' }], clock: { now: () => 0 } });
    const source$ = defer(() => { effects++; return of('recorded'); }).pipe(recorder.trace('source'));
    expect(effects).toBe(0);
    source$.subscribe();
    const saved = recorder.getRecording();
    const replay = createReplay(saved);
    for (const seq of [3, 0, 1, 0, 3]) replay.seek(seq);
    replayEvents(saved, { scheduler: queueScheduler }).subscribe();
    replaySubscription(saved, saved.events[0]!.subscriptionId, { scheduler: queueScheduler }).subscribe();
    expect(effects).toBe(1);
    replay.dispose();
  });
});

describe('shared replay controller', () => {
  it('plays recorded time gaps with ordered cursor steps at equal frames', () => {
    const scheduler = virtual();
    const replay = createReplay(example(), { scheduler });
    const seen: [number, number][] = [];
    let previous = 0;
    replay.state$.subscribe(state => {
      if (state.cursor !== previous) seen.push([scheduler.now(), state.cursor]);
      previous = state.cursor;
    });
    replay.play();
    scheduler.flush();
    expect(seen).toEqual([[0, 1], [10, 2], [10, 3], [30, 4], [40, 5], [40, 6]]);
    expect(replay.getState().status).toBe('ended');
    replay.play();
    expect(replay.getState().status).toBe('ended');
    replay.dispose();
  });

  it('preserves a partially elapsed wait when paused and resumed', () => {
    const scheduler = virtual();
    const replay = createReplay(example(), { scheduler });
    const seen: [number, number][] = [];
    replay.state$.subscribe(state => { if (state.cursor === 4) seen.push([scheduler.now(), state.cursor]); });
    scheduler.schedule(() => replay.pause(), 15);
    scheduler.schedule(() => replay.play(), 45);
    replay.play();
    scheduler.flush();
    expect(seen).toEqual([[60, 4]]);
    replay.dispose();
  });

  it('changes speed using elapsed media time rather than restarting the wait', () => {
    const scheduler = virtual();
    const replay = createReplay(example(), { scheduler });
    const seen: number[] = [];
    replay.state$.subscribe(state => { if (state.cursor === 4) seen.push(scheduler.now()); });
    scheduler.schedule(() => replay.setSpeed(2), 15);
    replay.play();
    scheduler.flush();
    expect(seen).toEqual([22.5]);
    replay.dispose();
  });

  it('changes speed while paused without advancing the playhead', () => {
    const scheduler = virtual();
    const replay = createReplay(example(), { scheduler });
    const seen: number[] = [];
    replay.state$.subscribe(state => { if (state.cursor === 2) seen.push(scheduler.now()); });
    scheduler.schedule(() => replay.pause(), 5);
    scheduler.schedule(() => replay.setSpeed(4), 10);
    scheduler.schedule(() => replay.play(), 20);
    replay.play();
    scheduler.flush();
    expect(seen).toEqual([21.25]);
    replay.dispose();
  });

  it('seeking cancels an outstanding tick and subsequent play starts at the selected event time', () => {
    const scheduler = virtual();
    const replay = createReplay(example(), { scheduler });
    const seen: [number, number][] = [];
    let previous = 0;
    replay.state$.subscribe(state => {
      if (state.cursor !== previous) seen.push([scheduler.now(), state.cursor]);
      previous = state.cursor;
    });
    scheduler.schedule(() => replay.seek(4), 5);
    scheduler.schedule(() => replay.play(), 8);
    replay.play();
    scheduler.flush();
    expect(seen).toEqual([[0, 1], [5, 4], [18, 5], [18, 6]]);
    replay.dispose();
  });

  it('serializes a reentrant pause before scheduling the next equal-time event', () => {
    const scheduler = virtual();
    const replay = createReplay(example(), { scheduler });
    const seen: [number, number][] = [];
    let previous = 0;
    let paused = false;
    replay.state$.subscribe(state => {
      if (state.cursor !== previous) seen.push([scheduler.now(), state.cursor]);
      previous = state.cursor;
      if (state.cursor === 2 && !paused) { paused = true; replay.pause(); }
    });
    scheduler.schedule(() => replay.play(), 20);
    replay.play();
    scheduler.flush();
    expect(seen).toEqual([[0, 1], [10, 2], [20, 3], [40, 4], [50, 5], [50, 6]]);
    replay.dispose();
  });

  it('keeps one explicit lifetime and gives late observers current state', () => {
    const scheduler = virtual();
    const replay = createReplay(example(), { scheduler });
    replay.play();
    scheduler.flush();
    const late: number[] = [];
    replay.state$.pipe(take(1)).subscribe(state => late.push(state.cursor));
    expect(late).toEqual([6]);
    replay.step(-1);
    expect(replay.getState()).toMatchObject({ cursor: 5, status: 'paused' });
    replay.dispose();
  });

  it('disposal cancels work, completes state, and remains visible to late observers', () => {
    const scheduler = virtual();
    const replay = createReplay(example(), { scheduler });
    let completed = 0;
    replay.state$.subscribe({ complete: () => completed++ });
    scheduler.schedule(() => replay.dispose(), 5);
    replay.play();
    scheduler.flush();
    expect(replay.getState()).toMatchObject({ cursor: 1, status: 'disposed' });
    expect(completed).toBe(1);
    let lateStatus = '';
    replay.state$.subscribe({ next: state => { lateStatus = state.status; }, complete: () => completed++ });
    expect(lateStatus).toBe('disposed');
    expect(completed).toBe(2);
    replay.play();
    replay.seek(0);
    expect(replay.getState().status).toBe('disposed');
  });

  it('time bias and sequence stepping expose both sides of equal-time deliveries', () => {
    const replay = createReplay(example());
    replay.seekTime(10, 'before');
    expect(replay.getState().cursor).toBe(3);
    replay.seekTime(10, 'after');
    expect(replay.getState().cursor).toBe(2);
    replay.seekTime(20, 'before');
    expect(replay.getState().cursor).toBe(3);
    replay.seekTime(20, 'after');
    expect(replay.getState().cursor).toBe(4);
    replay.seek(0);
    replay.step(-1);
    expect(replay.getState().cursor).toBe(0);
    replay.step();
    expect(replay.getState().cursor).toBe(1);
    expect(() => replay.setSpeed(0)).toThrow(RangeError);
    expect(() => replay.seekTime(Infinity)).toThrow(RangeError);
    expect(() => replay.seek(-1)).toThrow(RangeError);
    replay.dispose();
    const empty = createReplay(recording([]));
    expect(empty.getState().status).toBe('ended');
    empty.dispose();
  });
});

describe('recorded streams', () => {
  it('replayEvents is cold, retains errors as data, and cancels pending batches', () => {
    const scheduler = virtual();
    const trace = recording([
      { ...base(1, 0), kind: 'subscribe' },
      { ...base(2, 5), kind: 'error', error: { status: 'captured', data: 'failure' } },
      { ...base(3, 5), kind: 'unsubscribe', reason: 'error' },
    ]);
    const events$ = replayEvents(trace, { scheduler });
    const first: [number, string][] = [];
    const second: [number, string][] = [];
    let completed = false;
    events$.subscribe({ next: event => first.push([scheduler.now(), event.kind]), complete: () => { completed = true; } });
    scheduler.schedule(() => events$.pipe(take(1)).subscribe(event => second.push([scheduler.now(), event.kind])), 2);
    scheduler.flush();
    expect(first).toEqual([[0, 'subscribe'], [5, 'error'], [5, 'unsubscribe']]);
    expect(second).toEqual([[2, 'subscribe']]);
    expect(completed).toBe(true);
  });

  it('subscription playback is relative to subscription time and cancellation closes silently', () => {
    const scheduler = virtual();
    const trace = recording([
      { ...base(1, 20), kind: 'subscribe' },
      { ...base(2, 25), kind: 'next', value: { status: 'captured', data: 'value' } },
      { ...base(3, 30), kind: 'unsubscribe', reason: 'cancel' },
    ]);
    const values: [number, unknown][] = [];
    let completed = false;
    const sub = replaySubscription(trace, 's1', { scheduler }).subscribe({ next: value => values.push([scheduler.now(), value]), complete: () => { completed = true; } });
    scheduler.flush();
    expect(values).toEqual([[5, 'value']]);
    expect(sub.closed).toBe(true);
    expect(completed).toBe(false);
  });

  it('reconstructs completion and explicitly decoded values at recorded times', () => {
    const scheduler = virtual();
    const values: [number, string][] = [];
    let completedAt = -1;
    replaySubscription<string>(example(), 's1', {
      scheduler, speed: 2,
      decodeValue: value => value.status === 'captured' ? String(value.data).toUpperCase() : 'unavailable',
    }).subscribe({ next: value => values.push([scheduler.now(), value]), complete: () => { completedAt = scheduler.now(); } });
    scheduler.flush();
    expect(values).toEqual([[5, 'A'], [5, 'B'], [15, 'C']]);
    expect(completedAt).toBe(20);
  });

  it('does not invent a completion for an incomplete retained prefix', () => {
    const scheduler = virtual();
    const prefix = recording(example().events.slice(0, 3), 10);
    let completed = false;
    const sub = replaySubscription(prefix, 's1', { scheduler }).subscribe({ complete: () => { completed = true; } });
    scheduler.flush();
    expect(sub.closed).toBe(false);
    expect(completed).toBe(false);
    sub.unsubscribe();
  });

  it('requires decoding incomplete snapshots and isolates decoder failures to the replay execution', () => {
    const trace = recording([
      { ...base(1, 0), kind: 'subscribe' },
      { ...base(2, 0), kind: 'next', value: { status: 'unavailable', reason: 'capture failed' } },
    ]);
    let failure: unknown;
    replaySubscription(trace, 's1', { scheduler: queueScheduler }).subscribe({ error: error => { failure = error; } });
    expect(failure).toBeInstanceOf(TypeError);
    expect(String(failure)).toContain('decodeValue');
    const expected = new Error('decoder failure');
    replaySubscription<string>(trace, 's1', { scheduler: queueScheduler, decodeValue: () => { throw expected; } }).subscribe({ error: error => { failure = error; } });
    expect(failure).toBe(expected);
  });

  it('defaults errors to their saved JSON and supports an explicit domain error decoder', () => {
    const savedError = { message: 'request failed' };
    const trace = recording([
      { ...base(1, 0), kind: 'subscribe' },
      { ...base(2, 0), kind: 'error', error: { status: 'captured', data: savedError } },
      { ...base(3, 0), kind: 'unsubscribe', reason: 'error' },
    ]);
    let error: unknown;
    replaySubscription(trace, 's1', { scheduler: queueScheduler }).subscribe({ error: value => { error = value; } });
    expect(error).toBe(savedError);
    replaySubscription(trace, 's1', { scheduler: queueScheduler, decodeError: () => new Error('decoded request failed') }).subscribe({ error: value => { error = value; } });
    expect(error).toBeInstanceOf(Error);
    expect(String(error)).toContain('decoded request failed');
    expect(() => replaySubscription(trace, 'missing')).toThrow('No recorded subscription start');
  });

  it('stops same-frame delivery immediately when its consumer unsubscribes', () => {
    const trace = recording([
      { ...base(1, 0), kind: 'subscribe' },
      { ...base(2, 0), kind: 'next', value: { status: 'captured', data: 1 } },
      { ...base(3, 0), kind: 'next', value: { status: 'captured', data: 2 } },
    ]);
    let decoded = 0;
    const seen: number[] = [];
    replaySubscription<number>(trace, 's1', { scheduler: queueScheduler, decodeValue: () => ++decoded }).pipe(take(1)).subscribe(value => seen.push(value));
    expect(seen).toEqual([1]);
    expect(decoded).toBe(1);
  });
});
