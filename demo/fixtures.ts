import { VirtualTimeScheduler, map, scan, switchMap, take, timer } from 'rxjs';
import { createRecorder } from '../src/recorder.js';
import type { Recording } from '../src/types.js';

/** Real switchMap execution: two inner subscriptions are replaced, the latest finishes. */
export function recordSearch(): Recording {
  const scheduler = new VirtualTimeScheduler();
  const recorder = createRecorder({
    name: 'Only the latest search survives',
    clock: { name: 'virtual search clock', now: () => scheduler.now() },
    streams: [
      { id: 'queries', label: 'queries$', lane: 'source', sharing: 'independent' },
      { id: 'requests', label: 'request$', lane: 'internal', inputs: ['queries'], sharing: 'independent' },
      { id: 'results', label: 'results$', lane: 'result', inputs: ['requests'], sharing: 'independent' },
    ],
  });
  const queries = ['r', 'rx', 'rxjs'];
  function queryAt(index: number): string { return queries[index]!; }
  function loadResult$(query: string) {
    return timer(900, scheduler).pipe(
      map(() => ({ query, title: `Results for ${query}` })),
      recorder.trace('requests'),
    );
  }
  timer(0, 700, scheduler).pipe(
    take(queries.length),
    map(queryAt),
    recorder.trace('queries'),
    switchMap(loadResult$),
    recorder.trace('results'),
  ).subscribe();
  scheduler.flush();
  return recorder.stop();
}

/** Explicit domain snapshots show scan's accumulated state without accessing operator internals. */
export function recordCounter(): Recording {
  const scheduler = new VirtualTimeScheduler();
  const recorder = createRecorder({
    name: 'State is a stream that remembers',
    clock: { name: 'virtual counter clock', now: () => scheduler.now() },
    streams: [
      { id: 'actions', label: 'adjustments$', lane: 'source', sharing: 'independent' },
      { id: 'state', label: 'accumulator$', lane: 'internal', inputs: ['actions'], sharing: 'independent' },
      { id: 'view', label: 'view$', lane: 'result', inputs: ['state'], sharing: 'independent' },
    ],
  });
  const adjustments = [1, 1, -1, 3];
  interface CounterState { readonly count: number; readonly updates: number }
  function adjustmentAt(index: number): number { return adjustments[index]!; }
  function updateCounter(state: CounterState, adjustment: number): CounterState {
    return { count: state.count + adjustment, updates: state.updates + 1 };
  }
  function readState(state: CounterState): CounterState { return state; }
  function renderLabel(state: CounterState): string { return `Count: ${state.count}`; }
  timer(300, 600, scheduler).pipe(
    take(adjustments.length),
    map(adjustmentAt),
    recorder.trace('actions'),
    scan(updateCounter, { count: 0, updates: 0 }),
    recorder.trace('state', { captureState: readState, stateLabel: 'scan accumulator' }),
    map(renderLabel),
    recorder.trace('view'),
  ).subscribe();
  scheduler.flush();
  return recorder.stop();
}
