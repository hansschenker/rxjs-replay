import { Observable, Subject, Subscription, asyncScheduler, distinctUntilChanged, scan, shareReplay, startWith } from 'rxjs';
import type { SchedulerLike, Subscriber } from 'rxjs';
import { resolveCursor } from './history.js';
import type { CapturedValue, PlaybackOptions, Recording, ReplayCommand, ReplayController, ReplayState, TraceEvent } from './types.js';

type NextEvent = Extract<TraceEvent, { readonly kind: 'next' }>;
type ErrorEvent = Extract<TraceEvent, { readonly kind: 'error' }>;
type ErrorDecoder = (error: CapturedValue, event: ErrorEvent) => unknown;

export interface ReplaySubscriptionOptions<T> extends PlaybackOptions {
  /** Decode the JSON snapshot explicitly; original identity and prototypes are not retained. */
  readonly decodeValue: (value: CapturedValue, event: NextEvent) => T;
  readonly decodeError?: ErrorDecoder;
}

export interface ReplayUnknownSubscriptionOptions extends PlaybackOptions {
  readonly decodeValue?: undefined;
  readonly decodeError?: ErrorDecoder;
}

function speedValue(speed: number | undefined): number {
  const value = speed ?? 1;
  if (!Number.isFinite(value) || value <= 0) throw new RangeError('Playback speed must be finite and greater than zero.');
  return value;
}

function eventIndex(recording: Recording, seq: number): number {
  let low = 0;
  let high = recording.events.length;
  while (low < high) {
    const middle = low + Math.floor((high - low) / 2);
    if (recording.events[middle]!.seq <= seq) low = middle + 1;
    else high = middle;
  }
  return low;
}

function timeForCursor(recording: Recording, seq: number): number {
  return seq === 0 ? 0 : recording.events[eventIndex(recording, seq) - 1]!.at;
}

function steppedCursor(recording: Recording, cursor: number, direction: 1 | -1): number {
  const nextIndex = eventIndex(recording, cursor);
  if (direction === 1) return recording.events[nextIndex]?.seq ?? cursor;
  return recording.events[nextIndex - 2]?.seq ?? 0;
}

/** Pure Input Controller transition: commands change a cursor; application code is never called. */
function transition(recording: Recording, state: ReplayState, command: ReplayCommand): ReplayState {
  if (state.status === 'disposed') return state;
  const last = recording.events.at(-1)?.seq ?? 0;
  let cursor = state.cursor;
  let status: ReplayState['status'] = state.status;
  let speed = state.speed;
  switch (command.type) {
    case 'seek':
      cursor = resolveCursor(recording, command.seq);
      status = cursor === last ? 'ended' : 'paused';
      break;
    case 'step':
      cursor = steppedCursor(recording, cursor, command.direction);
      status = cursor === last ? 'ended' : 'paused';
      break;
    case 'play':
      status = cursor === last ? 'ended' : 'playing';
      break;
    case 'pause':
      if (status === 'playing') status = 'paused';
      break;
    case 'speed':
      speed = command.speed;
      break;
    case 'tick':
      if (status !== 'playing') return state;
      cursor = resolveCursor(recording, command.seq);
      if (cursor === last) status = 'ended';
      break;
    case 'dispose':
      status = 'disposed';
      break;
  }
  return cursor === state.cursor && status === state.status && speed === state.speed
    ? state
    : Object.freeze({ cursor, status, speed });
}

/**
 * One explicitly shared controller lifetime. scan remembers commands; shareReplay remembers
 * the latest view. The owned connection lasts until dispose(), including when no UI is listening.
 */
export function createReplay(recording: Recording, options: PlaybackOptions = {}): ReplayController {
  const scheduler = options.scheduler ?? asyncScheduler;
  const initial: ReplayState = Object.freeze({
    cursor: 0,
    status: recording.events.length === 0 ? 'ended' : 'paused',
    speed: speedValue(options.speed),
  });
  const commands = new Subject<ReplayCommand>();
  const state$ = commands.pipe(
    scan((state, command) => transition(recording, state, command), initial),
    startWith(initial),
    distinctUntilChanged(),
    shareReplay({ bufferSize: 1, refCount: false }),
  );
  let current = initial;
  const connection = state$.subscribe(state => { current = state; });
  let mediaTime = 0;
  let anchorTime = scheduler.now();
  let anchorMedia = 0;
  let scheduled: Subscription | undefined;
  let generation = 0;
  let processing = false;
  const pending: ReplayCommand[] = [];

  function isDisposed(): boolean { return current.status === 'disposed'; }

  function cancelScheduled(): void {
    generation++;
    scheduled?.unsubscribe();
    scheduled = undefined;
  }

  function scheduleNext(): void {
    if (current.status !== 'playing') return;
    const next = recording.events[eventIndex(recording, current.cursor)];
    if (!next) return;
    const expected = generation;
    const action = new Subscription();
    scheduled = action;
    action.add(scheduler.schedule(() => {
      if (expected !== generation || current.status !== 'playing') return;
      dispatch({ type: 'tick', seq: next.seq });
    }, Math.max(0, (next.at - mediaTime) / current.speed)));
  }

  function validateCommand(command: ReplayCommand): void {
    if (command.type === 'seek' || command.type === 'tick') resolveCursor(recording, command.seq);
    if (command.type === 'speed') speedValue(command.speed);
    if (command.type === 'step' && command.direction !== 1 && command.direction !== -1) {
      throw new RangeError('Step direction must be 1 or -1.');
    }
  }

  function dispatch(command: ReplayCommand): void {
    if (isDisposed()) return;
    validateCommand(command);
    pending.push(command);
    // A state observer may dispatch another command. Finish the current transition first.
    if (processing) return;
    processing = true;
    try {
      while (pending.length > 0 && !isDisposed()) {
        const nextCommand = pending.shift()!;
        cancelScheduled();
        if (current.status === 'playing') {
          mediaTime = anchorMedia + Math.max(0, scheduler.now() - anchorTime) * current.speed;
        }
        if (nextCommand.type === 'seek') {
          mediaTime = timeForCursor(recording, resolveCursor(recording, nextCommand.seq));
        } else if (nextCommand.type === 'step') {
          mediaTime = timeForCursor(recording, steppedCursor(recording, current.cursor, nextCommand.direction));
        } else if (nextCommand.type === 'tick') {
          mediaTime = Math.max(mediaTime, timeForCursor(recording, resolveCursor(recording, nextCommand.seq)));
        }
        commands.next(nextCommand);
        anchorTime = scheduler.now();
        anchorMedia = mediaTime;
      }
      if (isDisposed()) {
        pending.length = 0;
        commands.complete();
        connection.unsubscribe();
      }
    } finally {
      processing = false;
    }
    scheduleNext();
  }

  function seekTime(at: number, bias: 'before' | 'after' = 'before'): void {
    if (!Number.isFinite(at) || at < 0) throw new RangeError('History time must be finite and non-negative.');
    if (bias !== 'before' && bias !== 'after') throw new TypeError('Time bias must be before or after.');
    let selected = 0;
    if (bias === 'before') {
      for (const event of recording.events) {
        if (event.at > at) break;
        selected = event.seq;
      }
    } else {
      selected = recording.events.find(event => event.at >= at)?.seq ?? recording.events.at(-1)?.seq ?? 0;
    }
    dispatch({ type: 'seek', seq: selected });
  }

  return Object.freeze({
    state$,
    getState: () => current,
    dispatch,
    seek: (seq: number) => dispatch({ type: 'seek', seq }),
    seekTime,
    step: (direction: 1 | -1 = 1) => dispatch({ type: 'step', direction }),
    play: () => dispatch({ type: 'play' }),
    pause: () => dispatch({ type: 'pause' }),
    setSpeed: (speed: number) => dispatch({ type: 'speed', speed }),
    dispose: () => dispatch({ type: 'dispose' }),
  });
}

/** Schedule one bounded batch per recorded timestamp, preserving seq order within that batch. */
function scheduleEvents<T>(
  events: readonly TraceEvent[],
  origin: number,
  scheduler: SchedulerLike,
  speed: number,
  destination: Subscriber<T>,
  deliver: (event: TraceEvent) => void,
  done: () => void,
): void {
  const work = new Subscription();
  destination.add(work);
  let index = 0;
  const started = scheduler.now();
  function scheduleNextBatch(): void {
    if (destination.closed) return;
    const next = events[index];
    if (!next) { done(); return; }
    const due = (next.at - origin) / speed;
    work.add(scheduler.schedule(() => {
      while (!destination.closed && events[index]?.at === next.at) deliver(events[index++]!);
      if (!destination.closed) scheduleNextBatch();
    }, Math.max(0, due - (scheduler.now() - started))));
  }
  scheduleNextBatch();
}

/** Cold playback of events as data. A recorded error is a value in this stream, not its error channel. */
export function replayEvents(recording: Recording, options: PlaybackOptions = {}): Observable<TraceEvent> {
  const scheduler = options.scheduler ?? asyncScheduler;
  const speed = speedValue(options.speed);
  return new Observable<TraceEvent>(destination => {
    scheduleEvents(recording.events, 0, scheduler, speed, destination, event => destination.next(event), () => destination.complete());
  });
}

function capturedData(value: CapturedValue, decoder: 'decodeValue' | 'decodeError'): unknown {
  if (value.status !== 'captured') {
    throw new TypeError(`${decoder} is required to replay a ${value.status} snapshot.`);
  }
  return value.data;
}

export function replaySubscription(recording: Recording, subscriptionId: string, options?: ReplayUnknownSubscriptionOptions): Observable<unknown>;
export function replaySubscription<T>(recording: Recording, subscriptionId: string, options: ReplaySubscriptionOptions<T>): Observable<T>;
/**
 * Reconstruct one observed subscription from its saved notifications. Its clock starts at the
 * recorded subscribe event. Values default to JSON snapshots as unknown, never an unchecked T.
 * Cancellation closes silently. An unfinished retained prefix stays open after its final event.
 */
export function replaySubscription(
  recording: Recording,
  subscriptionId: string,
  options: PlaybackOptions & {
    readonly decodeValue?: ((value: CapturedValue, event: NextEvent) => unknown) | undefined;
    readonly decodeError?: ErrorDecoder;
  } = {},
): Observable<unknown> {
  const scheduler = options.scheduler ?? asyncScheduler;
  const speed = speedValue(options.speed);
  const events = recording.events.filter(event => event.subscriptionId === subscriptionId);
  const start = events[0];
  if (!start || start.kind !== 'subscribe') throw new TypeError(`No recorded subscription start for ${subscriptionId}.`);
  return new Observable<unknown>(destination => {
    scheduleEvents(events, start.at, scheduler, speed, destination, event => {
      try {
        switch (event.kind) {
          case 'next':
            destination.next(options.decodeValue ? options.decodeValue(event.value, event) : capturedData(event.value, 'decodeValue'));
            break;
          case 'error':
            destination.error(options.decodeError ? options.decodeError(event.error, event) : capturedData(event.error, 'decodeError'));
            break;
          case 'complete':
            destination.complete();
            break;
          case 'unsubscribe':
            // Unsubscription is not an Observable complete notification.
            if (event.reason === 'cancel') destination.unsubscribe();
            break;
          case 'subscribe':
          case 'state':
            break;
        }
      } catch (error) {
        destination.error(error);
      }
    }, () => { /* Saved evidence ends here. Do not invent an unobserved terminal. */ });
  });
}
