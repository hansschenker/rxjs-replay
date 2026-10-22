import { Observable, Subscriber } from 'rxjs';
import type { MonoTypeOperatorFunction } from 'rxjs';
import { captureSnapshot, freezeOwned } from './snapshot.js';
import type { EventBase, Recorder, RecorderOptions, Recording, StreamDefinition, TraceEvent, TraceOptions } from './types.js';

type EventDetails = TraceEvent extends infer E ? E extends EventBase ? Omit<E, keyof EventBase> : never : never;

export function createRecorder(options: RecorderOptions): Recorder {
  const maxEvents = options.maxEvents ?? 50_000;
  if (!Number.isSafeInteger(maxEvents) || maxEvents < 1 || maxEvents > 250_000) {
    throw new RangeError('maxEvents must be an integer between 1 and 250000.');
  }
  const name = options.name ?? 'RxJS recording';
  if (typeof name !== 'string' || !name.trim() || name.length > 256) throw new TypeError('name must be a nonempty string of at most 256 characters.');
  if (!Array.isArray(options.streams) || options.streams.length > 1_000) throw new TypeError('streams must be an array of at most 1000 definitions.');
  const ids = new Set<string>();
  const validId = (id: unknown): id is string => typeof id === 'string' && id.trim().length > 0 && id.length <= 256;
  const streams: readonly StreamDefinition[] = freezeOwned(options.streams.map(stream => {
    if (!validId(stream.id) || !validId(stream.label) || ids.has(stream.id)) throw new TypeError('Each stream needs a unique id and nonempty label (at most 256 characters).');
    if (!['source', 'internal', 'result'].includes(stream.lane)) throw new TypeError(`Invalid lane for ${stream.id}.`);
    if (stream.sharing !== undefined && !['independent', 'shared', 'live'].includes(stream.sharing)) throw new TypeError('Invalid declared sharing policy.');
    if (stream.inputs !== undefined && (!Array.isArray(stream.inputs) || !stream.inputs.every(validId) || new Set(stream.inputs).size !== stream.inputs.length)) throw new TypeError('inputs must contain unique stream ids.');
    ids.add(stream.id);
    return { id: stream.id, label: stream.label, lane: stream.lane,
      ...(stream.inputs ? { inputs: [...stream.inputs] } : {}),
      ...(stream.sharing ? { sharing: stream.sharing } : {}),
    };
  }));
  for (const stream of streams) for (const input of stream.inputs ?? []) {
    if (!ids.has(input)) throw new TypeError(`Unknown input stream: ${input}.`);
  }
  const clock = options.clock ?? { name: 'performance', now: () => performance.now() };
  const clockName = clock.name ?? 'custom';
  if (!validId(clockName) || typeof clock.now !== 'function') throw new TypeError('clock needs a name and a now function.');
  if (options.captureValue !== undefined && typeof options.captureValue !== 'function') throw new TypeError('captureValue must be a function.');
  const captureValue = options.captureValue;
  let origin: number | undefined;
  let at = 0;
  let clockErrors = 0;
  function readTime(): number {
    try {
      const current = clock.now();
      if (!Number.isFinite(current)) throw new Error('invalid clock');
      origin ??= current;
      const elapsed = current - origin;
      if (!Number.isFinite(elapsed) || elapsed < at) throw new Error('clock moved backwards');
      at = elapsed;
    } catch { clockErrors++; }
    return at;
  }
  readTime();

  const events: TraceEvent[] = [];
  let totalEvents = 0;
  let subscriptionCount = 0;
  let stopped = false;
  let context: { readonly seq: number; readonly subscriptionId: string } | undefined;
  function record(streamId: string, subscriptionId: string, details: () => EventDetails): number | undefined {
    if (stopped) return undefined;
    const seq = ++totalEvents;
    if (events.length >= maxEvents) return undefined;
    const event = freezeOwned({ seq, at: readTime(), streamId, subscriptionId,
      ...(context ? { causeSeq: context.seq } : {}), ...details(),
    }) as TraceEvent;
    events.push(event);
    return seq;
  }
  function deliver(seq: number | undefined, subscriptionId: string, forward: () => void): void {
    const previous = context;
    context = seq === undefined ? undefined : { seq, subscriptionId };
    try { forward(); } finally { context = previous; }
  }
  function trace<T>(streamId: string, traceOptions: TraceOptions<T> = {}): MonoTypeOperatorFunction<T> {
    if (!ids.has(streamId)) throw new TypeError(`Unknown stream: ${streamId}. Declare it in createRecorder({ streams }).`);
    const captureState = traceOptions.captureState;
    const stateLabel = traceOptions.stateLabel ?? 'captured state';
    if (captureState !== undefined && typeof captureState !== 'function') throw new TypeError('captureState must be a function.');
    if (!validId(stateLabel)) throw new TypeError('stateLabel must be nonempty and at most 256 characters.');
    return source => new Observable<T>(destination => {
      if (destination.closed) return;
      const subscriptionId = `s${++subscriptionCount}`;
      const parentSubscriptionId = context?.subscriptionId;
      record(streamId, subscriptionId, () => ({ kind: 'subscribe', ...(parentSubscriptionId ? { parentSubscriptionId } : {}) }));
      let outcome: 'cancel' | 'complete' | 'error' = 'cancel';
      const forwarding = new Subscriber<T>({
        next(value) {
          const seq = record(streamId, subscriptionId, () => ({ kind: 'next', value: captureSnapshot(value, captureValue) }));
          if (captureState) deliver(seq, subscriptionId, () => {
            record(streamId, subscriptionId, () => ({ kind: 'state', label: stateLabel, state: captureSnapshot(value, input => captureState(input as T)) }));
          });
          deliver(seq, subscriptionId, () => destination.next(value));
        },
        error(error: unknown) {
          outcome = 'error';
          const seq = record(streamId, subscriptionId, () => ({ kind: 'error', error: captureSnapshot(error, captureValue) }));
          deliver(seq, subscriptionId, () => destination.error(error));
        },
        complete() {
          outcome = 'complete';
          const seq = record(streamId, subscriptionId, () => ({ kind: 'complete' }));
          deliver(seq, subscriptionId, () => destination.complete());
        },
      });
      // Wire disposal before upstream starts, including for synchronous sources and take(1).
      destination.add(forwarding);
      forwarding.add(() => { record(streamId, subscriptionId, () => ({ kind: 'unsubscribe', reason: outcome })); });
      if (!forwarding.closed) source.subscribe(forwarding);
    });
  }
  function getRecording(): Recording {
    return freezeOwned({ format: 'rxjs-replay', schemaVersion: 1, rxjsVersion: '7.8.2', name,
      clock: { name: clockName, unit: 'ms' }, streams, events: [...events],
      capture: { maxEvents, totalEvents, droppedEvents: totalEvents - events.length, clockErrors, stopped },
    });
  }
  return Object.freeze({ trace, getRecording, stop: () => { stopped = true; return getRecording(); } });
}
