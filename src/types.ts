import type { MonoTypeOperatorFunction, Observable, SchedulerLike } from 'rxjs';

export type JsonValue = null | boolean | number | string | readonly JsonValue[] | { readonly [key: string]: JsonValue };
export type CapturedValue =
  | { readonly status: 'captured' | 'truncated'; readonly data: JsonValue }
  | { readonly status: 'unavailable'; readonly reason: string };
export type Lane = 'source' | 'internal' | 'result';

/** Graph and sharing metadata is declared by the application, never guessed from values. */
export interface StreamDefinition {
  readonly id: string;
  readonly label: string;
  readonly lane: Lane;
  readonly inputs?: readonly string[];
  readonly sharing?: 'independent' | 'shared' | 'live';
}

export interface EventBase {
  readonly seq: number;
  readonly at: number;
  readonly streamId: string;
  readonly subscriptionId: string;
  /** Observed synchronous delivery context only; not inferred asynchronous causality. */
  readonly causeSeq?: number;
}

export type TraceEvent = EventBase & (
  | { readonly kind: 'subscribe'; readonly parentSubscriptionId?: string }
  | { readonly kind: 'next'; readonly value: CapturedValue }
  | { readonly kind: 'error'; readonly error: CapturedValue }
  | { readonly kind: 'complete' }
  | { readonly kind: 'unsubscribe'; readonly reason: 'cancel' | 'complete' | 'error' }
  | { readonly kind: 'state'; readonly state: CapturedValue; readonly label: string }
);
export type EventKind = TraceEvent['kind'];

/** An immutable retained prefix. Omitted suffix events are counted explicitly. */
export interface Recording {
  readonly format: 'rxjs-replay';
  readonly schemaVersion: 1;
  readonly rxjsVersion: '7.8.2';
  readonly name: string;
  readonly clock: { readonly name: string; readonly unit: 'ms' };
  readonly streams: readonly StreamDefinition[];
  readonly events: readonly TraceEvent[];
  readonly capture: {
    readonly maxEvents: number;
    readonly totalEvents: number;
    readonly droppedEvents: number;
    readonly clockErrors: number;
    readonly stopped: boolean;
  };
}

export interface RecorderOptions {
  readonly name?: string;
  readonly streams: readonly StreamDefinition[];
  readonly clock?: { readonly name?: string; readonly now: () => number };
  readonly maxEvents?: number;
  /** Pure capture-time projection. It must not emit, mutate, or subscribe. */
  readonly captureValue?: (value: unknown) => unknown;
}
export interface TraceOptions<T> {
  /** Explicitly supplied domain state; this is not access to RxJS private state. */
  readonly captureState?: (value: T) => unknown;
  readonly stateLabel?: string;
}
export interface Recorder {
  readonly trace: <T>(streamId: string, options?: TraceOptions<T>) => MonoTypeOperatorFunction<T>;
  readonly getRecording: () => Recording;
  /** Stop capture only. Application subscriptions keep their ordinary lifecycle. */
  readonly stop: () => Recording;
}

export interface EventQuery {
  readonly streamId?: string;
  readonly subscriptionId?: string;
  readonly kinds?: readonly EventKind[];
  readonly fromSeq?: number;
  readonly toSeq?: number;
  readonly fromTime?: number;
  readonly toTime?: number;
}
export interface SubscriptionView {
  readonly id: string;
  readonly streamId: string;
  readonly start: TraceEvent;
  readonly end?: TraceEvent;
  readonly outcome: 'active' | 'complete' | 'error' | 'cancel';
  readonly values: number;
  readonly latestValue?: CapturedValue;
  readonly latestState?: CapturedValue;
  readonly parentSubscriptionId?: string;
}
export interface HistoryView {
  readonly cursor: number;
  readonly event?: TraceEvent;
  readonly subscriptions: readonly SubscriptionView[];
  readonly observedEvents: number;
  readonly hasOmittedEvents: boolean;
}
export interface ReplayState {
  /** Zero means before the first event; otherwise a recorded event sequence. */
  readonly cursor: number;
  readonly status: 'paused' | 'playing' | 'ended' | 'disposed';
  readonly speed: number;
}
export type ReplayCommand =
  | { readonly type: 'seek'; readonly seq: number }
  | { readonly type: 'step'; readonly direction: 1 | -1 }
  | { readonly type: 'play' }
  | { readonly type: 'pause' }
  | { readonly type: 'speed'; readonly speed: number }
  | { readonly type: 'tick'; readonly seq: number }
  | { readonly type: 'dispose' };
export interface ReplayController {
  /** One explicitly shared controller lifetime. Late observers receive current state. */
  readonly state$: Observable<ReplayState>;
  readonly getState: () => ReplayState;
  readonly dispatch: (command: ReplayCommand) => void;
  readonly seek: (seq: number) => void;
  readonly seekTime: (at: number, bias?: 'before' | 'after') => void;
  readonly step: (direction?: 1 | -1) => void;
  readonly play: () => void;
  readonly pause: () => void;
  readonly setSpeed: (speed: number) => void;
  readonly dispose: () => void;
}
export interface PlaybackOptions {
  readonly scheduler?: SchedulerLike;
  readonly speed?: number;
}
