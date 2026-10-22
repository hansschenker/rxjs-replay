import type { MonoTypeOperatorFunction, Observable, ObservableInput, ObservedValueOf, OperatorFunction, SchedulerLike } from 'rxjs';

export type JsonValue = null | boolean | number | string | readonly JsonValue[] | { readonly [key: string]: JsonValue };
export type CapturedValue =
  | { readonly status: 'captured' | 'truncated'; readonly data: JsonValue }
  | { readonly status: 'unavailable'; readonly reason: string };
export type Lane = 'source' | 'internal' | 'result';
export type ExecutionRole = 'boundary' | 'operator' | 'inner';
export type ExecutionPort = 'input' | 'output';

/** Graph and sharing metadata is declared by the application, never guessed from values. */
export interface StreamDefinition {
  readonly id: string;
  readonly label: string;
  readonly lane: Lane;
  readonly inputs?: readonly string[];
  readonly sharing?: 'independent' | 'shared' | 'live';
  /** Operator policy and domain function are explicit labels, not inferred from values. */
  readonly operator?: string;
  readonly domain?: string;
}

export interface ExecutionMetadata {
  /** Groups the separately replayable input/output channels of an operator. */
  readonly executionId: string;
  readonly role: ExecutionRole;
  readonly port: ExecutionPort;
  /** Explicit inner ownership; distinct from synchronous subscription nesting. */
  readonly ownerExecutionId?: string;
  readonly innerIndex?: number;
}

/** Version 1 events have no execution metadata. Version 2 requires it on every event. */
export interface EventBase extends Partial<ExecutionMetadata> {
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
export type RecordedTraceEvent = TraceEvent & ExecutionMetadata;

/** An immutable retained prefix. Omitted suffix events are counted explicitly. */
export interface Recording {
  readonly format: 'rxjs-replay';
  readonly schemaVersion: 1 | 2;
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
export interface CurrentRecording extends Recording {
  readonly schemaVersion: 2;
  readonly events: readonly RecordedTraceEvent[];
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
  /** Delegates the latest-inner policy to RxJS 7.8.2 and records its actual executions. */
  readonly switchMap: <T, O extends ObservableInput<unknown>>(streamId: string, project: (value: T, index: number) => O) => OperatorFunction<T, ObservedValueOf<O>>;
  readonly getRecording: () => CurrentRecording;
  /** Stop capture only. Application subscriptions keep their ordinary lifecycle. */
  readonly stop: () => CurrentRecording;
}

export interface EventQuery {
  readonly streamId?: string;
  readonly subscriptionId?: string;
  readonly executionId?: string;
  readonly ownerExecutionId?: string;
  readonly role?: ExecutionRole;
  readonly port?: ExecutionPort;
  readonly kinds?: readonly EventKind[];
  readonly fromSeq?: number;
  readonly toSeq?: number;
  readonly fromTime?: number;
  readonly toTime?: number;
}
export interface SubscriptionView extends Partial<ExecutionMetadata> {
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
