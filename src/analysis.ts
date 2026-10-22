import { resolveCursor } from './history.js';
import type { Recording, TraceEvent } from './types.js';

export type ExecutionStatus = 'active' | 'complete' | 'error' | 'cancel' | 'unknown';

export interface ExecutionSummary {
  readonly id: string;
  readonly streamId: string;
  readonly role: 'boundary' | 'operator' | 'inner';
  readonly ownerExecutionId?: string;
  readonly innerIndex?: number;
  readonly inputSubscriptionId?: string;
  readonly outputSubscriptionId?: string;
  readonly startSeq?: number;
  readonly startTime?: number;
  /** The first output terminal notification or cancellation, independent of later cleanup. */
  readonly endSeq?: number;
  readonly endTime?: number;
  readonly status: ExecutionStatus;
  readonly inputNextCount: number;
  readonly outputNextCount: number;
  /** Whether output disposal has been observed through this cursor. */
  readonly disposed: boolean;
}

export interface SwitchMapState {
  readonly executionId: string;
  readonly provenance: 'reconstructed' | 'unavailable';
  readonly outerCompleted: boolean;
  readonly activeInnerId: string | null;
  readonly outputStatus: ExecutionStatus;
  readonly inputSubscriptionId?: string;
  readonly outputSubscriptionId?: string;
  readonly explanation: string;
  /** Relevant observed event identities, suitable for explicit navigation. */
  readonly evidenceSeqs: readonly number[];
}

type MutableExecution = { -readonly [Key in keyof ExecutionSummary]: ExecutionSummary[Key] };

function cursor(recording: Recording, seq?: number): number {
  return resolveCursor(recording, seq ?? recording.events.at(-1)?.seq ?? 0);
}

/** Legacy recordings have one boundary channel per subscription, without operator ownership. */
function executionIdentity(event: TraceEvent): string { return event.executionId ?? event.subscriptionId; }

/** Group independent notification channels by execution without merging their lifecycles. */
export function getExecutions(recording: Recording, seq?: number): readonly ExecutionSummary[] {
  const selected = cursor(recording, seq);
  const executions = new Map<string, MutableExecution>();
  for (const event of recording.events) {
    if (event.seq > selected) break;
    const id = executionIdentity(event);
    let execution = executions.get(id);
    if (!execution) {
      execution = {
        id, streamId: event.streamId, role: event.role ?? 'boundary',
        ...(event.ownerExecutionId === undefined ? {} : { ownerExecutionId: event.ownerExecutionId }),
        ...(event.innerIndex === undefined ? {} : { innerIndex: event.innerIndex }),
        status: 'unknown', inputNextCount: 0, outputNextCount: 0, disposed: false,
      };
      executions.set(id, execution);
    }
    if (event.port === 'input') {
      execution.inputSubscriptionId = event.subscriptionId;
      if (event.kind === 'next') execution.inputNextCount++;
      continue;
    }
    execution.outputSubscriptionId = event.subscriptionId;
    if (event.kind === 'subscribe') {
      execution.startSeq = event.seq;
      execution.startTime = event.at;
      execution.status = 'active';
    }
    if (event.kind === 'next') execution.outputNextCount++;
    if (event.kind === 'complete' || event.kind === 'error' || event.kind === 'unsubscribe') {
      if (event.kind === 'unsubscribe') execution.disposed = true;
      const ending = event.kind === 'unsubscribe' ? event.reason : event.kind;
      if (execution.endSeq === undefined) {
        execution.status = ending;
        execution.endSeq = event.seq;
        execution.endTime = event.at;
      }
    }
  }
  return Object.freeze([...executions.values()].map(execution => Object.freeze(execution)));
}

type InnerLife = {
  readonly subscriptionId: string;
  readonly startSeq: number;
  terminal?: 'complete' | 'error' | 'cancel';
  disposed: boolean;
};

/**
 * A bounded switchMap model over recorded observations. No source, projector, or saved
 * application state is evaluated. Unsupported reentrant overlap remains visible as evidence.
 */
export function getSwitchMapState(recording: Recording, executionId: string, seq?: number): SwitchMapState {
  const selected = cursor(recording, seq);
  const execution = getExecutions(recording, selected).find(item => item.id === executionId);
  const relevant = recording.events.filter(event => event.seq <= selected && (
    event.executionId === executionId || event.ownerExecutionId === executionId
  ));
  const outputStart = relevant.find(event => event.executionId === executionId && event.port === 'output' && event.kind === 'subscribe');
  const inputStart = relevant.find(event => event.executionId === executionId && event.port === 'input' && event.kind === 'subscribe');
  const definition = outputStart && recording.streams.find(stream => stream.id === outputStart.streamId);
  let outerCompleted = false;
  let activeInnerId: string | null = null;
  let outputStatus = execution?.status ?? 'unknown';
  let lastInputSeq: number | undefined;
  let outerCompletionSeq: number | undefined;
  let lastInnerSeq: number | undefined;
  let problemSeq: number | undefined;
  const evidence = (): readonly number[] => Object.freeze([...new Set([
    outputStart?.seq, inputStart?.seq, lastInputSeq, outerCompletionSeq,
    lastInnerSeq, execution?.endSeq, problemSeq,
  ].filter((value): value is number => value !== undefined))].sort((left, right) => left - right));
  function result(provenance: SwitchMapState['provenance'], explanation: string): SwitchMapState {
    return Object.freeze({
      executionId, provenance, outerCompleted,
      activeInnerId: provenance === 'reconstructed' ? activeInnerId : null,
      outputStatus,
      ...(execution?.inputSubscriptionId === undefined ? {} : { inputSubscriptionId: execution.inputSubscriptionId }),
      ...(execution?.outputSubscriptionId === undefined ? {} : { outputSubscriptionId: execution.outputSubscriptionId }),
      explanation, evidenceSeqs: evidence(),
    });
  }

  if (recording.schemaVersion === 1) {
    return result('unavailable', 'Schema v1 contains boundary observations without operator ownership. A switchMap state cannot be reconstructed from that evidence.');
  }
  if (!execution) return result('unavailable', 'This execution has not been observed through the selected event.');
  if (!outputStart || outputStart.role !== 'operator' || definition?.operator !== 'switchMap') {
    return result('unavailable', 'This execution has no observed output subscription with supported switchMap instrumentation. Its recorded events remain available.');
  }
  if (!inputStart) {
    return result('unavailable', 'The operator output subscribed, but its input subscription has not been observed through this cursor. The model cannot infer missing input evidence.');
  }
  if (inputStart.role !== 'operator' || inputStart.subscriptionId === outputStart.subscriptionId) {
    return result('unavailable', 'The input and output do not have the distinct observed subscription channels required by this switchMap model.');
  }

  const inners = new Map<string, InnerLife>();
  const active = new Set<string>();
  let outerTerminal: 'complete' | 'error' | 'cancel' | undefined;
  let outputTerminal: 'complete' | 'error' | 'cancel' | undefined;
  let outputDisposed = false;
  let inputObserved = false;
  let outputObserved = false;
  let overlap = false;
  let unsupported = false;
  function unsupportedAt(event: TraceEvent): void { unsupported = true; problemSeq ??= event.seq; }

  for (const event of relevant) {
    if (event.executionId === executionId) {
      if (event.role !== 'operator' || event.streamId !== outputStart.streamId) unsupportedAt(event);
      if (event.port === 'input') {
        if (event.subscriptionId !== inputStart.subscriptionId) unsupportedAt(event);
        if (event.kind === 'subscribe') {
          if (inputObserved || !outputObserved) unsupportedAt(event);
          inputObserved = true;
        } else if (!inputObserved) unsupportedAt(event);
        if (event.kind === 'next') {
          if (outerTerminal !== undefined || outputTerminal !== undefined) unsupportedAt(event);
          lastInputSeq = event.seq;
        }
        if (event.kind === 'complete' || event.kind === 'error') {
          if (outerTerminal !== undefined) unsupportedAt(event);
          outerTerminal = event.kind;
          if (event.kind === 'complete') {
            outerCompleted = true;
            outerCompletionSeq = event.seq;
          }
        }
        if (event.kind === 'unsubscribe') {
          if (outerTerminal === undefined) {
            if (event.reason !== 'cancel') unsupportedAt(event);
            outerTerminal = event.reason;
          } else if (event.reason !== outerTerminal) unsupportedAt(event);
        }
      } else if (event.port === 'output') {
        if (event.subscriptionId !== outputStart.subscriptionId) unsupportedAt(event);
        if (event.kind === 'subscribe') {
          if (outputObserved) unsupportedAt(event);
          outputObserved = true;
        } else if (!outputObserved) unsupportedAt(event);
        if (event.kind === 'next' && outputTerminal !== undefined) unsupportedAt(event);
        if (event.kind === 'complete' || event.kind === 'error') {
          if (outputTerminal !== undefined) unsupportedAt(event);
          if (event.kind === 'complete' && (!outerCompleted || active.size > 0)) unsupportedAt(event);
          outputTerminal = event.kind;
        }
        if (event.kind === 'unsubscribe') {
          if (outputDisposed) unsupportedAt(event);
          if (outputTerminal === undefined) {
            if (event.reason !== 'cancel') unsupportedAt(event);
            outputTerminal = event.reason;
          } else if (event.reason !== outputTerminal) unsupportedAt(event);
          outputDisposed = true;
        }
      } else unsupportedAt(event);
      continue;
    }

    if (event.role !== 'inner' || event.port !== 'output' || !event.executionId) {
      unsupportedAt(event);
      continue;
    }
    lastInnerSeq = event.seq;
    let inner = inners.get(event.executionId);
    if (event.kind === 'subscribe') {
      if (inner || !inputObserved || lastInputSeq === undefined || outputTerminal !== undefined) unsupportedAt(event);
      inner = { subscriptionId: event.subscriptionId, startSeq: event.seq, disposed: false };
      inners.set(event.executionId, inner);
      active.add(event.executionId);
      if (active.size > 1) { overlap = true; problemSeq ??= event.seq; }
    } else if (!inner || inner.subscriptionId !== event.subscriptionId) {
      unsupportedAt(event);
      continue;
    }
    if (!inner) continue;
    if (event.kind === 'next' && inner.terminal !== undefined) unsupportedAt(event);
    if (event.kind === 'complete' || event.kind === 'error') {
      if (inner.terminal !== undefined) unsupportedAt(event);
      inner.terminal = event.kind;
      active.delete(event.executionId);
    }
    if (event.kind === 'unsubscribe') {
      if (inner.disposed) unsupportedAt(event);
      if (inner.terminal === undefined) {
        if (event.reason !== 'cancel') unsupportedAt(event);
        inner.terminal = event.reason;
        active.delete(event.executionId);
      } else if (inner.terminal !== event.reason) unsupportedAt(event);
      inner.disposed = true;
    }
  }
  activeInnerId = active.size === 1 ? active.values().next().value ?? null : null;
  outputStatus = outputTerminal ?? (outputObserved ? 'active' : 'unknown');
  if (overlap) return result('unavailable', 'Observed overlapping inner executions exceed the supported single-active-inner switchMap model. Their real notifications and lifetimes remain available.');
  if (unsupported) return result('unavailable', 'The observed transitions exceed the supported switchMap model or omit required evidence. Recorded facts remain available without a confident operator explanation.');

  if (outputStatus === 'complete') {
    return result('reconstructed', 'The output delivered completion after the outer input completed and no inner execution remained active. Output disposal is a separate recorded event.');
  }
  if (outputStatus === 'error') {
    return result('reconstructed', 'The output delivered an error. Subscription cleanup can appear after that notification in recorded event order.');
  }
  if (outputStatus === 'cancel') {
    return result('reconstructed', active.size > 0
      ? 'The output subscription was cancelled. An inner has not yet reached its recorded ending at this cursor; cleanup can follow in later events.'
      : 'The output subscription was cancelled, and no inner execution remains active through this cursor.');
  }
  if (outerCompleted) {
    return result('reconstructed', activeInnerId !== null
      ? `The outer input completed. Inner execution ${activeInnerId} remains active at this event, so the output is still open.`
      : 'The outer input completed and no inner execution remains active at this cursor. Output completion has not yet been observed.');
  }
  return result('reconstructed', activeInnerId !== null
    ? `Inner execution ${activeInnerId} is active. The switchMap policy replaces the active inner when a new outer input is handled.`
    : 'No inner execution is active through this cursor, and no outer completion has been observed.');
}

/** Explain an observed event; any operator-policy interpretation is named explicitly. */
export function explainEvent(recording: Recording, event: TraceEvent): string {
  const definition = recording.streams.find(stream => stream.id === event.streamId);
  const label = definition?.label ?? event.streamId;
  const identity = executionIdentity(event);
  const subject = event.role === 'inner' ? `Inner execution ${identity}`
    : event.role === 'operator' ? `${label} ${event.port ?? 'output'} channel` : label;
  const context = event.causeSeq === undefined ? '' : ` within synchronous delivery context #${event.causeSeq}`;
  if (event.kind === 'subscribe') {
    return `${subject} subscribed as ${event.subscriptionId}${context}. This subscription has its own notification lifecycle.`;
  }
  if (event.kind === 'state') {
    return `${subject} recorded explicitly supplied domain state "${event.label}". This observation does not expose private RxJS operator memory.`;
  }
  if (event.kind === 'next') {
    return `${subject} delivered a captured value${context}.`;
  }
  if (event.kind === 'error') {
    return `${subject} delivered an error notification${context}. Disposal is recorded separately.`;
  }
  if (event.kind === 'complete') {
    return event.role === 'operator' && event.port === 'input'
      ? `${subject} delivered completion. The operator output has a separate subscription and can remain open for an active inner.`
      : `${subject} delivered a completion notification. Disposal is recorded separately.`;
  }
  if (event.reason !== 'cancel') {
    return `${subject} recorded disposal after its ${event.reason} notification. This is not an additional notification.`;
  }
  const cause = event.causeSeq === undefined ? undefined : recording.events.find(candidate => candidate.seq === event.causeSeq);
  if (event.role === 'inner' && definition?.operator === 'switchMap' && cause?.kind === 'next'
      && cause.role === 'operator' && cause.port === 'input' && cause.executionId === event.ownerExecutionId) {
    const innerStart = recording.events.find(candidate => candidate.kind === 'subscribe'
      && candidate.role === 'inner' && candidate.executionId === event.executionId
      && candidate.subscriptionId === event.subscriptionId);
    const ownerOutput = recording.events.filter(candidate => candidate.seq < event.seq
      && candidate.role === 'operator' && candidate.port === 'output'
      && candidate.executionId === event.ownerExecutionId);
    // Delivery context also covers new-inner self-disposal and downstream cancellation.
    // Replacement requires an older inner and an output still open at this cursor.
    if (innerStart && innerStart.seq < cause.seq && cause.seq < event.seq
        && ownerOutput.some(candidate => candidate.kind === 'subscribe')
        && !ownerOutput.some(candidate => candidate.kind === 'complete' || candidate.kind === 'error' || candidate.kind === 'unsubscribe')) {
      return `${subject} was cancelled during outer input #${cause.seq}. The switchMap policy identifies replacement by that input. Disposal does not by itself prove that an external transport stopped.`;
    }
  }
  return `${subject} was cancelled before a terminal notification${context}. This records subscription disposal, not external-effect rollback.`;
}
