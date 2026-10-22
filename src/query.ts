import type { EventQuery, Recording, TraceEvent } from './types.js';

export interface LogpointQuery extends EventQuery {
  /** A function over captured events. It cannot recover unrecorded runtime variables. */
  readonly predicate?: (event: TraceEvent) => boolean;
}

export interface LogpointResult<T> {
  readonly event: TraceEvent;
  readonly result: { readonly ok: true; readonly value: T } | { readonly ok: false; readonly error: string };
}

function validateQuery(query: EventQuery): void {
  for (const field of ['fromSeq', 'toSeq'] as const) {
    const value = query[field];
    if (value !== undefined && (!Number.isSafeInteger(value) || value < 0)) {
      throw new RangeError(`${field} must be a non-negative safe integer.`);
    }
  }
  for (const field of ['fromTime', 'toTime'] as const) {
    const value = query[field];
    if (value !== undefined && (!Number.isFinite(value) || value < 0)) {
      throw new RangeError(`${field} must be a finite non-negative number.`);
    }
  }
}

/** Inclusive filters preserve recorded delivery order, including events with equal timestamps. */
export function selectEvents(recording: Recording, query: EventQuery = {}): readonly TraceEvent[] {
  validateQuery(query);
  return Object.freeze(recording.events.filter(event => (
    (query.streamId === undefined || event.streamId === query.streamId)
    && (query.subscriptionId === undefined || event.subscriptionId === query.subscriptionId)
    && (query.executionId === undefined || event.executionId === query.executionId)
    && (query.ownerExecutionId === undefined || event.ownerExecutionId === query.ownerExecutionId)
    && (query.role === undefined || event.role === query.role)
    && (query.port === undefined || event.port === query.port)
    && (query.kinds === undefined || query.kinds.includes(event.kind))
    && (query.fromSeq === undefined || event.seq >= query.fromSeq)
    && (query.toSeq === undefined || event.seq <= query.toSeq)
    && (query.fromTime === undefined || event.at >= query.fromTime)
    && (query.toTime === undefined || event.at <= query.toTime)
  )));
}

function describeFailure(error: unknown): string {
  try { return error instanceof Error ? error.message : String(error); }
  catch { return 'Logpoint evaluation failed.'; }
}

/** Evaluate a new question against saved data. Predicate and projection failures stay local to each event. */
export function evaluateLogpoint<T>(
  recording: Recording,
  query: LogpointQuery,
  project: (event: TraceEvent) => T,
): readonly LogpointResult<T>[] {
  const results: LogpointResult<T>[] = [];
  for (const event of selectEvents(recording, query)) {
    try {
      if (query.predicate && !query.predicate(event)) continue;
      const value = project(event);
      results.push(Object.freeze({ event, result: Object.freeze({ ok: true as const, value }) }));
    } catch (error) {
      results.push(Object.freeze({ event, result: Object.freeze({ ok: false as const, error: describeFailure(error) }) }));
    }
  }
  return Object.freeze(results);
}
