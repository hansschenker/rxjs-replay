import { describe, expect, it } from 'vitest';
import { Observable, Subject } from 'rxjs';
import type { Subscription } from 'rxjs';
import { explainEvent, getExecutions, getSwitchMapState } from '../src/analysis.js';
import { createRecorder } from '../src/recorder.js';
import type { Recording, TraceEvent } from '../src/types.js';

function operator(seq: number, port: 'input' | 'output', at = 0, executionId = 'op1') {
  return { seq, at, streamId: 'latest', executionId, role: 'operator' as const, port, subscriptionId: `${executionId}-${port}` };
}
function inner(seq: number, executionId: string, innerIndex: number, at = 0, ownerExecutionId = 'op1') {
  return { seq, at, streamId: 'latest', executionId, role: 'inner' as const, port: 'output' as const,
    subscriptionId: `${executionId}-output`, ownerExecutionId, innerIndex };
}
function recording(events: readonly TraceEvent[]): Recording {
  return {
    format: 'rxjs-replay', schemaVersion: 2, rxjsVersion: '7.8.2', name: 'Operator analysis',
    clock: { name: 'virtual', unit: 'ms' },
    streams: [{ id: 'latest', label: 'Latest result', lane: 'internal', operator: 'switchMap', domain: 'loadResult$' }],
    events, capture: { maxEvents: 100, totalEvents: events.length, droppedEvents: 0, clockErrors: 0, stopped: true },
  };
}
function replacedInner(): TraceEvent[] {
  return [
    { ...operator(1, 'output'), kind: 'subscribe' },
    { ...operator(2, 'input'), kind: 'subscribe' },
    { ...operator(3, 'input'), kind: 'next', value: { status: 'captured', data: 'a' } },
    { ...inner(4, 'innerA', 0), kind: 'subscribe', causeSeq: 3, parentSubscriptionId: 'op1-input' },
    { ...operator(5, 'input', 10), kind: 'next', value: { status: 'captured', data: 'b' } },
    { ...inner(6, 'innerA', 0, 10), kind: 'unsubscribe', reason: 'cancel', causeSeq: 5 },
    { ...inner(7, 'innerB', 1, 10), kind: 'subscribe', causeSeq: 5, parentSubscriptionId: 'op1-input' },
  ];
}
function finishedRecording(): Recording {
  return recording([
    ...replacedInner(),
    { ...operator(8, 'input', 11), kind: 'complete' },
    { ...operator(9, 'input', 11), kind: 'unsubscribe', reason: 'complete' },
    { ...inner(10, 'innerB', 1, 20), kind: 'next', value: { status: 'captured', data: 'b' } },
    { ...operator(11, 'output', 20), kind: 'next', value: { status: 'captured', data: 'b' }, causeSeq: 10 },
    { ...inner(12, 'innerB', 1, 21), kind: 'complete' },
    { ...operator(13, 'output', 21), kind: 'complete' },
    { ...operator(14, 'output', 21), kind: 'unsubscribe', reason: 'complete' },
    { ...inner(15, 'innerB', 1, 21), kind: 'unsubscribe', reason: 'complete' },
  ]);
}

describe('execution groups', () => {
  it('groups input/output channels without mixing lifecycle or next counts', () => {
    const trace = finishedRecording();
    const executions = getExecutions(trace);
    expect(executions).toHaveLength(3);
    expect(executions[0]).toMatchObject({
      id: 'op1', role: 'operator', streamId: 'latest', inputSubscriptionId: 'op1-input', outputSubscriptionId: 'op1-output',
      inputNextCount: 2, outputNextCount: 1, status: 'complete', startSeq: 1, endSeq: 13, disposed: true,
    });
    expect(executions[1]).toMatchObject({ id: 'innerA', role: 'inner', ownerExecutionId: 'op1', innerIndex: 0, status: 'cancel', endSeq: 6, disposed: true });
    expect(executions[2]).toMatchObject({ id: 'innerB', status: 'complete', endSeq: 12, disposed: true });
    expect(Object.isFrozen(executions)).toBe(true);
    expect(Object.isFrozen(executions[0])).toBe(true);
  });

  it('keeps output active after input completion and separates terminal notification from cleanup', () => {
    const trace = finishedRecording();
    expect(getExecutions(trace, 9)[0]).toMatchObject({ status: 'active', disposed: false });
    expect(getExecutions(trace, 13)[0]).toMatchObject({ status: 'complete', disposed: false, endSeq: 13 });
    expect(getExecutions(trace, 14)[0]).toMatchObject({ status: 'complete', disposed: true, endSeq: 13 });
  });

  it('provides boundary summaries for legacy recordings without inventing operator roles', () => {
    const legacy: Recording = {
      ...finishedRecording(), schemaVersion: 1,
      streams: [{ id: 'latest', label: 'Legacy result', lane: 'result' }],
      events: [
        { seq: 1, at: 0, streamId: 'latest', subscriptionId: 'legacy', kind: 'subscribe' },
        { seq: 2, at: 1, streamId: 'latest', subscriptionId: 'legacy', kind: 'next', value: { status: 'captured', data: 2 } },
      ],
    };
    expect(getExecutions(legacy)).toMatchObject([{ id: 'legacy', role: 'boundary', status: 'active', outputNextCount: 1 }]);
    expect(getSwitchMapState(legacy, 'legacy')).toMatchObject({ provenance: 'unavailable', activeInnerId: null });
    expect(getSwitchMapState(legacy, 'legacy').explanation).toContain('Schema v1');
  });
});

describe('bounded switchMap explanations', () => {
  it('distinguishes same-time input, cancellation, and replacement subscription', () => {
    const trace = finishedRecording();
    expect(getSwitchMapState(trace, 'op1', 5)).toMatchObject({ provenance: 'reconstructed', activeInnerId: 'innerA', outerCompleted: false });
    expect(getSwitchMapState(trace, 'op1', 6)).toMatchObject({ provenance: 'reconstructed', activeInnerId: null });
    expect(getSwitchMapState(trace, 'op1', 7)).toMatchObject({ provenance: 'reconstructed', activeInnerId: 'innerB' });
    expect(getSwitchMapState(trace, 'op1', 7).evidenceSeqs).toContain(5);
  });

  it('explains waiting after outer completion without promising a future result', () => {
    const state = getSwitchMapState(finishedRecording(), 'op1', 9);
    expect(state).toMatchObject({ provenance: 'reconstructed', outerCompleted: true, activeInnerId: 'innerB', outputStatus: 'active' });
    expect(state.explanation).toContain('output is still open');
    expect(state.evidenceSeqs).toContain(8);
    expect(Object.isFrozen(state)).toBe(true);
    expect(Object.isFrozen(state.evidenceSeqs)).toBe(true);
  });

  it('does not double-end an inner when cleanup follows its terminal notification', () => {
    const trace = finishedRecording();
    expect(getSwitchMapState(trace, 'op1', 12)).toMatchObject({ provenance: 'reconstructed', outerCompleted: true, activeInnerId: null, outputStatus: 'active' });
    expect(getSwitchMapState(trace, 'op1', 12).explanation).toContain('completion has not yet been observed');
    expect(getSwitchMapState(trace, 'op1', 13)).toMatchObject({ provenance: 'reconstructed', outputStatus: 'complete' });
    expect(getSwitchMapState(trace, 'op1', 15)).toMatchObject({ provenance: 'reconstructed', outputStatus: 'complete', activeInnerId: null });
  });

  it('tolerates cancellation before the resulting inner cleanup is recorded', () => {
    const trace = recording([
      ...replacedInner(),
      { ...operator(8, 'output', 11), kind: 'unsubscribe', reason: 'cancel' },
      { ...operator(9, 'input', 11), kind: 'unsubscribe', reason: 'cancel' },
      { ...inner(10, 'innerB', 1, 11), kind: 'unsubscribe', reason: 'cancel' },
    ]);
    expect(getSwitchMapState(trace, 'op1', 8)).toMatchObject({ provenance: 'reconstructed', outputStatus: 'cancel', activeInnerId: 'innerB' });
    expect(getSwitchMapState(trace, 'op1', 10)).toMatchObject({ provenance: 'reconstructed', outputStatus: 'cancel', activeInnerId: null });
  });

  it('handles inner error followed by output error and independent cleanup', () => {
    const trace = recording([
      ...replacedInner(),
      { ...inner(8, 'innerB', 1, 11), kind: 'error', error: { status: 'captured', data: 'failure' } },
      { ...operator(9, 'output', 11), kind: 'error', error: { status: 'captured', data: 'failure' } },
      { ...operator(10, 'output', 11), kind: 'unsubscribe', reason: 'error' },
      { ...operator(11, 'input', 11), kind: 'unsubscribe', reason: 'cancel' },
      { ...inner(12, 'innerB', 1, 11), kind: 'unsubscribe', reason: 'error' },
    ]);
    expect(getSwitchMapState(trace, 'op1')).toMatchObject({ provenance: 'reconstructed', outputStatus: 'error', activeInnerId: null });
  });

  it('refuses the single-active-inner model after observed reentrant overlap', () => {
    const trace = recording([
      ...replacedInner().slice(0, 5),
      { ...inner(6, 'innerB', 1, 10), kind: 'subscribe', causeSeq: 5 },
      { ...inner(7, 'innerA', 0, 11), kind: 'unsubscribe', reason: 'cancel' },
    ]);
    expect(getSwitchMapState(trace, 'op1', 6)).toMatchObject({ provenance: 'unavailable', activeInnerId: null, outputStatus: 'active' });
    expect(getSwitchMapState(trace, 'op1', 6).explanation).toContain('overlapping');
    expect(getSwitchMapState(trace, 'op1', 7).provenance).toBe('unavailable');
    expect(getExecutions(trace, 6).filter(execution => execution.role === 'inner' && execution.status === 'active')).toHaveLength(2);
  });

  it('scopes state to the selected operator execution rather than its shared stream definition', () => {
    const trace = recording([
      ...replacedInner(),
      { ...operator(8, 'output', 11, 'op2'), kind: 'subscribe' },
      { ...operator(9, 'input', 11, 'op2'), kind: 'subscribe' },
      { ...operator(10, 'input', 11, 'op2'), kind: 'next', value: { status: 'captured', data: 'other' } },
      { ...inner(11, 'innerC', 0, 11, 'op2'), kind: 'subscribe', causeSeq: 10 },
    ]);
    expect(getSwitchMapState(trace, 'op1')).toMatchObject({ provenance: 'reconstructed', activeInnerId: 'innerB' });
    expect(getSwitchMapState(trace, 'op2')).toMatchObject({ provenance: 'reconstructed', activeInnerId: 'innerC' });
  });

  it('makes missing input evidence and unsupported declarations unavailable', () => {
    const trace = finishedRecording();
    expect(getSwitchMapState(trace, 'op1', 0)).toMatchObject({ provenance: 'unavailable', outputStatus: 'unknown' });
    expect(getSwitchMapState(trace, 'op1', 1)).toMatchObject({ provenance: 'unavailable', outputStatus: 'active' });
    expect(getSwitchMapState(trace, 'missing')).toMatchObject({ provenance: 'unavailable' });
    const unsupported: Recording = { ...trace, streams: [{ id: 'latest', label: 'Other operator', lane: 'internal', operator: 'mergeMap' }] };
    expect(getSwitchMapState(unsupported, 'op1').provenance).toBe('unavailable');
    const missing: Recording = { ...trace, events: trace.events.filter(event => event.seq !== 2) };
    expect(getSwitchMapState(missing, 'op1').provenance).toBe('unavailable');
  });

  it('refuses a completion that contradicts the supported state transitions', () => {
    const trace = recording([
      ...replacedInner().slice(0, 4),
      { ...operator(5, 'output'), kind: 'complete' },
    ]);
    expect(getSwitchMapState(trace, 'op1')).toMatchObject({ provenance: 'unavailable', outputStatus: 'complete' });
  });

  it('does not treat omitted future events as missing evidence for an earlier cursor', () => {
    const trace = finishedRecording();
    const prefix: Recording = { ...trace, events: trace.events.slice(0, 9), capture: { ...trace.capture, maxEvents: 9, droppedEvents: 6 } };
    expect(getSwitchMapState(prefix, 'op1')).toMatchObject({ provenance: 'reconstructed', outerCompleted: true, activeInnerId: 'innerB', outputStatus: 'active' });
  });
});

describe('event explanations', () => {
  it('distinguishes inner replacement, input completion, and terminal cleanup', () => {
    const trace = finishedRecording();
    expect(explainEvent(trace, trace.events[5]!)).toContain('replacement');
    expect(explainEvent(trace, trace.events[5]!)).toContain('does not by itself prove');
    expect(explainEvent(trace, trace.events[7]!)).toContain('separate subscription');
    expect(explainEvent(trace, trace.events[14]!)).toContain('not an additional notification');
  });

  it('does not call synchronous downstream cancellation during inner subscription a replacement', () => {
    const recorder = createRecorder({ streams: recording([]).streams, clock: { name: 'same-frame', now: () => 0 } });
    const outer = new Subject<number>();
    let subscription!: Subscription;
    subscription = outer.pipe(recorder.switchMap('latest', () => new Observable(() => subscription.unsubscribe()))).subscribe();
    outer.next(1);

    const trace = recorder.getRecording();
    const cancellation = trace.events.find(event => event.role === 'inner' && event.kind === 'unsubscribe')!;
    expect(subscription.closed).toBe(true);
    expect(cancellation).toMatchObject({ kind: 'unsubscribe', reason: 'cancel', causeSeq: 3 });
    expect(explainEvent(trace, cancellation)).not.toContain('replacement');
    expect(explainEvent(trace, cancellation)).toContain('cancelled before a terminal notification');
  });

  it('requires the cancelled inner to predate its outer input even when the output is open', () => {
    const trace = recording([
      ...replacedInner().slice(0, 4),
      { ...inner(5, 'innerA', 0), kind: 'unsubscribe', reason: 'cancel', causeSeq: 3 },
    ]);
    expect(getExecutions(trace)[0]!.status).toBe('active');
    expect(explainEvent(trace, trace.events[4]!)).not.toContain('replacement');
  });

  it.each(['complete', 'error', 'cancel'] as const)('does not infer replacement after the owning output ended with %s', reason => {
    const outputEnd: TraceEvent = reason === 'cancel'
      ? { ...operator(6, 'output', 10), kind: 'unsubscribe', reason }
      : reason === 'error'
        ? { ...operator(6, 'output', 10), kind: 'error', error: { status: 'captured', data: 'failure' } }
        : { ...operator(6, 'output', 10), kind: 'complete' };
    const trace = recording([
      ...replacedInner().slice(0, 5),
      outputEnd,
      { ...inner(7, 'innerA', 0, 10), kind: 'unsubscribe', reason: 'cancel', causeSeq: 5 },
    ]);
    expect(explainEvent(trace, trace.events[6]!)).not.toContain('replacement');
  });

  it('keeps replacement evidence local to its cursor when reentrant teardown later cancels the output', () => {
    const trace = recording([
      ...replacedInner().slice(0, 6),
      { ...operator(7, 'output', 10), kind: 'unsubscribe', reason: 'cancel', causeSeq: 5 },
      { ...operator(8, 'input', 10), kind: 'unsubscribe', reason: 'cancel', causeSeq: 5 },
    ]);
    expect(explainEvent(trace, trace.events[5]!)).toContain('replacement');
  });

  it('does not call every cancellation a replacement or supplied state private operator memory', () => {
    const trace = finishedRecording();
    const cancellation: TraceEvent = { ...inner(16, 'innerB', 1, 25), kind: 'unsubscribe', reason: 'cancel' };
    expect(explainEvent(trace, cancellation)).not.toContain('replacement');
    const state: TraceEvent = { ...operator(16, 'output', 25), kind: 'state', label: 'domain total', state: { status: 'captured', data: 5 } };
    expect(explainEvent(trace, state)).toContain('explicitly supplied domain state');
    expect(explainEvent(trace, state)).toContain('does not expose private');
  });

  it('rejects invalid cursors for both grouped execution and semantic views', () => {
    expect(() => getExecutions(finishedRecording(), Number.NaN)).toThrow(RangeError);
    expect(() => getSwitchMapState(finishedRecording(), 'op1', -1)).toThrow(RangeError);
  });
});
