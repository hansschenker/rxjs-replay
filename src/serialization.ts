import { freezeOwned } from './snapshot.js';
import type { Recording } from './types.js';

const MAX_TEXT = 32 * 1024 * 1024;
function fail(path: string, message: string): never { throw new TypeError(`Invalid recording at ${path}: ${message}`); }
function object(value: unknown, path: string): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) fail(path, 'expected an object');
  return value as Record<string, unknown>;
}
function keys(value: Record<string, unknown>, allowed: readonly string[], path: string): void {
  for (const key of Object.keys(value)) if (!allowed.includes(key)) fail(path, `unknown field ${key}`);
}
function text(value: unknown, path: string, max = 256): string {
  if (typeof value !== 'string' || !value.trim() || value.length > max) fail(path, `expected a nonempty string of at most ${max} characters`);
  return value;
}
function integer(value: unknown, path: string, min: number, max = Number.MAX_SAFE_INTEGER): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < min || value > max) fail(path, `expected an integer between ${min} and ${max}`);
  return value;
}
function member(value: unknown, allowed: readonly unknown[], path: string): void {
  if (!allowed.includes(value)) fail(path, 'unsupported value');
}

/** Validate shape, ordering, causal references, and the observed subscription protocol. */
export function parseRecording(json: string): Recording {
  if (typeof json !== 'string' || json.length > MAX_TEXT) throw new RangeError('Recording JSON must be a string of at most 32 Mi characters.');
  let parsed: unknown;
  try { parsed = JSON.parse(json) as unknown; } catch { throw new TypeError('Recording is not valid JSON.'); }
  const root = object(parsed, '$');
  keys(root, ['format', 'schemaVersion', 'rxjsVersion', 'name', 'clock', 'streams', 'events', 'capture'], '$');
  member(root.format, ['rxjs-replay'], 'format');
  member(root.schemaVersion, [1], 'schemaVersion');
  member(root.rxjsVersion, ['7.8.2'], 'rxjsVersion');
  text(root.name, 'name');
  const clock = object(root.clock, 'clock');
  keys(clock, ['name', 'unit'], 'clock');
  text(clock.name, 'clock.name');
  member(clock.unit, ['ms'], 'clock.unit');
  if (!Array.isArray(root.streams) || root.streams.length > 1_000) fail('streams', 'expected at most 1000 stream definitions');
  const streams = new Set<string>();
  for (const [index, input] of root.streams.entries()) {
    const path = `streams[${index}]`;
    const stream = object(input, path);
    keys(stream, ['id', 'label', 'lane', 'inputs', 'sharing'], path);
    const id = text(stream.id, `${path}.id`);
    if (streams.has(id)) fail(path, 'duplicate stream id');
    streams.add(id);
    text(stream.label, `${path}.label`);
    member(stream.lane, ['source', 'internal', 'result'], `${path}.lane`);
    if (stream.sharing !== undefined) member(stream.sharing, ['independent', 'shared', 'live'], `${path}.sharing`);
    if (stream.inputs !== undefined) {
      if (!Array.isArray(stream.inputs) || stream.inputs.length > 1_000 || new Set(stream.inputs).size !== stream.inputs.length) fail(path, 'inputs must be unique stream ids');
      stream.inputs.forEach((id: unknown) => text(id, `${path}.inputs`));
    }
  }
  for (const input of root.streams) {
    const stream = input as Record<string, unknown>;
    for (const id of (stream.inputs ?? []) as string[]) if (!streams.has(id)) fail('streams.inputs', 'unknown stream id');
  }
  const capture = object(root.capture, 'capture');
  keys(capture, ['maxEvents', 'totalEvents', 'droppedEvents', 'clockErrors', 'stopped'], 'capture');
  const maxEvents = integer(capture.maxEvents, 'capture.maxEvents', 1, 250_000);
  const totalEvents = integer(capture.totalEvents, 'capture.totalEvents', 0);
  const droppedEvents = integer(capture.droppedEvents, 'capture.droppedEvents', 0);
  integer(capture.clockErrors, 'capture.clockErrors', 0);
  if (typeof capture.stopped !== 'boolean') fail('capture.stopped', 'expected boolean');
  if (!Array.isArray(root.events) || root.events.length > maxEvents) fail('events', 'event count exceeds capacity');
  if (totalEvents !== root.events.length + droppedEvents || root.events.length !== Math.min(totalEvents, maxEvents)) fail('capture', 'retained-prefix counts disagree');

  let nodes = 0;
  function jsonValue(value: unknown, path: string, depth: number): void {
    if (++nodes > 2_000_000 || depth > 16) fail(path, 'snapshot size or depth limit exceeded');
    if (value === null || typeof value === 'boolean') return;
    if (typeof value === 'number') { if (!Number.isFinite(value)) fail(path, 'nonfinite number'); return; }
    if (typeof value === 'string') { if (value.length > 65_536) fail(path, 'snapshot string limit exceeded'); return; }
    if (Array.isArray(value)) { value.forEach(item => jsonValue(item, path, depth + 1)); return; }
    for (const [key, item] of Object.entries(object(value, path))) {
      if (key.length > 8_192) fail(path, 'snapshot key limit exceeded');
      jsonValue(item, path, depth + 1);
    }
  }
  function snapshot(value: unknown, path: string): void {
    const captured = object(value, path);
    member(captured.status, ['captured', 'truncated', 'unavailable'], `${path}.status`);
    if (captured.status === 'unavailable') {
      keys(captured, ['status', 'reason'], path);
      text(captured.reason, `${path}.reason`, 4_096);
    } else {
      keys(captured, ['status', 'data'], path);
      if (!Object.hasOwn(captured, 'data')) fail(path, 'missing captured data');
      jsonValue(captured.data, `${path}.data`, 0);
    }
  }
  type Life = { readonly streamId: string; status: 'active' | 'complete' | 'error' | 'disposed' };
  const subscriptions = new Map<string, Life>();
  let lastTime = 0;
  const common = ['seq', 'at', 'streamId', 'subscriptionId', 'causeSeq', 'kind'];
  for (const [index, input] of root.events.entries()) {
    const path = `events[${index}]`;
    const event = object(input, path);
    const seq = integer(event.seq, `${path}.seq`, 1);
    if (seq !== index + 1) fail(path, 'sequence must be a contiguous retained prefix starting at 1');
    if (typeof event.at !== 'number' || !Number.isFinite(event.at) || event.at < lastTime) fail(path, 'time must be finite, nonnegative, and monotonic');
    lastTime = event.at;
    const streamId = text(event.streamId, `${path}.streamId`);
    if (!streams.has(streamId)) fail(path, 'unknown stream');
    const subscriptionId = text(event.subscriptionId, `${path}.subscriptionId`);
    if (event.causeSeq !== undefined) integer(event.causeSeq, `${path}.causeSeq`, 1, seq - 1);
    const life = subscriptions.get(subscriptionId);
    if (event.kind === 'subscribe') {
      keys(event, [...common, 'parentSubscriptionId'], path);
      if (life) fail(path, 'subscription id reused');
      if (event.parentSubscriptionId !== undefined && !subscriptions.has(text(event.parentSubscriptionId, `${path}.parentSubscriptionId`))) fail(path, 'unknown parent subscription');
      subscriptions.set(subscriptionId, { streamId, status: 'active' });
      continue;
    }
    if (!life || life.streamId !== streamId) fail(path, 'event has no matching subscription');
    if (event.kind === 'unsubscribe') {
      keys(event, [...common, 'reason'], path);
      if (life.status === 'disposed') fail(path, 'duplicate disposal');
      const expected = life.status === 'active' ? 'cancel' : life.status;
      if (event.reason !== expected) fail(path, 'disposal reason disagrees with notifications');
      life.status = 'disposed';
      continue;
    }
    if (life.status !== 'active') fail(path, 'notification after termination or disposal');
    switch (event.kind) {
      case 'next': keys(event, [...common, 'value'], path); snapshot(event.value, `${path}.value`); break;
      case 'state': keys(event, [...common, 'state', 'label'], path); text(event.label, `${path}.label`); snapshot(event.state, `${path}.state`); break;
      case 'error': keys(event, [...common, 'error'], path); snapshot(event.error, `${path}.error`); life.status = 'error'; break;
      case 'complete': keys(event, common, path); life.status = 'complete'; break;
      default: fail(path, 'unknown event kind');
    }
  }
  return freezeOwned(parsed as Recording);
}

/** Export only recordings that can be opened again by this schema version. */
export function serializeRecording(recording: Recording, space?: number): string {
  if (space !== undefined && (!Number.isInteger(space) || space < 0 || space > 10)) throw new RangeError('space must be an integer from 0 to 10.');
  const json = JSON.stringify(recording, null, space);
  parseRecording(json);
  return json;
}
