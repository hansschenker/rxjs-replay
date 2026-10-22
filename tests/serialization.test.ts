import { describe, expect, it } from 'vitest';
import { of } from 'rxjs';
import { createRecorder } from '../src/recorder.js';
import { parseRecording, serializeRecording } from '../src/serialization.js';

function sample() {
  const recorder = createRecorder({ streams: [{ id: 'source', label: 'source', lane: 'source' }], clock: { now: () => 0 } });
  of({ safe: '<script>payload</script>' }).pipe(recorder.trace('source')).subscribe();
  return recorder.getRecording();
}
describe('portable recordings', () => {
  it('round-trips snapshots into detached deeply frozen objects', () => {
    const input = sample();
    const output = parseRecording(serializeRecording(input, 2));
    expect(output).toEqual(input); expect(output).not.toBe(input);
    expect(Object.isFrozen(output.events[1])).toBe(true);
  });
  it.each([
    ['unsupported schema', (value: any) => { value.schemaVersion = 999; }],
    ['unordered sequences', (value: any) => { value.events[1].seq = 1; }],
    ['backward time', (value: any) => { value.events[1].at = -1; }],
    ['dangling cause', (value: any) => { value.events[1].causeSeq = 500; }],
    ['unknown stream', (value: any) => { value.events[1].streamId = 'unknown'; }],
    ['notification after completion', (value: any) => { [value.events[1], value.events[2]] = [value.events[2], value.events[1]]; value.events.forEach((event: any, index: number) => event.seq = index + 1); }],
    ['incorrect cancellation reason', (value: any) => { value.events.at(-1).reason = 'cancel'; }],
    ['dishonest omission count', (value: any) => { value.capture.droppedEvents = 9; }],
    ['missing snapshot payload', (value: any) => { delete value.events[1].value.data; }],
  ])('rejects %s', (_, mutate) => {
    const value: unknown = JSON.parse(serializeRecording(sample()));
    mutate(value);
    expect(() => parseRecording(JSON.stringify(value))).toThrow();
  });
  it('rejects invalid JSON and input over its bounded size', () => {
    expect(() => parseRecording('{')).toThrow('not valid JSON');
    expect(() => parseRecording(' '.repeat(32 * 1024 * 1024 + 1))).toThrow('32 Mi');
  });
});
