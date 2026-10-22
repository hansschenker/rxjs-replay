import { writeFile } from 'node:fs/promises';
import { VirtualTimeScheduler, map, switchMap, take, timer } from 'rxjs';
import { createRecorder, evaluateLogpoint, inspectAt, replaySubscription, serializeRecording } from '../src/index.js';
import type { CapturedValue } from '../src/index.js';

const scheduler = new VirtualTimeScheduler();
const recorder = createRecorder({
  name: 'Search: only the latest request continues',
  clock: { name: 'virtual', now: () => scheduler.now() },
  streams: [
    { id: 'queries', label: 'queries$', lane: 'source', sharing: 'independent' },
    { id: 'requests', label: 'request$', lane: 'internal', inputs: ['queries'] },
    { id: 'results', label: 'results$', lane: 'result', inputs: ['requests'] },
  ],
});
const queries = ['r', 'rx', 'rxjs'];
function queryAt(index: number): string { return queries[index]!; }
function loadResults$(query: string) {
  return timer(9, scheduler).pipe(
    map(() => ({ query, results: [`${query}: result`] })),
    recorder.trace('requests'),
  );
}
function acceptResult(value: unknown): void { console.log('Live result:', value); }

// Blueprint first. The ordinary subscription below starts exactly one execution.
const results$ = timer(0, 7, scheduler).pipe(
  take(queries.length),
  map(queryAt),
  recorder.trace('queries'),
  switchMap(loadResults$),
  recorder.trace('results'),
);
results$.subscribe(acceptResult);
scheduler.flush();
const recording = recorder.stop();
await writeFile('search-recording.json', serializeRecording(recording, 2) + '\n');

const cancellations = evaluateLogpoint(recording, {
  kinds: ['unsubscribe'], predicate: event => event.kind === 'unsubscribe' && event.reason === 'cancel',
}, event => ({ subscription: event.subscriptionId, at: event.at, causedBy: event.causeSeq }));
console.log('Recorded cancellation logpoints:', cancellations.map(hit => hit.result));
console.log('Subscriptions:', inspectAt(recording).subscriptions.map(({ id, streamId, outcome }) => ({ id, streamId, outcome })));

// A new scheduler delivers captured source packages for a regression scenario.
// This does not call loadResults$ or repeat the original recording's effects.
const sourceSubscription = recording.events.find(event => event.kind === 'subscribe' && event.streamId === 'queries')!;
const replayClock = new VirtualTimeScheduler();
function decodeQuery(snapshot: CapturedValue): string {
  if (snapshot.status !== 'captured' || typeof snapshot.data !== 'string') throw new TypeError('Expected a captured query string.');
  return snapshot.data;
}
replaySubscription(recording, sourceSubscription.subscriptionId, {
  scheduler: replayClock, decodeValue: decodeQuery,
}).subscribe(value => console.log(`Replay at ${replayClock.now()} ms:`, value));
replayClock.flush();
console.log(`Saved ${recording.events.length} events to search-recording.json`);
