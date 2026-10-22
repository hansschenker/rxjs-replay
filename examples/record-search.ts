import { writeFile } from 'node:fs/promises';
import { VirtualTimeScheduler, map, take, timer } from 'rxjs';
import { createRecorder, evaluateLogpoint, getExecutions, getSwitchMapState, inspectAt, replaySubscription, serializeRecording } from '../src/index.js';
import type { CapturedValue } from '../src/index.js';

const scheduler = new VirtualTimeScheduler();
const recorder = createRecorder({
  name: 'Search: only the latest request continues',
  clock: { name: 'virtual', now: () => scheduler.now() },
  streams: [
    { id: 'queries', label: 'queries$', lane: 'source', sharing: 'independent' },
    { id: 'latest', label: 'Latest result', lane: 'internal', inputs: ['queries'], operator: 'switchMap', domain: 'loadResults$' },
    { id: 'results', label: 'results$', lane: 'result', inputs: ['latest'] },
  ],
});
const queries = ['r', 'rx', 'rxjs'];
function queryAt(index: number): string { return queries[index]!; }
function loadResults$(query: string) {
  return timer(9, scheduler).pipe(
    map(() => ({ query, results: [`${query}: result`] })),
  );
}
function acceptResult(value: unknown): void { console.log('Live result:', value); }

// Blueprint first. The ordinary subscription below starts exactly one execution.
const results$ = timer(0, 7, scheduler).pipe(
  take(queries.length),
  map(queryAt),
  recorder.trace('queries'),
  recorder.switchMap('latest', loadResults$),
  recorder.trace('results'),
);
results$.subscribe(acceptResult);
scheduler.flush();
const recording = recorder.stop();
await writeFile('search-recording.json', serializeRecording(recording, 2) + '\n');

const cancellations = evaluateLogpoint(recording, {
  role: 'inner', kinds: ['unsubscribe'], predicate: event => event.kind === 'unsubscribe' && event.reason === 'cancel',
}, event => ({ subscription: event.subscriptionId, at: event.at, causedBy: event.causeSeq }));
console.log('Recorded cancellation logpoints:', cancellations.map(hit => hit.result));
console.log('Subscriptions:', inspectAt(recording).subscriptions.map(({ id, streamId, outcome }) => ({ id, streamId, outcome })));

const operator = getExecutions(recording).find(execution => execution.role === 'operator')!;
const inputComplete = recording.events.find(event => event.executionId === operator.id && event.port === 'input' && event.kind === 'complete')!;
console.log('After outer completion:', getSwitchMapState(recording, operator.id, inputComplete.seq));
console.log('After output completion:', getSwitchMapState(recording, operator.id));

// Output has its own channel: outer completion must not truncate its replay.
const outputClock = new VirtualTimeScheduler();
replaySubscription(recording, operator.outputSubscriptionId!, { scheduler: outputClock }).subscribe({
  next: value => console.log(`Operator output replay at ${outputClock.now()} ms:`, value),
  complete: () => console.log(`Operator output replay completed at ${outputClock.now()} ms`),
});
outputClock.flush();

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
