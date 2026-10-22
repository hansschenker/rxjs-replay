import type { HistoryView, Recording, SubscriptionView, TraceEvent } from './types.js';

/** Sequence numbers, rather than timestamps, identify an exact point in delivery order. */
export function resolveCursor(recording: Recording, seq: number): number {
  if (!Number.isSafeInteger(seq) || seq < 0) {
    throw new RangeError('The history cursor must be a non-negative safe integer.');
  }
  const events = recording.events;
  let low = 0;
  let high = events.length;
  while (low < high) {
    const middle = low + Math.floor((high - low) / 2);
    if (events[middle]!.seq <= seq) low = middle + 1;
    else high = middle;
  }
  return low === 0 ? 0 : events[low - 1]!.seq;
}

type MutableView = { -readonly [Key in keyof SubscriptionView]: SubscriptionView[Key] };

/** Reconstruct only the observed prefix. This function never evaluates an application source. */
export function inspectAt(recording: Recording, seq = recording.events.at(-1)?.seq ?? 0): HistoryView {
  const cursor = resolveCursor(recording, seq);
  const subscriptions = new Map<string, MutableView>();
  let selected: TraceEvent | undefined;
  let observedEvents = 0;
  for (const event of recording.events) {
    if (event.seq > cursor) break;
    selected = event;
    observedEvents++;
    if (event.kind === 'subscribe') {
      subscriptions.set(event.subscriptionId, {
        id: event.subscriptionId,
        streamId: event.streamId,
        start: event,
        outcome: 'active',
        values: 0,
        ...(event.parentSubscriptionId === undefined ? {} : { parentSubscriptionId: event.parentSubscriptionId }),
      });
      continue;
    }
    const subscription = subscriptions.get(event.subscriptionId);
    // Valid recordings retain a prefix, so every observed execution has its subscribe event.
    if (!subscription) continue;
    switch (event.kind) {
      case 'next':
        subscription.values++;
        subscription.latestValue = event.value;
        break;
      case 'state':
        subscription.latestState = event.state;
        break;
      case 'complete':
      case 'error':
        subscription.outcome = event.kind;
        subscription.end = event;
        break;
      case 'unsubscribe':
        subscription.outcome = event.reason;
        subscription.end = event;
        break;
    }
  }
  return Object.freeze({
    cursor,
    ...(selected === undefined ? {} : { event: selected }),
    subscriptions: Object.freeze([...subscriptions.values()].map(value => Object.freeze(value))),
    observedEvents,
    hasOmittedEvents: recording.capture.droppedEvents > 0,
  });
}
