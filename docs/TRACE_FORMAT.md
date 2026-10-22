# Trace format — schema version 1

A recording is immutable JSON-safe evidence from explicitly instrumented RxJS 7.8.2 stream boundaries. The schema distinguishes event order, time, notification delivery, subscription disposal, and captured domain state.

## Recording envelope

| Field | Meaning |
| --- | --- |
| `format` | Literal `"rxjs-replay"`. |
| `schemaVersion` | Literal `1`. |
| `rxjsVersion` | Literal `"7.8.2"`. |
| `name` | Human-readable recording name. |
| `clock.name` | Clock identifier such as `performance` or an application-supplied name. |
| `clock.unit` | Literal `"ms"`. |
| `streams` | Declared stream boundaries and optional graph/sharing metadata. |
| `events` | Retained chronological prefix, ordered by `seq`. |
| `capture` | Budget, observation/loss counts, clock diagnostics, and capture-stop flag. |

### Stream definitions

Every stream has an `id`, `label`, and `lane` (`source`, `internal`, or `result`). Optional `inputs` list declared upstream stream IDs. Optional `sharing` is `independent`, `shared`, or `live`.

These declarations describe the author's dataflow. A `shared` label does not insert `share()`, and edges do not prove that one particular event caused another.

### Capture metadata

| Field | Meaning |
| --- | --- |
| `maxEvents` | Maximum number of events retained. Default 50,000; supported maximum 250,000. |
| `totalEvents` | Events observed while capture was active, including omitted events. |
| `droppedEvents` | Suffix events omitted after the retention budget was exhausted. |
| `clockErrors` | Invalid or backward clock readings detected during capture. |
| `stopped` | Whether the recorder was explicitly stopped. |

`stopped: true` is a property of recording ownership. It does not imply completion or disposal of any application subscription.

## Common event fields

| Field | Meaning |
| --- | --- |
| `seq` | Strictly increasing positive session-wide sequence number. |
| `at` | Nonnegative milliseconds from the recorder's clock origin. |
| `streamId` | The declared boundary where the event was observed. |
| `subscriptionId` | The particular subscription at that boundary. |
| `causeSeq` | Optional observed synchronous delivery context. |

The recorder attempts to capture its origin when `createRecorder()` runs. If that initial clock read fails, the first valid reading establishes the origin. The default clock uses `performance.now()`. A supplied scheduler clock can produce deterministic virtual timestamps. Timestamp clamping preserves nondecreasing time; `seq` preserves actual observation order.

Two events with the same `at` remain distinct. Playback must preserve their `seq` order, including when all of a synchronous source's notifications occur in one virtual-time frame.

`causeSeq` is intentionally narrow: it records synchronous context observed by the instrumentation. It is not a complete causal graph across timers, network operations, schedulers, or threads.

## Event variants

| `kind` | Additional fields | Meaning |
| --- | --- | --- |
| `subscribe` | Optional `parentSubscriptionId` | An application subscription entered an instrumented boundary. |
| `next` | `value: CapturedValue` | A value package was observed. |
| `error` | `error: CapturedValue` | An error notification was observed. |
| `complete` | None | A completion notification was observed. |
| `unsubscribe` | `reason: "cancel" \| "complete" \| "error"` | Subscription disposal was observed. |
| `state` | `state: CapturedValue`, `label: string` | Explicitly supplied domain-state snapshot. |

Each traced boundary has its own subscription identifiers. A pipeline with several trace operators can therefore have several related subscription records for one consumer connection. The identifiers should not be collapsed merely because their notifications contain equal values.

`parentSubscriptionId` represents observed subscription nesting when available. Stream topology and subscription nesting answer different questions.

### Terminal notifications and disposal

A normal completed subscription has a completion notification and a disposal event. An errored subscription has an error notification and a disposal event. Explicit cancellation can have a disposal event without a completion notification.

Nested synchronous delivery can place observations at other boundaries between these events. Preserve the recorded global sequence rather than inventing adjacency.

A retained prefix may end before a terminal notification or disposal. Do not add one during import, viewing, or replay. In particular, absence of an ending cannot establish a resource leak.

## Captured values

Every captured payload has one of these representations:

```ts
type CapturedValue =
  | { readonly status: 'captured' | 'truncated'; readonly data: JsonValue }
  | { readonly status: 'unavailable'; readonly reason: string };
```

`captured` indicates an available detached representation. `truncated` indicates that snapshot limits affected that representation. `unavailable` reports that a representation could not be obtained. JSON-safe previews of values such as errors or other non-JSON objects are representations, not reconstructed originals.

Value truncation and recording truncation are separate:

- A single event can have `value.status === 'truncated'` while every event was retained.
- The recording can have `capture.droppedEvents > 0` while every retained value was fully captured.

The optional recorder `captureValue` projection runs before snapshotting. It is a suitable place to select the data needed for a test or remove fields from a recording. The function must be pure. An explicit `captureState` similarly supplies domain state; it cannot reveal an uninstrumented operator's hidden memory.

By default, notification replay returns captured JSON representations. Decoders are the application-controlled bridge back to domain values. Snapshotting cannot preserve original object identity, error prototypes, functions, subscriptions, sockets, or live resources.

## Minimal complete example

This example records one value and a normal ending at one boundary:

```json
{
  "format": "rxjs-replay",
  "schemaVersion": 1,
  "rxjsVersion": "7.8.2",
  "name": "One number",
  "clock": { "name": "virtual", "unit": "ms" },
  "streams": [
    { "id": "input", "label": "Input", "lane": "source", "sharing": "independent" }
  ],
  "events": [
    { "seq": 1, "at": 0, "streamId": "input", "subscriptionId": "sub-1", "kind": "subscribe" },
    { "seq": 2, "at": 10, "streamId": "input", "subscriptionId": "sub-1", "kind": "next", "value": { "status": "captured", "data": 7 } },
    { "seq": 3, "at": 20, "streamId": "input", "subscriptionId": "sub-1", "kind": "complete" },
    { "seq": 4, "at": 20, "streamId": "input", "subscriptionId": "sub-1", "kind": "unsubscribe", "reason": "complete" }
  ],
  "capture": {
    "maxEvents": 50000,
    "totalEvents": 4,
    "droppedEvents": 0,
    "clockErrors": 0,
    "stopped": true
  }
}
```

This is illustrative serialized data. Subscription IDs are identifiers, not an encoding of time or application identity.

## Prefix retention and unknown outcomes

The retention policy keeps the beginning of the observed sequence and counts omitted suffix events. It does not overwrite earlier records.

For example, a three-event budget could retain subscribe, next, and complete while omitting the later disposal. The recorded completion is evidence of completion; the file still does not contain evidence of disposal. A two-event budget in the same run would contain only subscribe and next, leaving the subsequent ending unknown.

Historical views interpret what has been observed through the chosen cursor. Their `hasOmittedEvents` flag exposes file-level loss. An `active` view can mean “before this subscription ended,” “capture stopped before an ending,” or “an ending fell in the omitted suffix.” It does not mean “the application is currently running this subscription.”

## Import and export

- `serializeRecording(recording, space?)` validates and returns JSON text.
- `parseRecording(text)` validates the versioned envelope, event structure, and protocol relationships, then returns a deeply frozen recording.
- Import rejects unsupported/malformed data; it does not execute captured content.
- A viewer renders imported payloads as text.

JSON input and export are limited to 32 Mi characters. Parsed snapshot data is also bounded by a 16-level depth limit and a total budget of 2,000,000 JSON nodes. The browser picker limits files to 20 MiB. Capture uses smaller per-value budgets: 8 levels, 64 entries per collection, 1,000 nodes, and 4,096-character string previews. Reaching a capture budget marks that snapshot `truncated`; exceeding an import/export bound reports an error.

The declared schema version is the compatibility boundary. Do not infer support for another format from a similar file extension or a superficially similar event shape.

## Replay interpretation

`replayEvents` delivers the retained trace as data and completes at its retained end. A trace `error` event does not error this inspection stream.

`replaySubscription` delivers one subscription's captured notifications on a supplied scheduler, relative to that subscription's recorded `subscribe.at`. It interprets explicit cancellation as connection closure without completion. If no ending was observed, it remains open after the retained notifications.

Neither operation recreates the original application. Re-execution of a pipeline, its dependencies, or its effects requires a separate explicit application/test setup.
