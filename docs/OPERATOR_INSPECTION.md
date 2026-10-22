# Operator inspection

`rxjs-replay` 0.2 links recorded notifications to a supported explanation of operator behavior. The first adapter is `recorder.switchMap()`, which delegates to RxJS 7.8.2. Operator policy stays in RxJS; application meaning stays in named domain functions.

## What flows over time

Consider a live query input and a domain function `loadResult$`. Query A starts one inner execution. Query B arrives before A finishes, so the latest-inner policy unsubscribes A and starts B. The query input then completes while B is still running. B can still emit its result and complete; only then does the operator output complete.

| Position | Recorded observation | Supported interpretation |
| --- | --- | --- |
| First input | A arrives; inner A subscribes | A is the active inner. |
| New input | B arrives; A is disposed; B subscribes | The latest input replaces A. |
| Input completion | The outer input completes | The output remains open while B is active. |
| Final result | B emits | The operator forwards the inner value. |
| Final completion | B completes, then the output completes | No inner remains active after outer completion. |

The demo records this scenario with a `VirtualTimeScheduler`. The four lanes preserve time and delivery order. Graph selection identifies a declared stage; execution selection identifies one recorded connection to it. Selecting the outer completion event shows the waiting state, while selecting a later output event shows what actually followed.

## Three identities, three questions

| Identity | Question answered |
| --- | --- |
| `streamId` | Which declared stage or boundary was observed? |
| `executionId` | Which particular boundary, operator, or inner execution was observed? |
| `subscriptionId` | Which independently replayable notification channel was observed? |

An operator execution has one output subscription and, once observed, one input subscription. These share an execution ID but have different subscription IDs. An inner has its own execution and output subscription; its `ownerExecutionId` points to the operator that owns it. `innerIndex` preserves the index RxJS passed to the domain projector.

This distinction prevents an input completion from ending replay of the output. It also lets the lifetime view show both channels separately, with the owned inners underneath their operator.

Ordinary `parentSubscriptionId` describes observed synchronous subscription nesting. It is not an alternative spelling of operator ownership. `causeSeq` describes observed synchronous delivery context; declared graph edges and nearby timestamps are not causal evidence.

## Input Controller and Action Policy

For teaching, the supported `switchMap` model separates intake from consequences:

- The **Input Controller** distinguishes outer values, outer completion/error, inner notifications, and cancellation observations. Its remembered view includes whether the outer completed, which inner is active, and the output outcome.
- The **Action Policy** explains the latest-inner rule: a new outer value replaces the active inner; inner values pass to the output; outer completion waits for the active inner; an error terminates the output.

In a formal Mealy description, `T: S × Z → S` updates remembered state from an input observation, and `G: S × Z → A*` describes the resulting actions. Here `S` is the operator model state, `Z` the relevant inputs, and `A*` zero or more actions such as cancelling an inner, subscribing a new inner, forwarding a value, or completing the output.

This is an explanation of a supported recorded execution. The analysis functions never invoke the domain projector or execute those actions. The live adapter uses RxJS's actual implementation, including its observed reentrant behavior.

## Recorded facts and reconstructed explanations

`getExecutions(recording, seq?)` reports execution/channel identities, counts, outcome, and whether output disposal has been observed. A terminal notification and its disposal remain separate observations. An output can have completed while its disposal is not yet visible through the cursor.

`getSwitchMapState(recording, executionId, seq?)` returns:

- `provenance`: `reconstructed` or `unavailable`.
- `outerCompleted`, `activeInnerId`, and `outputStatus`.
- The observed input/output subscription IDs, when available.
- An explanation and relevant `evidenceSeqs` for navigation.

`explainEvent(recording, event)` describes an individual observation. Operator-policy interpretations are identified as such. Cancellation does not by itself prove that an HTTP transport stopped or an external effect was undone.

### A cursor is an observation boundary

Several observations can occur at the same timestamp. `seq` is the ordering authority, so stepping can reveal an intermediate position: an inner has completed but the output's completion has not yet been recorded, or output cancellation has been recorded before inner cleanup.

The analysis respects that order. It does not compress a synchronous chain into an atomic transition or look ahead to a future event to claim that it has already happened.

### When explanation is unavailable

The current model deliberately supports a single active inner. RxJS 7.8.2 reentrancy can produce overlapping inner executions; their real events and lifetimes remain visible, while the model reports unavailable. Missing input-start evidence, a boundary probe without operator instrumentation, unsupported operator policy, or legacy version 1 data also cannot supply a confident operator explanation.

A recording that ends without a terminal event does not establish how the execution ended. The model describes the observed cursor; it does not diagnose a resource leak or restore an unobserved result from a cancelled inner.

## Use the APIs

Declare the policy and named domain function, then instrument that stage. The domain function can return any `ObservableInput` accepted by RxJS 7.8.2. Observable sources remain lazy; externally created live sources retain their own lifecycle. The adapter introduces no sharing.

```ts
const recorder = createRecorder({
  streams: [
    { id: 'queries', label: 'Queries', lane: 'source' },
    {
      id: 'latest', label: 'Latest result', lane: 'internal',
      inputs: ['queries'], operator: 'switchMap', domain: 'loadResult$',
    },
  ],
});

const results$ = queries$.pipe(
  recorder.trace<string>('queries'),
  recorder.switchMap('latest', loadResult$),
);
```

After the application's ordinary subscription has produced a recording, inspection uses only saved data:

```ts
const recording = recorder.getRecording();
const operator = getExecutions(recording).find(item => item.role === 'operator');
if (operator) {
  const state = getSwitchMapState(recording, operator.id, selectedSequence);
  const innerEvents = selectEvents(recording, {
    ownerExecutionId: operator.id,
    role: 'inner',
  });
  console.log(state.provenance, state.explanation, innerEvents);

  if (operator.outputSubscriptionId) {
    const output$ = replaySubscription(recording, operator.outputSubscriptionId, {
      scheduler: replayScheduler,
    });
    // A new consumer can subscribe to saved output notifications.
    // Input completion belongs to a different channel and cannot end this stream.
    const replayConnection = output$.subscribe(showCapturedResult);
    // The consumer owns replayConnection and its cancellation.
  }
}
```

Default notification replay supplies captured JSON representations. Supply `decodeValue` and, when needed, `decodeError` to reconstruct domain types. A scheduled replay has its own subscription lifetime and does not reconnect to the original application.

See the complete [record-search example](../examples/record-search.ts), [trace format](TRACE_FORMAT.md), and [architecture](ARCHITECTURE.md).
