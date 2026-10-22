# Roadmap

The project targets RxJS **7.8.2**. The central contract is explicit boundary recording, preserved delivery order, visible lifecycle behavior, and useful replay without hidden application subscriptions.

## Initial implementation

- Lazy tracing at declared stream boundaries.
- Recorded `subscribe`, `next`, `error`, `complete`, disposal, and explicit state observations.
- Clock injection, elapsed time, and a sequence number independent of timestamp precision.
- Detached payload capture with visible unavailable/truncated states.
- Bounded prefix retention and explicit omitted-event counts.
- Versioned JSON import/export with validation.
- Historical selection, inspection, and retrospective logpoint projections.
- Shared cursor state with seek, step, play/pause, speed control, and disposal.
- Cold trace replay and per-subscription notification replay on a supplied scheduler.
- Optional four-lane browser viewer and executable examples.

This list describes the initial scope. The repository's executable checks establish behavior; a roadmap entry is not a guarantee about untested environments or arbitrary RxJS internals.

## Implemented in 0.2: operator-aware inspection

- Schema version 2 execution roles, input/output channels, and explicit inner ownership.
- Distinct subscription IDs for operator input and output, preserving independent notification replay.
- Instrumented `switchMap` delegated to RxJS 7.8.2, with ordinary projection/conversion timing and synchronous disposal.
- Pure `getExecutions`, `getSwitchMapState`, and `explainEvent` APIs with cursor-scoped evidence.
- Reconstructed/unavailable provenance; unsupported reentrant overlap stays visible as recorded facts.
- Declared dataflow graph with operator and domain-function labels, synchronized with the four lanes.
- Grouped execution lifetimes and linked stage, channel, inner, and causal navigation.
- Version 1 Replay recordings still validate, open, and replay unchanged; no invented operator metadata.
- A runnable instrumented search showing cancellation and output waiting after input completion.

## Candidate next work

### Recording-driven regression workflows

Build helpers that turn selected input/output boundaries into reviewable test fixtures. Preserve equal-frame order and subscription lifetime assertions. Require explicit treatment of effects, unsupported payloads, and incomplete recordings.

Generated expectations should remain reviewable: a captured bug is evidence of what happened, not automatically a specification of what should happen.

### Integration with other RxJS tools

The operator-aware features inspired by `rxjs-inspector` are integrated in Replay's own recording/analysis/viewer layers. Importing Inspector's JSON remains separate future work: its suffix retention and lifecycle semantics differ from Replay's. Do not assume that similar event names imply schema compatibility.

Adapters should preserve record identity, lifecycle distinctions, clock information, and loss metadata. Importing a recording must remain a data operation.

### More operator models

Build explicit adapters and supported explanation models for `mergeMap`, `concatMap`, and `exhaustMap`, then selected state/termination operators. Expose overlap, queues, or ignored inputs only when the corresponding adapter actually records the required evidence. Each model needs a clear unavailable state and behavior checks against RxJS 7.8.2. Declaring an operator name alone never enables its model.

### Analysis reports

Explore subscription-overlap summaries, cancellation summaries, boundary throughput, and reusable query presets. Distinguish observed overlap from inferred causality and distinguish an unfinished trace from a leak diagnosis.

### Larger recordings

Consider indexed queries, chunked storage, and worker-based analysis while keeping sequence order and snapshot semantics explicit. Any alternative retention policy needs its own completeness contract; a rotating buffer cannot masquerade as a complete prefix.

### Payload codecs

Consider a versioned codec registry for domain values that need stronger reconstruction guarantees. The format should record codec identity/version and provide explicit errors for unavailable decoders. It must not deserialize arbitrary executable code.

## Scope boundaries

There is no plan in this version to implement browser VM recording, arbitrary historical local-variable evaluation, automatic HTTP interception, or global patching of RxJS internals. Those capabilities require a different observation boundary and substantially different guarantees.

Changes to the supported RxJS baseline or npm registry publication require an explicit project decision. Current installation is from GitHub.
