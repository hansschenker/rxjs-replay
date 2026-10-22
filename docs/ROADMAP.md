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

## Candidate next work

### Recording-driven regression workflows

Build helpers that turn selected input/output boundaries into reviewable test fixtures. Preserve equal-frame order and subscription lifetime assertions. Require explicit treatment of effects, unsupported payloads, and incomplete recordings.

Generated expectations should remain reviewable: a captured bug is evidence of what happened, not automatically a specification of what should happen.

### Integration with other RxJS tools

Investigate adapters for `rxjs-inspector` and related execution-viewer tooling. First compare their event and ownership semantics; do not assume that similar event names imply schema compatibility.

Adapters should preserve record identity, lifecycle distinctions, clock information, and loss metadata. Importing a recording must remain a data operation.

### Analysis reports

Explore subscription-overlap summaries, cancellation summaries, boundary throughput, and reusable query presets. Distinguish observed overlap from inferred causality and distinguish an unfinished trace from a leak diagnosis.

### Larger recordings

Consider indexed queries, chunked storage, and worker-based analysis while keeping sequence order and snapshot semantics explicit. Any alternative retention policy needs its own completeness contract; a rotating buffer cannot masquerade as a complete prefix.

### Payload codecs

Consider a versioned codec registry for domain values that need stronger reconstruction guarantees. The format should record codec identity/version and provide explicit errors for unavailable decoders. It must not deserialize arbitrary executable code.

## Scope boundaries

There is no plan in this version to implement browser VM recording, arbitrary historical local-variable evaluation, automatic HTTP interception, or global patching of RxJS internals. Those capabilities require a different observation boundary and substantially different guarantees.

Changes to the supported RxJS baseline or npm registry publication require an explicit project decision. Current installation is from GitHub.
