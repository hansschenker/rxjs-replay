# Inspiration from Replay

`rxjs-replay` is an original implementation inspired by public Replay source. The investigation focused on recording order, timeline navigation, retrospective queries, and the separation of analysis from rendering. It did not copy Replay's source code or attempt to reproduce its browser-recording infrastructure.

Replay's DevTools are available at [replayio/devtools](https://github.com/replayio/devtools). Its public protocol examples are available at [replayio/Protocol-Examples](https://github.com/replayio/Protocol-Examples). Mark Erikson explains related work in [Building Better React DevTools with Replay Time Travel](https://blog.isquaredsoftware.com/2023/10/presentations-react-devtools-replay/).

The links below are pinned to the source revisions inspected during development:

- DevTools: `105893be683cd5261756ce10d813b4a2c67432c5`.
- Protocol examples: `eea97786e4ed89dafa8237ceaf8bff0d72673a9c`.

## 1. Execution order has an identity separate from time

Replay's [`utils/time.ts`](https://github.com/replayio/devtools/blob/105893be683cd5261756ce10d813b4a2c67432c5/packages/replay-next/src/utils/time.ts) compares execution-point identifiers through `BigInt`. Timestamp formatting is a separate concern.

**Our interpretation:** a timestamp alone is insufficient to identify an observation. `rxjs-replay` therefore stores a monotonically increasing `seq` alongside elapsed `at` time. Equal timestamps retain delivery order, including synchronous notifications and virtual scheduler frames.

Our sequence numbers identify observed stream events. They do not claim the same meaning or precision as Replay VM execution points.

## 2. Navigation is a view over a recording

Replay's [`TimelineContext.tsx`](https://github.com/replayio/devtools/blob/105893be683cd5261756ce10d813b4a2c67432c5/packages/replay-next/src/contexts/TimelineContext.tsx) carries both time and execution-point position. Its [`points.ts`](https://github.com/replayio/devtools/blob/105893be683cd5261756ce10d813b4a2c67432c5/packages/replay-next/components/sources/utils/points.ts) includes exact/nearest, before, and after point lookup.

**Our interpretation:** maintain a replay cursor independently from the recorded application. Seeking and stepping change inspected history. Scheduler-driven playback moves that cursor over retained events. Navigation does not undo or rerun application effects.

## 3. New questions can be asked after capture

Replay's [`HitPointsCache.ts`](https://github.com/replayio/devtools/blob/105893be683cd5261756ce10d813b4a2c67432c5/packages/replay-next/src/suspense/HitPointsCache.ts) finds points for locations and ranges. When a condition is present, it uses the client evaluation API and orders returned results by execution point. The [`LogPointPanel.tsx`](https://github.com/replayio/devtools/blob/105893be683cd5261756ce10d813b4a2c67432c5/packages/replay-next/components/sources/log-point-panel/LogPointPanel.tsx) connects those searches to editable logpoint content and focus ranges.

**Our interpretation:** retrospective logpoints can be useful over recorded stream packages. `evaluateLogpoint` selects retained events and applies a user function to their captured data.

The scope is narrower than Replay's historical runtime evaluation. We cannot query an unrecorded local variable, evaluate arbitrary code in a past call frame, or recover a value omitted during capture. The implementation does not use string evaluation to simulate those capabilities.

## 4. Query infrastructure and UI can evolve independently

Replay's [`shared/client/types.ts`](https://github.com/replayio/devtools/blob/105893be683cd5261756ce10d813b4a2c67432c5/packages/shared/client/types.ts) defines the interface consumed by UI code: operations include finding points, creating pauses, inspecting data, evaluating expressions, and selecting stepping targets.

**Our interpretation:** keep typed recording, query, cursor, and playback operations in a headless core. Make a browser viewer one consumer of that core. Tests and scripts should be able to use a recording without mounting a UI.

`rxjs-replay` is local-first in this implementation and has no Replay backend dependency. The architectural separation does not imply that Replay's backend or VM functionality has been reproduced.

## 5. Analysis can produce new information attached to old positions

The protocol examples' [`reduxDevtools.ts`](https://github.com/replayio/Protocol-Examples/blob/eea97786e4ed89dafa8237ceaf8bff0d72673a9c/replay-routines/src/session/routines/reduxDevtools/reduxDevtools.ts) reads annotations, analyzes selected points, sorts the results, and produces derived annotations with point/time metadata. The [`coverage-extractor/main.ts`](https://github.com/replayio/Protocol-Examples/blob/eea97786e4ed89dafa8237ceaf8bff0d72673a9c/coverage-extractor/main.ts) creates a separate coverage report from recorded hit information.

**Our interpretation:** reconstruct subscription views, value counts, latest captured state, and query results from a recording, while keeping the original events intact. Preserve the source event identity when displaying derived information.

The current library provides inspection and logpoint results. More extensive reports are future work; see the [Roadmap](ROADMAP.md). A finite recording alone cannot establish every operator state or prove that a resource leaked.

## 6. Limits and missing information should be visible

Replay's [`HitPointsCache.ts`](https://github.com/replayio/devtools/blob/105893be683cd5261756ce10d813b4a2c67432c5/packages/replay-next/src/suspense/HitPointsCache.ts) distinguishes complete results, point limits, evaluation limits, and errors. Its logpoint UI can narrow an analysis focus range.

**Our interpretation:** bounded capture must report what it omitted. `rxjs-replay` retains the original event prefix and counts the omitted suffix. Missing endings remain unknown; the viewer and replay functions must not fabricate future behavior.

## Historical examples require care

The [`react-mount-detection/main.ts`](https://github.com/replayio/Protocol-Examples/blob/eea97786e4ed89dafa8237ceaf8bff0d72673a9c/react-mount-detection/main.ts) example explicitly notes that its older `Analysis` API was removed, identifying `Session.findPoints` and `Session.runEvaluation` as replacements. It is useful design history, not evidence that the old example runs unchanged today.

## What this library promises

`rxjs-replay` records explicitly instrumented Observable boundaries and replays their captured notifications. It supports historical inspection and scheduled playback of that trace. It does not record or restore the browser VM, capture uninstrumented execution, or reproduce external effects automatically.

The four-lane viewer adds an RxJS teaching perspective: Timeline, Source emissions, Internal behavior, and Resulting emissions. These lanes are this project's presentation model, not a claim about Replay's UI architecture.

Replay.io, its contributors, and Mark Erikson deserve credit for the public work that made this investigation possible. This project is independent and is not affiliated with or endorsed by Replay.io.
