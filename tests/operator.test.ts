import { describe, expect, it } from 'vitest';
import { EMPTY, Observable, Subject, Subscriber, UnsubscriptionError, defer, observable as observableSymbol, of, share, switchMap, take, throwError } from 'rxjs';
import type { ObservableInput } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import { replaySubscription } from '../src/playback.js';
import { createRecorder } from '../src/recorder.js';
import { parseRecording, serializeRecording } from '../src/serialization.js';
import type { Recorder, RecorderOptions } from '../src/types.js';

const streams = [
  { id: 'source', label: 'Outer input', lane: 'source' as const },
  { id: 'latest', label: 'Latest result', lane: 'internal' as const, operator: 'switchMap', domain: 'loadResult$', inputs: ['source'] },
  { id: 'result', label: 'Result', lane: 'result' as const, inputs: ['latest'] },
];
function makeRecorder(options: Partial<RecorderOptions> = {}): Recorder {
  return createRecorder({ streams, clock: { name: 'same-frame', now: () => 0 }, ...options });
}
function apply<T, R>(source: Observable<T>, recorder: Recorder, instrument: boolean, project: (value: T, index: number) => ObservableInput<R>): Observable<R> {
  return source.pipe(instrument ? recorder.switchMap('latest', project) : switchMap(project));
}
function portable(recorder: Recorder) {
  const recording = recorder.getRecording();
  expect(parseRecording(serializeRecording(recording))).toEqual(recording);
  return recording;
}

describe('switchMap adapter connection and channel identity', () => {
  it('stays lazy and creates independent channel groups and projector indices for each connection', () => {
    const recorder = makeRecorder();
    let runs = 0;
    const indices: number[] = [];
    const project = (value: number, index: number) => { indices.push(index); return of(value * 2); };
    const result$ = defer(() => { runs++; return of(1, 2); }).pipe(recorder.switchMap('latest', project));
    expect(runs).toBe(0);
    expect(recorder.getRecording().events).toEqual([]);
    const values: number[] = [];
    result$.subscribe(value => values.push(value));
    result$.subscribe(value => values.push(value));
    expect(runs).toBe(2);
    expect(values).toEqual([2, 4, 2, 4]);
    expect(indices).toEqual([0, 1, 0, 1]);
    const recording = portable(recorder);
    expect(recording.schemaVersion).toBe(2);
    const starts = recording.events.filter(event => event.kind === 'subscribe');
    const outputs = starts.filter(event => event.role === 'operator' && event.port === 'output');
    expect(outputs).toHaveLength(2);
    expect(new Set(outputs.map(event => event.executionId)).size).toBe(2);
    for (const output of outputs) {
      const input = starts.filter(event => event.role === 'operator' && event.port === 'input' && event.executionId === output.executionId);
      expect(input).toHaveLength(1);
      expect(input[0]!.subscriptionId).not.toBe(output.subscriptionId);
      const inners = starts.filter(event => event.role === 'inner' && event.ownerExecutionId === output.executionId);
      expect(inners.map(event => event.innerIndex)).toEqual([0, 1]);
      expect(new Set(inners.map(event => event.executionId)).size).toBe(2);
      expect(inners.every(event => event.executionId !== output.executionId)).toBe(true);
    }
    expect(recording.events.every(event => typeof event.executionId === 'string' && typeof event.port === 'string' && typeof event.role === 'string')).toBe(true);
  });

  it('preserves synchronous inner completion and ObservableInput arrays', () => {
    const run = (instrument: boolean) => {
      const recorder = makeRecorder();
      const values: number[] = [];
      let completions = 0;
      apply(of(1, 2), recorder, instrument, value => [value, value + 10])
        .subscribe({ next: value => values.push(value), complete: () => completions++ });
      return { values, completions, recorder };
    };
    const baseline = run(false);
    const observed = run(true);
    expect(observed.values).toEqual(baseline.values);
    expect(observed.values).toEqual([1, 11, 2, 12]);
    expect(observed.completions).toBe(baseline.completions);
    const innerEnds = portable(observed.recorder).events.filter(event => event.role === 'inner' && event.kind === 'unsubscribe');
    expect(innerEnds).toHaveLength(2);
    expect(innerEnds.every(event => event.kind === 'unsubscribe' && event.reason === 'complete')).toBe(true);
  });

  it('completes empty input without projecting or inventing an inner execution', () => {
    const recorder = makeRecorder();
    let projections = 0;
    let completed = false;
    EMPTY.pipe(recorder.switchMap('latest', () => { projections++; return EMPTY; }))
      .subscribe({ complete: () => { completed = true; } });
    expect(projections).toBe(0);
    expect(completed).toBe(true);
    expect(portable(recorder).events.some(event => event.role === 'inner')).toBe(false);
  });

  it('accepts Promise inputs and preserves latest-only delivery after synchronous outer completion', async () => {
    const run = async (instrument: boolean) => {
      const recorder = makeRecorder();
      const values: number[] = [];
      const projections: number[] = [];
      await new Promise<void>((resolve, reject) => {
        apply(of(1, 2), recorder, instrument, (value, index) => { projections.push(index); return Promise.resolve(value); })
          .subscribe({ next: value => values.push(value), error: reject, complete: resolve });
      });
      return { values, projections, recorder };
    };
    const baseline = await run(false);
    const observed = await run(true);
    expect(observed.values).toEqual(baseline.values);
    expect(observed.values).toEqual([2]);
    expect(observed.projections).toEqual(baseline.projections);
    portable(observed.recorder);
  });

  it('requires an explicitly declared switchMap policy', () => {
    const recorder = makeRecorder();
    expect(() => recorder.switchMap('source', (value: number) => of(value))).toThrow();
    expect(() => recorder.switchMap('missing', (value: number) => of(value))).toThrow();
    expect(recorder.getRecording().events).toEqual([]);
  });

  it('preserves source setup and teardown for an already-closed outer destination without inventing a trace', () => {
    const run = (instrument: boolean) => {
      const recorder = makeRecorder();
      const steps: string[] = [];
      const source = new Observable<number>(subscriber => {
        steps.push(`source:closed=${subscriber.closed}`);
        subscriber.next(1);
        return () => steps.push('source teardown');
      });
      const destination = new Subscriber<number>();
      destination.unsubscribe();
      apply(source, recorder, instrument, value => { steps.push('project'); return of(value); }).subscribe(destination);
      return { steps, recorder };
    };
    const baseline = run(false);
    const observed = run(true);
    expect(observed.steps).toEqual(baseline.steps);
    expect(observed.steps).toEqual(['source:closed=true', 'source teardown']);
    expect(portable(observed.recorder).events).toEqual([]);
  });
});

describe('switchMap adapter timing and teardown parity', () => {
  it('separates completed input from continuing output and replays the complete output channel', () => {
    const run = (instrument: boolean) => {
      const scheduler = new TestScheduler((actual, expected) => expect(actual).toEqual(expected));
      const recorder = makeRecorder({ clock: { name: 'virtual', now: () => scheduler.now() } });
      const steps: [number, string][] = [];
      scheduler.run(({ cold }) => {
        const source = cold<string>('a-b|');
        const project = (value: string) => cold('----x|', { x: value });
        apply(source, recorder, instrument, project).subscribe({
          next: value => steps.push([scheduler.now(), value]),
          complete: () => steps.push([scheduler.now(), 'complete']),
        });
      });
      return { steps, recorder };
    };
    const baseline = run(false);
    const observed = run(true);
    expect(observed.steps).toEqual(baseline.steps);
    expect(observed.steps).toEqual([[6, 'b'], [7, 'complete']]);
    const saved = portable(observed.recorder);
    const output = saved.events.find(event => event.role === 'operator' && event.port === 'output' && event.kind === 'subscribe')!;
    const input = saved.events.find(event => event.role === 'operator' && event.port === 'input' && event.kind === 'subscribe')!;
    expect(output.subscriptionId).not.toBe(input.subscriptionId);
    expect(saved.events.find(event => event.subscriptionId === input.subscriptionId && event.kind === 'complete')?.at).toBe(3);
    expect(saved.events.find(event => event.subscriptionId === output.subscriptionId && event.kind === 'complete')?.at).toBe(7);
    expect(saved.events.filter(event => event.role === 'inner' && event.kind === 'unsubscribe')).toMatchObject([{ reason: 'cancel', at: 2 }, { reason: 'complete', at: 7 }]);
    const scheduler = new TestScheduler((actual, expected) => expect(actual).toEqual(expected));
    const replayed: [number, unknown][] = [];
    replaySubscription(saved, output.subscriptionId, { scheduler }).subscribe({
      next: value => replayed.push([scheduler.now(), value]),
      complete: () => replayed.push([scheduler.now(), 'complete']),
    });
    scheduler.flush();
    expect(replayed).toEqual(observed.steps);
    expect(saved.events.map(event => event.seq)).toEqual(saved.events.map((_, index) => index + 1));
  });

  it('propagates take(1) synchronously through the inner and outer without extra production', () => {
    const run = (instrument: boolean) => {
      const recorder = makeRecorder();
      const steps: string[] = [];
      const source = new Observable<number>(subscriber => {
        for (let value = 1; value <= 3 && !subscriber.closed; value++) { steps.push(`outer:${value}`); subscriber.next(value); }
        if (!subscriber.closed) subscriber.complete();
        return () => steps.push('outer teardown');
      });
      const project = (value: number) => new Observable<number>(subscriber => {
        for (let index = 0; index < 3 && !subscriber.closed; index++) { steps.push(`inner:${index}`); subscriber.next(value + index); }
        if (!subscriber.closed) subscriber.complete();
        return () => steps.push('inner teardown');
      });
      apply(source, recorder, instrument, project).pipe(take(1))
        .subscribe({ next: value => steps.push(`next:${value}`), complete: () => steps.push('complete') });
      return { steps, recorder };
    };
    const baseline = run(false);
    const observed = run(true);
    expect(observed.steps).toEqual(baseline.steps);
    expect(observed.steps.filter(step => step.startsWith('outer:'))).toEqual(['outer:1']);
    expect(observed.steps.filter(step => step.startsWith('inner:'))).toEqual(['inner:0']);
    const saved = portable(observed.recorder);
    expect(saved.events.some(event => event.kind === 'complete')).toBe(false);
    expect(saved.events.filter(event => event.kind === 'unsubscribe')).toHaveLength(3);
  });

  it('propagates explicit downstream disposal inside next before producing another value', () => {
    const run = (instrument: boolean) => {
      const recorder = makeRecorder();
      const values: number[] = [];
      let projected = 0;
      const sink = new Subscriber<number>({
        next: value => { values.push(value); sink.unsubscribe(); },
        error: () => {},
        complete: () => { throw new Error('Explicit disposal must not become completion'); },
      });
      apply(of(1, 2, 3), recorder, instrument, value => { projected++; return of(value, value + 10); }).subscribe(sink);
      return { values, projected, recorder };
    };
    const baseline = run(false);
    const observed = run(true);
    expect(observed.values).toEqual(baseline.values);
    expect(observed.projected).toBe(baseline.projected);
    expect(observed.values).toEqual([1]);
    portable(observed.recorder);
  });

  it('keeps input and active inner cleanup owned by explicit output disposal exactly once', () => {
    const recorder = makeRecorder();
    let outerStops = 0;
    let innerStops = 0;
    const source = new Observable<number>(subscriber => { subscriber.next(1); return () => outerStops++; });
    const result = source.pipe(recorder.switchMap('latest', () => new Observable<number>(() => () => innerStops++)));
    const subscription = result.subscribe();
    subscription.unsubscribe();
    subscription.unsubscribe();
    expect(outerStops).toBe(1);
    expect(innerStops).toBe(1);
    const endings = portable(recorder).events.filter(event => event.kind === 'unsubscribe');
    expect(endings).toHaveLength(3);
    expect(endings.every(event => event.kind === 'unsubscribe' && event.reason === 'cancel')).toBe(true);
  });
});

describe('switchMap adapter error parity', () => {
  it('cancels the old inner before calling a throwing projector and does not invent a new inner', () => {
    const failure = new Error('project failed');
    const run = (instrument: boolean) => {
      const recorder = makeRecorder();
      const source = new Subject<number>();
      const steps: string[] = [];
      let caught: unknown;
      apply(source, recorder, instrument, value => {
        steps.push(`project:${value}`);
        if (value === 2) throw failure;
        return new Observable<number>(() => { steps.push(`subscribe:${value}`); return () => steps.push(`teardown:${value}`); });
      }).subscribe({ error: error => { caught = error; steps.push('error'); } });
      source.next(1); source.next(2); source.next(3);
      return { steps, caught, recorder };
    };
    const baseline = run(false);
    const observed = run(true);
    expect(observed.steps).toEqual(baseline.steps);
    expect(observed.caught).toBe(failure);
    expect(observed.steps).toEqual(['project:1', 'subscribe:1', 'teardown:1', 'project:2', 'error']);
    expect(portable(observed.recorder).events.filter(event => event.role === 'inner' && event.kind === 'subscribe')).toHaveLength(1);
  });

  it('handles invalid ObservableInput conversion at the projector boundary', () => {
    const run = (instrument: boolean) => {
      const recorder = makeRecorder();
      let caught: unknown;
      let projected = 0;
      apply(of(1, 2), recorder, instrument, () => { projected++; return null as unknown as ObservableInput<number>; })
        .subscribe({ error: error => { caught = error; } });
      return { caught, projected, recorder };
    };
    const baseline = run(false);
    const observed = run(true);
    expect(observed.caught).toBeInstanceOf(TypeError);
    expect(String(observed.caught)).toBe(String(baseline.caught));
    expect(observed.projected).toBe(baseline.projected);
    expect(portable(observed.recorder).events.some(event => event.role === 'inner')).toBe(false);
  });

  it('preserves a throwing ObservableInput getter without claiming an inner connection', () => {
    const failure = new Error('then getter failed');
    const run = (instrument: boolean) => {
      const recorder = makeRecorder();
      let gets = 0;
      let caught: unknown;
      const input = Object.defineProperty({}, 'then', { get: () => { gets++; throw failure; } });
      apply(of(1), recorder, instrument, () => input as ObservableInput<number>)
        .subscribe({ error: error => { caught = error; } });
      return { gets, caught, recorder };
    };
    const baseline = run(false);
    const observed = run(true);
    expect(observed.gets).toBe(baseline.gets);
    expect(observed.caught).toBe(failure);
    expect(portable(observed.recorder).events.some(event => event.role === 'inner')).toBe(false);
  });

  it('preserves inner error identity and immediately stops the synchronous outer', () => {
    const failure = new Error('inner failed');
    const run = (instrument: boolean) => {
      const recorder = makeRecorder();
      const projected: number[] = [];
      let caught: unknown;
      apply(of(1, 2, 3), recorder, instrument, value => { projected.push(value); return throwError(() => failure); })
        .subscribe({ error: error => { caught = error; } });
      return { projected, caught, recorder };
    };
    const baseline = run(false);
    const observed = run(true);
    expect(observed.projected).toEqual(baseline.projected);
    expect(observed.projected).toEqual([1]);
    expect(observed.caught).toBe(failure);
    const errors = portable(observed.recorder).events.filter(event => event.kind === 'error');
    expect(errors.map(event => [event.role, event.port])).toEqual([['inner', 'output'], ['operator', 'output']]);
  });

  it('preserves outer error identity while disposing the active inner', () => {
    const failure = new Error('outer failed');
    const run = (instrument: boolean) => {
      const recorder = makeRecorder();
      const source = new Subject<number>();
      let stopped = 0;
      let caught: unknown;
      apply(source, recorder, instrument, () => new Observable<number>(() => () => stopped++))
        .subscribe({ error: error => { caught = error; } });
      source.next(1); source.error(failure);
      return { caught, stopped, recorder };
    };
    const baseline = run(false);
    const observed = run(true);
    expect(observed.caught).toBe(failure);
    expect(observed.stopped).toBe(baseline.stopped);
    expect(observed.stopped).toBe(1);
    expect(portable(observed.recorder).events.filter(event => event.kind === 'error').map(event => event.port)).toEqual(['input', 'output']);
  });

  it('preserves a synchronous replacement-teardown failure before a new projector is called', () => {
    const failure = new Error('inner teardown failed');
    const run = (instrument: boolean) => {
      const recorder = makeRecorder();
      const source = new Subject<number>();
      const steps: string[] = [];
      let caught: unknown;
      apply(source, recorder, instrument, value => {
        steps.push(`project:${value}`);
        return new Observable<number>(() => () => { steps.push(`teardown:${value}`); throw failure; });
      }).subscribe({ error: error => { caught = error; steps.push('error'); } });
      source.next(1); source.next(2);
      return { steps, caught, recorder };
    };
    const baseline = run(false);
    const observed = run(true);
    expect(observed.steps).toEqual(baseline.steps);
    expect(observed.steps).toEqual(['project:1', 'teardown:1', 'error']);
    expect(observed.caught).toBeInstanceOf(UnsubscriptionError);
    expect((observed.caught as UnsubscriptionError).errors).toEqual([failure]);
    expect((observed.caught as UnsubscriptionError).errors[0]).toBe(failure);
    portable(observed.recorder);
  });
});

describe('switchMap adapter reentrancy parity', () => {
  it('preserves reentrant projector ordering and records overlapping inner ownership honestly', () => {
    const run = (instrument: boolean) => {
      const recorder = makeRecorder();
      const source = new Subject<number>();
      const inners = new Map<number, Subscriber<string>>();
      const steps: string[] = [];
      apply(source, recorder, instrument, value => {
        steps.push(`project:${value}`);
        if (value === 1) source.next(2);
        return new Observable<string>(subscriber => {
          steps.push(`subscribe:${value}`); inners.set(value, subscriber);
          return () => steps.push(`teardown:${value}`);
        });
      }).subscribe({ next: value => steps.push(`next:${value}`), complete: () => steps.push('complete') });
      source.next(1);
      inners.get(1)!.next('A'); inners.get(2)!.next('B'); source.complete();
      inners.get(1)!.complete(); inners.get(2)!.complete();
      return { steps, recorder };
    };
    const baseline = run(false);
    const observed = run(true);
    expect(observed.steps).toEqual(baseline.steps);
    expect(observed.steps.filter(step => step.startsWith('next:'))).toEqual(['next:A', 'next:B']);
    const saved = portable(observed.recorder);
    const starts = saved.events.filter(event => event.role === 'inner' && event.kind === 'subscribe');
    const inputs = saved.events.filter(event => event.role === 'operator' && event.port === 'input' && event.kind === 'next');
    expect(starts.map(event => event.innerIndex)).toEqual([1, 0]);
    expect(starts.map(event => event.causeSeq)).toEqual([inputs[1]!.seq, inputs[0]!.seq]);
    expect(new Set(starts.map(event => event.ownerExecutionId)).size).toBe(1);
  });

  it('preserves a reentrant outer value emitted during replacement teardown', () => {
    const run = (instrument: boolean) => {
      const recorder = makeRecorder();
      const source = new Subject<number>();
      const inners = new Map<number, Subscriber<number>>();
      const steps: string[] = [];
      let reentered = false;
      const subscription = apply(source, recorder, instrument, value => {
        steps.push(`project:${value}`);
        return new Observable<number>(subscriber => {
          steps.push(`subscribe:${value}`); inners.set(value, subscriber);
          return () => {
            steps.push(`teardown:${value}`);
            if (value === 1 && !reentered) { reentered = true; source.next(3); }
          };
        });
      }).subscribe(value => steps.push(`next:${value}`));
      source.next(1); source.next(2); inners.get(3)!.next(3); inners.get(2)!.next(2); subscription.unsubscribe();
      return { steps, recorder };
    };
    const baseline = run(false);
    const observed = run(true);
    expect(observed.steps).toEqual(baseline.steps);
    expect(observed.steps.filter(step => step.startsWith('subscribe:'))).toEqual(['subscribe:1', 'subscribe:3', 'subscribe:2']);
    expect(observed.steps.filter(step => step.startsWith('next:'))).toEqual(['next:3', 'next:2']);
    portable(observed.recorder);
  });

  it('preserves downstream reentrant next ordering', () => {
    const run = (instrument: boolean) => {
      const recorder = makeRecorder();
      const source = new Subject<number>();
      const values: number[] = [];
      let completed = 0;
      apply(source, recorder, instrument, value => of(value)).subscribe({
        next: value => { values.push(value); if (value === 1) source.next(2); },
        complete: () => completed++,
      });
      source.next(1); source.complete();
      return { values, completed, recorder };
    };
    const baseline = run(false);
    const observed = run(true);
    expect(observed.values).toEqual(baseline.values);
    expect(observed.completed).toBe(baseline.completed);
    portable(observed.recorder);
  });

  it('performs ObservableInput conversion at the original projection point when its getter reenters the outer', () => {
    const run = (instrument: boolean) => {
      const recorder = makeRecorder();
      const source = new Subject<number>();
      const inners = new Map<number, Subscriber<number>>();
      const steps: string[] = [];
      let reentered = false;
      const inner = (value: number) => new Observable<number>(subscriber => {
        steps.push(`subscribe:${value}`); inners.set(value, subscriber);
        return () => steps.push(`teardown:${value}`);
      });
      const project = (value: number): ObservableInput<number> => {
        steps.push(`project:${value}`);
        if (value === 2) return inner(2);
        return Object.defineProperty({}, observableSymbol, {
          get: () => {
            steps.push('convert:1');
            if (!reentered) { reentered = true; source.next(2); }
            return () => inner(1);
          },
        }) as ObservableInput<number>;
      };
      const subscription = apply(source, recorder, instrument, project).subscribe(value => steps.push(`next:${value}`));
      source.next(1); inners.get(1)!.next(1); inners.get(2)!.next(2); subscription.unsubscribe();
      return { steps, recorder };
    };
    const baseline = run(false);
    const observed = run(true);
    expect(observed.steps).toEqual(baseline.steps);
    expect(observed.steps.filter(step => step.startsWith('subscribe:'))).toEqual(['subscribe:2', 'subscribe:1']);
    expect(portable(observed.recorder).events.filter(event => event.role === 'inner' && event.kind === 'subscribe').map(event => event.innerIndex)).toEqual([1, 0]);
  });

  it.each(['complete', 'error'] as const)('preserves subscribe-side effects with a closed inner destination after projector-triggered %s', terminal => {
    const failure = new Error('reentrant outer failure');
    const run = (instrument: boolean) => {
      const recorder = makeRecorder();
      const source = new Subject<number>();
      const steps: string[] = [];
      let caught: unknown;
      apply(source, recorder, instrument, value => {
        steps.push(`project:${value}`);
        if (terminal === 'complete') source.complete(); else source.error(failure);
        return new Observable<number>(subscriber => {
          steps.push(`subscribe:${value}:closed=${subscriber.closed}`);
          return () => steps.push(`teardown:${value}`);
        });
      }).subscribe({
        complete: () => steps.push('complete'),
        error: error => { caught = error; steps.push('error'); },
      });
      source.next(1);
      return { steps, caught, recorder };
    };
    const baseline = run(false);
    const observed = run(true);
    expect(observed.steps).toEqual(baseline.steps);
    expect(observed.steps).toContain('subscribe:1:closed=true');
    expect(observed.steps.at(-1)).toBe('teardown:1');
    if (terminal === 'error') expect(observed.caught).toBe(failure);
    expect(portable(observed.recorder).events.some(event => event.role === 'inner')).toBe(false);
  });
});

describe('switchMap adapter sharing and capture ownership', () => {
  it.each(['after-operator', 'before-operator'] as const)('preserves explicit sharing %s without extra source or inner subscriptions', position => {
    const run = (instrument: boolean) => {
      const recorder = makeRecorder();
      const source = new Subject<number>();
      let starts = 0;
      let stops = 0;
      let projects = 0;
      let innerStops = 0;
      const source$ = new Observable<number>(subscriber => {
        starts++;
        const connection = source.subscribe(subscriber);
        return () => { stops++; connection.unsubscribe(); };
      });
      const project = (value: number) => { projects++; return new Observable<number>(subscriber => { subscriber.next(value); return () => innerStops++; }); };
      const result$ = position === 'after-operator'
        ? apply(source$, recorder, instrument, project).pipe(share())
        : apply(source$.pipe(share()), recorder, instrument, project);
      const leftValues: number[] = [];
      const rightValues: number[] = [];
      const left = result$.subscribe(value => leftValues.push(value));
      const right = result$.subscribe(value => rightValues.push(value));
      source.next(1); left.unsubscribe();
      const stopsAfterLeft = stops;
      source.next(2); right.unsubscribe();
      return { observations: { starts, stops, projects, innerStops, stopsAfterLeft, leftValues, rightValues }, recorder };
    };
    const baseline = run(false);
    const observed = run(true);
    expect(observed.observations).toEqual(baseline.observations);
    expect(observed.observations.starts).toBe(1);
    expect(observed.observations.stopsAfterLeft).toBe(0);
    expect(observed.observations.stops).toBe(1);
    const outputs = portable(observed.recorder).events.filter(event => event.role === 'operator' && event.port === 'output' && event.kind === 'subscribe');
    expect(outputs).toHaveLength(position === 'after-operator' ? 1 : 2);
  });

  it('continues switching after bounded capture and after stop without retaining later events', () => {
    const recorder = makeRecorder({ maxEvents: 2 });
    const source = new Subject<number>();
    const values: number[] = [];
    let projects = 0;
    const subscription = source.pipe(recorder.switchMap('latest', value => { projects++; return of(value); })).subscribe(value => values.push(value));
    source.next(1);
    const stopped = recorder.stop();
    source.next(2); source.next(3); source.complete();
    expect(values).toEqual([1, 2, 3]);
    expect(projects).toBe(3);
    expect(subscription.closed).toBe(true);
    expect(stopped.events).toHaveLength(2);
    expect(stopped.capture.droppedEvents).toBeGreaterThan(0);
    expect(recorder.getRecording()).toEqual(stopped);
    portable(recorder);
  });
});
