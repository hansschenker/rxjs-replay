import { describe, expect, expectTypeOf, it } from 'vitest';
import { of } from 'rxjs';
import type { Observable } from 'rxjs';
import { createRecorder } from '../src/recorder.js';

function makeRecorder() {
  return createRecorder({
    streams: [{ id: 'latest', label: 'Latest', lane: 'internal', operator: 'switchMap' }],
    clock: { now: () => 0 },
  });
}

describe('switchMap projection type inference', () => {
  it('infers the union of values from heterogeneous Observable branches', () => {
    const recorder = makeRecorder();
    const chooseResult = (value: number) => value > 0 ? of('text') : of(42);
    const result$ = of(1, 0).pipe(recorder.switchMap('latest', chooseResult));

    expectTypeOf(result$).toEqualTypeOf<Observable<string | number>>();

    const values: (string | number)[] = [];
    let completed = false;
    result$.subscribe({
      next: value => values.push(value),
      complete: () => { completed = true; },
    });
    expect(values).toEqual(['text', 42]);
    expect(completed).toBe(true);
  });

  it('infers values across an array and an Observable without widening to unknown', () => {
    const recorder = makeRecorder();
    const chooseInput = (value: number) => value > 0 ? [value, value + 1] : of('fallback');
    const result$ = of(1, 0).pipe(recorder.switchMap('latest', chooseInput));

    expectTypeOf(result$).toEqualTypeOf<Observable<number | string>>();

    const values: (number | string)[] = [];
    result$.subscribe(value => values.push(value));
    expect(values).toEqual([1, 2, 'fallback']);
  });
});
