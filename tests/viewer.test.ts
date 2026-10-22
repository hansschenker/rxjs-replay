import { parseHTML } from 'linkedom';
import { VirtualTimeScheduler, of } from 'rxjs';
import { describe, expect, it } from 'vitest';
import { createRecorder } from '../src/recorder.js';
import { parseRecording } from '../src/serialization.js';
import { mountReplay } from '../src/viewer.js';
import { recordCounter, recordSearch } from '../demo/fixtures.js';

function mountFixture() {
  const { document, window } = parseHTML('<html><body><main><p>Host content</p></main></body></html>');
  const host = document.querySelector('main') as unknown as HTMLElement;
  const scheduler = new VirtualTimeScheduler();
  const recording = recordSearch();
  const viewer = mountReplay(host, recording, { scheduler });
  function click(selector: string): void {
    const node = document.querySelector(selector);
    expect(node, `Expected viewer control ${selector}`).not.toBeNull();
    node!.dispatchEvent(new window.Event('click', { bubbles: true }));
  }
  return { document, window, host, scheduler, recording, viewer, click };
}

describe('optional browser viewer', () => {
  it('navigates saved events and preserves host content on disposal', () => {
    const fixture = mountFixture();
    const { document, viewer, click, scheduler } = fixture;
    expect(document.querySelector('.rr-position')?.textContent).toContain('#0 /');
    click('[data-action="next"]');
    expect(document.querySelector('.rr-position')?.textContent).toContain('#1 /');
    click('[data-action="end"]');
    expect(document.querySelector('.rr-position')?.textContent).toContain(`#${fixture.recording.events.at(-1)!.seq} /`);
    expect(document.querySelector('.rr-lifetimes')?.textContent).toContain('cancel');
    click('[data-action="previous"]');
    click('[data-action="start"]');
    click('[data-action="play"]');
    viewer.destroy();
    scheduler.flush();
    expect(document.querySelector('[data-rxjs-replay]')).toBeNull();
    expect(document.querySelector('main')?.textContent).toBe('Host content');
    viewer.destroy();
  });

  it('replaces recordings and shows explicitly captured state', () => {
    const { document, viewer, click } = mountFixture();
    const counter = recordCounter();
    viewer.setRecording(counter);
    expect(document.querySelector('h2')?.textContent).toBe(counter.name);
    const state = counter.events.find(event => event.kind === 'state')!;
    click(`.rr-mark[data-seq="${state.seq}"]`);
    expect(document.querySelector('.rr-panel:last-child')?.textContent).toContain('scan accumulator');
    expect(document.querySelector('pre')?.textContent).toContain('"count": 1');
    viewer.destroy();
    expect(() => viewer.setRecording(counter)).toThrow('destroyed');
  });

  it('renders hostile captured strings and labels as text', () => {
    const { document } = parseHTML('<html><body><main></main></body></html>');
    const recorder = createRecorder({ streams: [{ id: 'value', label: '<script>bad()</script>', lane: 'source' }] });
    of('<img src=x onerror="bad()">').pipe(recorder.trace('value')).subscribe();
    const recording = recorder.stop();
    const viewer = mountReplay(document.querySelector('main') as unknown as HTMLElement, recording);
    expect(document.querySelectorAll('img,script').length).toBe(0);
    expect(document.querySelector('table')?.textContent).toContain(JSON.stringify('<img src=x onerror="bad()">'));
    expect(document.querySelector('table')?.textContent).toContain('<script>bad()</script>');
    viewer.destroy();
  });

  it('filters the event table without changing the history cursor', () => {
    const { document, window, viewer, click } = mountFixture();
    click('[data-action="end"]');
    const position = document.querySelector('.rr-position')!.textContent;
    const select = document.querySelector('[aria-label="Filter by event kind"]') as unknown as HTMLSelectElement;
    // linkedom exposes select.value as a getter; selecting its option matches browser behavior.
    for (const option of Array.from(select.options)) option.selected = option.value === 'unsubscribe';
    select.dispatchEvent(new window.Event('change', { bubbles: true }));
    expect(document.querySelector('.rr-position')!.textContent).toBe(position);
    const rows = document.querySelectorAll('tbody tr');
    expect(rows.length).toBeGreaterThan(0);
    for (const row of rows) expect(row.textContent).toContain('unsubscribe');
    viewer.destroy();
  });

  it('links declared stages to operator executions and explains waiting after outer completion', () => {
    const { document, recording, viewer, click } = mountFixture();
    const operator = recording.events.find(event => event.role === 'operator' && event.kind === 'subscribe' && event.port === 'output')!;
    const outerComplete = recording.events.find(event => event.role === 'operator' && event.port === 'input' && event.kind === 'complete')!;
    expect(document.querySelectorAll('.rr-stage').length).toBe(3);
    expect(document.querySelector('.rr-connections')?.textContent).toContain('queries$ → latest result');
    expect(document.querySelector('.rr-connections')?.textContent).toContain('latest result → results$');
    expect(document.querySelector('[data-action="stage"][data-stream="latest"]')?.textContent).toContain('loadResult$');

    click(`[data-action="stage"][data-stream="latest"]`);
    expect(document.querySelector('[data-action="stage"][data-stream="latest"]')?.getAttribute('aria-pressed')).toBe('true');
    expect(document.querySelector('.rr-position')?.textContent).toContain(`#${operator.seq} /`);
    click(`.rr-mark[data-seq="${outerComplete.seq}"]`);
    const explanation = document.querySelector('.rr-operator-panel')!;
    expect(explanation.querySelector('[data-provenance="reconstructed"]')).not.toBeNull();
    expect(explanation.textContent).toContain('Outer completed');
    expect(explanation.textContent).toContain('Only latest');
    expect(explanation.textContent).toContain('active');
    const children = recording.events.filter(event => event.role === 'inner' && event.kind === 'subscribe');
    const latestInner = children.at(-1)!;
    expect(explanation.textContent).toContain(latestInner.executionId);
    const group = document.querySelector(`.rr-execution-group[data-execution="${operator.executionId}"]`)!;
    expect(group.querySelectorAll('.rr-inner-group').length).toBe(2);
    expect(group.textContent).toContain('input');
    expect(group.textContent).toContain('output');
    expect(group.textContent).toContain('cancel');
    viewer.destroy();
  });

  it('navigates from the operator to its active inner and back through distinct channels', () => {
    const { document, recording, viewer, click } = mountFixture();
    const outerComplete = recording.events.find(event => event.role === 'operator' && event.port === 'input' && event.kind === 'complete')!;
    const latestInner = recording.events.filter(event => event.role === 'inner' && event.kind === 'subscribe').at(-1)!;
    click(`.rr-mark[data-seq="${outerComplete.seq}"]`);
    click(`.rr-operator-panel [data-action="execution"][data-execution="${latestInner.executionId}"]`);
    expect(document.querySelector('.rr-position')?.textContent).toContain(`#${latestInner.seq} /`);
    expect(document.querySelector('.rr-focus')?.textContent).toContain(`Execution ${latestInner.executionId}`);
    expect(document.querySelectorAll('tbody tr').length).toBeGreaterThan(0);
    for (const row of document.querySelectorAll('tbody tr')) expect(row.textContent).toContain('inner output');
    click(`.rr-links [data-action="execution"][data-execution="${latestInner.ownerExecutionId}"]`);
    expect(document.querySelector('.rr-focus')?.textContent).toContain(`Execution ${latestInner.ownerExecutionId}`);
    const input = document.querySelector('.rr-operator-panel [data-action="subscription"]')!;
    const id = input.getAttribute('data-subscription')!;
    click(`.rr-operator-panel [data-action="subscription"][data-subscription="${id}"]`);
    expect(document.querySelector('.rr-focus')?.textContent).toContain(`Subscription ${id}`);
    for (const row of document.querySelectorAll('tbody tr')) expect(row.textContent).toContain(id);
    click('[data-action="clear-focus"]');
    expect(document.querySelector('.rr-focus')?.hasAttribute('hidden')).toBe(true);
    viewer.destroy();
  });

  it('does not group ordinary synchronous nesting as operator-owned inner execution', () => {
    const { document } = parseHTML('<html><body><main></main></body></html>');
    const recorder = createRecorder({ streams: [
      { id: 'outer', label: 'outer$', lane: 'source' },
      { id: 'nested', label: 'nested$', lane: 'result' },
    ] });
    of(1).pipe(recorder.trace('outer')).subscribe(() => of(2).pipe(recorder.trace('nested')).subscribe());
    const recording = recorder.stop();
    expect(recording.events.some(event => event.kind === 'subscribe' && event.parentSubscriptionId)).toBe(true);
    const viewer = mountReplay(document.querySelector('main') as unknown as HTMLElement, recording);
    const end = document.querySelector('[data-action="end"]')!;
    end.dispatchEvent(new document.defaultView!.Event('click', { bubbles: true }));
    expect(document.querySelectorAll('.rr-execution-group').length).toBe(2);
    expect(document.querySelectorAll('.rr-inner-group').length).toBe(0);
    viewer.destroy();
  });

  it('keeps legacy boundary recordings inspectable and does not invent operator evidence', () => {
    const { document, window } = parseHTML('<html><body><main></main></body></html>');
    const recorder = createRecorder({ streams: [{ id: 'legacy', label: 'legacy stage', lane: 'internal', operator: 'switchMap' }] });
    of(7).pipe(recorder.trace('legacy')).subscribe();
    const current = recorder.stop();
    const recording = parseRecording(JSON.stringify({
      ...current,
      schemaVersion: 1,
      streams: current.streams.map(({ operator, domain, ...stream }) => stream),
      events: current.events.map(({ executionId, role, port, ownerExecutionId, innerIndex, ...event }) => event),
    }));
    const viewer = mountReplay(document.querySelector('main') as unknown as HTMLElement, recording);
    document.querySelector('.rr-mark[data-seq="2"]')!.dispatchEvent(new window.Event('click', { bubbles: true }));
    expect(document.querySelector('.rr-operator-panel [data-provenance="unavailable"]')).not.toBeNull();
    expect(document.querySelector('.rr-panel:last-child')?.textContent).toContain('Version 1 boundary observation');
    expect(document.querySelector('.rr-execution-group')?.textContent).toContain('boundary');
    expect(document.querySelector('pre')?.textContent).toBe('7');
    viewer.destroy();
  });

  it('focuses legacy execution links through their recorded subscription identity', () => {
    const { document, window } = parseHTML('<html><body><main></main></body></html>');
    const recorder = createRecorder({ streams: [{ id: 'legacy', label: 'legacy$', lane: 'source' }] });
    of(7).pipe(recorder.trace('legacy')).subscribe();
    const current = recorder.stop();
    const recording = parseRecording(JSON.stringify({
      ...current,
      schemaVersion: 1,
      events: current.events.map(({ executionId, role, port, ownerExecutionId, innerIndex, ...event }) => event),
    }));
    const viewer = mountReplay(document.querySelector('main') as unknown as HTMLElement, recording);
    function click(selector: string): void {
      document.querySelector(selector)!.dispatchEvent(new window.Event('click', { bubbles: true }));
    }
    click('.rr-mark[data-seq="2"]');
    click('[data-action="focus-execution"]');
    expect(document.querySelector('.rr-focus')?.textContent).toContain('Execution s1');
    expect(document.querySelectorAll('tbody tr').length).toBe(recording.events.length);
    expect(document.querySelector('tbody')?.textContent).toContain('7');
    click('[data-action="clear-focus"]');
    click('.rr-execution-title');
    expect(document.querySelectorAll('tbody tr').length).toBe(recording.events.length);
    viewer.destroy();
  });
});
