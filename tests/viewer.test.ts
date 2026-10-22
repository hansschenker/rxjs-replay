import { parseHTML } from 'linkedom';
import { VirtualTimeScheduler, of } from 'rxjs';
import { describe, expect, it } from 'vitest';
import { createRecorder } from '../src/recorder.js';
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
});
