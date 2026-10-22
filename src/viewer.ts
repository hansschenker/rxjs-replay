import { EMPTY, Subscription, catchError, from, fromEvent, map, switchMap } from 'rxjs';
import { inspectAt } from './history.js';
import { createReplay } from './playback.js';
import { selectEvents } from './query.js';
import { parseRecording, serializeRecording } from './serialization.js';
import type { CapturedValue, EventKind, EventQuery, PlaybackOptions, Recording, ReplayState, TraceEvent } from './types.js';

export interface ReplayViewer {
  readonly setRecording: (recording: Recording) => void;
  readonly destroy: () => void;
}

const EVENT_KINDS: readonly EventKind[] = ['subscribe', 'next', 'state', 'complete', 'error', 'unsubscribe'];
const TIMELINE_WINDOW = 300;
const TABLE_WINDOW = 120;

const STYLES = `
[data-rxjs-replay]{--rr-bg:#101821;--rr-panel:#17222e;--rr-line:#2c3b4a;--rr-text:#e5edf6;--rr-muted:#9eb1c3;--rr-source:#68c7f8;--rr-internal:#b6a3f7;--rr-result:#6ce3b2;font:14px/1.5 ui-sans-serif,system-ui,sans-serif;color:var(--rr-text);background:var(--rr-bg);border:1px solid var(--rr-line);border-radius:16px;overflow:hidden;color-scheme:dark;text-align:left}
[data-rxjs-replay] *{box-sizing:border-box}
[data-rxjs-replay] h2,[data-rxjs-replay] h3,[data-rxjs-replay] p{margin:0;max-width:none}
[data-rxjs-replay] button,[data-rxjs-replay] select,[data-rxjs-replay] input{font:inherit}
[data-rxjs-replay] button,[data-rxjs-replay] select{background:#20303f;color:var(--rr-text);border:1px solid #3b4c5d;border-radius:7px;padding:7px 11px;cursor:pointer}
[data-rxjs-replay] select{width:auto}
[data-rxjs-replay] button:hover,[data-rxjs-replay] select:hover{background:#2b4053}
[data-rxjs-replay] button:focus-visible,[data-rxjs-replay] select:focus-visible,[data-rxjs-replay] input:focus-visible{outline:2px solid #89d5ff;outline-offset:3px}
[data-rxjs-replay] button:disabled{opacity:.4;cursor:default}
[data-rxjs-replay] .rr-header{padding:22px 24px 16px;display:flex;gap:20px;justify-content:space-between;align-items:flex-start;flex-wrap:wrap}
[data-rxjs-replay] .rr-kicker{font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--rr-result);font-weight:700;margin-bottom:7px}
[data-rxjs-replay] h2{font-size:24px;line-height:1.25;letter-spacing:-.025em;margin-bottom:7px}
[data-rxjs-replay] h3{font-size:14px;letter-spacing:.02em;margin-bottom:12px}
[data-rxjs-replay] .rr-muted{color:var(--rr-muted);font-size:12px}
[data-rxjs-replay] .rr-files{display:flex;gap:8px}
[data-rxjs-replay] .rr-controls{display:flex;gap:8px;align-items:center;flex-wrap:wrap;padding:14px 24px;background:var(--rr-panel);border-block:1px solid var(--rr-line)}
[data-rxjs-replay] .rr-primary{background:#19654f;border-color:#3a997b;min-width:72px}
[data-rxjs-replay] .rr-primary:hover{background:#237d63}
[data-rxjs-replay] .rr-seek{flex:1;min-width:130px;accent-color:var(--rr-result)}
[data-rxjs-replay] .rr-position{font:12px ui-monospace,SFMono-Regular,monospace;min-width:115px;color:var(--rr-muted)}
[data-rxjs-replay] .rr-message{padding:9px 24px;color:#f3cc85;background:#342a1d;border-bottom:1px solid #5e4930}
[data-rxjs-replay] .rr-message[hidden]{display:none}
[data-rxjs-replay] .rr-timeline{padding:20px 24px 16px;overflow-x:auto}
[data-rxjs-replay] .rr-lane{display:grid;grid-template-columns:150px minmax(400px,1fr);gap:16px;min-width:580px;border-bottom:1px solid var(--rr-line)}
[data-rxjs-replay] .rr-lane-name{font-size:12px;font-weight:600;padding:16px 0;align-self:start}
[data-rxjs-replay] .rr-lane[data-lane=source] .rr-lane-name{color:var(--rr-source)}
[data-rxjs-replay] .rr-lane[data-lane=internal] .rr-lane-name{color:var(--rr-internal)}
[data-rxjs-replay] .rr-lane[data-lane=result] .rr-lane-name{color:var(--rr-result)}
[data-rxjs-replay] .rr-track{position:relative;min-height:58px;margin:0 14px;background:repeating-linear-gradient(to right,transparent 0,transparent calc(25% - 1px),#263442 calc(25% - 1px),#263442 25%)}
[data-rxjs-replay] .rr-ruler .rr-track{min-height:35px;background:none}
[data-rxjs-replay] .rr-ruler .rr-lane-name{padding:3px 0;color:var(--rr-muted)}
[data-rxjs-replay] .rr-tick{position:absolute;top:4px;transform:translateX(-50%);color:var(--rr-muted);font:10px ui-monospace,monospace;white-space:nowrap}
[data-rxjs-replay] .rr-mark{position:absolute;transform:translateX(-50%);min-width:25px;height:23px;padding:0 4px;border-radius:5px;border:1px solid var(--rr-source);background:#17384b;color:var(--rr-source);font:10px/21px ui-monospace,monospace;z-index:2}
[data-rxjs-replay] [data-lane=internal] .rr-mark{border-color:var(--rr-internal);color:var(--rr-internal);background:#342e4a}
[data-rxjs-replay] [data-lane=result] .rr-mark{border-color:var(--rr-result);color:var(--rr-result);background:#1b3b32}
[data-rxjs-replay] .rr-mark[data-kind=unsubscribe]{border-style:dashed}
[data-rxjs-replay] .rr-mark[data-kind=error]{color:#ffc1b8;border-color:#ed8a7b;background:#4a2928}
[data-rxjs-replay] .rr-mark[aria-current=true]{outline:2px solid white;outline-offset:2px;z-index:4}
[data-rxjs-replay] .rr-mark.rr-future{opacity:.38}
[data-rxjs-replay] .rr-cursor{position:absolute;inset-block:0;width:1px;background:#e5edf688;pointer-events:none;z-index:1}
[data-rxjs-replay] .rr-caption{padding-top:12px;color:var(--rr-muted);font-size:11px}
[data-rxjs-replay] .rr-body{display:grid;grid-template-columns:minmax(0,1.45fr) minmax(280px,1fr);gap:1px;background:var(--rr-line);border-top:1px solid var(--rr-line)}
[data-rxjs-replay] .rr-panel{background:var(--rr-bg);padding:20px 24px;min-width:0}
[data-rxjs-replay] .rr-filters{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px}
[data-rxjs-replay] label{display:flex;align-items:center;gap:7px;color:var(--rr-muted);font-size:12px}
[data-rxjs-replay] .rr-table-wrap{overflow:auto;max-height:330px;border:1px solid var(--rr-line);border-radius:8px}
[data-rxjs-replay] table{width:100%;border-collapse:collapse;font-size:12px;white-space:nowrap}
[data-rxjs-replay] th{text-align:left;color:var(--rr-muted);font-weight:500;padding:8px;background:var(--rr-panel);position:sticky;top:0;z-index:2}
[data-rxjs-replay] td{padding:6px 8px;border-top:1px solid #22313f;max-width:230px;overflow:hidden;text-overflow:ellipsis}
[data-rxjs-replay] tr[data-selected=true]{background:#203a49}
[data-rxjs-replay] .rr-event-link{font:inherit;border:0;padding:1px 3px;background:transparent;color:var(--rr-source)}
[data-rxjs-replay] .rr-detail-meta{color:var(--rr-muted);font:12px ui-monospace,monospace;margin-bottom:10px;overflow-wrap:anywhere}
[data-rxjs-replay] pre{margin:12px 0 0;background:#0b121a;border:1px solid var(--rr-line);border-radius:8px;padding:12px;overflow:auto;max-height:240px;white-space:pre-wrap;overflow-wrap:anywhere;font:12px/1.6 ui-monospace,monospace}
[data-rxjs-replay] .rr-badge{display:inline-block;font-size:10px;text-transform:uppercase;letter-spacing:.07em;color:var(--rr-internal);padding:3px 6px;border:1px solid #524b6c;border-radius:4px;margin-bottom:8px}
[data-rxjs-replay] .rr-lifetimes{margin-top:22px;border-top:1px solid var(--rr-line);padding-top:18px}
[data-rxjs-replay] .rr-life{margin:11px 0}
[data-rxjs-replay] .rr-life-label{display:flex;justify-content:space-between;gap:12px;font-size:11px;margin-bottom:5px;overflow-wrap:anywhere}
[data-rxjs-replay] .rr-life-track{height:6px;background:#263442;border-radius:4px;position:relative}
[data-rxjs-replay] .rr-life-bar{height:6px;position:absolute;border-radius:4px;background:var(--rr-source)}
[data-rxjs-replay] .rr-life-bar[data-outcome=cancel]{background:#e7b66d}
[data-rxjs-replay] .rr-life-bar[data-outcome=error]{background:#ed8a7b}
[data-rxjs-replay] .rr-life-bar[data-outcome=complete]{background:var(--rr-result)}
[data-rxjs-replay] .rr-footer{padding:12px 24px;border-top:1px solid var(--rr-line);color:var(--rr-muted);font-size:11px}
@media(max-width:820px){[data-rxjs-replay] .rr-body{grid-template-columns:1fr}[data-rxjs-replay] .rr-header,[data-rxjs-replay] .rr-panel{padding:18px}[data-rxjs-replay] .rr-controls{padding:12px 18px}[data-rxjs-replay] .rr-timeline{padding:16px 18px}}
`;

function timeLabel(at: number): string {
  return `${Number(at.toFixed(2))} ms`;
}

function capturedText(value: CapturedValue | undefined, pretty = false): string {
  if (!value) return '';
  if (value.status === 'unavailable') return `Unavailable: ${value.reason}`;
  return `${value.status === 'truncated' ? '[truncated] ' : ''}${JSON.stringify(value.data, null, pretty ? 2 : undefined)}`;
}

function eventValue(event: TraceEvent): CapturedValue | undefined {
  if (event.kind === 'next') return event.value;
  if (event.kind === 'error') return event.error;
  if (event.kind === 'state') return event.state;
  return undefined;
}

function selectValue(select: HTMLSelectElement, value: string): void {
  for (const option of Array.from(select.options)) option.selected = option.value === value;
}

/** Mount an isolated viewer over saved data. This never subscribes to application sources. */
export function mountReplay(element: HTMLElement, initialRecording: Recording, options: PlaybackOptions = {}): ReplayViewer {
  const doc = element.ownerDocument;
  const subscriptions = new Subscription();
  let stateSubscription: Subscription | undefined;
  let recording = initialRecording;
  let controller = createReplay(recording, options);
  let destroyed = false;

  function el<K extends keyof HTMLElementTagNameMap>(tag: K, className?: string, text?: string): HTMLElementTagNameMap[K] {
    const node = doc.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  }
  function button(text: string, action: string, title?: string): HTMLButtonElement {
    const node = el('button', undefined, text);
    node.type = 'button';
    node.dataset.action = action;
    if (title) node.setAttribute('aria-label', title);
    return node;
  }
  const root = el('section');
  root.dataset.rxjsReplay = '';
  root.setAttribute('aria-label', 'RxJS recorded execution viewer');
  const style = el('style');
  style.textContent = STYLES;
  root.append(style);

  const header = el('header', 'rr-header');
  const heading = el('div');
  heading.append(el('p', 'rr-kicker', 'rxjs-replay · recorded execution'));
  const title = el('h2');
  const metadata = el('p', 'rr-muted');
  heading.append(title, metadata);
  const files = el('div', 'rr-files');
  files.append(button('Open JSON', 'import'), button('Export JSON', 'export'));
  const fileInput = el('input');
  fileInput.type = 'file';
  fileInput.accept = '.json,application/json';
  fileInput.hidden = true;
  fileInput.setAttribute('aria-label', 'Open recording JSON');
  files.append(fileInput);
  header.append(heading, files);

  const controls = el('div', 'rr-controls');
  const start = button('Start', 'start', 'Go to beginning');
  const previous = button('←', 'previous', 'Previous event');
  const play = button('Play', 'play', 'Play recorded history');
  play.className = 'rr-primary';
  const next = button('→', 'next', 'Next event');
  const end = button('End', 'end', 'Go to end');
  const seek = el('input', 'rr-seek');
  seek.type = 'range';
  seek.min = '0';
  seek.step = '1';
  seek.setAttribute('aria-label', 'Recorded event position');
  const position = el('output', 'rr-position');
  const speed = el('select');
  speed.setAttribute('aria-label', 'Playback speed');
  for (const value of [0.25, 0.5, 1, 2, 4]) {
    const option = el('option', undefined, `${value}×`);
    option.value = String(value);
    speed.append(option);
  }
  controls.append(start, previous, play, next, end, seek, position, speed);

  const message = el('div', 'rr-message');
  message.setAttribute('role', 'status');
  message.hidden = true;
  const timeline = el('div', 'rr-timeline');
  const body = el('div', 'rr-body');
  const eventPanel = el('section', 'rr-panel');
  eventPanel.append(el('h3', undefined, 'Recorded events'));
  const filters = el('div', 'rr-filters');
  const streamFilter = el('select');
  streamFilter.setAttribute('aria-label', 'Filter by stream');
  const streamLabel = el('label', undefined, 'Stream');
  streamLabel.append(streamFilter);
  const kindFilter = el('select');
  kindFilter.setAttribute('aria-label', 'Filter by event kind');
  const allKinds = el('option', undefined, 'All events');
  allKinds.value = '';
  kindFilter.append(allKinds);
  for (const kind of EVENT_KINDS) {
    const option = el('option', undefined, kind);
    option.value = kind;
    kindFilter.append(option);
  }
  const kindLabel = el('label', undefined, 'Kind');
  kindLabel.append(kindFilter);
  filters.append(streamLabel, kindLabel);
  const eventTable = el('div', 'rr-table-wrap');
  const queryCaption = el('p', 'rr-caption');
  eventPanel.append(filters, eventTable, queryCaption);
  const detailPanel = el('section', 'rr-panel');
  const detail = el('div');
  const lifetimes = el('div', 'rr-lifetimes');
  detailPanel.append(detail, lifetimes);
  body.append(eventPanel, detailPanel);
  const footer = el('p', 'rr-footer', 'History navigation reads saved observations. It does not rerun application effects. Sequence numbers preserve delivery order when timestamps are equal.');
  root.append(header, controls, message, timeline, body, footer);
  element.append(root);

  function notify(text: string): void {
    message.textContent = text;
    message.hidden = text.length === 0;
  }
  function streamName(id: string): string {
    return recording.streams.find(stream => stream.id === id)?.label ?? id;
  }
  function currentIndex(state: ReplayState): number {
    return recording.events.findIndex(event => event.seq === state.cursor);
  }
  function totalTime(): number {
    return recording.events.at(-1)?.at ?? 0;
  }
  function percent(at: number): number {
    return (at / (totalTime() || 1)) * 100;
  }

  function renderTimeline(state: ReplayState): void {
    timeline.replaceChildren();
    const index = currentIndex(state);
    const first = Math.max(0, Math.min(index - Math.floor(TIMELINE_WINDOW / 3), recording.events.length - TIMELINE_WINDOW));
    const visible = recording.events.slice(first, first + TIMELINE_WINDOW);
    const cursorTime = recording.events[index]?.at ?? 0;
    const ruler = el('div', 'rr-lane rr-ruler');
    ruler.append(el('div', 'rr-lane-name', 'Timeline'));
    const rulerTrack = el('div', 'rr-track');
    for (const fraction of [0, 0.25, 0.5, 0.75, 1]) {
      const tick = el('span', 'rr-tick', timeLabel(totalTime() * fraction));
      tick.style.left = `${fraction * 100}%`;
      rulerTrack.append(tick);
    }
    ruler.append(rulerTrack);
    timeline.append(ruler);
    const definitions = [
      ['source', 'Source emissions'],
      ['internal', 'Internal behavior'],
      ['result', 'Resulting emissions'],
    ] as const;
    for (const [lane, label] of definitions) {
      const row = el('div', 'rr-lane');
      row.dataset.lane = lane;
      row.append(el('div', 'rr-lane-name', label));
      const track = el('div', 'rr-track');
      const stacks = new Map<number, number>();
      let maxStack = 0;
      for (const event of visible) {
        if (recording.streams.find(stream => stream.id === event.streamId)?.lane !== lane) continue;
        const stack = stacks.get(event.at) ?? 0;
        stacks.set(event.at, stack + 1);
        maxStack = Math.max(maxStack, stack);
        const mark = button(String(event.seq), 'seek');
        mark.className = `rr-mark${event.seq > state.cursor ? ' rr-future' : ''}`;
        mark.dataset.seq = String(event.seq);
        mark.dataset.kind = event.kind;
        mark.style.left = `${percent(event.at)}%`;
        mark.style.top = `${12 + stack * 27}px`;
        mark.setAttribute('aria-current', String(event.seq === state.cursor));
        const summary = `#${event.seq} · ${timeLabel(event.at)} · ${streamName(event.streamId)} · ${event.kind}${event.kind === 'unsubscribe' ? ` (${event.reason})` : ''}`;
        mark.title = summary;
        mark.setAttribute('aria-label', summary);
        track.append(mark);
      }
      track.style.minHeight = `${Math.max(58, 39 + maxStack * 27)}px`;
      const line = el('span', 'rr-cursor');
      line.style.left = `${percent(cursorTime)}%`;
      track.append(line);
      row.append(track);
      timeline.append(row);
    }
    timeline.append(el('p', 'rr-caption', `Each marker is one recorded event. Dashed borders show disposal. ${recording.events.length > TIMELINE_WINDOW ? `Showing events ${first + 1}–${first + visible.length} of ${recording.events.length}; the axis covers the entire recording.` : 'Events sharing a timestamp stack in sequence order.'}`));
  }

  function renderEvents(state: ReplayState): void {
    const query: EventQuery = {
      ...(streamFilter.value ? { streamId: streamFilter.value } : {}),
      ...(kindFilter.value ? { kinds: [kindFilter.value as EventKind] } : {}),
    };
    const matches = selectEvents(recording, query);
    const index = matches.findIndex(event => event.seq >= state.cursor);
    const first = Math.max(0, Math.min((index < 0 ? matches.length - 1 : index) - 30, matches.length - TABLE_WINDOW));
    const visible = matches.slice(first, first + TABLE_WINDOW);
    const table = el('table');
    const head = el('thead');
    const headerRow = el('tr');
    for (const text of ['Seq', 'Time', 'Stream', 'Event', 'Captured value']) headerRow.append(el('th', undefined, text));
    head.append(headerRow);
    const tbody = el('tbody');
    for (const event of visible) {
      const row = el('tr');
      row.dataset.selected = String(event.seq === state.cursor);
      const seq = el('td');
      const link = button(`#${event.seq}`, 'seek', `Inspect event ${event.seq}`);
      link.dataset.seq = String(event.seq);
      link.className = 'rr-event-link';
      seq.append(link);
      const summary = event.kind === 'unsubscribe' ? `unsubscribe · ${event.reason}` : event.kind;
      const value = capturedText(eventValue(event));
      row.append(seq, el('td', undefined, timeLabel(event.at)), el('td', undefined, streamName(event.streamId)), el('td', undefined, summary), el('td', undefined, value.slice(0, 180)));
      tbody.append(row);
    }
    table.append(head, tbody);
    eventTable.replaceChildren(table);
    queryCaption.textContent = matches.length === 0 ? 'No recorded events match these filters.' : `${matches.length} matching events${matches.length > TABLE_WINDOW ? ` · showing ${first + 1}–${first + visible.length}` : ''}. Filters query the saved recording.`;
  }

  function renderDetail(state: ReplayState): void {
    const view = inspectAt(recording, state.cursor);
    const event = view.event;
    detail.replaceChildren(el('h3', undefined, 'At the cursor'));
    if (event) {
      detail.append(el('span', 'rr-badge', 'Recorded observation'));
      detail.append(el('p', 'rr-detail-meta', `#${event.seq} · ${timeLabel(event.at)} · ${event.subscriptionId}`));
      detail.append(el('p', undefined, `${streamName(event.streamId)} → ${event.kind}${event.kind === 'unsubscribe' ? ` (${event.reason})` : ''}`));
      if (event.kind === 'state') detail.append(el('p', 'rr-muted', `Explicit capture: ${event.label}`));
      if (eventValue(event)) detail.append(el('pre', undefined, capturedText(eventValue(event), true)));
      if (event.causeSeq !== undefined) {
        const cause = button(`Go to synchronous cause #${event.causeSeq}`, 'seek');
        cause.dataset.seq = String(event.causeSeq);
        cause.style.marginTop = '12px';
        detail.append(cause);
      }
      const current = view.subscriptions.find(subscription => subscription.id === event.subscriptionId);
      if (current?.latestState && event.kind !== 'state') {
        detail.append(el('p', 'rr-caption', 'Latest explicitly captured state at this position'));
        detail.append(el('pre', undefined, capturedText(current.latestState, true)));
      }
    } else {
      detail.append(el('p', 'rr-muted', 'Before the first event. Step forward or select a recorded event to inspect it.'));
    }
    lifetimes.replaceChildren(el('h3', undefined, 'Subscription lifetimes'));
    lifetimes.append(el('p', 'rr-muted', 'Reconstructed from recorded lifecycle events up to this cursor.'));
    const subscriptionsAtCursor = view.subscriptions.slice(-50);
    if (subscriptionsAtCursor.length === 0) lifetimes.append(el('p', 'rr-caption', 'No subscription has started at this position.'));
    for (const subscription of subscriptionsAtCursor) {
      const row = el('div', 'rr-life');
      const label = el('div', 'rr-life-label');
      label.append(el('span', undefined, `${streamName(subscription.streamId)} · ${subscription.id}`), el('span', undefined, `${subscription.outcome} · ${subscription.values} values`));
      const track = el('div', 'rr-life-track');
      const bar = el('span', 'rr-life-bar');
      bar.dataset.outcome = subscription.outcome;
      bar.style.left = `${percent(subscription.start.at)}%`;
      bar.style.width = `${Math.max(0.7, percent((subscription.end?.at ?? event?.at ?? 0) - subscription.start.at))}%`;
      track.append(bar);
      row.append(label, track);
      lifetimes.append(row);
    }
    if (view.subscriptions.length > subscriptionsAtCursor.length) lifetimes.append(el('p', 'rr-caption', 'Showing the latest 50 observed subscriptions.'));
    if (view.hasOmittedEvents) lifetimes.append(el('p', 'rr-caption', 'This recording omits later events. An open subscription does not prove a leak.'));
  }

  function render(state: ReplayState): void {
    if (destroyed) return;
    const index = currentIndex(state);
    play.textContent = state.status === 'playing' ? 'Pause' : 'Play';
    play.setAttribute('aria-label', state.status === 'playing' ? 'Pause recorded history' : 'Play recorded history');
    play.disabled = recording.events.length === 0 || state.status === 'ended';
    previous.disabled = start.disabled = state.cursor === 0;
    next.disabled = end.disabled = index === recording.events.length - 1 || recording.events.length === 0;
    seek.max = String(recording.events.length);
    seek.value = String(index + 1);
    seek.disabled = recording.events.length === 0;
    selectValue(speed, String(state.speed));
    position.textContent = `#${state.cursor} / ${recording.events.at(-1)?.seq ?? 0} · ${timeLabel(recording.events[index]?.at ?? 0)}`;
    renderTimeline(state);
    renderEvents(state);
    renderDetail(state);
  }

  function populateRecording(): void {
    title.textContent = recording.name;
    metadata.textContent = `${recording.events.length} captured events · ${recording.streams.length} streams · ${timeLabel(totalTime())} · RxJS ${recording.rxjsVersion}`;
    const all = el('option', undefined, 'All streams');
    all.value = '';
    streamFilter.replaceChildren(all);
    for (const stream of recording.streams) {
      const option = el('option', undefined, stream.label);
      option.value = stream.id;
      streamFilter.append(option);
    }
    selectValue(kindFilter, '');
    selectValue(streamFilter, '');
    notify(recording.capture.droppedEvents > 0 ? `Capture limit reached: ${recording.capture.droppedEvents} later events were omitted. The retained prefix remains inspectable.` : '');
    stateSubscription = controller.state$.subscribe(render);
  }

  function setRecording(nextRecording: Recording): void {
    if (destroyed) throw new Error('Cannot replace a recording after the viewer is destroyed.');
    stateSubscription?.unsubscribe();
    controller.dispose();
    recording = nextRecording;
    controller = createReplay(recording, options);
    populateRecording();
  }

  function exportRecording(): void {
    const blob = new Blob([serializeRecording(recording, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    try {
      const anchor = el('a');
      anchor.href = url;
      anchor.download = 'rxjs-recording.json';
      anchor.click();
    } finally {
      URL.revokeObjectURL(url);
    }
  }

  subscriptions.add(fromEvent<MouseEvent>(root, 'click').subscribe(event => {
    const target = event.target as Element | null;
    const control = typeof target?.closest === 'function' ? target.closest<HTMLElement>('[data-action]') : null;
    if (!control || !root.contains(control)) return;
    try {
      switch (control.dataset.action) {
        case 'start': controller.seek(0); break;
        case 'previous': controller.step(-1); break;
        case 'next': controller.step(1); break;
        case 'end': controller.seek(recording.events.at(-1)?.seq ?? 0); break;
        case 'play': controller.getState().status === 'playing' ? controller.pause() : controller.play(); break;
        case 'seek': controller.seek(Number(control.dataset.seq)); break;
        case 'import': fileInput.click(); break;
        case 'export': exportRecording(); break;
      }
    } catch (error) {
      notify(error instanceof Error ? error.message : String(error));
    }
  }));
  subscriptions.add(fromEvent(seek, 'input').subscribe(() => {
    controller.seek(recording.events[Number(seek.value) - 1]?.seq ?? 0);
  }));
  subscriptions.add(fromEvent(speed, 'change').subscribe(() => controller.setSpeed(Number(speed.value))));
  subscriptions.add(fromEvent(streamFilter, 'change').subscribe(() => renderEvents(controller.getState())));
  subscriptions.add(fromEvent(kindFilter, 'change').subscribe(() => renderEvents(controller.getState())));
  subscriptions.add(fromEvent(fileInput, 'change').pipe(
    switchMap(() => {
      const file = fileInput.files?.[0];
      fileInput.value = '';
      if (!file) return EMPTY;
      if (file.size > 20 * 1024 * 1024) {
        notify('Choose a recording smaller than 20 MiB.');
        return EMPTY;
      }
      return from(file.text()).pipe(
        map(parseRecording),
        catchError((error: unknown) => {
          notify(`Could not open recording: ${error instanceof Error ? error.message : String(error)}`);
          return EMPTY;
        }),
      );
    }),
  ).subscribe(setRecording));
  populateRecording();

  function destroy(): void {
    if (destroyed) return;
    destroyed = true;
    stateSubscription?.unsubscribe();
    subscriptions.unsubscribe();
    controller.dispose();
    root.remove();
  }
  return Object.freeze({ setRecording, destroy });
}
