import { EMPTY, Subscription, catchError, from, fromEvent, map, switchMap } from 'rxjs';
import { explainEvent, getExecutions, getSwitchMapState } from './analysis.js';
import type { ExecutionSummary } from './analysis.js';
import { inspectAt } from './history.js';
import { createReplay } from './playback.js';
import { selectEvents } from './query.js';
import { parseRecording, serializeRecording } from './serialization.js';
import type { CapturedValue, EventKind, EventQuery, ExecutionPort, ExecutionRole, PlaybackOptions, Recording, ReplayState, StreamDefinition, SubscriptionView, TraceEvent } from './types.js';

export interface ReplayViewer {
  readonly setRecording: (recording: Recording) => void;
  readonly destroy: () => void;
}

const EVENT_KINDS: readonly EventKind[] = ['subscribe', 'next', 'state', 'complete', 'error', 'unsubscribe'];
const TIMELINE_WINDOW = 300;
const TABLE_WINDOW = 120;
const MAX_GRAPH_NODES = 48;
let viewNumber = 0;

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
[data-rxjs-replay] .rr-graph-section{padding:18px 24px;border-bottom:1px solid var(--rr-line)}
[data-rxjs-replay] .rr-section-head{display:flex;justify-content:space-between;align-items:baseline;gap:8px 20px;flex-wrap:wrap;margin-bottom:12px}
[data-rxjs-replay] .rr-section-head h3{margin:0}
[data-rxjs-replay] .rr-graph{position:relative;padding:12px 0}
[data-rxjs-replay] .rr-graph-nodes{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:32px;position:relative}
[data-rxjs-replay] .rr-stage{display:grid;gap:4px;align-content:start;text-align:left;padding:12px 14px;min-width:0;position:relative;z-index:1;background:var(--rr-panel);border-color:var(--rr-line)}
[data-rxjs-replay] .rr-stage[data-selected=true]{background:#203a49;border-color:var(--rr-source);box-shadow:0 0 0 1px var(--rr-source)}
[data-rxjs-replay] .rr-stage-title{font-weight:650;overflow-wrap:anywhere}
[data-rxjs-replay] .rr-stage-kind{font-size:11px;text-transform:uppercase;letter-spacing:.07em;color:var(--rr-muted)}
[data-rxjs-replay] .rr-stage-domain{font:12px ui-monospace,monospace;color:var(--rr-internal);overflow-wrap:anywhere}
[data-rxjs-replay] .rr-graph-lines{position:absolute;inset:0;width:100%;height:100%;pointer-events:none;overflow:visible;color:#70899e}
[data-rxjs-replay] .rr-graph-edge{fill:none;stroke:currentColor;stroke-width:1.4}
[data-rxjs-replay] .rr-graph-edge[data-selected=true]{color:var(--rr-source);stroke-width:2}
[data-rxjs-replay] .rr-connections{display:flex;flex-wrap:wrap;align-items:center;gap:5px 12px;margin-top:6px;font-size:11px;color:var(--rr-muted)}
[data-rxjs-replay] .rr-connection{border:0;background:transparent;font-size:11px;padding:2px;color:var(--rr-muted)}
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
[data-rxjs-replay] .rr-links{display:flex;flex-wrap:wrap;gap:6px;margin-top:12px}
[data-rxjs-replay] .rr-links button{font-size:11px;padding:5px 8px}
[data-rxjs-replay] .rr-focus{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin:0 0 12px;color:var(--rr-internal);font-size:11px}
[data-rxjs-replay] .rr-focus[hidden]{display:none}
[data-rxjs-replay] .rr-focus button{font-size:11px;padding:3px 6px}
[data-rxjs-replay] .rr-operator-panel{margin-top:22px;border-top:1px solid var(--rr-line);padding-top:18px}
[data-rxjs-replay] .rr-badge[data-provenance=unavailable]{color:#e7b66d;border-color:#695334}
[data-rxjs-replay] .rr-facts{display:grid;grid-template-columns:auto minmax(0,1fr);gap:6px 12px;font-size:12px;margin:12px 0}
[data-rxjs-replay] .rr-facts dt{color:var(--rr-muted)}
[data-rxjs-replay] .rr-facts dd{margin:0;overflow-wrap:anywhere}
[data-rxjs-replay] .rr-facts button{font-size:11px;padding:2px 6px}
[data-rxjs-replay] pre{margin:12px 0 0;background:#0b121a;border:1px solid var(--rr-line);border-radius:8px;padding:12px;overflow:auto;max-height:240px;white-space:pre-wrap;overflow-wrap:anywhere;font:12px/1.6 ui-monospace,monospace}
[data-rxjs-replay] .rr-badge{display:inline-block;font-size:10px;text-transform:uppercase;letter-spacing:.07em;color:var(--rr-internal);padding:3px 6px;border:1px solid #524b6c;border-radius:4px;margin-bottom:8px}
[data-rxjs-replay] .rr-lifetimes{margin-top:22px;border-top:1px solid var(--rr-line);padding-top:18px}
[data-rxjs-replay] .rr-life{margin:11px 0}
[data-rxjs-replay] .rr-life-label{display:flex;justify-content:space-between;gap:12px;font-size:11px;margin-bottom:5px;overflow-wrap:anywhere}
[data-rxjs-replay] .rr-execution-group{border:1px solid var(--rr-line);padding:10px;margin-top:10px;border-radius:8px;min-width:0}
[data-rxjs-replay] .rr-execution-group[data-selected=true]{border-color:var(--rr-source)}
[data-rxjs-replay] .rr-execution-title{border:0;background:transparent;padding:0;font-size:12px;text-align:left;color:var(--rr-source);overflow-wrap:anywhere}
[data-rxjs-replay] .rr-inner-group{margin:10px 0 0 10px;padding:8px 0 0 10px;border-left:2px solid #514569}
[data-rxjs-replay] .rr-inner-group[data-selected=true]{border-left-color:var(--rr-internal);background:#241e3333}
[data-rxjs-replay] .rr-inner-group .rr-execution-title{color:var(--rr-internal);font-size:11px}
[data-rxjs-replay] .rr-life-name{border:0;background:transparent;padding:0;font-size:11px;color:var(--rr-muted);text-align:left;overflow-wrap:anywhere}
[data-rxjs-replay] .rr-life[data-selected=true] .rr-life-name{color:var(--rr-text)}
[data-rxjs-replay] .rr-life-track{height:6px;background:#263442;border-radius:4px;position:relative}
[data-rxjs-replay] .rr-life-bar{height:6px;position:absolute;border-radius:4px;background:var(--rr-source)}
[data-rxjs-replay] .rr-life-bar[data-outcome=cancel]{background:#e7b66d}
[data-rxjs-replay] .rr-life-bar[data-outcome=error]{background:#ed8a7b}
[data-rxjs-replay] .rr-life-bar[data-outcome=complete]{background:var(--rr-result)}
[data-rxjs-replay] .rr-footer{padding:12px 24px;border-top:1px solid var(--rr-line);color:var(--rr-muted);font-size:11px}
@media(max-width:820px){[data-rxjs-replay] .rr-body{grid-template-columns:1fr}[data-rxjs-replay] .rr-header,[data-rxjs-replay] .rr-panel{padding:18px}[data-rxjs-replay] .rr-controls{padding:12px 18px}[data-rxjs-replay] .rr-timeline,[data-rxjs-replay] .rr-graph-section{padding:16px 18px}}
@media(max-width:550px){[data-rxjs-replay] .rr-graph-nodes{grid-template-columns:1fr;gap:24px}}
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
  let focusedSubscriptionId: string | undefined;
  let focusedExecutionId: string | undefined;
  let streamsById = new Map<string, StreamDefinition>();
  let eventsByExecution = new Map<string, TraceEvent[]>();
  let eventsBySubscription = new Map<string, TraceEvent[]>();
  let graphButtons = new Map<string, HTMLButtonElement>();
  const arrowId = `rr-arrow-${++viewNumber}`;

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
  const graphSection = el('section', 'rr-graph-section');
  const graphHeader = el('div', 'rr-section-head');
  graphHeader.append(el('h3', undefined, 'Dataflow definitions'), el('span', 'rr-muted', 'Select a stage to inspect its execution'));
  const graph = el('div', 'rr-graph');
  const graphLines = doc.createElementNS('http://www.w3.org/2000/svg', 'svg');
  graphLines.classList.add('rr-graph-lines');
  graphLines.setAttribute('aria-hidden', 'true');
  const graphNodes = el('div', 'rr-graph-nodes');
  const connections = el('div', 'rr-connections');
  graph.append(graphLines, graphNodes);
  graphSection.append(graphHeader, graph, connections);
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
  function choiceFilter(label: string, choices: readonly string[]): HTMLSelectElement {
    const select = el('select');
    select.setAttribute('aria-label', `Filter by ${label.toLowerCase()}`);
    const all = el('option', undefined, `All ${label.toLowerCase()}s`);
    all.value = '';
    select.append(all);
    for (const choice of choices) {
      const option = el('option', undefined, choice);
      option.value = choice;
      select.append(option);
    }
    const container = el('label', undefined, label);
    container.append(select);
    filters.append(container);
    return select;
  }
  const roleFilter = choiceFilter('Role', ['boundary', 'operator', 'inner']);
  const portFilter = choiceFilter('Port', ['input', 'output']);
  const focus = el('div', 'rr-focus');
  focus.hidden = true;
  const eventTable = el('div', 'rr-table-wrap');
  const queryCaption = el('p', 'rr-caption');
  eventPanel.append(filters, focus, eventTable, queryCaption);
  const detailPanel = el('section', 'rr-panel');
  const detail = el('div');
  const operatorPanel = el('div', 'rr-operator-panel');
  const lifetimes = el('div', 'rr-lifetimes');
  eventPanel.append(operatorPanel);
  detailPanel.append(detail, lifetimes);
  body.append(eventPanel, detailPanel);
  const footer = el('p', 'rr-footer', 'History navigation reads saved observations. It does not rerun application effects. Sequence numbers preserve delivery order when timestamps are equal.');
  root.append(header, controls, message, graphSection, timeline, body, footer);
  element.append(root);

  function notify(text: string): void {
    message.textContent = text;
    message.hidden = text.length === 0;
  }
  function streamName(id: string): string {
    return streamsById.get(id)?.label ?? id;
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

  function executionId(event: TraceEvent): string {
    return event.executionId ?? event.subscriptionId;
  }
  function visibleWithSelection<T extends { readonly id: string }>(items: readonly T[], selectedId: string | undefined, maximum: number): readonly T[] {
    const visible = items.slice(0, maximum);
    if (selectedId && !visible.some(item => item.id === selectedId)) {
      const selected = items.find(item => item.id === selectedId);
      if (selected) return [...visible.slice(0, maximum - 1), selected];
    }
    return visible;
  }
  function nearestRecorded(events: readonly TraceEvent[]): TraceEvent | undefined {
    const cursor = controller.getState().cursor;
    return events.filter(event => event.seq <= cursor).at(-1) ?? events[0];
  }
  function selectExecution(id: string): void {
    const target = nearestRecorded(eventsByExecution.get(id) ?? []);
    if (target) controller.seek(target.seq);
    renderEvents(controller.getState());
  }
  function selectSubscription(id: string): void {
    const target = nearestRecorded(eventsBySubscription.get(id) ?? []);
    if (target) controller.seek(target.seq);
    renderEvents(controller.getState());
  }

  function drawGraphConnections(): void {
    if (destroyed) return;
    graphLines.replaceChildren();
    const bounds = graph.getBoundingClientRect();
    if (bounds.width === 0 || bounds.height === 0) return;
    graphLines.setAttribute('viewBox', `0 0 ${bounds.width} ${bounds.height}`);
    const defs = doc.createElementNS('http://www.w3.org/2000/svg', 'defs');
    const arrow = doc.createElementNS('http://www.w3.org/2000/svg', 'marker');
    arrow.id = arrowId;
    for (const [key, value] of Object.entries({ viewBox: '0 0 8 8', refX: '7', refY: '4', markerWidth: '7', markerHeight: '7', orient: 'auto' })) arrow.setAttribute(key, value);
    const tip = doc.createElementNS('http://www.w3.org/2000/svg', 'path');
    tip.setAttribute('d', 'M 1 1 L 7 4 L 1 7');
    tip.setAttribute('fill', 'none');
    tip.setAttribute('stroke', 'currentColor');
    arrow.append(tip);
    defs.append(arrow);
    graphLines.append(defs);
    const selectedStage = recording.events[currentIndex(controller.getState())]?.streamId;
    for (const target of recording.streams) {
      for (const sourceId of target.inputs ?? []) {
        const sourceButton = graphButtons.get(sourceId);
        const targetButton = graphButtons.get(target.id);
        if (!sourceButton || !targetButton) continue;
        const source = sourceButton.getBoundingClientRect();
        const destination = targetButton.getBoundingClientRect();
        let path: string;
        if (source.top === destination.top && destination.left > source.right) {
          const x1 = source.right - bounds.left;
          const x2 = destination.left - bounds.left;
          const y = source.top + source.height / 2 - bounds.top;
          if (x2 - x1 < 50) path = `M ${x1} ${y} L ${x2} ${y}`;
          else path = `M ${x1} ${y} L ${x1 + 10} ${y} L ${x1 + 10} 2 L ${x2 - 10} 2 L ${x2 - 10} ${y} L ${x2} ${y}`;
        } else if (destination.top > source.bottom && destination.top - source.bottom < 50) {
          const x1 = source.left + source.width / 2 - bounds.left;
          const x2 = destination.left + destination.width / 2 - bounds.left;
          const y1 = source.bottom - bounds.top;
          const y2 = destination.top - bounds.top;
          const middle = (y1 + y2) / 2;
          path = `M ${x1} ${y1} C ${x1} ${middle}, ${x2} ${middle}, ${x2} ${y2}`;
        } else {
          // Nonadjacent/wrapped connections travel outside the cards, never through an intervening stage.
          const x1 = source.right - bounds.left;
          const y1 = source.top + source.height / 2 - bounds.top;
          const x2 = destination.right - bounds.left;
          const y2 = destination.top + destination.height / 2 - bounds.top;
          const right = bounds.width + 9;
          path = sourceId === target.id
            ? `M ${x1} ${y1} C ${right} ${y1}, ${right} 0, ${source.left + source.width / 2 - bounds.left} ${source.top - bounds.top}`
            : `M ${x1} ${y1} L ${right} ${y1} L ${right} ${y2} L ${x2} ${y2}`;
        }
        const edge = doc.createElementNS('http://www.w3.org/2000/svg', 'path');
        edge.setAttribute('d', path);
        edge.setAttribute('marker-end', `url(#${arrowId})`);
        edge.classList.add('rr-graph-edge');
        edge.dataset.from = sourceId;
        edge.dataset.to = target.id;
        edge.dataset.selected = String(selectedStage === sourceId || selectedStage === target.id);
        graphLines.append(edge);
      }
    }
  }

  function renderGraph(state: ReplayState): void {
    const event = recording.events[currentIndex(state)];
    const executions = getExecutions(recording, state.cursor);
    const visible = visibleWithSelection(recording.streams, event?.streamId, MAX_GRAPH_NODES);
    graphButtons = new Map();
    graphNodes.replaceChildren();
    for (const stream of visible) {
      const card = button('', 'stage', `Inspect stage ${stream.label}`);
      card.className = 'rr-stage';
      card.dataset.stream = stream.id;
      card.dataset.selected = String(event?.streamId === stream.id);
      card.setAttribute('aria-pressed', String(event?.streamId === stream.id));
      card.append(el('span', 'rr-stage-kind', stream.operator ? `Operator · ${stream.operator}` : stream.lane));
      card.append(el('span', 'rr-stage-title', stream.label));
      if (stream.domain) card.append(el('span', 'rr-stage-domain', stream.domain));
      const count = executions.filter(execution => execution.streamId === stream.id && execution.role !== 'inner').length;
      card.append(el('span', 'rr-muted', `${count} execution${count === 1 ? '' : 's'} at cursor${stream.sharing ? ` · ${stream.sharing}` : ''}`));
      card.disabled = !recording.events.some(item => item.streamId === stream.id);
      graphButtons.set(stream.id, card);
      graphNodes.append(card);
    }
    connections.replaceChildren(el('span', undefined, 'Declared connections:'));
    let edgeCount = 0;
    for (const target of recording.streams) {
      for (const sourceId of target.inputs ?? []) {
        edgeCount++;
        if (edgeCount > 80) continue;
        const link = button(`${streamName(sourceId)} → ${target.label}`, 'stage');
        link.className = 'rr-connection';
        link.dataset.stream = target.id;
        connections.append(link);
      }
    }
    if (edgeCount === 0) connections.append(el('span', undefined, 'None declared. Event order does not create graph edges.'));
    if (edgeCount > 80) connections.append(el('span', undefined, `Showing 80 of ${edgeCount} connections.`));
    if (visible.length < recording.streams.length) connections.append(el('span', undefined, `Showing ${visible.length} of ${recording.streams.length} stages, including the selected stage.`));
    drawGraphConnections();
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
        if (streamsById.get(event.streamId)?.lane !== lane) continue;
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
        const summary = `#${event.seq} · ${timeLabel(event.at)} · ${streamName(event.streamId)} · ${event.role ?? 'boundary'} ${event.port ?? 'output'} · ${event.kind}${event.kind === 'unsubscribe' ? ` (${event.reason})` : ''}`;
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
      ...(roleFilter.value ? { role: roleFilter.value as ExecutionRole } : {}),
      ...(portFilter.value ? { port: portFilter.value as ExecutionPort } : {}),
      ...(focusedSubscriptionId ? { subscriptionId: focusedSubscriptionId } : {}),
      ...(focusedExecutionId
        ? recording.schemaVersion === 1 ? { subscriptionId: focusedExecutionId } : { executionId: focusedExecutionId }
        : {}),
    };
    focus.hidden = !focusedSubscriptionId && !focusedExecutionId;
    focus.replaceChildren(el('span', undefined, focusedSubscriptionId ? `Subscription ${focusedSubscriptionId}` : `Execution ${focusedExecutionId ?? ''}`), button('Clear focus', 'clear-focus'));
    const matches = selectEvents(recording, query);
    const index = matches.findIndex(event => event.seq >= state.cursor);
    const first = Math.max(0, Math.min((index < 0 ? matches.length - 1 : index) - 30, matches.length - TABLE_WINDOW));
    const visible = matches.slice(first, first + TABLE_WINDOW);
    const table = el('table');
    const head = el('thead');
    const headerRow = el('tr');
    for (const text of ['Seq', 'Time', 'Stage', 'Channel', 'Event', 'Captured value']) headerRow.append(el('th', undefined, text));
    head.append(headerRow);
    const tbody = el('tbody');
    let selectedRow: HTMLTableRowElement | undefined;
    for (const event of visible) {
      const row = el('tr');
      row.dataset.selected = String(event.seq === state.cursor);
      if (event.seq === state.cursor) selectedRow = row;
      const seq = el('td');
      const link = button(`#${event.seq}`, 'seek', `Inspect event ${event.seq}`);
      link.dataset.seq = String(event.seq);
      link.className = 'rr-event-link';
      seq.append(link);
      const summary = event.kind === 'unsubscribe' ? `unsubscribe · ${event.reason}` : event.kind;
      const value = capturedText(eventValue(event));
      const channel = `${event.role ?? 'boundary'} ${event.port ?? 'output'} · ${event.subscriptionId}`;
      row.append(seq, el('td', undefined, timeLabel(event.at)), el('td', undefined, streamName(event.streamId)), el('td', undefined, channel), el('td', undefined, summary), el('td', undefined, value.slice(0, 180)));
      tbody.append(row);
    }
    table.append(head, tbody);
    eventTable.replaceChildren(table);
    if (selectedRow && eventTable.clientHeight > 0) {
      const rowTop = selectedRow.getBoundingClientRect().top - eventTable.getBoundingClientRect().top + eventTable.scrollTop;
      eventTable.scrollTop = Math.max(0, rowTop - (eventTable.clientHeight - selectedRow.clientHeight) / 2);
    }
    queryCaption.textContent = matches.length === 0 ? 'No recorded events match these filters.' : `${matches.length} matching events${matches.length > TABLE_WINDOW ? ` · showing ${first + 1}–${first + visible.length}` : ''}. Filters query the saved recording.${state.cursor > 0 && !matches.some(event => event.seq === state.cursor) ? ' The selected event is outside these filters.' : ''}`;
  }

  function renderDetail(state: ReplayState): void {
    const view = inspectAt(recording, state.cursor);
    const event = view.event;
    detail.replaceChildren(el('h3', undefined, 'At the cursor'));
    if (event) {
      detail.append(el('span', 'rr-badge', 'Recorded observation'));
      detail.append(el('p', 'rr-detail-meta', `#${event.seq} · ${timeLabel(event.at)} · ${event.subscriptionId} · ${executionId(event)}`));
      detail.append(el('p', undefined, `${streamName(event.streamId)} → ${event.kind}${event.kind === 'unsubscribe' ? ` (${event.reason})` : ''}`));
      detail.append(el('p', 'rr-caption', explainEvent(recording, event)));
      detail.append(el('p', 'rr-caption', event.role ? `${event.role} execution · ${event.port} port${event.innerIndex === undefined ? '' : ` · inner index ${event.innerIndex}`}` : 'Version 1 boundary observation; execution metadata was not recorded.'));
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
      const links = el('div', 'rr-links');
      const focusExecution = button('Focus execution', 'focus-execution');
      focusExecution.dataset.execution = executionId(event);
      const focusSubscription = button('Focus subscription', 'focus-subscription');
      focusSubscription.dataset.subscription = event.subscriptionId;
      links.append(focusExecution, focusSubscription);
      if (event.ownerExecutionId) {
        const owner = button(`Operator ${event.ownerExecutionId}`, 'execution');
        owner.dataset.execution = event.ownerExecutionId;
        links.append(owner);
      }
      if (current?.parentSubscriptionId) {
        const parent = button(`Synchronous parent ${current.parentSubscriptionId}`, 'subscription');
        parent.dataset.subscription = current.parentSubscriptionId;
        links.append(parent);
      }
      const executionEvents = eventsByExecution.get(executionId(event)) ?? [];
      const earlier = executionEvents.filter(item => item.seq < event.seq).at(-1);
      const later = executionEvents.find(item => item.seq > event.seq);
      for (const [label, target] of [['Previous in execution', earlier], ['Next in execution', later]] as const) {
        const navigate = button(label, 'seek');
        navigate.disabled = !target;
        if (target) navigate.dataset.seq = String(target.seq);
        links.append(navigate);
      }
      detail.append(links);
    } else {
      detail.append(el('p', 'rr-muted', 'Before the first event. Step forward or select a recorded event to inspect it.'));
    }
    renderOperator(state, event);
    renderLifetimes(state, view.subscriptions);
  }

  function renderOperator(state: ReplayState, event: TraceEvent | undefined): void {
    operatorPanel.replaceChildren(el('h3', undefined, 'Operator explanation'));
    const stream = event ? streamsById.get(event.streamId) : undefined;
    const operatorId = event?.role === 'operator' ? event.executionId
      : event?.role === 'inner' ? event.ownerExecutionId
      : stream?.operator ? event && executionId(event) : undefined;
    if (!operatorId) {
      if (recording.schemaVersion === 1) {
        const unavailable = el('span', 'rr-badge', 'Operator state unavailable');
        unavailable.dataset.provenance = 'unavailable';
        operatorPanel.append(unavailable, el('p', 'rr-muted', 'This version 1 recording contains no operator execution metadata. Its boundary observations remain inspectable.'));
      } else operatorPanel.append(el('p', 'rr-muted', 'Select an operator stage or an inner execution to inspect its Action Policy.'));
      return;
    }
    const model = getSwitchMapState(recording, operatorId, state.cursor);
    const badge = el('span', 'rr-badge', model.provenance === 'reconstructed' ? 'Reconstructed · switchMap model' : 'State unavailable');
    badge.dataset.provenance = model.provenance;
    operatorPanel.append(badge);
    const facts = el('dl', 'rr-facts');
    function fact(label: string, content: string | HTMLElement): void {
      const value = el('dd');
      if (typeof content === 'string') value.textContent = content;
      else value.append(content);
      facts.append(el('dt', undefined, label), value);
    }
    const execution = button(operatorId, 'execution');
    execution.dataset.execution = operatorId;
    fact('Execution', execution);
    if (model.provenance === 'reconstructed') {
      fact('Input Controller', model.outerCompleted ? 'Outer completed' : 'No outer completion observed');
      fact('Action Policy', 'Only latest');
      if (model.activeInnerId) {
        const active = button(model.activeInnerId, 'execution');
        active.dataset.execution = model.activeInnerId;
        fact('Active inner', active);
      } else fact('Active inner', 'None');
      fact('Output', model.outputStatus);
    }
    operatorPanel.append(facts, el('p', 'rr-muted', model.explanation));
    const links = el('div', 'rr-links');
    for (const [label, id] of [['Input channel', model.inputSubscriptionId], ['Output channel', model.outputSubscriptionId]] as const) {
      if (!id) continue;
      const channel = button(`${label} ${id}`, 'subscription');
      channel.dataset.subscription = id;
      links.append(channel);
    }
    for (const seq of model.evidenceSeqs.slice(-4)) {
      const evidence = button(`Evidence #${seq}`, 'seek');
      evidence.dataset.seq = String(seq);
      links.append(evidence);
    }
    operatorPanel.append(links);
  }

  function renderLifetimes(state: ReplayState, subscriptionsAtCursor: readonly SubscriptionView[]): void {
    lifetimes.replaceChildren(el('h3', undefined, 'Grouped subscription lifetimes'));
    lifetimes.append(el('p', 'rr-muted', 'Operator channels share an execution. Nested rows are explicitly owned inner executions.'));
    const event = recording.events[currentIndex(state)];
    const executions = getExecutions(recording, state.cursor);
    const ids = new Set(executions.map(execution => execution.id));
    const roots = executions.filter(execution => execution.role !== 'inner' || !execution.ownerExecutionId || !ids.has(execution.ownerExecutionId));
    const selectedId = event ? executionId(event) : undefined;
    const selectedRoot = event?.ownerExecutionId ?? selectedId;
    const visible = visibleWithSelection(roots, selectedRoot, 30);
    if (visible.length === 0) lifetimes.append(el('p', 'rr-caption', 'No subscription has started at this position.'));

    function channelRow(subscription: SubscriptionView): HTMLElement {
      const row = el('div', 'rr-life');
      row.dataset.subscription = subscription.id;
      row.dataset.selected = String(event?.subscriptionId === subscription.id);
      const label = el('div', 'rr-life-label');
      const name = button(`${subscription.port ?? 'output'} · ${subscription.id}`, 'subscription');
      name.className = 'rr-life-name';
      name.dataset.subscription = subscription.id;
      label.append(name, el('span', undefined, `${subscription.outcome} · ${subscription.values} values`));
      const track = el('div', 'rr-life-track');
      const bar = el('span', 'rr-life-bar');
      bar.dataset.outcome = subscription.outcome;
      bar.style.left = `${percent(subscription.start.at)}%`;
      bar.style.width = `${Math.max(0.7, percent((subscription.end?.at ?? event?.at ?? 0) - subscription.start.at))}%`;
      track.append(bar);
      row.append(label, track);
      return row;
    }
    function executionRows(execution: ExecutionSummary, inner: boolean): HTMLElement {
      const container = el('div', inner ? 'rr-inner-group' : 'rr-execution-group');
      container.dataset.execution = execution.id;
      container.dataset.role = execution.role;
      container.dataset.selected = String(inner ? selectedId === execution.id : selectedRoot === execution.id);
      const name = button(`${inner ? `inner ${execution.innerIndex ?? '?'} · ` : `${streamName(execution.streamId)} · `}${execution.id}`, 'execution');
      name.className = 'rr-execution-title';
      name.dataset.execution = execution.id;
      container.append(name, el('p', 'rr-caption', `${execution.role} · ${execution.status} · ${execution.outputNextCount} output values`));
      for (const subscription of subscriptionsAtCursor.filter(item => (item.executionId ?? item.id) === execution.id)) container.append(channelRow(subscription));
      return container;
    }
    for (const execution of visible) {
      const group = executionRows(execution, false);
      const children = executions.filter(inner => inner.role === 'inner' && inner.ownerExecutionId === execution.id);
      const shownChildren = visibleWithSelection(children, selectedId, 40);
      for (const child of shownChildren) group.append(executionRows(child, true));
      if (children.length > shownChildren.length) group.append(el('p', 'rr-caption', `Showing ${shownChildren.length} of ${children.length} inner executions, including the selected inner.`));
      lifetimes.append(group);
    }
    if (visible.length < roots.length) lifetimes.append(el('p', 'rr-caption', `Showing ${visible.length} of ${roots.length} execution groups, including the selected group.`));
    if (recording.capture.droppedEvents > 0) lifetimes.append(el('p', 'rr-caption', 'This recording omits later events. An open subscription does not prove a leak.'));
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
    renderGraph(state);
    renderTimeline(state);
    renderEvents(state);
    renderDetail(state);
  }

  function populateRecording(): void {
    streamsById = new Map(recording.streams.map(stream => [stream.id, stream]));
    eventsByExecution = new Map();
    eventsBySubscription = new Map();
    for (const event of recording.events) {
      const executionEvents = eventsByExecution.get(executionId(event)) ?? [];
      executionEvents.push(event);
      eventsByExecution.set(executionId(event), executionEvents);
      const subscriptionEvents = eventsBySubscription.get(event.subscriptionId) ?? [];
      subscriptionEvents.push(event);
      eventsBySubscription.set(event.subscriptionId, subscriptionEvents);
    }
    focusedSubscriptionId = undefined;
    focusedExecutionId = undefined;
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
    selectValue(roleFilter, '');
    selectValue(portFilter, '');
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

  function resetFilters(): void {
    for (const select of [streamFilter, kindFilter, roleFilter, portFilter]) selectValue(select, '');
  }

  subscriptions.add(fromEvent<MouseEvent>(root, 'click').subscribe(event => {
    const target = event.target as Element | null;
    const control = typeof target?.closest === 'function' ? target.closest<HTMLElement>('[data-action]') : null;
    if (!control || !root.contains(control) || control.hasAttribute('disabled')) return;
    try {
      switch (control.dataset.action) {
        case 'start': controller.seek(0); break;
        case 'previous': controller.step(-1); break;
        case 'next': controller.step(1); break;
        case 'end': controller.seek(recording.events.at(-1)?.seq ?? 0); break;
        case 'play': controller.getState().status === 'playing' ? controller.pause() : controller.play(); break;
        case 'seek': controller.seek(Number(control.dataset.seq)); break;
        case 'stage': {
          const id = control.dataset.stream ?? '';
          focusedSubscriptionId = undefined;
          focusedExecutionId = undefined;
          resetFilters();
          selectValue(streamFilter, id);
          const target = nearestRecorded(recording.events.filter(event => event.streamId === id && event.role !== 'inner'));
          if (target) controller.seek(target.seq);
          renderEvents(controller.getState());
          break;
        }
        case 'execution': {
          focusedExecutionId = control.dataset.execution;
          focusedSubscriptionId = undefined;
          resetFilters();
          if (focusedExecutionId) selectExecution(focusedExecutionId);
          break;
        }
        case 'subscription': {
          focusedSubscriptionId = control.dataset.subscription;
          focusedExecutionId = undefined;
          resetFilters();
          if (focusedSubscriptionId) selectSubscription(focusedSubscriptionId);
          break;
        }
        case 'focus-execution': {
          focusedExecutionId = control.dataset.execution;
          focusedSubscriptionId = undefined;
          resetFilters();
          renderEvents(controller.getState());
          break;
        }
        case 'focus-subscription': {
          focusedSubscriptionId = control.dataset.subscription;
          focusedExecutionId = undefined;
          resetFilters();
          renderEvents(controller.getState());
          break;
        }
        case 'clear-focus': {
          focusedSubscriptionId = undefined;
          focusedExecutionId = undefined;
          renderEvents(controller.getState());
          break;
        }
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
  subscriptions.add(fromEvent(roleFilter, 'change').subscribe(() => renderEvents(controller.getState())));
  subscriptions.add(fromEvent(portFilter, 'change').subscribe(() => renderEvents(controller.getState())));
  const Resize = (doc.defaultView as unknown as { ResizeObserver?: typeof ResizeObserver } | null)?.ResizeObserver;
  if (Resize) {
    const observer = new Resize(drawGraphConnections);
    observer.observe(graph);
    subscriptions.add(() => observer.disconnect());
  } else if (doc.defaultView) subscriptions.add(fromEvent(doc.defaultView, 'resize').subscribe(drawGraphConnections));
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
