import { fromEvent, map } from 'rxjs';
import { mountReplay } from '../src/viewer.js';
import { recordCounter, recordSearch } from './fixtures.js';

const container = document.querySelector<HTMLElement>('#replay')!;
const selector = document.querySelector<HTMLSelectElement>('#scenario')!;
const viewer = mountReplay(container, recordSearch());

function selectScenario() {
  if (selector.value === 'counter') {
    return recordCounter();
  }
  return recordSearch();
}
const scenarioSubscription = fromEvent(selector, 'change').pipe(map(selectScenario)).subscribe(viewer.setRecording);
fromEvent(window, 'pagehide').subscribe(() => {
  scenarioSubscription.unsubscribe();
  viewer.destroy();
});
