import type { CapturedValue, JsonValue } from './types.js';

/** Freeze data we own. Never call this with an application value. */
export function freezeOwned<T>(value: T): T {
  if (value !== null && typeof value === 'object' && !Object.isFrozen(value)) {
    for (const key of Object.keys(value)) freezeOwned((value as Record<string, unknown>)[key]);
    Object.freeze(value);
  }
  return value;
}

/** Bounded, detached previews. Getters and toJSON methods are never evaluated. */
export function captureSnapshot(value: unknown, project?: (value: unknown) => unknown): CapturedValue {
  let budget = 1_000;
  let truncated = false;
  const path = new WeakSet<object>();
  function boundedString(input: string): string {
    if (input.length <= 4_096) return input;
    truncated = true;
    return input.slice(0, 4_096) + '…';
  }
  function omit(reason: string): JsonValue {
    truncated = true;
    return { $type: 'omitted', reason };
  }
  function visit(input: unknown, depth: number): JsonValue {
    if (--budget < 0) return omit('node limit');
    if (input === null || typeof input === 'boolean') return input;
    if (typeof input === 'string') return boundedString(input);
    if (typeof input === 'number') return Number.isFinite(input) ? input : { $type: 'number', value: String(input) };
    if (typeof input === 'undefined') return { $type: 'undefined' };
    if (typeof input === 'bigint') return { $type: 'bigint', value: boundedString(String(input)) };
    if (typeof input === 'symbol') return { $type: 'symbol', value: boundedString(String(input)) };
    if (typeof input === 'function') return omit('function');
    if (depth >= 8) return omit('depth limit');
    if (path.has(input)) return omit('circular reference');
    path.add(input);
    try {
      if (input instanceof Date) {
        const time = Date.prototype.getTime.call(input);
        return { $type: 'Date', value: Number.isFinite(time) ? new Date(time).toISOString() : 'Invalid Date' };
      }
      if (Array.isArray(input)) {
        const output: JsonValue[] = [];
        const length = Math.min(input.length, 64);
        for (let index = 0; index < length && budget > 0; index++) {
          const property = Object.getOwnPropertyDescriptor(input, String(index));
          output.push(!property ? { $type: 'array-hole' } : 'value' in property ? visit(property.value, depth + 1) : omit('accessor'));
        }
        if (output.length < input.length) output.push(omit('array entry limit'));
        return output;
      }
      const output: Record<string, JsonValue> = Object.create(null) as Record<string, JsonValue>;
      const names = Object.getOwnPropertyNames(input);
      let count = 0;
      for (const name of names) {
        if (count++ >= 64 || budget <= 0) {
          output.$omitted = omit('object entry limit');
          break;
        }
        const property = Object.getOwnPropertyDescriptor(input, name);
        if (!property) continue;
        const key = name.length <= 4_096 ? name : name.slice(0, 4_096) + '…';
        if (key !== name) truncated = true;
        output[key] = 'value' in property ? visit(property.value, depth + 1) : omit('accessor');
      }
      if (input instanceof Error) output.$type = 'Error';
      else {
        const prototype = Object.getPrototypeOf(input);
        if (prototype !== null && prototype !== Object.prototype) output.$preview = omit('non-plain object internals');
      }
      if (Object.getOwnPropertySymbols(input).length) output.$symbols = omit('symbol properties');
      return output;
    } finally { path.delete(input); }
  }
  try {
    const data = visit(project ? project(value) : value, 0);
    return freezeOwned({ status: truncated ? 'truncated' : 'captured', data });
  } catch {
    return Object.freeze({ status: 'unavailable', reason: 'Capture failed; live delivery was preserved.' });
  }
}
