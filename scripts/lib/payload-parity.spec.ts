import { readFileSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { LIVE_CASES } from './live-cases';

/**
 * Payload parity với snapshot chính thức của AI server
 * (`scripts/payloads/ai/*.json`, nguồn: `autoresearching/scripts/payloads/`).
 *
 * Quy tắc (quyết định 2026-11-01 §1.8):
 * - Key BẮT BUỘC của AI phải có mặt và **cùng kiểu** trong payload BE.
 * - BE được phép gửi THÊM key (AI không cấm), trừ structured-output params (§1.6).
 * - `model_id` KHÔNG so sánh với snapshot (snapshot để placeholder `__MODEL_ID__`);
 *   BE luôn gửi `deepseek-flash` theo `AI_DEFAULT_MODEL` (xem `model-id.policy.spec.ts`).
 * - Các key định danh do E2E sinh (document_id/session_id/...) không so giá trị.
 */

const SNAPSHOT_DIR = join(process.cwd(), 'scripts', 'payloads', 'ai');

/** Key mà giá trị do môi trường sinh ra, chỉ cần đúng kiểu. */
const VOLATILE_KEYS = new Set([
  'model_id',
  'document_id',
  'session_id',
  'conv_id',
  'user_id',
  'job_id',
  'data_id',
  'thread_id',
]);

type Shape = 'string' | 'number' | 'boolean' | 'array' | 'object' | 'null';

function shapeOf(value: unknown): Shape {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'array';
  if (typeof value === 'object') return 'object';
  if (typeof value === 'number') return 'number';
  if (typeof value === 'boolean') return 'boolean';
  return 'string';
}

function compare(
  path: string,
  expected: unknown,
  actual: unknown,
  problems: string[],
): void {
  if (actual === undefined) {
    problems.push(`${path}: thiếu hẳn`);
    return;
  }
  const expectedShape = shapeOf(expected);
  const actualShape = shapeOf(actual);
  if (expectedShape !== actualShape) {
    problems.push(`${path}: kiểu ${actualShape} ≠ ${expectedShape} của AI`);
    return;
  }
  if (expectedShape === 'array') {
    const exp = expected as unknown[];
    const act = actual as unknown[];
    if (exp.length > 0 && act.length > 0) {
      compare(`${path}[0]`, exp[0], act[0], problems);
    }
    return;
  }
  if (expectedShape === 'object') {
    for (const [key, value] of Object.entries(
      expected as Record<string, unknown>,
    )) {
      if (VOLATILE_KEYS.has(key)) continue;
      compare(`${path}.${key}`, value, (actual as Record<string, unknown>)[key], problems);
    }
  }
}

const CTX = {
  documentId: 'e2e-live-1-parity',
  sessionId: 'sess-parity',
  userId: '00000000-0000-0000-0000-000000000001',
  modelId: 'deepseek-flash',
};

const snapshots = readdirSync(SNAPSHOT_DIR).filter((f) => f.endsWith('.json'));

describe.each(snapshots)('payload parity — %s', (file) => {
  const snapshot = JSON.parse(
    readFileSync(join(SNAPSHOT_DIR, file), 'utf8'),
  ) as Record<string, unknown>;
  const eventType = String(snapshot.event_type ?? file.replace('.json', ''));
  const liveCase = LIVE_CASES.find((item) => item.eventType === eventType);

  it(`có case live cho ${eventType}`, () => {
    expect(liveCase).toBeDefined();
  });

  it('payload BE phủ đủ key bắt buộc của AI (đúng kiểu)', () => {
    if (!liveCase) return;
    const payload = liveCase.buildPayload(CTX) as Record<string, unknown>;
    const problems: string[] = [];

    for (const [key, value] of Object.entries(snapshot)) {
      if (VOLATILE_KEYS.has(key)) {
        if (payload[key] === undefined) problems.push(`${key}: thiếu hẳn`);
        continue;
      }
      compare(key, value, payload[key], problems);
    }

    expect(problems).toEqual([]);
  });

  it('model_id không lấy từ snapshot mà do BE sinh (deepseek-flash)', () => {
    if (!liveCase) return;
    const payload = liveCase.buildPayload(CTX) as Record<string, unknown>;
    expect(payload.model_id).toBe('deepseek-flash');
  });
});

describe('payload contract — headings_percent thang 0..1 (AI Q1)', () => {
  const PERCENT_KEYS = ['headings_percent', 'new_headings_percent'];

  it.each(LIVE_CASES.map((item) => [item.id, item] as const))(
    '%s: không gửi percent thang 0..100',
    (_id, liveCase) => {
      const payload = liveCase.buildPayload(CTX) as Record<string, unknown>;
      for (const key of PERCENT_KEYS) {
        const value = payload[key];
        if (!Array.isArray(value) || value.length === 0) continue;
        const numbers = value as number[];
        const sum = numbers.reduce((total, item) => total + item, 0);
        expect(sum).toBeCloseTo(1, 4);
        for (const percent of numbers) {
          expect(percent).toBeLessThanOrEqual(1);
        }
      }

      // percent lồng trong heading/outline cũng phải ≤ 1
      const collect = (list: unknown) =>
        Array.isArray(list)
          ? list
              .map((item) => (item as Record<string, unknown>)?.percent)
              .filter((percent): percent is number => typeof percent === 'number')
          : [];
      const nested = [
        ...collect(payload.headings),
        ...collect(payload.new_headings),
        ...collect((payload.outline as Record<string, unknown>)?.outline),
        ...collect((payload.outline as Record<string, unknown>)?.headings),
      ];
      for (const percent of nested) {
        expect(percent).toBeLessThanOrEqual(1);
      }
    },
  );
});

