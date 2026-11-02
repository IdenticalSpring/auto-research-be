import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { LIVE_CASES } from './live-cases';

/**
 * Guard theo quyết định của AI server (2026-11-01):
 * - §1.6: BE KHÔNG gửi tham số structured output (`response_format`,
 *   `json_schema`, `tool_choice`); model tự đi plain-JSON.
 * - §1.3: request có `section_id` là hợp lệ, nhưng không case nào được kỳ vọng
 *   AI echo lại field này.
 */

const FORBIDDEN_PAYLOAD_KEYS = [
  'response_format',
  'responseFormat',
  'json_schema',
  'jsonSchema',
  'tool_choice',
  'toolChoice',
];

function walk(dir: string, out: string[] = []): string[] {
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) {
      walk(full, out);
    } else if (entry.endsWith('.ts') && !entry.endsWith('.spec.ts')) {
      out.push(full);
    }
  }
  return out;
}

const CTX = {
  documentId: 'e2e-live-1-contract',
  sessionId: 'sess-contract',
  userId: '00000000-0000-0000-0000-000000000001',
  modelId: 'deepseek-flash',
};

describe('payload contract — không gửi structured output params (§1.6)', () => {
  it('không case live nào gửi key structured output', () => {
    for (const item of LIVE_CASES) {
      const payload = item.buildPayload(CTX);
      for (const key of FORBIDDEN_PAYLOAD_KEYS) {
        expect(payload).not.toHaveProperty(key);
      }
    }
  });

  it('source src/ không tham chiếu response_format/json_schema/tool_choice', () => {
    const offenders: string[] = [];
    for (const file of walk(join(process.cwd(), 'src'))) {
      const content = readFileSync(file, 'utf8');
      for (const key of ['response_format', 'json_schema', 'tool_choice']) {
        if (content.includes(key)) offenders.push(`${file} ↔ ${key}`);
      }
    }
    expect(offenders).toEqual([]);
  });
});
