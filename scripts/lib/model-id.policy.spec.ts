import { jest } from '@jest/globals';
import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { LIVE_CASES } from './live-cases';
import { SCRIPT_AI_MODEL_FALLBACK, SCRIPT_DEFAULT_ALLOWED_MODELS } from './ai-model';

const BASE_CTX = {
  documentId: 'e2e-live-123-model-policy',
  sessionId: 'sess-model-policy',
  userId: '00000000-0000-0000-0000-000000000001',
  modelId: 'deepseek-flash',
};

describe('Model policy — deepseek-flash', () => {
  it('fallback của script phải là deepseek-flash (không phải gemini)', () => {
    expect(SCRIPT_AI_MODEL_FALLBACK).toBe('deepseek-flash');
    expect(SCRIPT_DEFAULT_ALLOWED_MODELS[0]).toBe('deepseek-flash');
  });

  it('mọi case live gửi model_id đúng ctx, không hardcode model khác', () => {
    for (const item of LIVE_CASES) {
      const payload = item.buildPayload(BASE_CTX);
      expect(payload.model_id).toBe('deepseek-flash');
    }
  });

  it('model_id lấy từ ctx (sentinel) ⇒ chứng minh không case nào hardcode', () => {
    for (const item of LIVE_CASES) {
      const payload = item.buildPayload({
        ...BASE_CTX,
        modelId: 'sentinel-model',
      });
      expect(payload.model_id).toBe('sentinel-model');
    }
  });

  it('snapshot payload của AI (nếu có) không chứa gemini', () => {
    const dir = join(process.cwd(), 'scripts', 'payloads', 'ai');
    if (!existsSync(dir)) {
      // Snapshot chưa được vendor — rule này kích hoạt ngay khi có file.
      expect(true).toBe(true);
      return;
    }
    for (const file of readdirSync(dir).filter((f) => f.endsWith('.json'))) {
      const content = readFileSync(join(dir, file), 'utf8');
      expect(content.toLowerCase()).not.toContain('gemini');
    }
  });
});
