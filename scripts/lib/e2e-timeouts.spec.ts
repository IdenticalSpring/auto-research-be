import { EVENT_TIMEOUT_CONFIG as BE_TIMEOUT_CONFIG } from '../../src/common/services/timeout-manager.service';
import {
  AI_TIMEOUT_DICT_SECONDS,
  aiTimeoutFloorMs,
  DEFAULT_EVENT_TIMEOUT_MS,
  EVENT_TIMEOUT_CONFIG,
  MIN_E2E_TIMEOUT_MS,
  resolveEventTimeout,
} from './e2e-timeouts';

describe('e2e-timeouts', () => {
  it('parity với EVENT_TIMEOUT_CONFIG của BE', () => {
    expect(EVENT_TIMEOUT_CONFIG).toEqual(BE_TIMEOUT_CONFIG);
  });

  it('áp biên 20% và sàn tối thiểu', () => {
    // chuyen_de_2: BE 1800s × 1.2 = 2160s
    expect(resolveEventTimeout('chuyen_de_2')).toBe(2_160_000);
    // ai_in_doc: BE 240s × 1.2 = 288s
    expect(resolveEventTimeout('ai_in_doc')).toBe(288_000);
  });

  it('event không biết ⇒ default 120s nhưng vẫn bị sàn 240s', () => {
    expect(
      resolveEventTimeout('khong_ton_tai', { env: {} }),
    ).toBe(MIN_E2E_TIMEOUT_MS);
    expect(
      resolveEventTimeout(undefined, { env: {} }),
    ).toBe(Math.max(DEFAULT_EVENT_TIMEOUT_MS, MIN_E2E_TIMEOUT_MS));
  });

  it('override từ env HARNESS_TIMEOUT_MS thắng cấu hình', () => {
    expect(
      resolveEventTimeout('chuyen_de_2', {
        env: { HARNESS_TIMEOUT_MS: '60000' },
      }),
    ).toBe(60_000);
  });

  it('override trực tiếp thắng cả env', () => {
    expect(
      resolveEventTimeout('write_content', {
        override: 5_000,
        env: { HARNESS_TIMEOUT_MS: '90000' },
      }),
    ).toBe(5_000);
  });
});

describe('e2e-timeouts — sàn TIME_OUT_DICT của AI (quyết định 2026-11-01)', () => {
  it('BE phải cấu hình ≥ AI cho mọi event AI có đặt timeout', () => {
    for (const [event, seconds] of Object.entries(AI_TIMEOUT_DICT_SECONDS)) {
      if (seconds <= 0) continue;
      const beSeconds = (EVENT_TIMEOUT_CONFIG[event] ?? 0) / 1000;
      expect({ event, beSeconds }).toEqual({
        event,
        beSeconds: expect.any(Number),
      });
      expect(beSeconds).toBeGreaterThanOrEqual(seconds);
    }
  });

  it('harness chờ ≥ AI kể cả khi BE config thấp hơn', () => {
    expect(resolveEventTimeout('search_papers')).toBeGreaterThanOrEqual(
      300_000,
    );
    expect(resolveEventTimeout('generate_outline')).toBeGreaterThanOrEqual(
      1_500_000,
    );
    expect(resolveEventTimeout('chuyen_de_1')).toBeGreaterThanOrEqual(1_800_000);
    expect(resolveEventTimeout('outer_chatbot')).toBeGreaterThanOrEqual(240_000);
  });

  it('AI_TIMEOUT_DICT marker 0 ⇒ sàn 3600s cho 4 event tài liệu', () => {
    for (const event of [
      'admin_documents',
      'user_guide_documents',
      'user_documents',
      'embed_user_documents',
    ]) {
      expect(AI_TIMEOUT_DICT_SECONDS[event]).toBe(3600);
      expect(
        (EVENT_TIMEOUT_CONFIG[event] ?? 0) / 1000,
      ).toBeGreaterThanOrEqual(3600);
      expect(resolveEventTimeout(event)).toBeGreaterThanOrEqual(3_600_000);
    }
  });

  it('aiTimeoutFloorMs trả 0 cho event AI không đặt timeout', () => {
    expect(aiTimeoutFloorMs('khong_ton_tai')).toBe(0);
    expect(aiTimeoutFloorMs(undefined)).toBe(0);
  });
});
