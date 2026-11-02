import {
  buildKafkaRequestEnvelope,
  KafkaEnvelopeParams,
} from './kafka-envelope.helper';

const ENV_KEYS = ['AI_DEFAULT_MODEL', 'DEFAULT_AI_MODEL', 'AI_ALLOWED_MODELS'] as const;

const baseParams: KafkaEnvelopeParams = {
  eventType: 'search_papers',
  userId: 'user-uuid-1234',
  documentId: 'doc-1',
};

let snapshot: Record<string, string | undefined> = {};

beforeEach(() => {
  snapshot = {};
  for (const key of ENV_KEYS) {
    snapshot[key] = process.env[key];
    delete process.env[key];
  }
});

afterEach(() => {
  for (const key of ENV_KEYS) {
    const value = snapshot[key];
    if (value === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = value;
    }
  }
});

describe('buildKafkaRequestEnvelope - model_id theo cấu hình tập trung', () => {
  it('T12: env đặt deepseek-flash thì payload mang deepseek-flash', () => {
    process.env.AI_ALLOWED_MODELS =
      'gemini-3.5-flash-lite,deepseek-flash,localhost';
    process.env.AI_DEFAULT_MODEL = 'deepseek-flash';

    const envelope = buildKafkaRequestEnvelope(baseParams);

    expect(envelope.model_id).toBe('deepseek-flash');
    expect(envelope.event_type).toBe('search_papers');
    expect(envelope.language).toBe('Vietnamese');
    expect(envelope.session_id).toBe('default');
  });

  it('T13: env không đặt thì payload dùng deepseek-flash', () => {
    const envelope = buildKafkaRequestEnvelope(baseParams);

    expect(envelope.model_id).toBe('deepseek-flash');
  });

  it('T13b: env không hợp lệ thì payload dùng allowed[0]', () => {
    process.env.AI_ALLOWED_MODELS = 'gemini-3.5-flash-lite,deepseek-flash';
    process.env.AI_DEFAULT_MODEL = 'unknown-model';

    const envelope = buildKafkaRequestEnvelope(baseParams);

    expect(envelope.model_id).toBe('gemini-3.5-flash-lite');
  });

  it('modelId truyền tường minh vẫn được ưu tiên hơn cấu hình', () => {
    process.env.AI_ALLOWED_MODELS = 'gemini-3.5-flash-lite,deepseek-flash';
    process.env.AI_DEFAULT_MODEL = 'gemini-3.5-flash-lite';

    const envelope = buildKafkaRequestEnvelope({
      ...baseParams,
      modelId: 'deepseek-flash',
    });

    expect(envelope.model_id).toBe('deepseek-flash');
  });
});
