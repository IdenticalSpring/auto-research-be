import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';
import { BaseKafkaRequestDto } from './base-kafka-request.dto';

const ENV_KEYS = ['AI_DEFAULT_MODEL', 'DEFAULT_AI_MODEL', 'AI_ALLOWED_MODELS'] as const;

const validPayload = {
  event_type: 'search_papers',
  model_id: 'deepseek-flash',
  user_id: 'user-uuid-1234',
  document_id: 'doc-1',
  session_id: 'session-1',
  language: 'vi',
};

let snapshot: Record<string, string | undefined> = {};

async function validateModel(modelId: string) {
  const dto = plainToInstance(BaseKafkaRequestDto, {
    ...validPayload,
    model_id: modelId,
  });
  return validate(dto);
}

beforeEach(() => {
  snapshot = {};
  for (const key of ENV_KEYS) {
    snapshot[key] = process.env[key];
    delete process.env[key];
  }
  process.env.AI_ALLOWED_MODELS =
    'gemini-3.5-flash-lite,deepseek-flash,localhost';
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

describe('BaseKafkaRequestDto.model_id', () => {
  it('T15: chấp nhận deepseek-flash nằm trong AI_ALLOWED_MODELS', async () => {
    const errors = await validateModel('deepseek-flash');

    expect(errors).toHaveLength(0);
  });

  it('T15b: chấp nhận model mới được thêm vào AI_ALLOWED_MODELS tại runtime', async () => {
    process.env.AI_ALLOWED_MODELS =
      'gemini-3.5-flash-lite,deepseek-flash,new-model-x';

    const errors = await validateModel('new-model-x');

    expect(errors).toHaveLength(0);
  });

  it('T15c: từ chối model không nằm trong whitelist', async () => {
    const errors = await validateModel('gpt-4o');

    const modelError = errors.find((e) => e.property === 'model_id');
    expect(modelError).toBeDefined();
    const messages = Object.values(modelError?.constraints ?? {});
    expect(messages.join(' ')).toContain('model_id must be one of');
    expect(messages.join(' ')).toContain('deepseek-flash');
  });

  it('T15d: từ chối model rỗng', async () => {
    const errors = await validateModel('');

    const modelError = errors.find((e) => e.property === 'model_id');
    expect(modelError).toBeDefined();
  });
});
