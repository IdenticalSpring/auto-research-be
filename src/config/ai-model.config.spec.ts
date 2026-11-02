import {
  AI_MODEL_FALLBACK,
  DEFAULT_ALLOWED_MODELS,
  getAiProvider,
  parseAllowedModels,
  resolveAiModelConfig,
} from './ai-model.config';

const ENV_KEYS = [
  'AI_DEFAULT_MODEL',
  'DEFAULT_AI_MODEL',
  'AI_ALLOWED_MODELS',
] as const;

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

describe('resolveAiModelConfig - ưu tiên AI_DEFAULT_MODEL', () => {
  it('T1: chọn AI_DEFAULT_MODEL khi thuộc whitelist và giữ nguyên thứ tự allowed', () => {
    process.env.AI_ALLOWED_MODELS =
      'gemini-3.5-flash-lite,deepseek-flash,localhost';
    process.env.AI_DEFAULT_MODEL = 'gemini-3.5-flash-lite';

    const config = resolveAiModelConfig();

    expect(config.defaultModel).toBe('gemini-3.5-flash-lite');
    expect(config.allowedModels).toEqual([
      'gemini-3.5-flash-lite',
      'deepseek-flash',
      'localhost',
    ]);
    expect(config.warnings).toEqual([]);
  });

  it('T2: AI_DEFAULT_MODEL=deepseek-flash được ưu tiên cao nhất', () => {
    process.env.AI_ALLOWED_MODELS =
      'gemini-3.5-flash-lite,deepseek-flash,localhost';
    process.env.AI_DEFAULT_MODEL = 'deepseek-flash';

    const config = resolveAiModelConfig();

    expect(config.defaultModel).toBe('deepseek-flash');
    expect(config.requestedModel).toBe('deepseek-flash');
    expect(config.warnings).toEqual([]);
  });

  it('T3: danh sách nhiều model — default vẫn thắng, các model khác là dự phòng', () => {
    process.env.AI_ALLOWED_MODELS =
      'gemini-3.5-flash-lite,deepseek-flash,localhost,other-model';
    process.env.AI_DEFAULT_MODEL = 'deepseek-flash';

    const config = resolveAiModelConfig();

    expect(config.defaultModel).toBe('deepseek-flash');
    expect(config.allowedModels).toEqual([
      'gemini-3.5-flash-lite',
      'deepseek-flash',
      'localhost',
      'other-model',
    ]);
    expect(config.fallbackModel).toBe('gemini-3.5-flash-lite');
  });
});

describe('resolveAiModelConfig - fallback & cảnh báo', () => {
  it('T4: không đặt biến nào — mặc định deepseek-flash', () => {
    const config = resolveAiModelConfig();

    expect(config.defaultModel).toBe('deepseek-flash');
    expect(config.allowedModels).toEqual([...DEFAULT_ALLOWED_MODELS]);
    expect(config.requestedModel).toBeNull();
    expect(config.warnings.map((w) => w.reason)).toEqual([
      'EMPTY_ALLOWED_MODELS',
      'EMPTY_DEFAULT_MODEL',
    ]);
  });

  it('T5: AI_DEFAULT_MODEL rỗng/khoảng trắng — fallback allowed[0] + cảnh báo', () => {
    process.env.AI_ALLOWED_MODELS = 'deepseek-flash,gemini-3.5-flash-lite';
    process.env.AI_DEFAULT_MODEL = '   ';

    const config = resolveAiModelConfig();

    expect(config.defaultModel).toBe('deepseek-flash');
    expect(config.requestedModel).toBeNull();
    expect(config.warnings.map((w) => w.reason)).toEqual(['EMPTY_DEFAULT_MODEL']);
    expect(config.warnings[0].selectedModel).toBe('deepseek-flash');
  });

  it('T6: AI_DEFAULT_MODEL không hợp lệ — fallback allowed[0] + cảnh báo', () => {
    process.env.AI_ALLOWED_MODELS = 'gemini-3.5-flash-lite,deepseek-flash';
    process.env.AI_DEFAULT_MODEL = 'unknown-model';

    const config = resolveAiModelConfig();

    expect(config.defaultModel).toBe('gemini-3.5-flash-lite');
    expect(config.requestedModel).toBe('unknown-model');
    expect(config.warnings).toHaveLength(1);
    expect(config.warnings[0].reason).toBe('DEFAULT_MODEL_NOT_ALLOWED');
    expect(config.warnings[0].requestedModel).toBe('unknown-model');
  });

  it('T7: AI_ALLOWED_MODELS rỗng/không hợp lệ — dùng whitelist mặc định', () => {
    process.env.AI_ALLOWED_MODELS = ' , ,, ';
    process.env.AI_DEFAULT_MODEL = 'deepseek-flash';

    const config = resolveAiModelConfig();

    expect(config.allowedModels).toEqual([...DEFAULT_ALLOWED_MODELS]);
    // deepseek-flash thuộc whitelist mặc định nên vẫn được ưu tiên.
    expect(config.defaultModel).toBe('deepseek-flash');
    expect(config.warnings.map((w) => w.reason)).toEqual([
      'EMPTY_ALLOWED_MODELS',
    ]);
  });

  it('T7b: whitelist rỗng và default không thuộc whitelist mặc định — về AI_MODEL_FALLBACK', () => {
    process.env.AI_ALLOWED_MODELS = '';
    process.env.AI_DEFAULT_MODEL = 'unknown-model';

    const config = resolveAiModelConfig();

    expect(config.defaultModel).toBe(AI_MODEL_FALLBACK);
    expect(config.warnings.map((w) => w.reason)).toEqual([
      'EMPTY_ALLOWED_MODELS',
      'DEFAULT_MODEL_NOT_ALLOWED',
    ]);
  });

  it('T8: giá trị có dấu nháy và khoảng trắng vẫn được nhận diện đúng', () => {
    process.env.AI_ALLOWED_MODELS =
      '"deepseek-flash, gemini-3.5-flash-lite"';
    process.env.AI_DEFAULT_MODEL = ' deepseek-flash ';

    const config = resolveAiModelConfig();

    expect(config.allowedModels).toEqual([
      'deepseek-flash',
      'gemini-3.5-flash-lite',
    ]);
    expect(config.defaultModel).toBe('deepseek-flash');
    expect(config.warnings).toEqual([]);
  });

  it("T8b: giá trị dùng nháy đơn vẫn được strip", () => {
    process.env.AI_ALLOWED_MODELS = "'deepseek-flash'";
    process.env.AI_DEFAULT_MODEL = "deepseek-flash";

    const config = resolveAiModelConfig();

    expect(config.allowedModels).toEqual(['deepseek-flash']);
    expect(config.defaultModel).toBe('deepseek-flash');
  });

  it('T9: whitelist trùng lặp được dedupe, giữ thứ tự xuất hiện đầu', () => {
    process.env.AI_ALLOWED_MODELS =
      'deepseek-flash,deepseek-flash,localhost,localhost';

    const config = resolveAiModelConfig();

    expect(config.allowedModels).toEqual(['deepseek-flash', 'localhost']);
  });

  it('T10: KHÔNG unshift model mặc định vào whitelist', () => {
    process.env.AI_ALLOWED_MODELS = 'gemini-3.5-flash-lite,localhost';
    process.env.AI_DEFAULT_MODEL = 'deepseek-flash';

    const config = resolveAiModelConfig();

    expect(config.allowedModels).toEqual(['gemini-3.5-flash-lite', 'localhost']);
    expect(config.allowedModels).not.toContain('deepseek-flash');
    expect(config.defaultModel).toBe('gemini-3.5-flash-lite');
    expect(config.warnings[0].reason).toBe('DEFAULT_MODEL_NOT_ALLOWED');
  });

  it('T11: tương thích ngược với DEFAULT_AI_MODEL', () => {
    process.env.AI_ALLOWED_MODELS = 'gemini-3.5-flash-lite,deepseek-flash';
    process.env.DEFAULT_AI_MODEL = 'deepseek-flash';

    const config = resolveAiModelConfig();

    expect(config.defaultModel).toBe('deepseek-flash');
    expect(config.warnings).toEqual([]);
  });
});

describe('parseAllowedModels', () => {
  it('trả mảng rỗng với giá trị undefined', () => {
    expect(parseAllowedModels(undefined)).toEqual([]);
  });

  it('bỏ phần tử rỗng và trim từng phần tử', () => {
    expect(parseAllowedModels(' a , , b ')).toEqual(['a', 'b']);
  });

  it('strip nháy bao ngoài toàn chuỗi và từng phần tử', () => {
    expect(parseAllowedModels('"a,b"')).toEqual(['a', 'b']);
    expect(parseAllowedModels('"a","b"')).toEqual(['a', 'b']);
    expect(parseAllowedModels('x')).toEqual(['x']);
    expect(parseAllowedModels('a,"b')).toEqual(['a', 'b']);
  });
});

describe('getAiProvider', () => {
  it('T14: map đúng provider theo tên model', () => {
    expect(getAiProvider('gemini-3.5-flash-lite')).toBe('gemini');
    expect(getAiProvider('deepseek-flash')).toBe('deepseek');
    expect(getAiProvider('localhost')).toBe('local');
    expect(getAiProvider('localllm-v1')).toBe('local');
    expect(getAiProvider('gpt-4o')).toBe('unknown');
    expect(getAiProvider('  "deepseek-flash"  ')).toBe('deepseek');
  });
});
