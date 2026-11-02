import { resolveAiModelConfig } from '../../config/ai-model.config';
import {
  AI_DEFAULT_MODEL,
  AI_FALLBACK_MODEL,
  DEFAULT_AI_MODEL,
  SUPPORTED_KAFKA_MODELS,
  getAiModelConfig,
} from './kafka.constants';

describe('kafka.constants - model mặc định', () => {
  it('AI_DEFAULT_MODEL khớp với resolver tập trung', () => {
    expect(AI_DEFAULT_MODEL).toBe(resolveAiModelConfig().defaultModel);
  });

  it('DEFAULT_AI_MODEL giữ alias tương thích ngược', () => {
    expect(DEFAULT_AI_MODEL).toBe(AI_DEFAULT_MODEL);
  });

  it('AI_FALLBACK_MODEL là phần tử đầu tiên của whitelist khởi tạo', () => {
    expect(AI_FALLBACK_MODEL).toBe(SUPPORTED_KAFKA_MODELS[0]);
  });

  it('SUPPORTED_KAFKA_MODELS chỉ chứa model thuộc whitelist cấu hình', () => {
    const allowed = resolveAiModelConfig().allowedModels;
    expect(SUPPORTED_KAFKA_MODELS).toEqual(allowed);
    expect(SUPPORTED_KAFKA_MODELS).toContain(AI_DEFAULT_MODEL);
  });

  it('getAiModelConfig() resolve lại theo process.env hiện hành', () => {
    const previousDefault = process.env.AI_DEFAULT_MODEL;
    const previousAllowed = process.env.AI_ALLOWED_MODELS;
    try {
      process.env.AI_ALLOWED_MODELS =
        'gemini-3.5-flash-lite,deepseek-flash,localhost';
      process.env.AI_DEFAULT_MODEL = 'deepseek-flash';

      const config = getAiModelConfig();

      expect(config.defaultModel).toBe('deepseek-flash');
      expect(config.allowedModels).toContain('deepseek-flash');
    } finally {
      if (previousDefault === undefined) {
        delete process.env.AI_DEFAULT_MODEL;
      } else {
        process.env.AI_DEFAULT_MODEL = previousDefault;
      }
      if (previousAllowed === undefined) {
        delete process.env.AI_ALLOWED_MODELS;
      } else {
        process.env.AI_ALLOWED_MODELS = previousAllowed;
      }
    }
  });
});
