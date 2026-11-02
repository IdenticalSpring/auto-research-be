import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { jest } from '@jest/globals';
import { resolveAiModelConfig } from '../../src/config/ai-model.config';
import {
  SCRIPT_DEFAULT_ALLOWED_MODELS,
  assertStrictModel,
  hydrateEnvFromFile,
  logScriptModelConfig,
  resolveScriptAllowedModels,
  resolveScriptModelConfig,
} from './ai-model';

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

describe('resolveScriptModelConfig', () => {
  it('A-t1: default ngoài whitelist ⇒ fallback allowed[0] + lý do', () => {
    process.env.AI_ALLOWED_MODELS = 'gemini-3.5-flash-lite,localhost';
    process.env.AI_DEFAULT_MODEL = 'deepseek-flash';

    const config = resolveScriptModelConfig({ hydrate: false });

    expect(config.selectedModel).toBe('gemini-3.5-flash-lite');
    expect(config.requestedModel).toBe('deepseek-flash');
    expect(config.fallbackReason).toBe('DEFAULT_MODEL_NOT_ALLOWED');
    expect(config.allowedModels).toEqual(['gemini-3.5-flash-lite', 'localhost']);
  });

  it('A-t2: default rỗng ⇒ fallback allowed[0] + EMPTY_DEFAULT_MODEL', () => {
    process.env.AI_ALLOWED_MODELS = 'deepseek-flash,gemini-3.5-flash-lite';
    process.env.AI_DEFAULT_MODEL = '   ';

    const config = resolveScriptModelConfig({ hydrate: false });

    expect(config.selectedModel).toBe('deepseek-flash');
    expect(config.requestedModel).toBeNull();
    expect(config.fallbackReason).toBe('EMPTY_DEFAULT_MODEL');
  });

  it('A-t3: whitelist rỗng ⇒ dùng danh sách mặc định + EMPTY_ALLOWED_MODELS', () => {
    process.env.AI_ALLOWED_MODELS = ' , ,, ';

    const config = resolveScriptModelConfig({ hydrate: false });

    expect(config.allowedModels).toEqual([...SCRIPT_DEFAULT_ALLOWED_MODELS]);
    expect(config.selectedModel).toBe(SCRIPT_DEFAULT_ALLOWED_MODELS[0]);
    expect(config.warnings.map((warning) => warning.reason)).toEqual([
      'EMPTY_ALLOWED_MODELS',
      'EMPTY_DEFAULT_MODEL',
    ]);
    expect(config.fallbackReason).toBe('EMPTY_DEFAULT_MODEL');
  });

  it('A-t4: default thuộc whitelist ⇒ không fallback', () => {
    process.env.AI_ALLOWED_MODELS =
      'gemini-3.5-flash-lite,deepseek-flash,localhost';
    process.env.AI_DEFAULT_MODEL = 'deepseek-flash';

    const config = resolveScriptModelConfig({ hydrate: false });

    expect(config.selectedModel).toBe('deepseek-flash');
    expect(config.fallbackReason).toBeUndefined();
    expect(config.warnings).toEqual([]);
  });
});

describe('logScriptModelConfig', () => {
  it('in cảnh báo fallback trước dòng AI Model', () => {
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
    process.env.AI_ALLOWED_MODELS = 'gemini-3.5-flash-lite,localhost';
    process.env.AI_DEFAULT_MODEL = 'deepseek-flash';

    const config = resolveScriptModelConfig({ hydrate: false });
    logScriptModelConfig(config);

    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining('[DEFAULT_MODEL_NOT_ALLOWED]'),
    );
    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining('AI_DEFAULT_MODEL="deepseek-flash"'),
    );
    expect(logSpy).toHaveBeenCalledWith(
      expect.stringContaining('AI Model: gemini-3.5-flash-lite'),
    );
    expect(logSpy).toHaveBeenCalledWith(
      expect.stringContaining('requested: deepseek-flash'),
    );

    warnSpy.mockRestore();
    logSpy.mockRestore();
  });

  it('không cảnh báo khi cấu hình hợp lệ', () => {
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
    process.env.AI_ALLOWED_MODELS = 'deepseek-flash,gemini-3.5-flash-lite';
    process.env.AI_DEFAULT_MODEL = 'gemini-3.5-flash-lite';

    logScriptModelConfig(resolveScriptModelConfig({ hydrate: false }));

    expect(warnSpy).not.toHaveBeenCalled();
    expect(logSpy).toHaveBeenCalled();

    warnSpy.mockRestore();
    logSpy.mockRestore();
  });
});

describe('hydrateEnvFromFile', () => {
  it('A-t5: chỉ bù biến còn thiếu, không ghi đè biến đã có', () => {
    const dir = mkdtempSync(join(tmpdir(), 'ai-model-env-'));
    const envFile = join(dir, '.env');
    writeFileSync(
      envFile,
      'AI_DEFAULT_MODEL="deepseek-flash"\nAI_ALLOWED_MODELS="gemini-3.5-flash-lite,deepseek-flash"\n',
    );
    try {
      process.env.AI_DEFAULT_MODEL = 'gemini-3.5-flash-lite';

      const hydrated = hydrateEnvFromFile(envFile);

      expect(hydrated).toEqual(['AI_ALLOWED_MODELS']);
      expect(process.env.AI_DEFAULT_MODEL).toBe('gemini-3.5-flash-lite');
      expect(process.env.AI_ALLOWED_MODELS).toBe(
        'gemini-3.5-flash-lite,deepseek-flash',
      );
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('bỏ qua file không tồn tại và dòng comment', () => {
    const dir = mkdtempSync(join(tmpdir(), 'ai-model-env-'));
    const envFile = join(dir, '.env');
    writeFileSync(envFile, '# comment only\n\n');
    try {
      expect(hydrateEnvFromFile(join(dir, 'khong-ton-tai'))).toEqual([]);
      expect(hydrateEnvFromFile(envFile)).toEqual([]);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe('parity với src/config/ai-model.config.ts', () => {
  const cases: Array<{ defaultModel: string; allowed: string }> = [
    {
      defaultModel: 'deepseek-flash',
      allowed: 'gemini-3.5-flash-lite,deepseek-flash,localhost',
    },
    {
      defaultModel: 'gemini-3.5-flash-lite',
      allowed: 'gemini-3.5-flash-lite,deepseek-flash,localhost',
    },
    {
      defaultModel: 'unknown-model',
      allowed: 'gemini-3.5-flash-lite,deepseek-flash',
    },
    {
      defaultModel: 'deepseek-flash',
      allowed: 'gemini-3.5-flash-lite,localhost',
    },
  ];

  it.each(cases)(
    'A-t6: default=$defaultModel allowed=$allowed khớp resolver chính',
    ({ defaultModel, allowed }) => {
      process.env.AI_ALLOWED_MODELS = allowed;
      process.env.AI_DEFAULT_MODEL = defaultModel;

      const canonical = resolveAiModelConfig();
      const scriptConfig = resolveScriptModelConfig({ hydrate: false });

      expect(scriptConfig.selectedModel).toBe(canonical.defaultModel);
      expect(scriptConfig.allowedModels).toEqual(canonical.allowedModels);
      expect(resolveScriptAllowedModels()).toEqual(canonical.allowedModels);
    },
  );
});

describe('assertStrictModel', () => {
  it('thoát code 2 khi có fallback và bật strict', () => {
    const exitSpy = jest
      .spyOn(process, 'exit')
      .mockImplementation((() => undefined) as never);
    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    process.env.AI_ALLOWED_MODELS = 'gemini-3.5-flash-lite,localhost';
    process.env.AI_DEFAULT_MODEL = 'deepseek-flash';
    const config = resolveScriptModelConfig({ hydrate: false });

    assertStrictModel(config, true);

    expect(exitSpy).toHaveBeenCalledWith(2);
    expect(errorSpy).toHaveBeenCalledWith(
      expect.stringContaining('[--strict-model]'),
    );
    exitSpy.mockRestore();
    errorSpy.mockRestore();
  });

  it('không thoát khi cấu hình hợp lệ', () => {
    const exitSpy = jest
      .spyOn(process, 'exit')
      .mockImplementation((() => undefined) as never);
    process.env.AI_ALLOWED_MODELS = 'deepseek-flash,gemini-3.5-flash-lite';
    process.env.AI_DEFAULT_MODEL = 'deepseek-flash';

    assertStrictModel(resolveScriptModelConfig({ hydrate: false }), true);

    expect(exitSpy).not.toHaveBeenCalled();
    exitSpy.mockRestore();
  });
});
