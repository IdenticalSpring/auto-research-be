import { Logger } from '@nestjs/common';
import { jest } from '@jest/globals';
import { loadEnvFile, logResolvedAiModel } from './env.loader';

describe('env.loader', () => {
  it('bỏ qua nạp .env khi NODE_ENV=test', () => {
    const previousNodeEnv = process.env.NODE_ENV;
    try {
      process.env.NODE_ENV = 'test';
      expect(loadEnvFile('.env')).toBe(false);
    } finally {
      if (previousNodeEnv === undefined) {
        delete process.env.NODE_ENV;
      } else {
        process.env.NODE_ENV = previousNodeEnv;
      }
    }
  });

  it('bỏ qua khi biến AI_DEFAULT_MODEL đã có sẵn trong process env', () => {
    const previousDefault = process.env.AI_DEFAULT_MODEL;
    const previousNodeEnv = process.env.NODE_ENV;
    try {
      process.env.NODE_ENV = 'production';
      process.env.AI_DEFAULT_MODEL = 'deepseek-flash';
      expect(loadEnvFile('.env')).toBe(false);
    } finally {
      if (previousDefault === undefined) {
        delete process.env.AI_DEFAULT_MODEL;
      } else {
        process.env.AI_DEFAULT_MODEL = previousDefault;
      }
      if (previousNodeEnv === undefined) {
        delete process.env.NODE_ENV;
      } else {
        process.env.NODE_ENV = previousNodeEnv;
      }
    }
  });

  it('trả false khi file env không tồn tại', () => {
    const previousDefault = process.env.AI_DEFAULT_MODEL;
    const previousNodeEnv = process.env.NODE_ENV;
    try {
      process.env.NODE_ENV = 'production';
      delete process.env.AI_DEFAULT_MODEL;
      expect(loadEnvFile('.env.khong-ton-tai')).toBe(false);
    } finally {
      if (previousDefault !== undefined) {
        process.env.AI_DEFAULT_MODEL = previousDefault;
      }
      if (previousNodeEnv === undefined) {
        delete process.env.NODE_ENV;
      } else {
        process.env.NODE_ENV = previousNodeEnv;
      }
    }
  });

  it('log model đã resolve và cảnh báo fallback', () => {
    const logSpy = jest.spyOn(Logger.prototype, 'log').mockImplementation();
    const warnSpy = jest.spyOn(Logger.prototype, 'warn').mockImplementation();
    const previousDefault = process.env.AI_DEFAULT_MODEL;
    const previousAllowed = process.env.AI_ALLOWED_MODELS;
    try {
      process.env.AI_ALLOWED_MODELS = 'gemini-3.5-flash-lite,deepseek-flash';
      process.env.AI_DEFAULT_MODEL = 'unknown-model';

      logResolvedAiModel();

      expect(warnSpy).toHaveBeenCalled();
      expect(logSpy).toHaveBeenCalledWith(
        expect.stringContaining('AI model: gemini-3.5-flash-lite'),
      );
    } finally {
      logSpy.mockRestore();
      warnSpy.mockRestore();
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
