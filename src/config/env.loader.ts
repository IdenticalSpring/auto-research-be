import { existsSync } from 'node:fs';
import { Logger } from '@nestjs/common';
import { resolveAiModelConfig } from './ai-model.config';

declare const process: {
  env: Record<string, string | undefined>;
  loadEnvFile?: (path?: string) => void;
};

const envLogger = new Logger('EnvLoader');

/**
 * Nạp `.env` vào `process.env` NGAY khi module này được import.
 *
 * Lý do: các hằng số như `AI_DEFAULT_MODEL` trong `kafka.constants.ts` được
 * evaluate lúc import module, tức là TRƯỚC khi `ConfigModule.forRoot()` chạy.
 * Nếu không nạp sớm, chạy local (`nest start`) sẽ luôn rơi về model mặc định
 * dù `.env` đã cấu hình model khác.
 *
 * An toàn:
 * - Không ghi đè khi biến đã tồn tại trong process env (Docker/compose/CI).
 * - Bỏ qua ở môi trường test để test tự kiểm soát `process.env`.
 * - Không throw nếu `.env` không tồn tại.
 */
export function loadEnvFile(envFilePath = '.env'): boolean {
  if (process.env.NODE_ENV === 'test' || process.env.AI_DEFAULT_MODEL) {
    return false;
  }
  if (typeof process.loadEnvFile !== 'function') {
    return false;
  }
  if (!existsSync(envFilePath)) {
    return false;
  }
  try {
    process.loadEnvFile(envFilePath);
    return true;
  } catch (error) {
    envLogger.warn(
      `Không nạp được ${envFilePath}: ${(error as Error).message}`,
    );
    return false;
  }
}

/**
 * Log cấu hình model đã resolve kèm cảnh báo fallback (chỉ gọi 1 lần lúc bootstrap
 * để tránh cảnh báo trùng lặp khi nhiều module cùng resolve).
 */
export function logResolvedAiModel(): void {
  const config = resolveAiModelConfig();

  for (const warning of config.warnings) {
    envLogger.warn(`[${warning.reason}] ${warning.message}`);
  }

  envLogger.log(
    `AI model: ${config.defaultModel} | allowed: ${config.allowedModels.join(', ')} | requested: ${config.requestedModel ?? '(empty)'}`,
  );
}

// Side effect: nạp env ngay tại thời điểm import, phải đứng trước mọi import khác.
loadEnvFile();
