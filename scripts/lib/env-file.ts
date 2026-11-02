/**
 * Đọc/ bù biến môi trường từ file .env cho các script (không phụ thuộc dotenv).
 *
 * Nguyên tắc: chỉ bù biến còn THIẾU — env của Docker/CI luôn thắng.
 */

import { existsSync, readFileSync } from 'node:fs';

declare const process: {
  env: Record<string, string | undefined>;
};

export const AI_MODEL_ENV_KEYS = [
  'AI_DEFAULT_MODEL',
  'DEFAULT_AI_MODEL',
  'AI_ALLOWED_MODELS',
] as const;

export function stripQuotes(value: string): string {
  const trimmed = value.trim();
  if (trimmed.length < 2) {
    return trimmed;
  }
  const first = trimmed[0];
  const last = trimmed[trimmed.length - 1];
  const isQuoted =
    (first === '"' && last === '"') || (first === "'" && last === "'");
  return isQuoted ? trimmed.slice(1, -1).trim() : trimmed;
}

/** Đọc các cặp KEY=VALUE từ file .env (bỏ comment, strip quote). */
export function readEnvFileValues(
  envFilePath = '.env',
): Record<string, string> {
  if (!existsSync(envFilePath)) {
    return {};
  }
  const values: Record<string, string> = {};
  for (const line of readFileSync(envFilePath, 'utf8').split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) {
      continue;
    }
    const separator = trimmed.indexOf('=');
    if (separator <= 0) {
      continue;
    }
    const key = trimmed.slice(0, separator).trim();
    values[key] = stripQuotes(trimmed.slice(separator + 1));
  }
  return values;
}

/**
 * Bù các biến env còn thiếu từ file .env.
 * Trả về danh sách key đã bù (rỗng nếu không cần/không có file).
 */
export function hydrateEnvFromFile(
  envFilePath = '.env',
  keys: readonly string[] = AI_MODEL_ENV_KEYS,
): string[] {
  const values = readEnvFileValues(envFilePath);
  const hydrated: string[] = [];
  for (const key of keys) {
    if (process.env[key] !== undefined) {
      continue;
    }
    const value = values[key];
    if (value === undefined || value.length === 0) {
      continue;
    }
    process.env[key] = value;
    hydrated.push(key);
  }
  return hydrated;
}

/** Tương thích: trả true nếu có ít nhất 1 biến được bù. */
export function loadEnvForScript(envFilePath = '.env'): boolean {
  return hydrateEnvFromFile(envFilePath).length > 0;
}
