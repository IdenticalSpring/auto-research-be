/**
 * Helper chọn model cho các script live-test / smoke-test.
 *
 * Không import từ `src/config/ai-model.config.ts` để chạy được bằng cả
 * `ts-node` và `node --experimental-strip-types` mà không phụ thuộc @nestjs/config.
 * Parity với resolver chính được bảo đảm bởi `scripts/lib/ai-model.spec.ts`.
 *
 * Khi `AI_DEFAULT_MODEL` bị fallback (rỗng / ngoài whitelist), helper LUÔN in
 * `⚠️ [REASON] ... requested: ...` trước dòng `AI Model:` để không ai nhầm
 * model được in là model đã yêu cầu.
 */

import { hydrateEnvFromFile, stripQuotes } from './env-file';

export {
  AI_MODEL_ENV_KEYS,
  hydrateEnvFromFile,
  loadEnvForScript,
  readEnvFileValues,
} from './env-file';

export const SCRIPT_AI_MODEL_FALLBACK = 'deepseek-flash';

export const SCRIPT_DEFAULT_ALLOWED_MODELS: readonly string[] = [
  SCRIPT_AI_MODEL_FALLBACK,
  'gemini-3.5-flash-lite',
  'localhost',
];

export type ScriptModelFallbackReason =
  | 'EMPTY_DEFAULT_MODEL'
  | 'DEFAULT_MODEL_NOT_ALLOWED'
  | 'EMPTY_ALLOWED_MODELS';

export interface ScriptModelWarning {
  reason: ScriptModelFallbackReason;
  message: string;
}

export interface ScriptModelResolution {
  /** Giá trị thô đọc từ AI_DEFAULT_MODEL / DEFAULT_AI_MODEL. */
  requestedModel: string | null;
  /** Model THỰC SỰ được gán vào payload. */
  selectedModel: string;
  /** Whitelist theo đúng thứ tự cấu hình. */
  allowedModels: string[];
  /** Lý do fallback (undefined nếu cấu hình hợp lệ). */
  fallbackReason?: ScriptModelFallbackReason;
  warnings: ScriptModelWarning[];
}

export interface ResolveScriptModelOptions {
  /** Nạp .env để bù các biến còn thiếu (mặc định true). */
  hydrate?: boolean;
  envFilePath?: string;
  /** Thoát code 2 nếu có fallback (mặc định theo argv `--strict-model`). */
  strict?: boolean;
}

declare const process: {
  env: Record<string, string | undefined>;
  argv: string[];
  exit: (code?: number) => never;
};

function parseAllowed(raw: string | undefined): string[] {
  const parsed = (raw ?? '')
    .split(',')
    .map((token) => token.trim().replace(/^["']+|["']+$/g, '').trim())
    .filter((model) => model.length > 0);
  const unique: string[] = [];
  for (const model of parsed) {
    if (!unique.includes(model)) {
      unique.push(model);
    }
  }
  return unique;
}

/** Whitelist model theo `AI_ALLOWED_MODELS` (fallback danh sách mặc định). */
export function resolveScriptAllowedModels(): string[] {
  const parsed = parseAllowed(process.env.AI_ALLOWED_MODELS);
  return parsed.length > 0
    ? parsed
    : SCRIPT_DEFAULT_ALLOWED_MODELS.map((model) => model);
}

/** Resolve model + lý do fallback (hàm thuần, không log, không exit). */
export function resolveScriptModelConfig(
  options: ResolveScriptModelOptions = {},
): ScriptModelResolution {
  const { hydrate = true, envFilePath = '.env' } = options;

  if (hydrate) {
    hydrateEnvFromFile(envFilePath);
  }

  const warnings: ScriptModelWarning[] = [];
  const parsedAllowed = parseAllowed(process.env.AI_ALLOWED_MODELS);
  const allowedModels =
    parsedAllowed.length > 0
      ? parsedAllowed
      : SCRIPT_DEFAULT_ALLOWED_MODELS.map((model) => model);

  if (parsedAllowed.length === 0) {
    warnings.push({
      reason: 'EMPTY_ALLOWED_MODELS',
      message: `AI_ALLOWED_MODELS rỗng hoặc không hợp lệ, dùng whitelist mặc định: ${allowedModels.join(', ')}`,
    });
  }

  const rawDefault = stripQuotes(
    process.env.AI_DEFAULT_MODEL || process.env.DEFAULT_AI_MODEL || '',
  );
  const requestedModel = rawDefault.length > 0 ? rawDefault : null;
  let selectedModel = allowedModels[0];

  if (!requestedModel) {
    warnings.push({
      reason: 'EMPTY_DEFAULT_MODEL',
      message: `AI_DEFAULT_MODEL rỗng, dùng model dự phòng: ${selectedModel}`,
    });
  } else if (!allowedModels.includes(requestedModel)) {
    warnings.push({
      reason: 'DEFAULT_MODEL_NOT_ALLOWED',
      message: `AI_DEFAULT_MODEL="${requestedModel}" không nằm trong AI_ALLOWED_MODELS (${allowedModels.join(', ')}) -> dùng ${selectedModel}`,
    });
  } else {
    selectedModel = requestedModel;
  }

  const lastWarning = warnings[warnings.length - 1];

  return {
    requestedModel,
    selectedModel,
    allowedModels,
    fallbackReason: lastWarning?.reason,
    warnings,
  };
}

const loggedMessages = new Set<string>();

function logOnce(kind: 'log' | 'warn', message: string): void {
  if (loggedMessages.has(message)) {
    return;
  }
  loggedMessages.add(message);
  if (kind === 'warn') {
    console.warn(message);
    return;
  }
  console.log(message);
}

/**
 * In cấu hình model đã resolve. Cảnh báo fallback được in TRƯỚC dòng
 * `🤖 AI Model:` để không ai nhầm model được in là model đã yêu cầu.
 */
export function logScriptModelConfig(config: ScriptModelResolution): void {
  for (const warning of config.warnings) {
    logOnce('warn', `⚠️ [${warning.reason}] ${warning.message}`);
  }
  logOnce(
    'log',
    `🤖 AI Model: ${config.selectedModel} | allowed: ${config.allowedModels.join(', ')} | requested: ${config.requestedModel ?? '(empty)'}`,
  );
}

/**
 * Thoát code 2 nếu có fallback khi bật `--strict-model` (hoặc `strict: true`).
 * Dùng chung cho `resolveScriptModelId()` và các script tự resolve config.
 */
export function assertStrictModel(
  config: ScriptModelResolution,
  strict?: boolean,
): void {
  const enabled = strict ?? process.argv.includes('--strict-model');
  if (!enabled || !config.fallbackReason) {
    return;
  }
  console.error(
    `❌ [--strict-model] Model bị fallback: requested=${config.requestedModel ?? '(empty)'} -> selected=${config.selectedModel}. Không gửi request nào.`,
  );
  process.exit(2);
}

/**
 * Model dùng cho script: in cấu hình rồi trả model đã chọn.
 * Với `--strict-model` (hoặc `options.strict`), fallback sẽ làm script thoát code 2.
 */
export function resolveScriptModelId(
  options: ResolveScriptModelOptions = {},
): string {
  const config = resolveScriptModelConfig(options);
  logScriptModelConfig(config);
  assertStrictModel(config, options.strict);

  return config.selectedModel;
}
