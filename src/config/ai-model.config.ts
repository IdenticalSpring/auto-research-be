import { registerAs } from '@nestjs/config';

declare const process: { env: Record<string, string | undefined> };

/**
 * Model dự phòng cuối cùng.
 * Đây là hằng số model DUY NHẤT được phép tồn tại trong `src/`.
 * Theo quyết định của AI server (2026-11-01): mặc định là `deepseek-flash`.
 */
export const AI_MODEL_FALLBACK = 'deepseek-flash';

/**
 * Whitelist mặc định khi `AI_ALLOWED_MODELS` rỗng/không hợp lệ.
 * Phần tử đầu tiên đồng thời là phương án dự phòng.
 */
export const DEFAULT_ALLOWED_MODELS: readonly string[] = [
  AI_MODEL_FALLBACK,
  'gemini-3.5-flash-lite',
  'localhost',
];

export type AiModelWarningReason =
  | 'EMPTY_DEFAULT_MODEL'
  | 'DEFAULT_MODEL_NOT_ALLOWED'
  | 'EMPTY_ALLOWED_MODELS';

export interface AiModelWarning {
  reason: AiModelWarningReason;
  message: string;
  requestedModel: string | null;
  selectedModel: string;
}

export interface AiModelConfig {
  /** Model được chọn cho mọi luồng gửi Kafka. */
  defaultModel: string;
  /** Whitelist theo đúng thứ tự cấu hình (đã trim, bỏ trùng). */
  allowedModels: string[];
  /** Phần tử đầu tiên của whitelist — dùng khi `defaultModel` không hợp lệ. */
  fallbackModel: string;
  /** Giá trị thô đọc từ `AI_DEFAULT_MODEL` / `DEFAULT_AI_MODEL`. */
  requestedModel: string | null;
  /** Danh sách cảnh báo khi phải fallback (rỗng nếu cấu hình hợp lệ). */
  warnings: AiModelWarning[];
}

export type AiProvider = 'gemini' | 'deepseek' | 'local' | 'unknown';

function stripQuotes(value: string): string {
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

/**
 * Chuẩn hóa danh sách model: trim, bỏ dấu nháy, bỏ rỗng, bỏ trùng
 * nhưng GIỮ NGUYÊN thứ tự cấu hình (thứ tự quyết định phương án dự phòng).
 * Hỗ trợ cả `AI_ALLOWED_MODELS="a,b"` (nháy bao ngoài) và `"a","b"` (từng phần tử).
 */
export function parseAllowedModels(raw: string | undefined): string[] {
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

/** Suy ra provider từ tên model — chỉ dùng cho log/telemetry, KHÔNG đổi payload. */
export function getAiProvider(model: string): AiProvider {
  const normalized = stripQuotes(model).toLowerCase();
  if (normalized.startsWith('gemini')) {
    return 'gemini';
  }
  if (normalized.includes('deepseek')) {
    return 'deepseek';
  }
  if (normalized === 'localhost' || normalized.startsWith('localllm')) {
    return 'local';
  }
  return 'unknown';
}

/**
 * Nguồn sự thật DUY NHẤT cho cấu hình AI Model toàn hệ thống.
 *
 * Luật chọn model:
 * 1. `AI_DEFAULT_MODEL` (fallback `DEFAULT_AI_MODEL`) là ưu tiên số 1 khi thuộc
 *    `AI_ALLOWED_MODELS`.
 * 2. Nếu rỗng/không hợp lệ/không thuộc whitelist → chọn phần tử đầu tiên của
 *    `AI_ALLOWED_MODELS`, cuối cùng là `AI_MODEL_FALLBACK`, kèm cảnh báo log.
 * 3. `AI_ALLOWED_MODELS` KHÔNG bị chèn thêm model ngoài cấu hình (không unshift).
 */
export function resolveAiModelConfig(): AiModelConfig {
  const warnings: AiModelWarning[] = [];

  const parsedAllowed = parseAllowedModels(process.env.AI_ALLOWED_MODELS);
  const allowedModels =
    parsedAllowed.length > 0
      ? parsedAllowed
      : DEFAULT_ALLOWED_MODELS.map((model) => model);

  if (parsedAllowed.length === 0) {
    warnings.push({
      reason: 'EMPTY_ALLOWED_MODELS',
      message: `AI_ALLOWED_MODELS rỗng hoặc không hợp lệ, dùng whitelist mặc định: ${allowedModels.join(', ')}`,
      requestedModel: null,
      selectedModel: allowedModels[0],
    });
  }

  const rawDefault = stripQuotes(
    process.env.AI_DEFAULT_MODEL || process.env.DEFAULT_AI_MODEL || '',
  );
  const requestedModel = rawDefault.length > 0 ? rawDefault : null;
  const fallbackModel = allowedModels[0] ?? AI_MODEL_FALLBACK;

  let defaultModel = fallbackModel;

  if (!requestedModel) {
    warnings.push({
      reason: 'EMPTY_DEFAULT_MODEL',
      message: `AI_DEFAULT_MODEL rỗng, dùng model dự phòng: ${fallbackModel}`,
      requestedModel: null,
      selectedModel: defaultModel,
    });
  } else if (!allowedModels.includes(requestedModel)) {
    warnings.push({
      reason: 'DEFAULT_MODEL_NOT_ALLOWED',
      message: `AI_DEFAULT_MODEL="${requestedModel}" không nằm trong AI_ALLOWED_MODELS (${allowedModels.join(', ')}), dùng model dự phòng: ${fallbackModel}`,
      requestedModel,
      selectedModel: defaultModel,
    });
  } else {
    defaultModel = requestedModel;
  }

  return {
    defaultModel,
    allowedModels,
    fallbackModel,
    requestedModel,
    warnings,
  };
}

export default registerAs('aiModel', resolveAiModelConfig);
