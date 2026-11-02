export const DOCUMENT_SETUP_REQUEST_TOPIC = 'document_setup_request';
export const DOCUMENT_SETUP_RESPONSE_TOPIC = 'document_setup_response';
export const WRITE_REPORT_REQUEST_TOPIC = 'write_section_request';
export const WRITE_REPORT_RESPONSE_TOPIC = 'write_section_response';
export const CHATBOT_REQUEST_TOPIC = 'chatbot_request';
export const CHATBOT_RESPONSE_TOPIC = 'chatbot_response';
export const ENHANCEMENT_REQUEST_TOPIC = 'enhancement_request';
export const ENHANCEMENT_RESPONSE_TOPIC = 'enhancement_response';

export const KAFKA_TOPICS = {
  DOCUMENT_SETUP_REQUEST: DOCUMENT_SETUP_REQUEST_TOPIC,
  DOCUMENT_SETUP_RESPONSE: DOCUMENT_SETUP_RESPONSE_TOPIC,
  WRITE_REPORT_REQUEST: WRITE_REPORT_REQUEST_TOPIC,
  WRITE_REPORT_RESPONSE: WRITE_REPORT_RESPONSE_TOPIC,
  CHATBOT_REQUEST: CHATBOT_REQUEST_TOPIC,
  CHATBOT_RESPONSE: CHATBOT_RESPONSE_TOPIC,
  ENHANCEMENT_REQUEST: ENHANCEMENT_REQUEST_TOPIC,
  ENHANCEMENT_RESPONSE: ENHANCEMENT_RESPONSE_TOPIC,
} as const;

export const ALL_BASE_TOPICS = [
  DOCUMENT_SETUP_REQUEST_TOPIC,
  DOCUMENT_SETUP_RESPONSE_TOPIC,
  WRITE_REPORT_REQUEST_TOPIC,
  WRITE_REPORT_RESPONSE_TOPIC,
  CHATBOT_REQUEST_TOPIC,
  CHATBOT_RESPONSE_TOPIC,
  ENHANCEMENT_REQUEST_TOPIC,
  ENHANCEMENT_RESPONSE_TOPIC,
] as const;

import {
  AiModelConfig,
  resolveAiModelConfig,
} from '../../config/ai-model.config';

/** Resolve MỘT lần để mọi hằng số bên dưới luôn nhất quán với nhau. */
const initialAiModelConfig = resolveAiModelConfig();

/**
 * Model được chọn cho toàn hệ thống.
 * Giá trị do `AI_DEFAULT_MODEL` quyết định (xem `resolveAiModelConfig`).
 */
export const AI_DEFAULT_MODEL = initialAiModelConfig.defaultModel;

/** @deprecated Sử dụng AI_DEFAULT_MODEL thay thế. */
export const DEFAULT_AI_MODEL = AI_DEFAULT_MODEL;

/** Phần tử đầu tiên của whitelist — phương án dự phòng khi model mặc định lỗi. */
export const AI_FALLBACK_MODEL = initialAiModelConfig.fallbackModel;

export const SUPPORTED_KAFKA_MODELS = initialAiModelConfig.allowedModels;

/**
 * Resolve lại cấu hình tại thời điểm gọi.
 * Dùng cho validation/DTO và các luồng cần phản ánh `process.env` hiện hành
 * thay vì giá trị snapshot lúc import module.
 */
export function getAiModelConfig(): AiModelConfig {
  return resolveAiModelConfig();
}

export type SupportedKafkaModel = string;

export const KAFKA_ERROR_CODES = {
  AUTHENTICATION_ERROR: 401,
  PERMISSION_DENIED: 403,
  QUOTA_LIMIT: 429,
  LLM_UNKNOWN_ERROR: 500,
  LLM_UNAVAILABLE: 503,
  NETWORK_ERROR: 600,
  INVALID_PARAMS: 603,
  TIME_OUT_REQUEST: 604,
  NOT_IMPLEMENT_MODEL: 605,
  EVENT_NOT_FOUND: 606,
} as const;
