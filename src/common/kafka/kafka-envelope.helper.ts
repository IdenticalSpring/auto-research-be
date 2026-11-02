import { getAiModelConfig } from './kafka.constants';

export interface KafkaEnvelopeParams {
  eventType: string;
  userId: string;
  documentId?: string;
  convId?: string;
  sessionId?: string;
  modelId?: string;
  language?: string;
  jobId?: string;
}

export interface StandardKafkaRequestEnvelope {
  event_type: string;
  user_id: string;
  document_id?: string;
  conv_id?: string;
  session_id: string;
  model_id: string;
  language: string;
  job_id?: string;
  [key: string]: unknown;
}

/**
 * Loại bỏ các key mang giá trị undefined trong payload object (đệ quy an toàn)
 * nhằm ngăn chặn lỗi serialization hoặc null pointer trên Kafka consumer.
 */
export function sanitizeKafkaPayload<T>(input: T): T {
  if (input === null || typeof input !== 'object') {
    return input;
  }

  if (Array.isArray(input)) {
    return input.map((item) => sanitizeKafkaPayload(item)) as unknown as T;
  }

  const sanitized: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(input as Record<string, unknown>)) {
    if (value !== undefined) {
      sanitized[key] = sanitizeKafkaPayload(value);
    }
  }

  return sanitized as T;
}

/**
 * Khởi tạo Message Request Envelope chuẩn mực 100% gửi vào Kafka Topics.
 * Mặc định tự động điền model_id theo cấu hình tập trung (`AI_DEFAULT_MODEL`),
 * language: 'Vietnamese', session_id: 'default'.
 */
export function buildKafkaRequestEnvelope(
  params: KafkaEnvelopeParams,
  extraPayload?: Record<string, unknown>,
): StandardKafkaRequestEnvelope {
  const envelope: StandardKafkaRequestEnvelope = {
    event_type: params.eventType,
    user_id: params.userId,
    session_id: params.sessionId || 'default',
    model_id: params.modelId || getAiModelConfig().defaultModel,
    language: params.language || 'Vietnamese',
    ...(params.documentId ? { document_id: params.documentId } : {}),
    ...(params.convId ? { conv_id: params.convId } : {}),
    ...(params.jobId ? { job_id: params.jobId } : {}),
    ...extraPayload,
  };

  return sanitizeKafkaPayload(envelope);
}
