/**
 * Chuẩn hóa message response từ AI server cho harness live-test.
 *
 * Mục tiêu: kết luận PASS/FAIL dựa trên đúng state machine của AI server
 * (process_status / error_code / ai_error / done_chat / has_finished) thay vì
 * chỉ dò `data.error` như bản cũ — tránh false PASS khi nhận streaming chunk.
 */

export type AiNotificationKind =
  | 'error'
  | 'suspended'
  | 'processing'
  | 'finished'
  | 'unknown';

export interface AiNotification {
  documentId?: string;
  eventType?: string;
  sessionId?: string;
  kind: AiNotificationKind;
  /** 1..5 theo webhook AI (1 pending, 2 processing, 3 finished, 4 error, 5 suspended). */
  processStatus?: number;
  errorCode?: number | string;
  errorMessage?: string;
  hasFinished?: boolean;
  doneChat?: boolean;
}

type Rec = Record<string, unknown>;

const PROCESS_STATUS_KIND: Record<number, AiNotificationKind> = {
  1: 'processing',
  2: 'processing',
  3: 'finished',
  4: 'error',
  5: 'suspended',
};

function asRecord(value: unknown): Rec | undefined {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Rec)
    : undefined;
}

function layersOf(raw: Rec): Rec[] {
  return [raw, asRecord(raw.data), asRecord(raw.payload)].filter(
    (layer): layer is Rec => Boolean(layer),
  );
}

function firstString(layers: Rec[], keys: string[]): string | undefined {
  for (const layer of layers) {
    for (const key of keys) {
      const value = layer[key];
      if (typeof value === 'string' && value.trim().length > 0) {
        return value.trim();
      }
    }
  }
  return undefined;
}

function toNumber(value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === 'string' && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

function firstNumber(
  layers: Rec[],
  keys: string[],
): number | undefined {
  for (const layer of layers) {
    for (const key of keys) {
      const parsed = toNumber(layer[key]);
      if (parsed !== undefined) {
        return parsed;
      }
    }
  }
  return undefined;
}

function firstBoolean(
  layers: Rec[],
  keys: string[],
): boolean | undefined {
  for (const layer of layers) {
    for (const key of keys) {
      const value = layer[key];
      if (typeof value === 'boolean') {
        return value;
      }
    }
  }
  return undefined;
}

function firstRecord(layers: Rec[], keys: string[]): Rec | undefined {
  for (const layer of layers) {
    for (const key of keys) {
      const value = asRecord(layer[key]);
      if (value) {
        return value;
      }
    }
  }
  return undefined;
}

/** Key ghép response với request: document_id + event_type. */
export function buildResponseKey(
  documentId: string,
  eventType: string,
): string {
  return `${documentId}:${eventType}`;
}

export function normalizeAiNotification(raw: unknown): AiNotification {
  const record = asRecord(raw);
  if (!record || Object.keys(record).length === 0) {
    return { kind: 'unknown' };
  }

  const layers = layersOf(record);
  const processStatus = firstNumber(layers, [
    'process_status',
    'processStatus',
  ]);
  const explicitCode = firstNumber(layers, ['error_code', 'errorCode']);
  const aiError = firstRecord(layers, ['ai_error', 'aiError']);
  const nestedCode = aiError
    ? toNumber(aiError.errorCode ?? aiError.error_code)
    : undefined;
  const nestedMessage = aiError
    ? firstString([aiError], ['error', 'message'])
    : undefined;
  const errorString = firstString(layers, ['error']);
  // `ai_message` là message hỏi lại người dùng (interrupt), KHÔNG phải chunk
  // streaming (chunk dùng `section`). Thiếu nhận diện này ⇒ harness chờ mãi tới
  // idle timeout dù AI đã ngắt để chờ user (propose_method, need_data_analyze…).
  const aiMessage = firstString(layers, ['ai_message', 'aiMessage']);

  const notification: AiNotification = {
    documentId: firstString(layers, ['document_id', 'documentId']),
    eventType: firstString(layers, ['event_type', 'eventType']),
    // Chatbot không có document_id và AI có thể chỉ echo `conv_id` ⇒ phải đọc
    // cả conv_id để không rơi vào timeout oan (xem F6 trong docs/e2e-suite.md).
    sessionId: firstString(layers, [
      'session_id',
      'sessionId',
      'conv_id',
      'convId',
    ]),
    kind: 'finished',
    processStatus,
    errorCode: explicitCode ?? nestedCode,
    errorMessage: errorString ?? nestedMessage,
    hasFinished: firstBoolean(layers, ['has_finished', 'hasFinished']),
    doneChat: firstBoolean(layers, ['done_chat', 'doneChat']),
  };

  const statusKind = processStatus
    ? PROCESS_STATUS_KIND[processStatus]
    : undefined;
  if (statusKind) {
    notification.kind = statusKind;
    return notification;
  }

  if (notification.errorCode !== undefined) {
    notification.kind = 'error';
    return notification;
  }

  if (notification.errorMessage) {
    notification.kind = 'error';
    notification.errorCode = 500;
    return notification;
  }

  const actionRequired = firstString(layers, [
    'action_required',
    'actionRequired',
  ]);
  if (
    notification.doneChat === false ||
    actionRequired === 'user_input' ||
    // Interrupt message: AI ngắt luồng để chờ user nhưng không set process_status
    (notification.hasFinished === false && Boolean(aiMessage))
  ) {
    notification.kind = 'suspended';
    notification.errorMessage = aiMessage;
    return notification;
  }

  if (notification.hasFinished === false) {
    notification.kind = 'processing';
    return notification;
  }

  return notification;
}
