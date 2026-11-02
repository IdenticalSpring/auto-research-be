/**
 * Harness gửi request Kafka và chờ response từ AI server.
 *
 * Khắc phục lỗi của bản cũ:
 * - Ghép response theo `document_id` + `event_type` (fallback `session_id`).
 * - KHÔNG resolve khi nhận streaming chunk (`has_finished: false`,
 *   `process_status: 2`) → tránh false PASS.
 * - FAIL-FAST khi `error_code` / `ai_error` / `process_status: 4` hoặc khi
 *   AI yêu cầu user_input (`done_chat: false`, `process_status: 5`).
 * - Idle timeout (reset mỗi chunk) + chặn trên tổng thời gian.
 */

import { Consumer, Kafka, Producer } from 'kafkajs';
import {
  AiNotification,
  buildResponseKey,
  normalizeAiNotification,
} from './ai-response';
import { OrphanResponseStore } from './orphan-store';
import { computeIdleDelay, computeTotalDelay } from './harness-timing';

const MAX_TOTAL_FACTOR = 1.5;

export interface HarnessSendOptions {
  topic: string;
  payload: Record<string, unknown>;
  timeoutMs: number;
}

interface PendingRequest {
  key: string;
  documentId: string;
  eventType: string;
  sessionId: string;
  modelId: string;
  chunks: number;
  idleMs: number;
  startedAt: number;
  maxTotalMs: number;
  timer?: ReturnType<typeof setTimeout>;
  totalTimer?: ReturnType<typeof setTimeout>;
  settle: (error?: Error, data?: Record<string, unknown>) => void;
}

export interface KafkaResponseHarnessOptions {
  defaultTimeoutMs?: number;
}

function formatDuration(ms: number): string {
  return ms >= 60_000 ? `${Math.round(ms / 60_000)} phút` : `${ms}ms`;
}

export class KafkaResponseHarness {
  private readonly producer: Producer;
  private readonly consumer: Consumer;
  private readonly pending = new Map<string, PendingRequest>();
  private readonly pendingBySession = new Map<string, PendingRequest>();
  private readonly orphans = new OrphanResponseStore();
  private readonly defaultTimeoutMs: number;

  constructor(
    kafka: Kafka,
    private readonly responseTopics: string[],
    options: KafkaResponseHarnessOptions = {},
  ) {
    const runId = `harness-${Date.now()}`;
    this.producer = kafka.producer();
    this.consumer = kafka.consumer({
      groupId: `group-${runId}`,
      sessionTimeout: 60_000,
      heartbeatInterval: 3_000,
      maxWaitTimeInMs: 1_000,
      // Response của các event sinh nội dung dài (propose_method, write_content,
      // gen_slides) có thể vượt 1MB mặc định ⇒ nếu để mặc định, message FINISHED
      // bị bỏ qua và case timeout oan dù AI đã trả kết quả.
      maxBytesPerPartition: 20 * 1024 * 1024,
      maxBytes: 50 * 1024 * 1024,
    });
    this.defaultTimeoutMs = options.defaultTimeoutMs ?? 400_000;
  }

  async start(): Promise<void> {
    await Promise.all([this.producer.connect(), this.consumer.connect()]);
    for (const topic of this.responseTopics) {
      await this.consumer.subscribe({ topic, fromBeginning: false });
    }
    this.consumer.on(this.consumer.events.CRASH, (event) => {
      console.log(`   ⚠️ consumer crash: ${String(event.payload?.error)}`);
    });
    await this.consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) return;
        try {
          this.onMessage(
            JSON.parse(message.value.toString()) as Record<string, unknown>,
          );
        } catch {
          // bỏ qua message không phải JSON
        }
      },
    });
  }

  async stop(): Promise<void> {
    for (const request of this.pending.values()) {
      request.settle(new Error('Harness dừng khi request còn chờ (interrupt)'));
    }
    await Promise.all([this.producer.disconnect(), this.consumer.disconnect()]);
  }

  async send(options: HarnessSendOptions): Promise<Record<string, unknown>> {
    const { topic, payload } = options;
    const rawDocumentId = payload.document_id ?? payload.documentId;
    // Một số event (`delete_report`) gửi `document_id` dạng MẢNG theo contract AI
    // ⇒ không được ép về string (chỉ dùng phần tử đầu để ghép key).
    const isListDocumentId = Array.isArray(rawDocumentId);
    const documentId = isListDocumentId
      ? String((rawDocumentId as unknown[])[0] ?? '')
      : String(rawDocumentId ?? '');
    const eventType = String(payload.event_type ?? payload.eventType ?? '');
    const sessionId = String(
      payload.session_id ??
        `sess-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    );
    const key = buildResponseKey(documentId, eventType);

    payload.session_id = sessionId;
    if (!isListDocumentId) {
      payload.document_id = documentId;
    }
    payload.event_type = eventType;

    const cached = this.orphans.take(documentId, eventType);
    if (cached) {
      return cached;
    }

    return new Promise<Record<string, unknown>>((resolve, reject) => {
      const request: PendingRequest = {
        key,
        documentId,
        eventType,
        sessionId,
        modelId: String(payload.model_id ?? ''),
        chunks: 0,
        idleMs: options.timeoutMs || this.defaultTimeoutMs,
        startedAt: Date.now(),
        maxTotalMs: Math.round(
          (options.timeoutMs || this.defaultTimeoutMs) * MAX_TOTAL_FACTOR,
        ),
        settle: (error, data) => {
          this.pending.delete(key);
          this.pendingBySession.delete(sessionId);
          if (error) {
            reject(error);
            return;
          }
          resolve(data ?? {});
        },
      };

      this.armTimers(request);
      this.pending.set(key, request);
      this.pendingBySession.set(sessionId, request);

      this.producer
        .send({
          topic,
          messages: [{ key: sessionId, value: JSON.stringify(payload) }],
        })
        .catch((error: Error) => request.settle(error));
    });
  }

  private armTimers(request: PendingRequest): void {
    if (request.timer) clearTimeout(request.timer);
    if (request.totalTimer) clearTimeout(request.totalTimer);

    request.timer = setTimeout(() => {
      request.settle(
        new Error(
          `Timeout sau ${formatDuration(request.idleMs)} không nhận thêm response ` +
            `(event=${request.eventType}, doc=${request.documentId}, session=${request.sessionId}, ` +
            `model=${request.modelId}, chunks=${request.chunks})`,
        ),
      );
    }, computeIdleDelay(request.idleMs));

    request.totalTimer = setTimeout(
      () =>
        request.settle(
          new Error(
            `Vượt tổng thời gian ${formatDuration(request.maxTotalMs)} ` +
              `(event=${request.eventType}, doc=${request.documentId}, chunks=${request.chunks})`,
          ),
        ),
      computeTotalDelay(request.startedAt, request.maxTotalMs),
    );
  }

  private onMessage(data: Record<string, unknown>): void {
    const notification = normalizeAiNotification(data);
    const request = this.findPending(notification);

    if (!request) {
      this.logUnmatched(notification, data);
      return;
    }

    switch (notification.kind) {
      case 'processing':
        request.chunks += 1;
        console.log(
          `   ⏳ chunk #${request.chunks} (event=${request.eventType}, doc=${request.documentId})`,
        );
        this.armTimers(request);
        return;
      case 'error':
        request.settle(
          new Error(
            `AI Error ${notification.errorCode ?? ''}: ${notification.errorMessage ?? '(không có mô tả)'}`,
          ),
        );
        return;
      case 'suspended':
        request.settle(
          new Error(
            `SUSPENDED: AI yêu cầu user_input (${notification.errorMessage ?? 'không có ai_message'})`,
          ),
        );
        return;
      default:
        request.settle(undefined, data);
    }
  }

  private findPending(
    notification: AiNotification,
  ): PendingRequest | undefined {
    if (notification.documentId && notification.eventType) {
      const byKey = this.pending.get(
        buildResponseKey(notification.documentId, notification.eventType),
      );
      if (byKey) return byKey;
    }
    if (notification.sessionId) {
      const bySession = this.pendingBySession.get(notification.sessionId);
      if (bySession) {
        console.log(
          `   ⚠️ match theo session_id (thiếu document_id/event_type): ${notification.sessionId}`,
        );
        return bySession;
      }
    }
    // AI error branch có thể chỉ trả event_type (không echo conv_id/document_id/
    // session_id) ⇒ nếu chỉ có đúng 1 request đang chờ cùng event_type thì match,
    // để báo lỗi thật (vd 603 Missing field) thay vì timeout 240s oan.
    const sameEvent = [...this.pending.values()].filter(
      (item) => item.eventType === notification.eventType,
    );
    if (notification.eventType && sameEvent.length === 1) {
      console.log(
        `   ⚠️ match theo event_type (response thiếu id): ${notification.eventType}`,
      );
      return sameEvent[0];
    }
    return undefined;
  }

  private logUnmatched(
    notification: AiNotification,
    data: Record<string, unknown>,
  ): void {
    if (notification.documentId && notification.eventType) {
      this.orphans.save(
        notification.documentId,
        notification.eventType,
        data,
      );
      console.log(
        `   📥 response đến sớm, lưu tạm (doc=${notification.documentId}, event=${notification.eventType})`,
      );
      return;
    }
    const code = notification.errorCode;
    const reason =
      code !== undefined
        ? ` error_code=${String(code)}${notification.errorMessage ? ` error=${notification.errorMessage}` : ''}`
        : '';
    console.log(
      `   📥 response không khớp request (doc=${notification.documentId ?? '?'}, event=${notification.eventType ?? '?'}, session=${notification.sessionId ?? '?'}) keys=${Object.keys(data).join(',')}${reason}`,
    );
  }
}
