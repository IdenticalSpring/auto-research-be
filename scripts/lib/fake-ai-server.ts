/**
 * "AI server giả" cho E2E T3: nghe request topic, bắt envelope để assert
 * contract rồi publish response tổng hợp về response topic tương ứng.
 *
 * Cho phép kiểm thử toàn bộ đường BE ⇄ Kafka mà không phụ thuộc AI server thật.
 */

import { Consumer, Kafka, logLevel, Producer } from 'kafkajs';

declare const process: { env: Record<string, string | undefined> };

export const REQUEST_TO_RESPONSE: Record<string, string> = {
  document_setup_request: 'document_setup_response',
  write_section_request: 'write_section_response',
  chatbot_request: 'chatbot_response',
  enhancement_request: 'enhancement_response',
};

export interface CapturedRequest {
  topic: string;
  payload: Record<string, any>;
  receivedAt: number;
}

export class FakeAiServer {
  private readonly producer: Producer;
  private readonly consumer: Consumer;
  private readonly queue: CapturedRequest[] = [];
  private readonly waiters: Array<() => void> = [];

  constructor(
    kafka: Kafka,
    private readonly suffix = process.env.KAFKA_TOPIC_SUFFIX || '',
  ) {
    const runId = `fake-ai-${Date.now()}`;
    this.producer = kafka.producer();
    this.consumer = kafka.consumer({
      groupId: `group-${runId}`,
      sessionTimeout: 60_000,
      heartbeatInterval: 3_000,
    });
  }

  private topic(base: string): string {
    return `${base}${this.suffix}`;
  }

  async start(): Promise<void> {
    await Promise.all([this.producer.connect(), this.consumer.connect()]);
    for (const base of Object.keys(REQUEST_TO_RESPONSE)) {
      await this.consumer.subscribe({
        topic: this.topic(base),
        fromBeginning: false,
      });
    }
    await this.consumer.run({
      eachMessage: async ({ topic, message }) => {
        if (!message.value) return;
        try {
          const payload = JSON.parse(message.value.toString());
          this.queue.push({
            topic: topic.replace(this.suffix, ''),
            payload,
            receivedAt: Date.now(),
          });
          while (this.waiters.length > 0) {
            (this.waiters.shift() as () => void)();
          }
        } catch {
          // bỏ qua message không phải JSON
        }
      },
    });
  }

  async stop(): Promise<void> {
    await Promise.all([
      this.producer.disconnect(),
      this.consumer.disconnect(),
    ]);
  }

  /** Lấy request kế tiếp khớp matcher (mặc định theo event_type). */
  async next(
    matcher: (request: CapturedRequest) => boolean,
    timeoutMs = 15_000,
    label = 'request',
  ): Promise<CapturedRequest> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const index = this.queue.findIndex(matcher);
      if (index >= 0) {
        return this.queue.splice(index, 1)[0];
      }
      const remaining = deadline - Date.now();
      if (remaining <= 0) {
        throw new Error(`Không bắt được ${label} trong ${timeoutMs}ms`);
      }
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, Math.min(remaining, 250));
        this.waiters.push(() => {
          clearTimeout(timer);
          resolve();
        });
      });
    }
  }

  nextEventType(eventType: string, timeoutMs = 15_000): Promise<CapturedRequest> {
    return this.next(
      (request) => request.payload?.event_type === eventType,
      timeoutMs,
      `request event_type=${eventType}`,
    );
  }

  nextJobId(jobId: string, timeoutMs = 15_000): Promise<CapturedRequest> {
    return this.next(
      (request) =>
        request.payload?.job_id === jobId || request.payload?.jobId === jobId,
      timeoutMs,
      `request job_id=${jobId}`,
    );
  }

  /** Publish response tổng hợp, tự echo identity để BE map đúng request. */
  async respond(
    request: CapturedRequest,
    response: Record<string, unknown>,
  ): Promise<void> {
    const responseTopic =
      REQUEST_TO_RESPONSE[request.topic] ||
      Object.values(REQUEST_TO_RESPONSE)[0];
    const payload = {
      document_id: request.payload?.document_id,
      session_id: request.payload?.session_id,
      user_id: request.payload?.user_id,
      event_type: request.payload?.event_type,
      model_id: request.payload?.model_id,
      ...response,
    };
    await this.producer.send({
      topic: this.topic(responseTopic),
      messages: [{ value: JSON.stringify(payload) }],
    });
  }

  /** Gửi 1 message bất kỳ lên response topic (dùng cho test harness). */
  async publishResponse(
    baseTopic: string,
    payload: Record<string, unknown>,
  ): Promise<void> {
    await this.producer.send({
      topic: this.topic(baseTopic),
      messages: [{ value: JSON.stringify(payload) }],
    });
  }
}

export function createFakeAiServer(
  brokerString: string,
  suffix?: string,
): FakeAiServer {
  const kafka = new Kafka({
    clientId: `fake-ai-${Date.now()}`,
    brokers: brokerString.split(',').map((broker) => broker.trim()),
    logLevel: logLevel.NOTHING,
    connectionTimeout: 10_000,
  });
  return new FakeAiServer(kafka, suffix);
}
