import { registerAs } from '@nestjs/config';

/**
 * Nguồn sự thật DUY NHẤT cho cờ bật/tắt Kafka.
 *
 * Trước đây mỗi nơi tự đọc `KAFKA_ENABLED` với default khác nhau
 * (`main.ts` = 'true', `job.service`/`interrupt-resume` = 'false') nên có thể
 * xảy ra trạng thái "producer bật, job chạy fallback mock" trong cùng một process.
 *
 * Quy ước:
 * - Không set hoặc set chuỗi rỗng  ⇒ mặc định BẬT (giữ tương thích hành vi cũ của main.ts).
 * - `'true'`  (không phân biệt hoa/thường) ⇒ BẬT.
 * - Mọi giá trị khác (`'false'`, `'0'`, `'no'`...) ⇒ TẮT.
 */
export function resolveKafkaEnabled(raw: string | undefined): boolean {
  if (raw === undefined || raw.trim() === '') {
    return true;
  }
  return raw.trim().toLowerCase() === 'true';
}

export function resolveKafkaTopicSuffix(
  explicitSuffix?: string,
  _useLocalTopics?: string | boolean,
): string {
  if (explicitSuffix !== undefined) {
    return explicitSuffix;
  }
  return '';
}

export default registerAs('kafka', () => {
  const enabled = resolveKafkaEnabled(process.env.KAFKA_ENABLED);
  const suffix = resolveKafkaTopicSuffix(
    process.env.KAFKA_TOPIC_SUFFIX,
    process.env.KAFKA_USE_LOCAL_TOPICS,
  );
  const isLocal = false;

  const rawBrokers =
    process.env.KAFKA || process.env.KAFKA_BROKERS || '167.254.70.125:9092';
  const brokers = rawBrokers.split(',').map((b) => b.trim());

  return {
    enabled,
    isLocal,
    brokers,
    clientId: process.env.KAFKA_CLIENT_ID || 'autoresearch-backend',
    groupId: process.env.KAFKA_GROUP_ID || 'autoresearch-group',
    ssl: process.env.KAFKA_SSL === 'true',
    topics: {
      document_setup: {
        request: `document_setup_request${suffix}`,
        response: `document_setup_response${suffix}`,
      },
      write_report: {
        request: `write_section_request${suffix}`,
        response: `write_section_response${suffix}`,
      },
      chatbot: {
        request: `chatbot_request${suffix}`,
        response: `chatbot_response${suffix}`,
      },
      enhancement: {
        request: `enhancement_request${suffix}`,
        response: `enhancement_response${suffix}`,
      },
    },
  };
});
