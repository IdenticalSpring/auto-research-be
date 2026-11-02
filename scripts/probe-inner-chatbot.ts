/**
 * PROBE TẠM (không commit): bắt raw response `inner_chatbot` để xem AI trả gì
 * khi (a) payload như E2E hiện tại — không có document_id, (b) có document_id.
 *
 * Usage (trong container):
 *   npx ts-node scripts/probe-inner-chatbot.ts
 */

import { Consumer, Kafka, logLevel, Producer } from 'kafkajs';

declare const process: {
  env: Record<string, string | undefined>;
  exit: (code?: number) => never;
};

const BROKER = process.env.KAFKA || 'kafka:29092';
const MODEL_ID = process.env.AI_DEFAULT_MODEL || 'deepseek-flash';
const USER_ID =
  process.env.E2E_USER_ID || '00000000-0000-0000-0000-000000000001';
const WAIT_MS = 40_000;

const PROPOSAL = {
  title: 'Nghiên cứu tác động của Chuyển đổi số đến hiệu quả SMEs',
  level: 'KHOA_LUAN',
  research_type: 'quantitative',
};

function objectId(seed: string): string {
  const hex = seed
    .toLowerCase()
    .replace(/[^0-9a-f]/g, '')
    .slice(-24);
  return hex.padStart(24, '0');
}

function buildPayload(
  convId: string,
  withDocumentId: boolean,
): Record<string, unknown> {
  const payload: Record<string, unknown> = {
    event_type: 'inner_chatbot',
    model_id: MODEL_ID,
    user_id: USER_ID,
    conv_id: convId,
    session_id: convId,
    language: 'vi',
    question: 'Tóm tắt tài liệu đã upload',
    chat_history: [],
    short_answer: true,
    direct_answer: true,
    use_web_search_chatbot: false,
    paragraphs: 'Nội dung tài liệu người dùng đã upload.',
    final_proposal: PROPOSAL,
    section: { code: '1.1', name: 'Tính cấp thiết của đề tài' },
  };
  if (withDocumentId) {
    payload.document_id = objectId(`probe-inner-${convId}`);
  }
  return payload;
}

async function main(): Promise<void> {
  const kafka = new Kafka({
    clientId: `probe-inner-${Date.now()}`,
    brokers: BROKER.split(',').map((item) => item.trim()),
    logLevel: logLevel.NOTHING,
    connectionTimeout: 10_000,
  });
  const producer: Producer = kafka.producer();
  const consumer: Consumer = kafka.consumer({
    groupId: `probe-inner-group-${Date.now()}`,
  });
  await Promise.all([producer.connect(), consumer.connect()]);
  await consumer.subscribe({
    topic: 'chatbot_response',
    fromBeginning: false,
  });

  const seen: Record<string, Record<string, unknown>[]> = {};
  const state = { current: '' };
  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      try {
        const parsed = JSON.parse(message.value.toString()) as Record<
          string,
          unknown
        >;
        if (parsed.event_type !== 'inner_chatbot') return;
        const convId = String(
          parsed.conv_id ?? parsed.session_id ?? state.current ?? '(none)',
        );
        (seen[convId] ??= []).push(parsed);
        console.log(`   📥 [${convId}] ${JSON.stringify(parsed).slice(0, 700)}`);
      } catch {
        // ignore non-JSON
      }
    },
  });

  const variants = [
    { id: 'I1', withDocumentId: false, label: 'không document_id (như E2E)' },
    { id: 'I2', withDocumentId: true, label: 'có document_id (24-hex)' },
  ];

  for (const variant of variants) {
    const convId = `probe-inner-${Date.now()}-${variant.id.toLowerCase()}`;
    const payload = buildPayload(convId, variant.withDocumentId);
    state.current = convId;
    console.log(`\n── ${variant.id} (${variant.label})`);
    console.log(`   keys=${Object.keys(payload).join(',')}`);
    const startedAt = Date.now();

    await producer.send({
      topic: 'chatbot_request',
      messages: [{ key: convId, value: JSON.stringify(payload) }],
    });

    // Chờ message đầu tiên (hoặc hết 40s), không dùng harness để tránh
    // yêu cầu echo id — mục tiêu là xem raw payload AI trả về.
    while (Date.now() - startedAt < WAIT_MS) {
      const got = (seen[convId] ?? []).length > 0;
      if (got) break;
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
    const duration = ((Date.now() - startedAt) / 1000).toFixed(1);
    const messages = seen[convId] ?? [];
    if (messages.length === 0) {
      console.log(`   ❌ ${duration}s | không nhận được message nào`);
    } else {
      const first = messages[0];
      console.log(
        `   ✅ ${duration}s | keys=${Object.keys(first).join(',')} | error_code=${String(first.error_code ?? '(none)')} | error=${String(first.error ?? '(none)')}`,
      );
    }
  }

  await Promise.all([producer.disconnect(), consumer.disconnect()]);
}

main().catch((error: Error) => {
  console.error('Fatal:', error.message);
  process.exit(1);
});
