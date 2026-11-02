import { Kafka, logLevel } from 'kafkajs';
import { resolveScriptModelId } from './lib/ai-model';

const BROKER = process.env.KAFKA || '100.120.3.64:9092';
const MODEL_ID = resolveScriptModelId();
const USER_ID = '00000000-0000-0000-0000-000000000001';
const DOC_ID = '00000000-0000-0000-0000-000000000002';

async function testWithSubdomains(): Promise<void> {
  const uid = `out-sub-${Date.now()}`;
  const kafka = new Kafka({
    clientId: `probe5-${uid}`,
    brokers: BROKER.split(',').map((b) => b.trim()),
    logLevel: logLevel.NOTHING,
  });

  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: `probe5-group-${uid}` });

  await Promise.all([producer.connect(), consumer.connect()]);
  await consumer.subscribe({ topic: 'document_setup_response', fromBeginning: false });

  const responseMap = new Map<string, (res: Record<string, unknown>) => void>();

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      try {
        const raw = JSON.parse(message.value.toString()) as Record<string, unknown>;
        const sess = String(raw.session_id || raw.sessionId || '');
        const r = responseMap.get(sess);
        if (r) {
          responseMap.delete(sess);
          r(raw);
        }
      } catch {}
    },
  });

  const send = (payload: Record<string, unknown>, sess: string, timeoutMs = 90000) => {
    return new Promise<Record<string, unknown>>((resolve, reject) => {
      const timer = setTimeout(() => {
        responseMap.delete(sess);
        reject(new Error(`Timeout ${timeoutMs / 1000}s`));
      }, timeoutMs);

      responseMap.set(sess, (data) => {
        clearTimeout(timer);
        resolve(data);
      });

      producer.send({
        topic: 'document_setup_request',
        messages: [{ key: sess, value: JSON.stringify(payload) }],
      }).catch(reject);
    });
  };

  console.log('\n--- TEST generate_outline VỚI ĐẦY ĐỦ subdomains, level, keywords ---');
  try {
    const s = `p-out-full-${Date.now()}`;
    const r = await send({
      event_type: 'generate_outline',
      model_id: MODEL_ID,
      user_id: USER_ID,
      document_id: DOC_ID,
      session_id: s,
      language: 'vi',
      field: 'Kinh tế số',
      domain: 'Thương mại điện tử',
      subdomains: ['B2C', 'Logistics'],
      keywords: ['AI', 'Live-commerce'],
      level: 'KHOA_LUAN',
      final_proposal: {
        title: 'Tác động của Live-commerce đến hành vi mua của Gen Z tại Việt Nam',
        problem_statement: 'Live-commerce bùng nổ nhưng còn nhiều rủi ro về niềm tin và chất lượng.',
        motivation: 'Cung cấp cơ sở lý luận và thực tiễn cho các doanh nghiệp bán lẻ.',
        research_gap: 'Thiếu các nghiên cứu định lượng sâu về tâm lý mua hàng trực tiếp qua video ngắn.',
      },
      research_type: { type: 2, reason: 'Phương pháp khảo sát hỗn hợp' },
      word_count_str: '8000',
      headings: [
        { title: 'Chương 1: Mở đầu', percent: 15 },
        { title: 'Chương 2: Cơ sở lý luận', percent: 25 },
        { title: 'Chương 3: Phương pháp nghiên cứu', percent: 25 },
        { title: 'Chương 4: Kết quả và thảo luận', percent: 25 },
        { title: 'Chương 5: Kết luận và kiến nghị', percent: 10 },
      ],
      headings_percent: [15, 25, 25, 25, 10],
      outline_code: ['chapter_1', 'chapter_2', 'chapter_3', 'chapter_4', 'chapter_5'],
    }, s, 90000);
    console.log('generate_outline Result:\n', JSON.stringify(r, null, 2));
  } catch (e: unknown) {
    console.log('generate_outline err:', (e as Error).message);
  }

  await producer.disconnect();
  await consumer.disconnect();
}

testWithSubdomains().catch(console.error);
