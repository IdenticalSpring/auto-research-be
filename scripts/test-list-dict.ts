import { Kafka, logLevel } from 'kafkajs';
import { resolveScriptModelId } from './lib/ai-model';

const BROKER = process.env.KAFKA || '100.120.3.64:9092';
const MODEL_ID = resolveScriptModelId();
const USER_ID = '00000000-0000-0000-0000-000000000001';
const DOC_ID = '00000000-0000-0000-0000-000000000002';

async function testListDict(): Promise<void> {
  const uid = `list-dict-${Date.now()}`;
  const kafka = new Kafka({
    clientId: `probe2-${uid}`,
    brokers: BROKER.split(',').map((b) => b.trim()),
    logLevel: logLevel.NOTHING,
  });

  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: `probe2-group-${uid}` });

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

  const send = (payload: Record<string, unknown>, sess: string, timeoutMs = 60000) => {
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

  console.log('\n--- 1. TEST generate_title_description VỚI knowledge_base = list[dict] ---');
  try {
    const s1 = `p-td-dict-${Date.now()}`;
    const r1 = await send({
      event_type: 'generate_title_description',
      model_id: MODEL_ID,
      user_id: USER_ID,
      document_id: DOC_ID,
      session_id: s1,
      language: 'vi',
      title: 'Tác động của AI trong Logistics TMĐT',
      field: 'Kinh tế số',
      domain: 'Thương mại điện tử',
      subdomains: ['Logistics'],
      keywords: ['AI', 'Logistics'],
      knowledge_base: [
        {
          title: 'Tối ưu hóa logistics bằng AI',
          content: 'Nghiên cứu ứng dụng thị giác máy tính và học sâu vào phân loại hàng hóa tự động.',
        },
      ],
      level: 'KHOA_LUAN',
    }, s1, 60000);
    console.log('r1:\n', JSON.stringify(r1, null, 2));
  } catch (e: unknown) {
    console.log('r1 err:', (e as Error).message);
  }

  console.log('\n--- 2. TEST generate_outline VỚI headings = list[dict] ---');
  try {
    const s2 = `p-out-dict-${Date.now()}`;
    const r2 = await send({
      event_type: 'generate_outline',
      model_id: MODEL_ID,
      user_id: USER_ID,
      document_id: DOC_ID,
      session_id: s2,
      language: 'vi',
      field: 'Kinh tế số',
      domain: 'Thương mại điện tử',
      final_proposal: {
        title: 'Tác động của Live-commerce đến Gen Z',
        problem_statement: 'Live-commerce bùng nổ nhưng còn nhiều rủi ro về niềm tin.',
      },
      research_type: { type: 2, reason: 'Phương pháp khảo sát hỗn hợp' },
      word_count_str: '8000 words',
      headings: [
        { title: 'Chương 1: Mở đầu', percent: 20 },
        { title: 'Chương 2: Cơ sở lý luận', percent: 20 },
        { title: 'Chương 3: Phương pháp nghiên cứu', percent: 20 },
        { title: 'Chương 4: Kết quả và thảo luận', percent: 25 },
        { title: 'Chương 5: Kết luận và kiến nghị', percent: 15 },
      ],
      headings_percent: [20, 20, 20, 25, 15],
      outline_code: [],
    }, s2, 60000);
    console.log('r2:\n', JSON.stringify(r2, null, 2));
  } catch (e: unknown) {
    console.log('r2 err:', (e as Error).message);
  }

  await producer.disconnect();
  await consumer.disconnect();
}

testListDict().catch(console.error);
