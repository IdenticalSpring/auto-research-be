import { Kafka, logLevel } from 'kafkajs';
import { resolveScriptModelId } from './lib/ai-model';

const BROKER = process.env.KAFKA || '100.120.3.64:9092';
const MODEL_ID = resolveScriptModelId();
const USER_ID = '00000000-0000-0000-0000-000000000001';
const DOC_ID = '00000000-0000-0000-0000-000000000002';

async function testFixedOutline(): Promise<void> {
  const uid = `outline-fix-${Date.now()}`;
  const kafka = new Kafka({
    clientId: `probe3-${uid}`,
    brokers: BROKER.split(',').map((b) => b.trim()),
    logLevel: logLevel.NOTHING,
  });

  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: `probe3-group-${uid}` });

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

  console.log('\n--- TEST generate_outline VỚI word_count_str = "8000" ---');
  try {
    const s2 = `p-out-fixed-${Date.now()}`;
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
      word_count_str: '8000',
      headings: [
        { title: 'Chương 1: Mở đầu', percent: 20 },
        { title: 'Chương 2: Cơ sở lý luận', percent: 20 },
        { title: 'Chương 3: Phương pháp nghiên cứu', percent: 20 },
        { title: 'Chương 4: Kết quả và thảo luận', percent: 25 },
        { title: 'Chương 5: Kết luận và kiến nghị', percent: 15 },
      ],
      headings_percent: [20, 20, 20, 25, 15],
      outline_code: [],
    }, s2, 90000);
    console.log('generate_outline Result:\n', JSON.stringify(r2, null, 2));
  } catch (e: unknown) {
    console.log('generate_outline err:', (e as Error).message);
  }

  await producer.disconnect();
  await consumer.disconnect();
}

testFixedOutline().catch(console.error);
