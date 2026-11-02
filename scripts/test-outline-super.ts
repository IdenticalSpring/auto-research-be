import { Kafka, logLevel } from 'kafkajs';
import { resolveScriptModelId } from './lib/ai-model';

const BROKER = process.env.KAFKA || '100.120.3.64:9092';
const MODEL_ID = resolveScriptModelId();
const USER_ID = '00000000-0000-0000-0000-000000000001';
const DOC_ID = '00000000-0000-0000-0000-000000000002';

async function testAllArrayFields(): Promise<void> {
  const uid = `all-arr-${Date.now()}`;
  const kafka = new Kafka({
    clientId: `probe6-${uid}`,
    brokers: BROKER.split(',').map((b) => b.trim()),
    logLevel: logLevel.NOTHING,
  });

  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: `probe6-group-${uid}` });

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

  const fullProposal = {
    title: 'Tác động của Live-commerce đến hành vi mua sắm của Gen Z tại Việt Nam',
    problem_statement: 'Live-commerce bùng nổ nhưng còn nhiều rủi ro về niềm tin và chất lượng.',
    motivation: 'Cung cấp cơ sở lý luận và thực tiễn cho các doanh nghiệp bán lẻ.',
    research_gap: 'Thiếu các nghiên cứu định lượng sâu về tâm lý mua hàng trực tiếp qua video ngắn.',
    final_model: 'Mô hình chấp nhận công nghệ TAM mở rộng',
    hypothesis: 'H1: Tính tương tác ảnh hưởng tích cực đến niềm tin; H2: Niềm tin ảnh hưởng đến ý định mua',
    variables: ['Tính tương tác', 'Niềm tin', 'Ý định mua'],
    survey_questions: ['Bạn có thường xuyên xem livestream bán hàng không?'],
    questions: ['Các yếu tố nào thúc đẩy bạn chốt đơn khi xem livestream?'],
  };

  console.log('\n--- TEST generate_outline VỚI SIÊU ĐẦY ĐỦ CÁC TRƯỜNG ARRAY ---');
  try {
    const s = `p-out-super-${Date.now()}`;
    const r = await send({
      event_type: 'generate_outline',
      model_id: MODEL_ID,
      user_id: USER_ID,
      document_id: DOC_ID,
      session_id: s,
      language: 'vi',
      field: 'Kinh tế số',
      domain: 'Thương mại điện tử',
      domains: ['Thương mại điện tử', 'Kinh tế số'],
      subdomains: ['B2C', 'Logistics', 'Live-commerce'],
      keywords: ['AI', 'Live-commerce', 'Gen Z', 'E-commerce'],
      level: 'KHOA_LUAN',
      final_proposal: fullProposal,
      proposals: [fullProposal],
      proposal: fullProposal,
      research_type: { type: 2, reason: 'Phương pháp khảo sát hỗn hợp' },
      word_count_str: '8000',
      headings: [
        { title: 'Chương 1: Tổng quan tình hình nghiên cứu', percent: 15 },
        { title: 'Chương 2: Cơ sở lý thuyết và mô hình nghiên cứu', percent: 25 },
        { title: 'Chương 3: Phương pháp nghiên cứu', percent: 25 },
        { title: 'Chương 4: Kết quả nghiên cứu và thảo luận', percent: 25 },
        { title: 'Chương 5: Kết luận và hàm ý quản trị', percent: 10 },
      ],
      headings_percent: [15, 25, 25, 25, 10],
      outline_code: ['chapter_1', 'chapter_2', 'chapter_3', 'chapter_4', 'chapter_5'],
      knowledge_base: [
        {
          title: 'Tổng quan Live-commerce',
          content: 'Nghiên cứu về các nền tảng TikTok Shop, Shopee Live và hành vi tiêu dùng.',
        },
      ],
    }, s, 90000);
    console.log('Result:\n', JSON.stringify(r, null, 2));
  } catch (e: unknown) {
    console.log('Error:', (e as Error).message);
  }

  await producer.disconnect();
  await consumer.disconnect();
}

testAllArrayFields().catch(console.error);
