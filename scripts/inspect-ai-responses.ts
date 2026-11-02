import { Kafka, logLevel } from 'kafkajs';
import { resolveScriptModelId } from './lib/ai-model';

const BROKER = process.env.KAFKA || '100.120.3.64:9092';
const MODEL_ID = resolveScriptModelId();
const USER_ID = '00000000-0000-0000-0000-000000000001';
const DOC_ID = '00000000-0000-0000-0000-000000000002';

async function inspect(): Promise<void> {
  const uid = `inspect-${Date.now()}`;
  const kafka = new Kafka({
    clientId: `inspect-${uid}`,
    brokers: BROKER.split(',').map((b) => b.trim()),
    logLevel: logLevel.NOTHING,
  });

  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: `inspect-group-${uid}` });

  await Promise.all([producer.connect(), consumer.connect()]);

  const topics = ['document_setup_response', 'write_section_response', 'chatbot_response'];
  for (const t of topics) {
    await consumer.subscribe({ topic: t, fromBeginning: false });
  }

  const responseMap = new Map<string, (res: Record<string, unknown>) => void>();

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      try {
        const raw = JSON.parse(message.value.toString()) as Record<string, unknown>;
        const sess = String(raw.session_id || raw.sessionId || raw.conv_id || '');
        const r = responseMap.get(sess);
        if (r) {
          responseMap.delete(sess);
          r(raw);
        }
      } catch {}
    },
  });

  const callAi = (topic: string, payload: Record<string, unknown>, sess: string, timeoutMs = 60000) => {
    return new Promise<Record<string, unknown>>((resolve, reject) => {
      const timer = setTimeout(() => {
        responseMap.delete(sess);
        reject(new Error(`Timeout after ${timeoutMs / 1000}s`));
      }, timeoutMs);

      responseMap.set(sess, (data) => {
        clearTimeout(timer);
        resolve(data);
      });

      producer.send({
        topic,
        messages: [{ key: sess, value: JSON.stringify(payload) }],
      }).catch(reject);
    });
  };

  console.log('\n--- 1. TEST CHATBOT WORKER (outer_chatbot với language: vi) ---');
  try {
    const sessChat = `sess-chat-${Date.now()}`;
    const resChat = await callAi('chatbot_request', {
      event_type: 'outer_chatbot',
      model_id: MODEL_ID,
      user_id: USER_ID,
      document_id: DOC_ID,
      conv_id: `conv-${Date.now()}`,
      session_id: sessChat,
      language: 'vi',
      query: 'Xin chào, bạn có thể tóm tắt nhiệm vụ của bạn không?',
      history: [],
    }, sessChat, 30000);
    console.log('Chatbot Response:\n', JSON.stringify(resChat, null, 2));
  } catch (e: unknown) {
    console.error('Chatbot error:', (e as Error).message);
  }

  console.log('\n--- 2. TEST WRITE SECTION WORKER (delete_report với đầy đủ param) ---');
  try {
    const sessDel = `sess-del-${Date.now()}`;
    const resDel = await callAi('write_section_request', {
      event_type: 'delete_report',
      model_id: MODEL_ID,
      user_id: USER_ID,
      document_id: DOC_ID,
      session_id: sessDel,
      language: 'Vietnamese',
      thread_id: ['th-001'],
    }, sessDel, 30000);
    console.log('delete_report Response:\n', JSON.stringify(resDel, null, 2));
  } catch (e: unknown) {
    console.error('delete_report error:', (e as Error).message);
  }

  console.log('\n--- 3. TEST generate_title_description (với knowledge_base không rỗng) ---');
  try {
    const sessTd = `sess-td-${Date.now()}`;
    const resTd = await callAi('document_setup_request', {
      event_type: 'generate_title_description',
      model_id: MODEL_ID,
      user_id: USER_ID,
      document_id: DOC_ID,
      session_id: sessTd,
      language: 'vi',
      title: 'Tác động của AI trong Logistics TMĐT',
      field: 'Kinh tế số',
      domain: 'Thương mại điện tử',
      subdomains: ['Logistics'],
      keywords: ['AI', 'Logistics'],
      knowledge_base: ['Nghiên cứu về chuyển đổi số kho hàng và giao vận chặng cuối.'],
      level: 'KHOA_LUAN',
    }, sessTd, 60000);
    console.log('generate_title_description Response:\n', JSON.stringify(resTd, null, 2));
  } catch (e: unknown) {
    console.error('generate_title_description error:', (e as Error).message);
  }

  console.log('\n--- 4. TEST generate_outline (với 5 headings chuẩn) ---');
  try {
    const sessOut = `sess-out-${Date.now()}`;
    const resOut = await callAi('document_setup_request', {
      event_type: 'generate_outline',
      model_id: MODEL_ID,
      user_id: USER_ID,
      document_id: DOC_ID,
      session_id: sessOut,
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
        'Chương 1: Tổng quan',
        'Chương 2: Cơ sở lý thuyết',
        'Chương 3: Phương pháp nghiên cứu',
        'Chương 4: Kết quả nghiên cứu',
        'Chương 5: Kết luận',
      ],
      headings_percent: [20, 20, 20, 20, 20],
      outline_code: [],
    }, sessOut, 60000);
    console.log('generate_outline Response:\n', JSON.stringify(resOut, null, 2));
  } catch (e: unknown) {
    console.error('generate_outline error:', (e as Error).message);
  }

  await producer.disconnect();
  await consumer.disconnect();
}

inspect().catch(console.error);
