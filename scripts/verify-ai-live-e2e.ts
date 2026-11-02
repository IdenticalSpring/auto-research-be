import { Kafka, logLevel } from 'kafkajs';
import { resolveScriptModelId } from './lib/ai-model';
import { MIN_E2E_TIMEOUT_MS } from './lib/e2e-timeouts';

interface TestStep {
  name: string;
  topic: string;
  payload: Record<string, unknown>;
  validate: (res: Record<string, unknown>) => boolean;
}

const BROKER = process.env.KAFKA || '100.120.3.64:9092';
const MODEL_ID = resolveScriptModelId();
const USER_ID = '00000000-0000-0000-0000-000000000001';
const DOC_ID = '00000000-0000-0000-0000-000000000002';

async function runLiveVerification(): Promise<void> {
  console.log('====================================================');
  console.log('🚀 LIVE E2E VERIFICATION WITH REAL AI SERVER (GEMINI)');
  console.log('====================================================');
  console.log(`📡 Broker:   ${BROKER}`);
  console.log(`🤖 Model:    ${MODEL_ID}\n`);

  const uid = `live-${Date.now()}`;
  const kafka = new Kafka({
    clientId: `verifier-${uid}`,
    brokers: BROKER.split(',').map((b) => b.trim()),
    logLevel: logLevel.NOTHING,
  });

  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: `verifier-group-${uid}` });

  await Promise.all([producer.connect(), consumer.connect()]);
  await consumer.subscribe({ topic: 'document_setup_response', fromBeginning: false });
  await consumer.subscribe({ topic: 'chatbot_response', fromBeginning: false });

  const responseMap = new Map<string, (res: Record<string, unknown>) => void>();

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      try {
        const data = JSON.parse(message.value.toString()) as Record<string, unknown>;
        const sess = String(data.session_id || data.sessionId || '');
        const resolver = responseMap.get(sess);
        if (resolver) {
          responseMap.delete(sess);
          resolver(data);
        }
      } catch {}
    },
  });

  const sendAndAwait = (topic: string, payload: Record<string, unknown>, timeoutMs = MIN_E2E_TIMEOUT_MS) => {
    const sess = String(payload.session_id);
    return new Promise<Record<string, unknown>>((resolve, reject) => {
      const timer = setTimeout(() => {
        responseMap.delete(sess);
        reject(new Error(`Timeout after ${timeoutMs / 1000}s awaiting response`));
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

  const tests: TestStep[] = [
    {
      name: 'BE-009: generate_domains',
      topic: 'document_setup_request',
      payload: {
        event_type: 'generate_domains',
        model_id: MODEL_ID,
        user_id: USER_ID,
        document_id: DOC_ID,
        session_id: `sess-dom-${Date.now()}`,
        language: 'vi',
        field: 'Kinh tế số',
        level: 'dai_hoc',
        domains_num: 3,
        is_tool: false,
      },
      validate: (res) => Array.isArray(res.domains) && res.domains.length > 0,
    },
    {
      name: 'BE-010: generate_subdomains',
      topic: 'document_setup_request',
      payload: {
        event_type: 'generate_subdomains',
        model_id: MODEL_ID,
        user_id: USER_ID,
        document_id: DOC_ID,
        session_id: `sess-sub-${Date.now()}`,
        language: 'vi',
        field: 'Kinh tế số',
        domain: 'Thương mại điện tử',
        subdomains_num: 3,
        is_tool: false,
      },
      validate: (res) => Array.isArray(res.subdomains) && res.subdomains.length > 0,
    },
    {
      name: 'BE-011: generate_keywords',
      topic: 'document_setup_request',
      payload: {
        event_type: 'generate_keywords',
        model_id: MODEL_ID,
        user_id: USER_ID,
        document_id: DOC_ID,
        session_id: `sess-kw-${Date.now()}`,
        language: 'vi',
        field: 'Kinh tế số',
        domain: 'Thương mại điện tử',
        subdomains: ['B2C'],
        proposal: { title: 'Tác động của Live-commerce đến Gen Z' },
      },
      validate: (res) => Array.isArray(res.keywords) || (typeof res.keywords === 'object' && res.keywords !== null),
    },
    {
      name: 'BE-016: get_research_type',
      topic: 'document_setup_request',
      payload: {
        event_type: 'get_research_type',
        model_id: MODEL_ID,
        user_id: USER_ID,
        document_id: DOC_ID,
        session_id: `sess-rt-${Date.now()}`,
        language: 'vi',
        field: 'Kinh tế số',
        domain: 'Thương mại điện tử',
        proposal: { title: 'Tác động của Live-commerce đến hành vi mua của Gen Z tại Việt Nam' },
      },
      validate: (res) => typeof res.research_type === 'string' && res.research_type.length > 0,
    },
    {
      name: 'BE-041: outer_chatbot',
      topic: 'chatbot_request',
      payload: {
        event_type: 'outer_chatbot',
        model_id: MODEL_ID,
        user_id: USER_ID,
        document_id: DOC_ID,
        conv_id: 'conv-test-01',
        session_id: `sess-chat-${Date.now()}`,
        language: 'vi',
        question: 'Hãy giải thích ngắn gọn OLS là gì trong kinh tế lượng?',
        chat_history: [],
        short_answer: true,
      },
      validate: (res) => typeof res.response === 'string' && res.response.length > 10,
    },
  ];

  console.log(`Bắt đầu chạy ${tests.length} luồng kiểm thử thực tế...\n`);
  let passCount = 0;

  for (const t of tests) {
    const t0 = Date.now();
    process.stdout.write(`▶ Đang test [${t.name}]... `);
    try {
      const res = await sendAndAwait(t.topic, t.payload);
      const ms = Date.now() - t0;
      if (res.error_code) {
        console.log(`❌ LỖI AI (${res.error_code}): ${res.error} (${ms}ms)`);
      } else if (t.validate(res)) {
        passCount++;
        console.log(`✅ PASS (${ms}ms, in: ${res.input_tokens ?? 0}, out: ${res.output_tokens ?? 0})`);
      } else {
        console.log(`⚠️  FAIL validate data (${ms}ms)`);
      }
    } catch (err) {
      console.log(`❌ TIMEOUT/ERROR: ${(err as Error).message}`);
    }
  }

  console.log('\n====================================================');
  console.log(`🏁 KẾT QUẢ TỔNG HỢP: ${passCount}/${tests.length} LUỒNG PASS`);
  console.log('====================================================\n');

  try {
    await consumer.disconnect();
    await producer.disconnect();
  } catch {}
  process.exit(passCount === tests.length ? 0 : 1);
}

void runLiveVerification();
