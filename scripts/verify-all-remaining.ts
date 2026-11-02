import { Kafka, logLevel } from 'kafkajs';
import http from 'node:http';
import { resolveScriptModelId } from './lib/ai-model';

interface Step {
  name: string;
  topic?: string;
  isHttp?: boolean;
  httpMethod?: string;
  httpPath?: string;
  payload: Record<string, unknown>;
  validate: (res: Record<string, unknown>) => boolean;
}

const BROKER = process.env.KAFKA || '100.120.3.64:9092';
const MODEL_ID = resolveScriptModelId();
const USER_ID = '00000000-0000-0000-0000-000000000001';
const DOC_ID = '00000000-0000-0000-0000-000000000002';

async function runRemainingVerification(): Promise<void> {
  console.log('====================================================');
  console.log('🧪 BATCH E2E TEST: KIỂM CHỨNG CÁC TASK DONE CÒN LẠI');
  console.log('====================================================\n');

  const uid = `rem-${Date.now()}`;
  const kafka = new Kafka({
    clientId: `batch-${uid}`,
    brokers: BROKER.split(',').map((b) => b.trim()),
    logLevel: logLevel.NOTHING,
  });

  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: `batch-grp-${uid}` });

  await Promise.all([producer.connect(), consumer.connect()]);
  await consumer.subscribe({ topic: 'document_setup_response', fromBeginning: false });

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

  const sendKafka = (topic: string, payload: Record<string, unknown>, timeoutMs = 60000) => {
    const sess = String(payload.session_id);
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

  const sendHttp = (method: string, path: string, body: Record<string, unknown>) => {
    return new Promise<Record<string, unknown>>((resolve, reject) => {
      const postData = JSON.stringify(body);
      const req = http.request(
        {
          hostname: '127.0.0.1',
          port: 3001,
          path,
          method,
          headers: {
            'Content-Type': 'application/json',
            'x-api-key': 'autoresearch-ai-secret-key',
            'Content-Length': Buffer.byteLength(postData),
          },
        },
        (res) => {
          let raw = '';
          res.on('data', (c) => (raw += c));
          res.on('end', () => {
            try {
              resolve(JSON.parse(raw));
            } catch {
              resolve({ statusCode: res.statusCode, raw });
            }
          });
        },
      );
      req.on('error', reject);
      req.write(postData);
      req.end();
    });
  };

  const steps: Step[] = [
    {
      name: 'BE-004: Webhook Health-Check',
      isHttp: true,
      httpMethod: 'PUT',
      httpPath: '/health-check/test-job-001',
      payload: { eventType: 'write_content', processStatus: 3, message: 'Processing data via AI' },
      validate: (res) => res.status === 'ok' || res.statusCode === 200,
    },
    {
      name: 'BE-012: generate_titles',
      topic: 'document_setup_request',
      payload: {
        event_type: 'generate_titles',
        model_id: MODEL_ID,
        user_id: USER_ID,
        document_id: DOC_ID,
        session_id: `sess-t-${Date.now()}`,
        language: 'vi',
        field: 'Kinh tế số',
        domain: 'Thương mại điện tử',
        subdomains: ['B2C'],
        level: 'KHOA_LUAN',
        use_web_search: false,
      },
      validate: (res) => Array.isArray(res.proposals) || Array.isArray(res.titles) || Array.isArray(res.data),
    },
    {
      name: 'BE-013: generate_title_description',
      topic: 'document_setup_request',
      payload: {
        event_type: 'generate_title_description',
        model_id: MODEL_ID,
        user_id: USER_ID,
        document_id: DOC_ID,
        session_id: `sess-td-${Date.now()}`,
        language: 'vi',
        title: 'Ứng dụng AI trong tối ưu hóa logistics TMĐT',
        field: 'Kinh tế số',
        domain: 'Thương mại điện tử',
        subdomains: ['Logistics'],
        keywords: ['AI', 'Logistics'],
        knowledge_base: [],
        level: 'KHOA_LUAN',
      },
      validate: (res) => typeof res.proposal === 'object' && res.proposal !== null,
    },
    {
      name: 'BE-014: generate_title_description_no_info',
      topic: 'document_setup_request',
      payload: {
        event_type: 'generate_title_description_no_info',
        model_id: MODEL_ID,
        user_id: USER_ID,
        document_id: DOC_ID,
        session_id: `sess-tdni-${Date.now()}`,
        language: 'vi',
        title: 'Tác động của Fintech đến hành vi tiết kiệm của Gen Z tại Việt Nam',
        level: 'KHOA_LUAN',
        use_web_search: false,
      },
      validate: (res) => typeof res.proposal === 'object' && res.proposal !== null,
    },
    {
      name: 'BE-015: mix_titles',
      topic: 'document_setup_request',
      payload: {
        event_type: 'mix_titles',
        model_id: MODEL_ID,
        user_id: USER_ID,
        document_id: DOC_ID,
        session_id: `sess-mix-${Date.now()}`,
        language: 'vi',
        field: 'Kinh tế số',
        domain: 'Thương mại điện tử',
        level: 'KHOA_LUAN',
        proposals: [
          { title: 'Tác động của Fintech đến bán lẻ trực tuyến' },
          { title: 'Tối ưu hóa chuỗi cung ứng bằng công nghệ IoT' },
        ],
      },
      validate: (res) => typeof res.proposal === 'object' && res.proposal !== null,
    },
    {
      name: 'BE-017: generate_outline',
      topic: 'document_setup_request',
      payload: {
        event_type: 'generate_outline',
        model_id: MODEL_ID,
        user_id: USER_ID,
        document_id: DOC_ID,
        session_id: `sess-out-${Date.now()}`,
        language: 'vi',
        field: 'Kinh tế số',
        domain: 'Thương mại điện tử',
        final_proposal: {
          title: 'Tác động của Live-commerce đến hành vi mua của Gen Z tại Việt Nam',
          problem_statement: 'Live-commerce bùng nổ nhưng còn nhiều rủi ro về niềm tin và chất lượng.',
        },
        research_type: { type: 2, reason: 'Phương pháp hỗn hợp' },
        word_count_str: '8000 words',
        headings: [],
        headings_percent: [],
        outline_code: [],
      },
      validate: (res) => typeof res.outline === 'object' && res.outline !== null,
    },
  ];

  console.log(`Đang chạy ${steps.length} kịch bản kiểm thử...\n`);
  let passCount = 0;

  for (const s of steps) {
    const t0 = Date.now();
    process.stdout.write(`▶ [${s.name}]... `);
    try {
      const res = s.isHttp
        ? await sendHttp(s.httpMethod!, s.httpPath!, s.payload)
        : await sendKafka(s.topic!, s.payload);
      const ms = Date.now() - t0;
      if (res.error_code) {
        console.log(`❌ LỖI AI (${res.error_code}): ${res.error} (${ms}ms)`);
      } else if (s.validate(res)) {
        passCount++;
        console.log(`✅ PASS (${ms}ms)`);
      } else {
        console.log(`⚠️  FAIL validation: ${JSON.stringify(res).slice(0, 150)} (${ms}ms)`);
      }
    } catch (err) {
      console.log(`❌ ERROR: ${(err as Error).message}`);
    }
  }

  console.log('\n====================================================');
  console.log(`🏁 KẾT THÚC BATCH: ${passCount}/${steps.length} TASKS PASS`);
  console.log('====================================================\n');

  try {
    await consumer.disconnect();
    await producer.disconnect();
  } catch {}
  process.exit(passCount === steps.length ? 0 : 1);
}

void runRemainingVerification();
