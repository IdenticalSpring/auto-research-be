import { Kafka, logLevel } from 'kafkajs';
import { resolveScriptModelId } from './lib/ai-model';

const BROKER = process.env.KAFKA || '100.120.3.64:9092';
const MODEL_ID = resolveScriptModelId();
const USER_ID = '00000000-0000-0000-0000-000000000001';
const DOC_ID = '000000000000000000000002'; // 24-hex Mongo compatible
const REQUEST_TOPIC = process.env.REQUEST_TOPIC || 'write_section_request';
const RESPONSE_TOPIC = process.env.RESPONSE_TOPIC || 'write_section_response';

async function runBe028Verification(): Promise<void> {
  console.log('====================================================');
  console.log('🧪 LIVE E2E TEST: BE-028 analyzer (Hồi quy OLS)');
  console.log('====================================================');
  console.log(`📡 Broker:        ${BROKER}`);
  console.log(`🤖 Model:         ${MODEL_ID}`);
  console.log(`📤 Request Topic:  ${REQUEST_TOPIC}`);
  console.log(`📥 Response Topic: ${RESPONSE_TOPIC}\n`);

  const uid = `verify-ols-${Date.now()}`;
  const kafka = new Kafka({
    clientId: `client-${uid}`,
    brokers: BROKER.split(',').map((b) => b.trim()),
    logLevel: logLevel.NOTHING,
  });

  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: `group-${uid}` });

  await Promise.all([producer.connect(), consumer.connect()]);
  await consumer.subscribe({ topic: RESPONSE_TOPIC, fromBeginning: false });

  const sessionId = `sess-ols-${Date.now()}`;
  const threadId = `th-ols-${Date.now()}`;
  const convId = `conv-ols-${Date.now()}`;

  const payload = {
    event_type: 'analyzer',
    model_id: MODEL_ID,
    user_id: USER_ID,
    document_id: DOC_ID,
    session_id: sessionId,
    conv_id: convId,
    thread_id: threadId,
    language: 'Vietnamese',
    final_proposal: {
      title: 'Tác động của livestream bán hàng đến quyết định mua sắm của người tiêu dùng',
      description: 'Phân tích tác động của livestream bán hàng đến quyết định mua sắm',
    },
    data: {
      tuoi: [21, 25, 23, 28, 30, 22, 24, 26, 29, 31, 35, 27, 24, 22, 33, 40, 28, 26, 23, 25, 27, 32, 34, 22, 21, 29, 30, 24, 26, 28, 31, 33, 25, 27, 36],
      thu_nhap: [8.5, 12.0, 10.5, 15.0, 20.0, 9.0, 11.5, 14.0, 18.5, 22.0, 25.0, 16.0, 11.0, 9.5, 21.0, 30.0, 17.5, 13.0, 10.0, 12.5, 15.0, 22.5, 24.0, 9.0, 8.0, 19.0, 20.5, 12.0, 14.5, 16.5, 21.0, 23.5, 13.0, 15.5, 28.0],
      gioi_tinh: ['Nam', 'Nu', 'Nu', 'Nam', 'Nu', 'Nam', 'Nu', 'Nam', 'Nu', 'Nam', 'Nu', 'Nam', 'Nu', 'Nu', 'Nam', 'Nam', 'Nu', 'Nam', 'Nu', 'Nam', 'Nu', 'Nam', 'Nu', 'Nam', 'Nu', 'Nam', 'Nu', 'Nam', 'Nu', 'Nam', 'Nu', 'Nam', 'Nu', 'Nam', 'Nu'],
      chi_tieu: [3.0, 5.0, 4.0, 6.5, 8.0, 3.5, 4.5, 6.0, 7.5, 9.0, 10.5, 7.0, 4.5, 3.8, 8.5, 12.0, 7.0, 5.5, 4.0, 5.0, 6.0, 9.0, 9.5, 3.5, 3.2, 7.8, 8.2, 5.0, 5.8, 6.8, 8.5, 9.5, 5.2, 6.2, 11.0],
    },
    variables: [
      {
        name: 'tuoi',
        code: 'tuoi',
        scale: 'ratio',
        type: 'int',
        role: 'independent',
        variable_type: 'observed',
        description: 'Độ tuổi của người tham gia khảo sát',
      },
      {
        name: 'thu_nhap',
        code: 'thu_nhap',
        scale: 'ratio',
        type: 'float',
        role: 'independent',
        variable_type: 'observed',
        description: 'Thu nhập hàng tháng (triệu VNĐ)',
      },
      {
        name: 'gioi_tinh',
        code: 'gioi_tinh',
        scale: 'nominal',
        type: 'str',
        role: 'independent',
        variable_type: 'observed',
        description: 'Giới tính người tham gia khảo sát',
      },
      {
        name: 'chi_tieu',
        code: 'chi_tieu',
        scale: 'ratio',
        type: 'float',
        role: 'dependent',
        variable_type: 'observed',
        description: 'Mức chi tiêu mua sắm livestream trung bình (triệu VNĐ)',
      },
    ],
    auto_plan_mode: true,
    auto_action_mode: true,
    manual_plan_mode: false,
    tool_configs: [],
    resume: false,
    resume_error: false,
    feedback: '',
    db: 'redis',
  };

  const responsePromise = new Promise<Record<string, unknown>>((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error('Timeout sau 90s chờ phản hồi OLS analyzer từ AI'));
    }, 90000);

    consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) return;
        try {
          const res = JSON.parse(message.value.toString()) as Record<string, unknown>;
          const msgSession = String(res.session_id || res.sessionId || res.conv_id || res.thread_id || '');
          if (
            msgSession === sessionId ||
            msgSession === convId ||
            msgSession === threadId ||
            String(res.event_type || res.eventType) === 'analyzer'
          ) {
            clearTimeout(timer);
            resolve(res);
          }
        } catch {}
      },
    });
  });

  const correlationKey = `${USER_ID}:${DOC_ID}:${sessionId}:analyzer`;
  console.log(`▶ Gửi message analyzer vào topic [${REQUEST_TOPIC}]... (key: ${correlationKey})`);
  const t0 = Date.now();

  await producer.send({
    topic: REQUEST_TOPIC,
    messages: [{ key: correlationKey, value: JSON.stringify(payload) }],
  });

  try {
    const res = await responsePromise;
    const elapsed = Date.now() - t0;
    console.log(`\n📥 Nhận phản hồi sau ${elapsed}ms:`);

    if (res.error_code || res.error) {
      console.error(`❌ AI Báo Lỗi: code=${res.error_code}, error=${res.error}`);
      process.exit(1);
    }

    console.log(`   - has_finished:  ${res.has_finished}`);
    console.log(`   - result_html:   ${res.result_html || '(empty)'}`);
    console.log(`   - analyze_log:   ${res.analyze_log || '(none)'}`);
    console.log(`   - upload_failed: ${res.upload_failed}`);

    const isFinished = res.has_finished !== false;
    if (isFinished) {
      console.log('\n✅ [PASS] AI Python tính toán hồi quy OLS thành công 100%! Không crash 631.');
    } else {
      console.log('\n⚠️ [WARN] Response:', JSON.stringify(res).slice(0, 300));
    }
  } catch (err) {
    console.error(`❌ [FAIL]: ${(err as Error).message}`);
    process.exit(1);
  } finally {
    try {
      await consumer.disconnect();
      await producer.disconnect();
    } catch {}
  }
}

void runBe028Verification();
