import { Kafka, logLevel } from 'kafkajs';
import { resolveScriptModelId } from './lib/ai-model';

const BROKER = process.env.KAFKA || '100.120.3.64:9092';
const MODEL_ID = resolveScriptModelId();
const USER_ID = '00000000-0000-0000-0000-000000000001';
const DOC_ID = '00000000-0000-0000-0000-000000000002';

interface TestItem {
  name: string;
  topicReq: string;
  topicRes: string;
  timeoutMs: number;
  payload: Record<string, unknown>;
  validate: (res: Record<string, unknown>) => { pass: boolean; detail: string };
}

async function runVerifyFixes(): Promise<void> {
  console.log('====================================================');
  console.log('🔍 VERIFY AI SERVER FIXES & BE CONTRACTS');
  console.log('====================================================');
  console.log(`📡 Broker:   ${BROKER}`);
  console.log(`🤖 Model:    ${MODEL_ID}\n`);

  const uid = `verify-fixes-${Date.now()}`;
  const kafka = new Kafka({
    clientId: `verifier-fixes-${uid}`,
    brokers: BROKER.split(',').map((b) => b.trim()),
    logLevel: logLevel.NOTHING,
  });

  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: `group-fixes-${uid}` });

  await Promise.all([producer.connect(), consumer.connect()]);

  const topicsRes = [
    'document_setup_response',
    'write_section_response',
    'chatbot_response',
  ];

  for (const t of topicsRes) {
    await consumer.subscribe({ topic: t, fromBeginning: false });
  }

  const responseMap = new Map<string, (res: Record<string, unknown>) => void>();

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      try {
        const raw = JSON.parse(message.value.toString()) as Record<string, unknown>;
        const sess = String(raw.session_id || raw.sessionId || raw.conv_id || '');
        const resolver = responseMap.get(sess);
        if (resolver) {
          responseMap.delete(sess);
          resolver(raw);
        }
      } catch {}
    },
  });

  const sendAndAwait = (
    topic: string,
    payload: Record<string, unknown>,
    sessKey: string,
    timeoutMs: number,
  ) => {
    return new Promise<Record<string, unknown>>((resolve, reject) => {
      const timer = setTimeout(() => {
        responseMap.delete(sessKey);
        reject(new Error(`Timeout after ${timeoutMs / 1000}s`));
      }, timeoutMs);

      responseMap.set(sessKey, (data) => {
        clearTimeout(timer);
        resolve(data);
      });

      producer
        .send({
          topic,
          messages: [{ key: sessKey, value: JSON.stringify(payload) }],
        })
        .catch(reject);
    });
  };

  const tests: TestItem[] = [
    {
      name: 'AI-BUG-01: Chatbot Worker (chatbot_request)',
      topicReq: 'chatbot_request',
      topicRes: 'chatbot_response',
      timeoutMs: 40000,
      payload: {
        event_type: 'outer_chatbot',
        model_id: MODEL_ID,
        user_id: USER_ID,
        document_id: DOC_ID,
        conv_id: `conv-${Date.now()}`,
        session_id: `sess-chat-${Date.now()}`,
        query: 'Chào bạn, cho tôi biết đề tài này nghiên cứu về cái gì?',
        history: [],
      },
      validate: (res) => {
        const hasText = !!(res.reply || res.response || res.message || res.answer || res.content);
        return {
          pass: hasText && !res.error && !res.error_code,
          detail: JSON.stringify(res).slice(0, 150),
        };
      },
    },
    {
      name: 'AI-BUG-03: delete_report (write_section_request)',
      topicReq: 'write_section_request',
      topicRes: 'write_section_response',
      timeoutMs: 30000,
      payload: {
        event_type: 'delete_report',
        user_id: USER_ID,
        document_id: DOC_ID,
        session_id: `sess-del-${Date.now()}`,
        report_id: 'report-demo-01',
      },
      validate: (res) => {
        const isNotCrash = res.error_code !== 640 && !String(res.error || '').includes('INVALID_PARAMS');
        return {
          pass: isNotCrash,
          detail: JSON.stringify(res).slice(0, 150),
        };
      },
    },
    {
      name: 'AI-BUG-04: search_papers (OpenAlex timeout test)',
      topicReq: 'document_setup_request',
      topicRes: 'document_setup_response',
      timeoutMs: 50000,
      payload: {
        event_type: 'search_papers',
        model_id: MODEL_ID,
        user_id: USER_ID,
        document_id: DOC_ID,
        session_id: `sess-sp-${Date.now()}`,
        language: 'vi',
        field: 'Computer Science',
        domain: 'Artificial Intelligence',
        subdomains: ['Machine Learning'],
        query: 'Deep learning in medical imaging',
        search_range: 'all',
        country_filter: 'both',
        oa_filter: false,
        is_oa: false,
        is_tool: false,
        downloaded_papers: [],
        cut_off_year_low: 2020,
        cut_off_year_high: 2026,
        use_web_search: false,
      },
      validate: (res) => {
        const hasPapers = Array.isArray(res.papers) || Array.isArray(res.data);
        return {
          pass: hasPapers && !res.error_code,
          detail: hasPapers
            ? `Found ${(res.papers as unknown[])?.length || (res.data as unknown[])?.length} papers`
            : JSON.stringify(res).slice(0, 150),
        };
      },
    },
    {
      name: 'BE-013: generate_title_description (với knowledge_base hợp lệ)',
      topicReq: 'document_setup_request',
      topicRes: 'document_setup_response',
      timeoutMs: 60000,
      payload: {
        event_type: 'generate_title_description',
        model_id: MODEL_ID,
        user_id: USER_ID,
        document_id: DOC_ID,
        session_id: `sess-td-${Date.now()}`,
        language: 'vi',
        title: 'Ứng dụng AI trong logistics thương mại điện tử',
        field: 'Kinh tế số',
        domain: 'Thương mại điện tử',
        subdomains: ['Logistics'],
        keywords: ['AI', 'Logistics'],
        knowledge_base: ['Nghiên cứu về chuyển đổi số chuỗi cung ứng và tự động hóa kho hàng.'],
        level: 'KHOA_LUAN',
      },
      validate: (res) => {
        const hasProposal = typeof res.proposal === 'object' && res.proposal !== null;
        return {
          pass: hasProposal && !res.error_code,
          detail: JSON.stringify(res).slice(0, 150),
        };
      },
    },
    {
      name: 'BE-017: generate_outline (với headings cấu trúc hợp lệ)',
      topicReq: 'document_setup_request',
      topicRes: 'document_setup_response',
      timeoutMs: 60000,
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
        research_type: { type: 2, reason: 'Phương pháp định lượng kết hợp khảo sát' },
        word_count_str: '8000 words',
        headings: [
          'Chương 1: Tổng quan tình hình nghiên cứu',
          'Chương 2: Cơ sở lý thuyết và mô hình nghiên cứu',
          'Chương 3: Phương pháp nghiên cứu',
          'Chương 4: Kết quả nghiên cứu và thảo luận',
          'Chương 5: Kết luận và hàm ý quản trị',
        ],
        headings_percent: [20, 25, 25, 20, 10],
        outline_code: [],
      },
      validate: (res) => {
        const hasOutline = typeof res.outline === 'object' && res.outline !== null;
        return {
          pass: hasOutline && !res.error_code,
          detail: JSON.stringify(res).slice(0, 150),
        };
      },
    },
    {
      name: 'BE-012: generate_titles (với timeout 90s)',
      topicReq: 'document_setup_request',
      topicRes: 'document_setup_response',
      timeoutMs: 90000,
      payload: {
        event_type: 'generate_titles',
        model_id: MODEL_ID,
        user_id: USER_ID,
        document_id: DOC_ID,
        session_id: `sess-titles-${Date.now()}`,
        language: 'vi',
        field: 'Kinh tế số',
        domain: 'Thương mại điện tử',
        subdomains: ['B2C'],
        level: 'KHOA_LUAN',
        use_web_search: false,
      },
      validate: (res) => {
        const hasTitles = Array.isArray(res.proposals) || Array.isArray(res.titles);
        return {
          pass: hasTitles && !res.error_code,
          detail: JSON.stringify(res).slice(0, 150),
        };
      },
    },
  ];

  for (const t of tests) {
    const t0 = Date.now();
    const sessKey = String(t.payload.session_id || t.payload.conv_id);
    process.stdout.write(`▶ Đang test [${t.name}]... `);
    try {
      const res = await sendAndAwait(t.topicReq, t.payload, sessKey, t.timeoutMs);
      const elapsed = Date.now() - t0;
      const v = t.validate(res);
      if (v.pass) {
        console.log(`✅ PASS (${elapsed}ms) - ${v.detail}`);
      } else {
        console.log(`❌ FAIL validation (${elapsed}ms) - Raw: ${v.detail}`);
      }
    } catch (err: unknown) {
      const elapsed = Date.now() - t0;
      console.log(`❌ ERROR (${elapsed}ms) - ${(err as Error).message}`);
    }
  }

  await producer.disconnect();
  await consumer.disconnect();
  console.log('\n====================================================');
  console.log('🏁 HOÀN THÀNH KIỂM CHỨNG CÁC FIX BÊN AI');
  console.log('====================================================');
}

runVerifyFixes().catch((err) => {
  console.error('Fatal test error:', err);
  process.exit(1);
});
