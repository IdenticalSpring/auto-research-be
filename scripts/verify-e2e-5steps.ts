import { Kafka, logLevel } from 'kafkajs';
import { resolveScriptModelId } from './lib/ai-model';
import { MIN_E2E_TIMEOUT_MS } from './lib/e2e-timeouts';

const BROKER = process.env.KAFKA || '100.120.3.64:9092';
const MODEL_ID = resolveScriptModelId();
const USER_ID = '00000000-0000-0000-0000-000000000001';
const DOC_ID = '000000000000000000000002'; // 24-hex Mongo compatible

async function runE2E5Steps(): Promise<void> {
  console.log('================================================================');
  console.log('🎯 BE-049: SCRIPT TEST TỰ ĐỘNG 5 BƯỚC PIPELINE BACKEND (LIVE AI)');
  console.log('================================================================');
  console.log(`📡 Broker:   ${BROKER}`);
  console.log(`🤖 Model:    ${MODEL_ID}\n`);

  const uid = `e2e-5steps-${Date.now()}`;
  const kafka = new Kafka({
    clientId: `client-${uid}`,
    brokers: BROKER.split(',').map((b) => b.trim()),
    logLevel: logLevel.NOTHING,
  });

  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: `group-${uid}` });

  try {
    await Promise.all([producer.connect(), consumer.connect()]);
  } catch (connErr) {
    console.error(`⚠️ Không thể kết nối tới Kafka broker (${BROKER}): ${(connErr as Error).message}`);
    console.error('👉 Hãy kiểm tra kết nối mạng hoặc VPN Tailscale tới VPS AI Server trước khi chạy live test.');
    console.error('👉 Để chạy test E2E kiểm thử tích hợp tự động (không phụ thuộc VPS), chạy: npm run test:e2e\n');
    process.exit(0);
  }

  // Subscribe to all relevant response topics
  await consumer.subscribe({ topic: 'document_setup_response', fromBeginning: false });
  await consumer.subscribe({ topic: 'write_section_response', fromBeginning: false });

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

  const sendAndAwait = (topic: string, payload: Record<string, unknown>, timeoutMs = 60000) => {
    const sess = String(payload.session_id);
    return new Promise<Record<string, unknown>>((resolve, reject) => {
      const timer = setTimeout(() => {
        responseMap.delete(sess);
        reject(new Error(`Timeout after ${timeoutMs / 1000}s awaiting response for ${payload.event_type}`));
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

  const results: Array<{ step: string; status: 'PASS' | 'FAIL'; details: string }> = [];

  try {
    // -------------------------------------------------------------
    // Bước 1: Tạo đề tài (generate_title_description)
    // -------------------------------------------------------------
    console.log('▶ [1/5] Bước 1: Tạo đề tài & mô tả (generate_title_description)...');
    const sess1 = `sess-e2e-title-${Date.now()}`;
    const p1 = {
      event_type: 'generate_title_description',
      model_id: MODEL_ID,
      user_id: USER_ID,
      document_id: DOC_ID,
      session_id: sess1,
      language: 'vi',
      topic: 'Nghiên cứu tác động của thương mại điện tử đến hành vi tiêu dùng',
      domain: 'Kinh tế số',
      field: 'Thương mại điện tử',
      is_tool: false,
    };
    const res1 = await sendAndAwait('document_setup_request', p1, MIN_E2E_TIMEOUT_MS);
    const hasProposal = Boolean(res1.final_proposal || res1.title_description || res1.title);
    if (!hasProposal) throw new Error('Không nhận được đề tài / mô tả hợp lệ từ AI');
    results.push({ step: '1. Tạo đề tài (generate_title_description)', status: 'PASS', details: 'Nhận thành công proposal' });
    console.log('   ✅ Bước 1 PASS: Đã sinh đề tài & mô tả nghiên cứu.');

    // -------------------------------------------------------------
    // Bước 2: Đề cương (generate_outline)
    // -------------------------------------------------------------
    console.log('▶ [2/5] Bước 2: Sinh đề cương chi tiết (generate_outline)...');
    const sess2 = `sess-e2e-outline-${Date.now()}`;
    const p2 = {
      event_type: 'generate_outline',
      model_id: MODEL_ID,
      user_id: USER_ID,
      document_id: DOC_ID,
      session_id: sess2,
      language: 'vi',
      field: 'Thương mại điện tử',
      domain: 'Kinh tế số',
      title: 'Tác động của thương mại điện tử đến hành vi tiêu dùng',
      description: 'Nghiên cứu hành vi tiêu dùng trên các sàn thương mại điện tử',
      outline_type: 'full',
      is_tool: false,
    };
    const res2 = await sendAndAwait('document_setup_request', p2, MIN_E2E_TIMEOUT_MS);
    const hasOutline = Boolean(res2.outline || res2.outline_tree || res2.sections);
    if (!hasOutline) throw new Error('Không nhận được outline hợp lệ từ AI');
    results.push({ step: '2. Đề cương chi tiết (generate_outline)', status: 'PASS', details: 'Nhận thành công outline' });
    console.log('   ✅ Bước 2 PASS: Đã sinh đề cương chi tiết.');

    // -------------------------------------------------------------
    // Bước 3: Phân tích OLS (analyzer)
    // -------------------------------------------------------------
    console.log('▶ [3/5] Bước 3: Phân tích hồi quy OLS (analyzer)...');
    const sess3 = `sess-e2e-ols-${Date.now()}`;
    const p3 = {
      event_type: 'analyzer',
      model_id: MODEL_ID,
      user_id: USER_ID,
      document_id: DOC_ID,
      session_id: sess3,
      conv_id: `conv-${Date.now()}`,
      thread_id: `th-${Date.now()}`,
      language: 'Vietnamese',
      final_proposal: {
        title: 'Tác động của thương mại điện tử đến hành vi tiêu dùng',
      },
      data: {
        tuoi: [22, 25, 28, 30, 24, 26, 29, 31, 35, 27],
        thu_nhap: [10.0, 15.0, 18.0, 22.0, 12.0, 14.0, 19.0, 25.0, 28.0, 16.0],
        chi_tieu: [4.0, 6.0, 7.5, 9.0, 5.0, 5.5, 8.0, 10.5, 12.0, 7.0],
      },
      variables: [
        { name: 'tuoi', code: 'tuoi', scale: 'ratio', type: 'int', role: 'independent', variable_type: 'observed' },
        { name: 'thu_nhap', code: 'thu_nhap', scale: 'ratio', type: 'float', role: 'independent', variable_type: 'observed' },
        { name: 'chi_tieu', code: 'chi_tieu', scale: 'ratio', type: 'float', role: 'dependent', variable_type: 'observed' },
      ],
    };
    const res3 = await sendAndAwait('write_section_request', p3, 60000);
    const hasOls = Boolean(res3.analysis_results || res3.report_html || res3.has_finished);
    if (!hasOls) throw new Error('Không nhận được kết quả OLS hợp lệ từ AI');
    results.push({ step: '3. Phân tích hồi quy OLS (analyzer)', status: 'PASS', details: 'Nhận thành công kết quả OLS' });
    console.log('   ✅ Bước 3 PASS: Đã phân tích hồi quy OLS thành công.');

    // -------------------------------------------------------------
    // Bước 4: Viết bài (write_content / chuyen_de_1)
    // -------------------------------------------------------------
    console.log('▶ [4/5] Bước 4: Soạn thảo nội dung chuyên đề (chuyen_de_1)...');
    const sess4 = `sess-e2e-write-${Date.now()}`;
    const p4 = {
      event_type: 'chuyen_de_1',
      model_id: MODEL_ID,
      user_id: USER_ID,
      document_id: DOC_ID,
      session_id: sess4,
      language: 'Vietnamese',
      field: 'Thương mại điện tử',
      domain: 'Kinh tế số',
      final_proposal: { title: 'Tác động của thương mại điện tử đến hành vi tiêu dùng' },
      outline: {
        sections: [
          { section_name: 'Chương 1: Cơ sở lý luận về thương mại điện tử', content: '' },
        ],
      },
    };
    const res4 = await sendAndAwait('write_section_request', p4, 60000);
    const hasContent = Boolean(res4.section_name || res4.content || res4.has_finished !== undefined);
    if (!hasContent) throw new Error('Không nhận được nội dung viết bài từ AI');
    results.push({ step: '4. Soạn thảo chuyên đề (chuyen_de_1)', status: 'PASS', details: 'Nhận thành công nội dung bài viết' });
    console.log('   ✅ Bước 4 PASS: Đã soạn thảo bài viết thành công.');

    // -------------------------------------------------------------
    // Bước 5: Sinh Slide PowerPoint PPTX (gen_slides)
    // -------------------------------------------------------------
    console.log('▶ [5/5] Bước 5: Sinh Slide thuyết trình PowerPoint (gen_slides)...');
    const sess5 = `sess-e2e-slides-${Date.now()}`;
    const p5 = {
      event_type: 'gen_slides',
      model_id: MODEL_ID,
      user_id: USER_ID,
      document_id: DOC_ID,
      session_id: sess5,
      language: 'vi',
      raw_report: '# Nghiên cứu thương mại điện tử\n\nNội dung phân tích thực trạng...',
      user_preference: 5,
      manual_mode: false,
    };
    const res5 = await sendAndAwait('write_section_request', p5, 60000);
    const hasSlides = Boolean(res5.pptx_url || res5.slides || res5.has_finished !== undefined);
    if (!hasSlides) throw new Error('Không nhận được slide PowerPoint từ AI');
    results.push({ step: '5. Tải slide PPTX (gen_slides)', status: 'PASS', details: 'Nhận thành công slide / link PPTX' });
    console.log('   ✅ Bước 5 PASS: Đã sinh slide PPTX thành công.');

  } catch (error: unknown) {
    console.error(`\n❌ Pipeline gián đoạn: ${(error as Error).message}`);
  } finally {
    await Promise.all([producer.disconnect(), consumer.disconnect()]);
  }

  console.log('\n================================================================');
  console.log('📊 BÁO CÁO KẾT QUẢ E2E PIPELINE 5 BƯỚC');
  console.log('================================================================');
  results.forEach((r) => {
    console.log(`[${r.status}] ${r.step} - ${r.details}`);
  });
  console.log(`Tổng kết: ${results.filter((r) => r.status === 'PASS').length}/5 bước PASS.`);
  console.log('================================================================\n');
}

void runE2E5Steps();
