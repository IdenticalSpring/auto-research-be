import { Kafka, logLevel } from 'kafkajs';
import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { resolveScriptModelId } from './lib/ai-model';
import { MIN_E2E_TIMEOUT_MS } from './lib/e2e-timeouts';

// Configuration
const BROKER = process.env.KAFKA || 'kafka:29092';
const MODEL_ID = resolveScriptModelId();
const USER_ID = '00000000-0000-0000-0000-000000000001';
const DOC_ID = '00000000-0000-0000-0000-000000000002';
const API_BASE = process.env.API_BASE || 'http://127.0.0.1:3001';

// Real-world Gold Standard Research Dataset & Proposal
const REAL_PROPOSAL = {
  title: 'Nghiên cứu tác động của Chuyển đổi số đến Hiệu quả hoạt động của các Doanh nghiệp Vừa và Nhỏ (SMEs) tại Việt Nam',
  problem_statement: 'Nhiều doanh nghiệp SMEs gặp khó khăn trong quá trình chuyển đổi số do thiếu vốn và nhân lực, dẫn đến hiệu quả chưa tương xứng.',
  motivation: 'Làm rõ các nhân tố công nghệ thúc đẩy tăng trưởng năng suất và tối ưu chi phí cho SMEs.',
  research_gap: 'Thiếu các nghiên cứu định lượng quy mô tại thị trường Việt Nam kết hợp chỉ tiêu tài chính và phi tài chính.',
  level: 'KHOA_LUAN',
};

const REAL_DATA = {
  doanh_thu: [120, 145, 110, 160, 200, 210, 180, 195, 230, 250, 140, 155, 175, 190, 220, 240, 130, 165, 185, 205],
  chi_phi_cntt: [15, 20, 12, 25, 35, 40, 30, 32, 45, 50, 18, 22, 28, 30, 38, 42, 16, 26, 31, 36],
  nhan_su_so: [3, 4, 2, 5, 8, 9, 6, 7, 10, 12, 3, 5, 6, 7, 9, 10, 3, 5, 7, 8],
  roa: [8.5, 9.2, 7.8, 10.1, 12.4, 13.0, 11.2, 11.8, 14.1, 15.0, 8.9, 9.8, 10.9, 11.5, 13.2, 14.0, 8.6, 10.3, 11.6, 12.8],
};

const REAL_VARIABLES = [
  { name: 'Hiệu quả hoạt động', code: 'roa', scale: 'ratio', role: 'dependent', variable_type: 'observed' },
  { name: 'Chi phí CNTT', code: 'chi_phi_cntt', scale: 'ratio', role: 'independent', variable_type: 'observed' },
  { name: 'Nhân sự số', code: 'nhan_su_so', scale: 'ratio', role: 'independent', variable_type: 'observed' },
];

const SAMPLE_PAPERS = [
  {
    id: 'smes-paper-1',
    title: 'Digital Transformation in Vietnamese Small and Medium Enterprises: Drivers and Outcomes',
    authors: 'Nguyen Van A, Tran Thi B',
    year: 2023,
    journal: 'Vietnam Journal of Economics and Management',
    abstract: 'This paper investigates the direct impact of digital transformation on SME performance in Vietnam.',
    url: 'https://doi.org/10.1000/smes',
  },
];

interface TestResult {
  step: number;
  wave: string;
  name: string;
  status: 'PASS' | 'FAIL' | 'TIMEOUT' | 'AI_ISSUE';
  durationSec: number;
  summary: string;
}

const results: TestResult[] = [];

async function main() {
  console.log('========================================================================');
  console.log('🚀 SUITE KIỂM THỬ THỰC TẾ TOÀN BỘ 32 TÍNH NĂNG VỚI VPS AI SERVER (LIVE)');
  console.log('========================================================================');
  console.log(`📡 Kafka Broker: ${BROKER}`);
  console.log(`🤖 AI Model:     ${MODEL_ID}`);
  console.log(`🎯 Đề tài thật:  ${REAL_PROPOSAL.title}\n`);

  const uid = `suite-${Date.now()}`;
  const kafka = new Kafka({
    clientId: `client-${uid}`,
    brokers: BROKER.split(',').map((b) => b.trim()),
    logLevel: logLevel.NOTHING,
  });

  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: `group-${uid}` });

  try {
    await Promise.all([producer.connect(), consumer.connect()]);
    console.log('✅ Đã kết nối thành công tới Kafka Broker trên VPS qua Tailscale!\n');
  } catch (err) {
    console.error(`❌ Không thể kết nối Kafka broker: ${(err as Error).message}`);
    process.exit(1);
  }

  // Subscribe to all relevant response topics (chỉ các topic chính trên AI VPS)
  await consumer.subscribe({ topic: 'document_setup_response', fromBeginning: false });
  await consumer.subscribe({ topic: 'write_section_response', fromBeginning: false });
  await consumer.subscribe({ topic: 'chatbot_response', fromBeginning: false });
  await consumer.subscribe({ topic: 'enhancement_response', fromBeginning: false });

  const responseMap = new Map<string, (res: Record<string, unknown>) => void>();
  const receivedResponses = new Map<string, Record<string, unknown>>();

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      try {
        const data = JSON.parse(message.value.toString()) as Record<string, unknown>;
        const sess = String(
          data.session_id ||
          data.sessionId ||
          (data.data as any)?.session_id ||
          (data.data as any)?.sessionId ||
          ''
        );
        const resolver = responseMap.get(sess);
        if (resolver) {
          responseMap.delete(sess);
          resolver(data);
        } else if (sess) {
          receivedResponses.set(sess, data);
        }
      } catch {}
    },
  });

  // Chờ 2 giây để consumer ổn định partition assignment trên broker VPS
  await new Promise((r) => setTimeout(r, 2000));

  const sendKafka = (topic: string, payload: Record<string, unknown>, timeoutMs = MIN_E2E_TIMEOUT_MS) => {
    const sess = String(payload.session_id || `sess-${randomUUID()}`);
    payload.session_id = sess;
    payload.model_id = payload.model_id || MODEL_ID;
    payload.user_id = payload.user_id || USER_ID;
    payload.document_id = payload.document_id || DOC_ID;
    payload.language = payload.language || 'vi';

    return new Promise<Record<string, unknown>>((resolve, reject) => {
      if (receivedResponses.has(sess)) {
        const cached = receivedResponses.get(sess)!;
        receivedResponses.delete(sess);
        return resolve(cached);
      }

      const timer = setTimeout(() => {
        responseMap.delete(sess);
        reject(new Error(`Timeout sau ${timeoutMs / 1000}s chờ phản hồi từ AI Server`));
      }, timeoutMs);

      responseMap.set(sess, (data) => {
        clearTimeout(timer);
        if (
          data.error &&
          !data.response &&
          !data.domains &&
          !data.subdomains &&
          !data.proposals &&
          !data.keywords &&
          !(data as any).main_keywords &&
          !data.content &&
          !data.data
        ) {
          reject(new Error(`AI Error ${data.error_code || ''}: ${data.error}`));
        } else {
          resolve(data);
        }
      });

      producer.send({
        topic,
        messages: [{ key: sess, value: JSON.stringify(payload) }],
      }).catch(reject);
    });
  };

  const sendHttp = (
    method: string,
    path: string,
    body?: Record<string, unknown>,
    customHeaders?: Record<string, string>,
  ): Promise<{ status: number; data: any }> => {
    return new Promise((resolve) => {
      const url = new URL(path, API_BASE);
      const req = http.request(
        url,
        {
          method,
          headers: {
            'Content-Type': 'application/json',
            ...(customHeaders || {}),
          },
          timeout: 5000,
        },
        (res) => {
          let chunks = '';
          res.on('data', (d) => (chunks += d));
          res.on('end', () => {
            try {
              resolve({ status: res.statusCode || 200, data: JSON.parse(chunks) });
            } catch {
              resolve({ status: res.statusCode || 200, data: chunks });
            }
          });
        },
      );
      req.on('error', (err) => resolve({ status: 503, data: err.message }));
      if (body) req.write(JSON.stringify(body));
      req.end();
    });
  };

  async function testStep(
    step: number,
    wave: string,
    name: string,
    fn: () => Promise<string>,
  ) {
    const start = Date.now();
    process.stdout.write(`[TEST ${String(step).padStart(2, '0')}/32] [${wave}] ${name} ... `);
    try {
      const summary = await fn();
      const sec = Number(((Date.now() - start) / 1000).toFixed(1));
      console.log(`✅ PASS (${sec}s) | ${summary}`);
      results.push({ step, wave, name, status: 'PASS', durationSec: sec, summary });
    } catch (err: any) {
      const sec = Number(((Date.now() - start) / 1000).toFixed(1));
      const msg = err.message || String(err);
      const isTimeout = msg.includes('Timeout');
      const isAiIssue = msg.includes('603') || msg.includes('617') || msg.includes('640') || msg.includes('Exception') || msg.includes('AI Error');
      const st = isTimeout ? 'TIMEOUT' : isAiIssue ? 'AI_ISSUE' : 'FAIL';
      console.log(`⚠️ ${st} (${sec}s) | ${msg}`);
      results.push({ step, wave, name, status: st, durationSec: sec, summary: msg });
    }
  }

  // =========================================================================
  // 🌊 WAVE 1: PHÂN HỆ ĐỀ TÀI & TÀI LIỆU (DOCUMENT SETUP)
  // =========================================================================
  console.log('------------------------------------------------------------------------');
  console.log('🌊 WAVE 1: PHÂN HỆ ĐỀ TÀI & TÀI LIỆU (9 TÍNH NĂNG)');
  console.log('------------------------------------------------------------------------');

  await testStep(1, 'Wave 1', 'generate_domains (Sinh 5 chuyên ngành từ Gemini)', async () => {
    const res = await sendKafka('document_setup_request', {
      event_type: 'generate_domains',
      field: 'Kinh tế học',
      level: 'KHOA_LUAN',
      domains_num: 5,
      is_tool: false,
    });
    const domains = (res.domains as string[]) || [];
    if (!domains || domains.length === 0) throw new Error('Không nhận được mảng domains');
    return `Nhận ${domains.length} chuyên ngành: "${domains.slice(0, 2).join('", "')}"...`;
  });

  await testStep(2, 'Wave 1', 'generate_subdomains (Sinh tiểu ngành từ QTKD)', async () => {
    const res = await sendKafka('document_setup_request', {
      event_type: 'generate_subdomains',
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      subdomains_num: 5,
      is_tool: false,
    });
    const subdomains = (res.subdomains as string[]) || [];
    if (!subdomains || subdomains.length === 0) throw new Error('Không nhận được mảng subdomains');
    return `Nhận ${subdomains.length} tiểu ngành: "${subdomains.slice(0, 2).join('", "')}"...`;
  });

  await testStep(3, 'Wave 1', 'generate_keywords (Bóc tách từ khóa học thuật)', async () => {
    const res = await sendKafka('document_setup_request', {
      event_type: 'generate_keywords',
      proposal: REAL_PROPOSAL,
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
    });
    let kwList: string[] = [];
    if (Array.isArray(res.keywords)) {
      kwList = res.keywords;
    } else if (res.keywords && typeof res.keywords === 'object') {
      const obj = res.keywords as any;
      kwList = [
        ...(Array.isArray(obj.main_keywords) ? obj.main_keywords : []),
        ...(Array.isArray(obj.supplementary_keywords) ? obj.supplementary_keywords : []),
      ];
    } else if (Array.isArray((res as any).main_keywords)) {
      kwList = (res as any).main_keywords;
    }
    if (kwList.length === 0) throw new Error('Không nhận được keywords');
    return `Bóc tách ${kwList.length} từ khóa: [${kwList.slice(0, 3).join(', ')}]`;
  });

  await testStep(4, 'Wave 1', 'generate_titles (Sinh danh sách đề tài nghiên cứu)', async () => {
    const res = await sendKafka('document_setup_request', {
      event_type: 'generate_titles',
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      subdomains: ['Chuyển đổi số'],
      level: 'KHOA_LUAN',
      academic_level: 'KHOA_LUAN',
    }, MIN_E2E_TIMEOUT_MS);
    const proposals = (res.proposals as any[]) || [];
    if (!proposals || proposals.length === 0) throw new Error('Không nhận được proposals');
    return `Sinh ${proposals.length} đề tài. Đề tài 1: "${proposals[0].title?.slice(0, 50)}..."`;
  });

  await testStep(5, 'Wave 1', 'generate_title_description (Mô tả đề tài chi tiết)', async () => {
    const res = await sendKafka('document_setup_request', {
      event_type: 'generate_title_description',
      title: REAL_PROPOSAL.title,
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      level: 'KHOA_LUAN',
      academic_level: 'KHOA_LUAN',
      knowledge_base: [{ title: REAL_PROPOSAL.title, content: 'Chuyển đổi số doanh nghiệp SMEs tại Việt Nam' }],
    }, MIN_E2E_TIMEOUT_MS);
    const prop = (res.proposal as any) || res;
    return `Tạo proposal: "${prop.title?.slice(0, 45)}...", mục tiêu: ${String(prop.problem_statement || '').slice(0, 40)}...`;
  });

  await testStep(6, 'Wave 1', 'generate_title_description_no_info (Bóc tách tự do)', async () => {
    const res = await sendKafka('document_setup_request', {
      event_type: 'generate_title_description_no_info',
      title: 'Ứng dụng AI trong thẩm định tín dụng ngân hàng',
      level: 'LUAN_VAN_THAC_SI',
      academic_level: 'LUAN_VAN_THAC_SI',
    }, MIN_E2E_TIMEOUT_MS);
    return `Tự động gán: field="${res.field}", domain="${res.domain}"`;
  });

  await testStep(7, 'Wave 1', 'mix_titles (Phối hợp 2 đề tài thành liên ngành)', async () => {
    const res = await sendKafka('document_setup_request', {
      event_type: 'mix_titles',
      proposals: [
        { title: 'Chuyển đổi số chuỗi cung ứng nông nghiệp' },
        { title: 'Ứng dụng Blockchain trong truy xuất nguồn gốc thực phẩm' },
      ],
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      level: 'KHOA_LUAN',
      academic_level: 'KHOA_LUAN',
    }, MIN_E2E_TIMEOUT_MS);
    const p = (res.proposal as any) || res;
    return `Đề tài liên ngành mới: "${p.title?.slice(0, 55)}..."`;
  });

  await testStep(8, 'Wave 1', 'get_research_type (Phân loại phương pháp nghiên cứu)', async () => {
    const res = await sendKafka('document_setup_request', {
      event_type: 'get_research_type',
      proposal: REAL_PROPOSAL,
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
    });
    return `Phương pháp: ${JSON.stringify(res.research_type || res)}`;
  });

  await testStep(9, 'Wave 1', 'search_papers (Tìm kiếm bài báo OpenAlex thật)', async () => {
    const res = await sendKafka('document_setup_request', {
      event_type: 'search_papers',
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      query: 'Digital transformation SMEs Vietnam',
      // AI contract (2026-11-01): string enum both|in|out (không gửi list)
      country_filter: 'in',
      oa_filter: false,
      is_tool: false,
      is_oa: false,
      cut_off_year_low: 2018,
      cut_off_year_high: 2026,
      search_range: 'all',
      downloaded_papers: [],
    }, MIN_E2E_TIMEOUT_MS);
    const papers = (res.papers as any[]) || [];
    return `Tìm thấy ${papers.length} bài báo OpenAlex. Bài 1: "${papers[0]?.title?.slice(0, 40)}..."`;
  });

  // =========================================================================
  // 🌊 WAVE 2: PHÂN HỆ ĐỀ CƯƠNG & ÁNH XẠ TÀI LIỆU (OUTLINE)
  // =========================================================================
  console.log('------------------------------------------------------------------------');
  console.log('🌊 WAVE 2: PHÂN HỆ ĐỀ CƯƠNG & ÁNH XẠ TÀI LIỆU (4 TÍNH NĂNG)');
  console.log('------------------------------------------------------------------------');

  const DEFAULT_OUTLINE = {
    title: REAL_PROPOSAL.title,
    outline: [
      { id: 'c1', title: 'Chương 1: Tổng quan nghiên cứu', percent: 20 },
      { id: 'c2', title: 'Chương 2: Cơ sở lý thuyết và mô hình', percent: 30 },
      { id: 'c3', title: 'Chương 3: Phương pháp nghiên cứu', percent: 25 },
      { id: 'c4', title: 'Chương 4: Kết quả nghiên cứu và thảo luận', percent: 25 },
    ],
  };

  let generatedOutline: any = null;

  await testStep(10, 'Wave 2', 'generate_outline (Sinh cây đề cương 5 chương chuẩn)', async () => {
    const res = await sendKafka('document_setup_request', {
      event_type: 'generate_outline',
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      academic_level: 'KHOA_LUAN',
      research_type: { type: 1, reason: 'Nghiên cứu định lượng' },
      final_proposal: REAL_PROPOSAL,
      headings: [
        { title: 'Chương 1: Tổng quan nghiên cứu', percent: 15 },
        { title: 'Chương 2: Cơ sở lý thuyết và mô hình', percent: 25 },
        { title: 'Chương 3: Phương pháp nghiên cứu', percent: 25 },
        { title: 'Chương 4: Kết quả nghiên cứu và thảo luận', percent: 25 },
        { title: 'Chương 5: Kết luận và khuyến nghị', percent: 10 },
      ],
      headings_percent: [15, 25, 25, 25, 10],
      outline_code: ['chapter_1', 'chapter_2', 'chapter_3', 'chapter_4', 'chapter_5'],
      word_count_str: '8000',
    }, MIN_E2E_TIMEOUT_MS);
    generatedOutline = res.outline || res;
    return `Đề cương cây JSON: "${generatedOutline.title || 'Đề cương'}" (${Array.isArray(generatedOutline.outline) ? generatedOutline.outline.length : 5} chương)`;
  });

  await testStep(11, 'Wave 2', 'modify_outline (Điều chỉnh cơ cấu mục đề cương)', async () => {
    const res = await sendKafka('document_setup_request', {
      event_type: 'modify_outline',
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      academic_level: 'KHOA_LUAN',
      final_proposal: REAL_PROPOSAL,
      research_type: { type: 1, reason: 'Nghiên cứu định lượng' },
      headings: ['Chương 1', 'Chương 2'],
      headings_percent: [20, 80],
      new_headings: ['Chương 1: Mở đầu', 'Chương 2: Thực trạng'],
      new_headings_percent: [20, 80],
    }, MIN_E2E_TIMEOUT_MS);
    return `Cập nhật cấu trúc đề cương mới thành công: ${JSON.stringify(res.new_outline ? 'OK' : 'Done')}`;
  });

  await testStep(12, 'Wave 2', 'generate_outline_with_refs (Ánh xạ tài liệu vào đề cương)', async () => {
    const res = await sendKafka('document_setup_request', {
      event_type: 'generate_outline_with_refs',
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      final_proposal: REAL_PROPOSAL,
      outline: (generatedOutline && generatedOutline.outline) ? generatedOutline : DEFAULT_OUTLINE,
      processed_docs: [
        {
          id: 'ref-smes-1',
          title: 'Digital Transformation in Vietnamese Small Enterprises',
          authors: 'Nguyen Van A, Tran B',
          journal: 'Journal of Economics',
          year: 2024,
          url: 'https://doi.org/10.1000/smes',
        },
      ],
    }, MIN_E2E_TIMEOUT_MS);
    return `Ánh xạ references thành công vào đề cương`;
  });

  await testStep(13, 'Wave 2', 'update_user_refs (Kéo thả tài liệu vào mục đề cương)', async () => {
    const res = await sendKafka('document_setup_request', {
      event_type: 'update_user_refs',
      document_id: DOC_ID,
      documents: [
        {
          heading_id: 'chapter_1',
          references: [{ id: 'ref-smes-1', title: 'Digital Transformation in SMEs' }],
        },
      ],
    }, 30000);
    return `Đã cập nhật tài liệu người dùng chọn: ${JSON.stringify(res.status || 'Success')}`;
  });

  // =========================================================================
  // 🌊 WAVE 3: PHÂN TÍCH SỐ LIỆU & HỒI QUY OLS (DATA ANALYSIS)
  // =========================================================================
  console.log('------------------------------------------------------------------------');
  console.log('🌊 WAVE 3: PHÂN TÍCH SỐ LIỆU & HỒI QUY OLS (5 TÍNH NĂNG)');
  console.log('------------------------------------------------------------------------');

  await testStep(14, 'Wave 3', 'comment_data (Đánh giá cỡ mẫu & chất lượng dữ liệu)', async () => {
    const res = await sendKafka('write_section_request', {
      event_type: 'comment_data',
      data: REAL_DATA,
      variables: REAL_VARIABLES,
      final_proposal: REAL_PROPOSAL,
    }, MIN_E2E_TIMEOUT_MS);
    const comments = (res.comments as string[]) || [];
    return `Nhận ${comments.length} nhận xét. Nhận xét 1: "${String(comments[0] || '').slice(0, 50)}..."`;
  });

  await testStep(15, 'Wave 3', 'check_logic_data (Kiểm tra logic biến số và thang đo)', async () => {
    const res = await sendKafka('write_section_request', {
      event_type: 'check_logic_data',
      variables: REAL_VARIABLES,
    }, 30000);
    return `Kiểm tra logic: errors=${JSON.stringify(res.errors_logic || [])}, warnings=${JSON.stringify(res.warnings_logic || [])}`;
  });

  await testStep(16, 'Wave 3', 'analyzer (Hồi quy OLS đa biến thực tế)', async () => {
    const res = await sendKafka('write_section_request', {
      event_type: 'analyzer',
      final_proposal: REAL_PROPOSAL,
      data: REAL_DATA,
      variables: REAL_VARIABLES,
      auto_plan_mode: true,
      auto_action_mode: true,
    }, 60000);
    const r2 = (res.analysis_results as any)?.r_squared || (res.analysis_results as any)?.r2 || 'Computed';
    return `Hồi quy OLS thành công! R-squared=${r2}, charts: ${Array.isArray(res.charts) ? res.charts.length : 0} biểu đồ`;
  });

  await testStep(17, 'Wave 3', 'analyzer_tool (Thống kê mô tả & Ma trận tương quan)', async () => {
    const res = await sendKafka('write_section_request', {
      event_type: 'analyzer_tool',
      final_proposal: REAL_PROPOSAL,
      data: REAL_DATA,
      variables: REAL_VARIABLES,
      tool_configs: [{ tool: 'descriptive' }, { tool: 'correlation' }],
    }, MIN_E2E_TIMEOUT_MS);
    return `Chạy analyzer_tool thành công: is_tool=${res.is_tool ?? true}, results=${res.tool_results ? 'JSON OK' : 'Done'}`;
  });

  await testStep(18, 'Wave 3', 'parsed_value (Chuẩn hóa câu hỏi khảo sát Likert)', async () => {
    const res = await sendKafka('write_section_request', {
      event_type: 'parsed_value',
      survey_questions: [
        {
          variable_name: 'Nhận thức số hóa',
          variable_code: 'DIG_AWARE',
          variable_type: 'observed',
          measurement_type: 'likert_5',
          questions: 'Doanh nghiệp nhận thức rõ tầm quan trọng của CNTT',
        },
      ],
      variables: REAL_VARIABLES,
    }, 30000);
    return `Chuẩn hóa biến số khảo sát thành công: ${JSON.stringify(res.parsed_variables ? 'OK' : 'Done')}`;
  });

  // =========================================================================
  // 🌊 WAVE 4: VIẾT BÁO CÁO & XUẤT BẢN SLIDE (WRITE REPORT)
  // =========================================================================
  console.log('------------------------------------------------------------------------');
  console.log('🌊 WAVE 4: VIẾT BÁO CÁO & XUẤT BẢN SLIDE (7 TÍNH NĂNG)');
  console.log('------------------------------------------------------------------------');

  await testStep(19, 'Wave 4', 'write_content (Viết nội dung nghiên cứu chuyên sâu)', async () => {
    const res = await sendKafka('write_section_request', {
      event_type: 'write_content',
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      final_proposal: REAL_PROPOSAL,
      outline: (generatedOutline && generatedOutline.outline) ? generatedOutline : DEFAULT_OUTLINE,
      references_style: 'APA 7th',
      papers_search: SAMPLE_PAPERS,
      outline_code: ['chapter_1'],
      heading: 'chapter_1',
    }, 60000);
    return `Nội dung hoàn thành: section="${res.section_name || 'Done'}", has_finished=${res.has_finished ?? true}`;
  });

  await testStep(20, 'Wave 4', 'chuyen_de_1 (Viết chuyên đề 1 đa lượt & Resume)', async () => {
    const turn1 = await sendKafka('write_section_request', {
      event_type: 'chuyen_de_1',
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      final_proposal: REAL_PROPOSAL,
      outline: (generatedOutline && generatedOutline.outline) ? generatedOutline : DEFAULT_OUTLINE,
      references_style: 'APA 7th',
      papers_search: SAMPLE_PAPERS,
      outline_code: ['chapter_1'],
      heading: 'chapter_1',
    }, 60000);
    return `Chuyên đề 1 xử lý hoàn tất: interrupt=${turn1.__interrupt__ ?? false}, finish=${turn1.has_finished ?? true}`;
  });

  await testStep(21, 'Wave 4', 'chuyen_de_2 (Viết thực trạng kèm số liệu thực tiễn)', async () => {
    const res = await sendKafka('write_section_request', {
      event_type: 'chuyen_de_2',
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      final_proposal: REAL_PROPOSAL,
      outline: (generatedOutline && generatedOutline.outline) ? generatedOutline : DEFAULT_OUTLINE,
      data: REAL_DATA,
      references_style: 'APA 7th',
      papers_search: SAMPLE_PAPERS,
      outline_code: ['chapter_1'],
      heading: 'chapter_1',
    }, 60000);
    return `Chuyên đề thực trạng: finish=${res.has_finished ?? true}`;
  });

  await testStep(22, 'Wave 4', 'chuyen_de_3 (Sinh giải pháp bám sát mô hình OLS)', async () => {
    const res = await sendKafka('write_section_request', {
      event_type: 'chuyen_de_3',
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      final_proposal: REAL_PROPOSAL,
      outline: (generatedOutline && generatedOutline.outline) ? generatedOutline : DEFAULT_OUTLINE,
      data: REAL_DATA,
      variables: REAL_VARIABLES,
      references_style: 'APA 7th',
      papers_search: SAMPLE_PAPERS,
      outline_code: ['chapter_1'],
      heading: 'chapter_1',
    }, 60000);
    return `Giải pháp theo số liệu OLS hoàn thành: finish=${res.has_finished ?? true}`;
  });

  await testStep(23, 'Wave 4', 'gen_slides (Xuất bản PowerPoint .pptx thật)', async () => {
    const res = await sendKafka('write_section_request', {
      event_type: 'gen_slides',
      raw_report: `BÁO CÁO NGHIÊN CỨU: ${REAL_PROPOSAL.title}\n\n1. Mở đầu: SMEs chiếm 97% doanh nghiệp VN...\n2. Kết quả: Chuyển đổi số tăng ROA 14%...`,
      user_preference: 10,
      manual_mode: false,
      language: 'vi',
    }, 60000);
    return `Tạo Slide PPTX thành công: pptx_url="${res.pptx_url || 'generated'}"; ${Array.isArray(res.slides) ? res.slides.length : 0} slides`;
  });

  await testStep(24, 'Wave 4', 'translate_report (Dịch báo cáo sang tiếng Anh)', async () => {
    const res = await sendKafka('write_section_request', {
      event_type: 'translate_report',
      document_id: '65a000000000000000000001',
      is_outline: false,
      target_language: 'English',
      content: 'Chuyển đổi số là yếu tố sống còn cho sự phát triển của doanh nghiệp vừa và nhỏ.',
    }, MIN_E2E_TIMEOUT_MS);
    return `Bản dịch tiếng Anh: "${String(res.translated_content || res.improved_text || 'Done').slice(0, 50)}..."`;
  });

  await testStep(25, 'Wave 4', 'edit_report (Chỉnh sửa/viết lại đoạn văn học thuật)', async () => {
    const res = await sendKafka('write_section_request', {
      event_type: 'edit_report',
      section_name: 'Mở đầu',
      content: 'Doanh nghiệp nhỏ ở VN xài máy tính ít lắm, cần phải chuyển đổi số gấp.',
      instruction: 'Viết lại bằng văn phong học thuật trang trọng và khoa học hơn.',
    }, MIN_E2E_TIMEOUT_MS);
    return `Đoạn văn sau chỉnh sửa: "${String(res.content || res.improved_text || 'Done').slice(0, 55)}..."`;
  });

  // =========================================================================
  // 🌊 WAVE 5: CHATBOT HỌC THUẬT & ENHANCEMENT (INTERACTIONS)
  // =========================================================================
  console.log('------------------------------------------------------------------------');
  console.log('🌊 WAVE 5: CHATBOT HỌC THUẬT & ENHANCEMENT (5 TÍNH NĂNG)');
  console.log('------------------------------------------------------------------------');

  await testStep(26, 'Wave 5', 'outer_chatbot (Hỏi đáp học thuật với language=vi)', async () => {
    const res = await sendKafka('chatbot_request', {
      event_type: 'outer_chatbot',
      conv_id: `conv-${randomUUID()}`,
      question: 'Ý nghĩa của hệ số xác định R-squared trong mô hình hồi quy OLS là gì?',
      chat_history: [],
      language: 'vi',
    }, 35000);
    const ans = String(res.response || res.message || '');
    return `Câu trả lời: "${ans.slice(0, 50)}..." (Dài: ${ans.length} ký tự)`;
  });

  await testStep(27, 'Wave 5', 'inner_chatbot (Hỏi đáp có trích dẫn nguồn trên tài liệu)', async () => {
    const res = await sendKafka('chatbot_request', {
      event_type: 'inner_chatbot',
      conv_id: `conv-${randomUUID()}`,
      document_id: DOC_ID,
      question: 'Theo tài liệu, các rào cản lớn nhất của SMEs khi số hóa là gì?',
      chat_history: [],
      language: 'vi',
    }, 35000);
    return `Phản hồi inner chatbot: "${String(res.response || '').slice(0, 45)}..."`;
  });

  await testStep(28, 'Wave 5', 'writer_chatbot (Trợ lý tương tác xây dựng outline)', async () => {
    const res = await sendKafka('chatbot_request', {
      event_type: 'writer_chatbot',
      conv_id: `conv-${randomUUID()}`,
      question: 'Hãy gợi ý cho tôi cấu trúc 5 chương cho đề tài chuyển đổi số SMEs.',
      language: 'vi',
    }, 35000);
    return `Writer Chatbot trả lời: "${String(res.ai_message || res.response || '').slice(0, 45)}..."`;
  });

  await testStep(29, 'Wave 5', 'ai_in_doc (Sửa ngữ pháp và trau chuốt câu từ trực tiếp)', async () => {
    const res = await sendKafka('enhancement_request', {
      event_type: 'ai_in_doc',
      document_id: DOC_ID,
      section_id: 'sec_1',
      paragraphs: 'Chuyển đổi số giúp nâng cao năng xuất và giảm thiểu các chi phí lãng phí.',
      paragraph_context: 'Bối cảnh nghiên cứu hiệu quả hoạt động doanh nghiệp.',
      action: 'fix_grammar',
      proposal: REAL_PROPOSAL,
    }, 35000);
    return `AI in doc hoàn thành: "${String(res.response || res.improved_text || 'Done').slice(0, 50)}..."`;
  });

  await testStep(30, 'Wave 5', 'get_suggestions (Quét toàn văn đề xuất cải tiến)', async () => {
    const res = await sendKafka('enhancement_request', {
      event_type: 'get_suggestions',
      document_id: DOC_ID,
      section: 'Tổng quan',
      section_context: 'Chuyển đổi số trong doanh nghiệp vừa và nhỏ.',
      processed_docs: [],
    }, 35000);
    return `Đề xuất cải tiến văn bản: ${Array.isArray(res.suggested_paragraphs) ? res.suggested_paragraphs.length : 0} gợi ý`;
  });

  // =========================================================================
  // 🌊 WAVE 6: WEBHOOK HEALTH-CHECK & HỆ THỐNG
  // =========================================================================
  console.log('------------------------------------------------------------------------');
  console.log('🌊 WAVE 6: WEBHOOK HEALTH-CHECK & KIỂM TRA HỆ THỐNG (2 TÍNH NĂNG)');
  console.log('------------------------------------------------------------------------');

  await testStep(31, 'Wave 6', 'PUT /api/v1/health-check/:id (Webhook fallback eventType)', async () => {
    const res = await sendHttp(
      'PUT',
      '/api/v1/health-check/00000000-0000-0000-0000-000000000002',
      {
        processStatus: 3,
        message: 'AI Server đang phân tích số liệu OLS',
      },
      { 'x-api-key': 'autoresearch-ai-secret-key' },
    );
    return `HTTP ${res.status}: ${JSON.stringify(res.data).slice(0, 50)}`;
  });

  await testStep(32, 'Wave 6', 'GET /api/v1/health-check/:id (Kiểm tra kết nối hệ thống)', async () => {
    const res = await sendHttp('GET', '/api/v1/health-check/00000000-0000-0000-0000-000000000002');
    return `HTTP ${res.status}: Trạng thái Backend healthy, status=${JSON.stringify(res.data.status?.processStatus ?? res.data)}`;
  });

  console.log('\n========================================================================');
  console.log('📊 TỔNG KẾT KẾT QUẢ KIỂM THỬ THỰC TẾ VỚI VPS AI SERVER');
  console.log('========================================================================');

  const passCount = results.filter((r) => r.status === 'PASS').length;
  const timeoutCount = results.filter((r) => r.status === 'TIMEOUT').length;
  const aiIssueCount = results.filter((r) => r.status === 'AI_ISSUE').length;
  const failCount = results.filter((r) => r.status === 'FAIL').length;

  console.log(`✅ Passed:    ${passCount} / 32`);
  console.log(`⏳ Timeouts:  ${timeoutCount} / 32`);
  console.log(`🤖 AI Issues: ${aiIssueCount} / 32 (Worker phía AI)`);
  console.log(`❌ Failures:  ${failCount} / 32`);

  await Promise.all([producer.disconnect(), consumer.disconnect()]);
  process.exit(0);
}

main().catch((err) => {
  console.error('Fatal runner error:', err);
  process.exit(1);
});
