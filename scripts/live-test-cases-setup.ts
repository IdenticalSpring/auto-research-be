import { randomUUID } from 'node:crypto';
import { REAL_PROPOSAL, DOC_ID } from './live-test-helpers';
import { TestCaseDef } from './live-test-cases';

export const CHATBOT_AND_SETUP_CASES: TestCaseDef[] = [
  // 1. Phân hệ Chatbot & Enhancement
  {
    step: 1,
    name: 'outer_chatbot (chatbot_request)',
    topic: 'chatbot_request',
    timeoutMs: 30000,
    payload: () => ({
      event_type: 'outer_chatbot',
      conv_id: `conv-${randomUUID()}`,
      question: 'Ý nghĩa của hệ số R-squared là gì?',
      chat_history: [],
      language: 'vi',
    }),
    validate: (res) => {
      const resp = Array.isArray(res.response) ? (res.response[0] as any)?.text : res.response || res.message;
      const ans = String(resp || '');
      return `Trả lời: "${ans.slice(0, 45)}..." (${ans.length} ký tự)`;
    },
  },
  {
    step: 2,
    name: 'inner_chatbot (chatbot_request)',
    topic: 'chatbot_request',
    timeoutMs: 30000,
    payload: () => ({
      event_type: 'inner_chatbot',
      conv_id: `conv-${randomUUID()}`,
      document_id: DOC_ID,
      question: 'Rào cản chuyển đổi số lớn nhất ở SMEs là gì?',
      chat_history: [],
      language: 'vi',
    }),
    validate: (res) => {
      const resp = Array.isArray(res.response) ? (res.response[0] as any)?.text : res.response;
      return `Phản hồi: "${String(resp || '').slice(0, 45)}..."`;
    },
  },
  {
    step: 3,
    name: 'writer_chatbot (chatbot_request)',
    topic: 'chatbot_request',
    timeoutMs: 30000,
    payload: () => ({
      event_type: 'writer_chatbot',
      conv_id: `conv-${randomUUID()}`,
      question: 'Gợi ý 5 chương cho đề tài chuyển đổi số SMEs.',
      language: 'vi',
    }),
    validate: (res) => `Trả lời: "${String(res.ai_message || res.response || '').slice(0, 45)}..."`,
  },
  {
    step: 4,
    name: 'ai_in_doc (enhancement_request)',
    topic: 'enhancement_request',
    timeoutMs: 30000,
    payload: () => ({
      event_type: 'ai_in_doc',
      document_id: DOC_ID,
      section_id: 'sec_1',
      content: 'Chuyển đổi số giúp tăng năng xuất và giảm chi phí.',
      paragraphs: 'Chuyển đổi số giúp tăng năng xuất và giảm chi phí.',
      paragraph_context: 'Hiệu quả hoạt động doanh nghiệp.',
      action: 'fix_grammar',
      proposal: REAL_PROPOSAL,
    }),
    validate: (res) => `Đã sửa: "${String(res.response || res.improved_text || 'Done').slice(0, 45)}..."`,
  },
  {
    step: 5,
    name: 'get_suggestions (enhancement_request)',
    topic: 'enhancement_request',
    timeoutMs: 30000,
    payload: () => ({
      event_type: 'get_suggestions',
      document_id: DOC_ID,
      section: 'Tổng quan',
      section_context: 'Chuyển đổi số trong doanh nghiệp vừa và nhỏ.',
      processed_docs: [],
    }),
    validate: (res) => `Gợi ý: ${JSON.stringify(res.suggested_paragraphs ? 'OK' : 'Done')}`,
  },

  // 2. Phân hệ Đề tài & Tài liệu
  {
    step: 6,
    name: 'generate_titles (document_setup_request)',
    topic: 'document_setup_request',
    timeoutMs: 60000,
    payload: () => ({
      event_type: 'generate_titles',
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      subdomains: ['Chuyển đổi số'],
      level: 'KHOA_LUAN',
      academic_level: 'KHOA_LUAN',
    }),
    validate: (res) => `Nhận ${((res.proposals as any[]) || []).length} đề tài`,
  },
  {
    step: 7,
    name: 'generate_title_description_no_info',
    topic: 'document_setup_request',
    timeoutMs: 60000,
    payload: () => ({
      event_type: 'generate_title_description_no_info',
      title: 'Ứng dụng AI trong thẩm định tín dụng ngân hàng',
      level: 'LUAN_VAN_THAC_SI',
      academic_level: 'LUAN_VAN_THAC_SI',
    }),
    validate: (res) => `Gán tự động: field="${res.field}", domain="${res.domain}"`,
  },
  {
    step: 8,
    name: 'mix_titles',
    topic: 'document_setup_request',
    timeoutMs: 60000,
    payload: () => ({
      event_type: 'mix_titles',
      proposals: [
        { title: 'Chuyển đổi số chuỗi cung ứng nông nghiệp' },
        { title: 'Ứng dụng Blockchain trong truy xuất nguồn gốc thực phẩm' },
      ],
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      level: 'KHOA_LUAN',
      academic_level: 'KHOA_LUAN',
    }),
    validate: (res) => `Đề tài mới: "${((res.proposal as any) || res).title?.slice(0, 45)}..."`,
  },
  {
    step: 9,
    name: 'search_papers (OpenAlex live)',
    topic: 'document_setup_request',
    timeoutMs: 60000,
    payload: () => ({
      event_type: 'search_papers',
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      query: 'Digital transformation SMEs Vietnam',
      // AI contract (2026-11-01): string enum both|in|out
      country_filter: 'in',
      cut_off_year_low: 2020,
      cut_off_year_high: 2025,
      oa_filter: false,
      is_tool: false,
      is_oa: false,
      downloaded_papers: [],
    }),
    validate: (res) => `Tìm thấy ${((res.papers as any[]) || []).length} bài báo OpenAlex`,
  },
  {
    step: 10,
    name: 'get_journals',
    topic: 'document_setup_request',
    timeoutMs: 30000,
    payload: () => ({
      event_type: 'get_journals',
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      last_updated: Date.now(),
    }),
    validate: (res) => `Nhận ${((res.journals as any[]) || (res.data as any[]) || []).length} tạp chí khoa học`,
  },
];
