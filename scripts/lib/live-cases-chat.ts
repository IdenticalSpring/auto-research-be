/** Case live T4: chatbot + enhancement. */

import {
  LiveCase,
  PROPOSAL,
  baseChatPayload,
  basePayload,
  objectId,
  textOf,
} from './live-cases';
import {
  cleanupProposeMethodFixture,
  seedOutlineAndConfig,
} from './mongo-fixtures';

export const CHAT_CASES: LiveCase[] = [
  {
    id: 'outer_chatbot',
    eventType: 'outer_chatbot',
    // Probe 2026-11-01: paragraphs/final_proposal/section là OPTIONAL — AI trả lời
    // 1.5–2.1s cho cả 5 biến thể (đầy đủ / bỏ từng field / rỗng). Dispatcher BE
    // không gửi 3 field này vẫn chạy đúng; case vẫn gửi kèm để phủ contract.
    tier: 'core',
    topic: 'chatbot_request',
    buildPayload: (ctx) =>
      baseChatPayload(ctx, 'outer_chatbot', {
        question: 'Chuyển đổi số là gì?',
        chat_history: [],
        short_answer: true,
        direct_answer: true,
        use_web_search_chatbot: false,
        paragraphs: 'Bối cảnh nghiên cứu về chuyển đổi số tại SMEs Việt Nam.',
        final_proposal: PROPOSAL,
        section: { code: '1.1', name: 'Tính cấp thiết của đề tài' },
      }),
    assert: (response) => {
      const text = textOf(response);
      if (text.trim().length === 0) throw new Error('chatbot trả rỗng');
      return text.slice(0, 60);
    },
  },
  {
    id: 'ai_in_doc',
    eventType: 'ai_in_doc',
    tier: 'smoke',
    topic: 'enhancement_request',
    buildPayload: (ctx) =>
      basePayload(ctx, 'ai_in_doc', {
        section_id: 'sec_1',
        paragraphs:
          'Chuyển đổi số giúp nâng cao năng xuất và giảm thiểu chi phí lãng phí.',
        paragraph_context: 'Bối cảnh nghiên cứu SMEs.',
        action: 'fix_grammar',
        proposal: { title: PROPOSAL.title },
      }),
    assert: (response) => {
      const text = textOf(response);
      if (text.trim().length === 0) throw new Error('response rỗng');
      return text.slice(0, 60);
    },
  },
  {
    id: 'inner_chatbot',
    eventType: 'inner_chatbot',
    tier: 'core',
    topic: 'chatbot_request',
    buildPayload: (ctx) =>
      baseChatPayload(ctx, 'inner_chatbot', {
        // inner_chatbot bắt buộc document_id (AI trả 603 Missing field 'document_id'
        // nếu thiếu). Dispatcher thật gửi session.documentId.
        document_id: objectId(ctx.documentId),
        question: 'Tóm tắt tài liệu đã upload',
        chat_history: [],
        short_answer: true,
        direct_answer: true,
        use_web_search_chatbot: false,
        paragraphs: 'Nội dung tài liệu người dùng đã upload.',
        final_proposal: PROPOSAL,
        section: { code: '1.1', name: 'Tính cấp thiết của đề tài' },
      }),
    assert: (response) => textOf(response).slice(0, 60) || 'inner chatbot OK',
  },
  {
    id: 'get_suggestions',
    eventType: 'get_suggestions',
    tier: 'core',
    topic: 'enhancement_request',
    buildPayload: (ctx) =>
      basePayload(ctx, 'get_suggestions', {
        section_id: 'sec_1',
        // get_suggestions validator yêu cầu `section` là string
        section: '1.1 - Tính cấp thiết của đề tài',
        section_context: 'Chương 1: Tổng quan nghiên cứu',
        paragraphs: 'Đoạn văn cần gợi ý cải thiện.',
        paragraph_context: 'Bối cảnh nghiên cứu SMEs.',
        final_proposal: PROPOSAL,
        proposal: PROPOSAL,
        processed_docs: [],
      }),
    assert: (response) =>
      String(
        (response.suggested_paragraphs as unknown[])?.length ?? 'OK',
      ).slice(0, 40),
  },
  {
    id: 'writer',
    eventType: 'writer',
    tier: 'full',
    topic: 'chatbot_request',
    // AI đọc outline của document trong Mongo ⇒ cần seed (thiếu ⇒ 653)
    fixture: 'mongo',
    setup: async (ctx) => {
      await seedOutlineAndConfig(ctx.documentId);
    },
    teardown: async (ctx) => {
      await cleanupProposeMethodFixture(ctx.documentId);
    },
    buildPayload: (ctx) =>
      // Mirror `payloads/writer.json` (§1.8): chat_history là **string**,
      // kèm summary/outline/domain/field/notes + giới hạn iterations.
      baseChatPayload(ctx, 'writer', {
        // Mongo _id phải là ObjectId 24-hex (khớp seedOutlineAndConfig)
        document_id: objectId(ctx.documentId),
        chat_history: '[]',
        summary: {
          main_topic: 'Ứng dụng AI trong giáo dục đại học',
          importance: 'Nâng cao chất lượng đào tạo và nghiên cứu',
        },
        outline:
          '1. Mở đầu\n2. Cơ sở lý thuyết\n3. Phương pháp\n4. Kết quả\n5. Kết luận',
        domain: 'Giáo dục',
        field: 'Công nghệ giáo dục',
        notes: [],
        use_web_search: false,
        max_research_iterations: 1,
        max_researcher_iterations: 1,
        max_concurrent_research_units: 1,
      }),
    assert: (response) => textOf(response).slice(0, 60) || 'writer OK',
  },
  {
    id: 'writer_chatbot',
    eventType: 'writer_chatbot',
    tier: 'full',
    topic: 'chatbot_request',
    // AI có thể ngắt để hỏi lại thông tin đề tài ⇒ SUSPENDED là kết quả hợp lệ
    allowSuspend: true,
    // mirror writer-chatbot.service.ts
    buildPayload: (ctx) =>
      baseChatPayload(ctx, 'writer_chatbot', {
        document_id: objectId(ctx.documentId),
        question: 'Gợi ý cách viết phần tổng quan tài liệu',
        chat_history: [],
        paragraphs: 'Nội dung hiện tại của chương 1.',
        section: { code: '1.1', name: 'Tính cấp thiết của đề tài' },
      }),
    assert: (response) => textOf(response).slice(0, 60) || 'writer_chatbot OK',
  },
  {
    id: 'enhance',
    eventType: 'enhance',
    tier: 'full',
    topic: 'enhancement_request',
    buildPayload: (ctx) =>
      // Mirror `payloads/enhance.json`: AI cần `suggested_paragraph` (dict) + `section`
      basePayload(ctx, 'enhance', {
        section_id: 'sec_1',
        section: '1.1 - Tính cấp thiết của đề tài',
        suggested_paragraph: {
          paragraphs: 'Đoạn văn cần cải thiện học thuật.',
          comment: 'làm rõ và học thuật hơn',
          paragraph_id: 'p-1',
        },
        action: 'make_academic',
        proposal: { title: PROPOSAL.title },
      }),
    assert: (response) => textOf(response).slice(0, 60) || 'enhance OK',
  },
];
