/** Case live T4: write-report (analyzer, chuyen_de, write_content, comment). */

import {
  AI_DATA,
  AI_PROPOSAL,
  AI_VARIABLES,
  DATA,
  LiveCase,
  OUTLINE,
  PROPOSAL,
  VARIABLES,
  basePayload,
  hasFinished,
  objectId,
  textOf,
} from './live-cases';
import {
  cleanupProposeMethodFixture,
  seedProposeMethodFixture,
} from './mongo-fixtures';

export const WRITE_CASES: LiveCase[] = [
  {
    id: 'analyzer',
    eventType: 'analyzer',
    // core: AI worker yêu cầu key `final_report` + schema đầy đủ (lỗi 631 khi thiếu)
    tier: 'core',
    topic: 'write_section_request',
    buildPayload: (ctx) =>
      basePayload(ctx, 'analyzer', {
        conv_id: ctx.sessionId,
        thread_id: `thread-${ctx.sessionId}`,
        final_proposal: PROPOSAL,
        final_report: PROPOSAL,
        data: DATA,
        variables: VARIABLES,
        is_tool: false,
        auto_plan_mode: true,
        auto_action_mode: true,
        manual_plan_mode: false,
        tool_configs: [],
        resume: false,
        resume_error: false,
        feedback: '',
        db: 'redis',
      }),
    assert: (response) => {
      if (!hasFinished(response)) throw new Error('has_finished=false');
      const html = String(response.result_html ?? '');
      return html ? `result_html=${html.slice(0, 40)}` : 'analyzer finished';
    },
  },
  {
    id: 'chuyen_de_2',
    eventType: 'chuyen_de_2',
    tier: 'smoke',
    topic: 'write_section_request',
    buildPayload: (ctx) =>
      basePayload(ctx, 'chuyen_de_2', {
        field: 'Kinh tế học',
        domain: 'Quản trị kinh doanh',
        final_proposal: PROPOSAL,
        outline: OUTLINE,
        data: DATA,
        references_style: 'APA 7th',
        papers_search: [
          {
            id: 'p1',
            title: 'Digital Transformation in SMEs',
            authors: 'Nguyen Van A',
            year: 2023,
          },
        ],
        outline_code: ['chapter_1'],
        heading: 'chapter_1',
      }),
    assert: (response) => {
      if (!hasFinished(response)) throw new Error('has_finished=false');
      return `section=${String(response.section_name ?? 'chapter_1')}`;
    },
  },
  {
    id: 'write_content',
    eventType: 'write_content',
    tier: 'core',
    topic: 'write_section_request',
    buildPayload: (ctx) =>
      basePayload(ctx, 'write_content', {
        // AI yêu cầu ObjectId 24-hex cho write_content
        document_id: objectId(ctx.documentId),
        field: 'Kinh tế học',
        domain: 'Quản trị kinh doanh',
        final_proposal: PROPOSAL,
        outline: OUTLINE,
        data: DATA,
        references_style: 'APA 7th',
        outline_code: ['chapter_1'],
      }),
    assert: (response) => {
      if (!hasFinished(response)) throw new Error('has_finished=false');
      return textOf(response).slice(0, 60) || 'write_content finished';
    },
  },
  {
    id: 'comment_data',
    eventType: 'comment_data',
    tier: 'core',
    topic: 'write_section_request',
    buildPayload: (ctx) =>
      // Mirror `autoresearching/scripts/payloads/comment_data.json` (§1.8):
      // final_proposal kèm description, data numeric, variables {name,type,description}
      basePayload(ctx, 'comment_data', {
        final_proposal: AI_PROPOSAL,
        data: AI_DATA,
        variables: AI_VARIABLES,
        variables_data: AI_VARIABLES,
        thread_id: `thread-${ctx.sessionId}`,
      }),
    assert: (response) =>
      hasFinished(response) ? 'comment finished' : 'chunk nhận được',
  },
  {
    id: 'propose_method',
    eventType: 'propose_method',
    tier: 'core',
    topic: 'write_section_request',
    // AI đọc Mongo admin.articles[_id=document_id] → article["title"] →
    // proposal_titles + document_configurations (write_listener.py:1594-1615).
    // Thiếu record ⇒ 637 NoneType, nên case tự seed/cleanup fixture.
    fixture: 'mongo',
    allowSuspend: true,
    setup: async (ctx) => {
      await seedProposeMethodFixture(ctx.documentId);
    },
    teardown: async (ctx) => {
      await cleanupProposeMethodFixture(ctx.documentId);
    },
    buildPayload: (ctx) =>
      // Mirror AI repo scripts/payloads/propose_method.json (payload chuẩn của AI)
      basePayload(ctx, 'propose_method', {
        // AI yêu cầu ObjectId 24-hex — trùng với `mongoDocumentId()` của fixture
        document_id: objectId(ctx.documentId),
        conv_id: ctx.sessionId,
        thread_id: `thread-${ctx.sessionId}`,
        language: 'Vietnamese',
        user_input: '',
        final_proposal: {
          ...PROPOSAL,
          description: 'Phân tích tác động của chuyển đổi số đến hiệu quả SMEs',
        },
        research_type: { type: 1, reason: 'Nghiên cứu định lượng' },
        auto_mode: true,
        inherit_mode: false,
        resume_method: false,
        resume_error: false,
        db: 'redis',
      }),
    assert: (response) =>
      hasFinished(response) ? 'propose finished' : 'chunk nhận được',
  },
  {
    id: 'gen_slides',
    eventType: 'gen_slides',
    tier: 'full',
    topic: 'write_section_request',
    buildPayload: (ctx) =>
      // Mirror `payloads/gen_slides.json`: AI cần `raw_report` (HTML) + user_preference
      basePayload(ctx, 'gen_slides', {
        raw_report:
          '<h1>Nghiên cứu livestream bán hàng</h1><h2>1. Mở đầu</h2>' +
          '<p>Livestream bán hàng là hình thức thương mại điện tử mới nổi, ' +
          'mang lại trải nghiệm tương tác thời gian thực.</p>' +
          '<h2>2. Kết luận</h2><p>Livestream thúc đẩy quyết định mua hàng.</p>',
        user_preference: 1,
        manual_mode: false,
      }),
    assert: (response) =>
      String(response.pptx_url ?? response.slides_url ?? 'gen_slides OK').slice(
        0,
        60,
      ),
  },
];
