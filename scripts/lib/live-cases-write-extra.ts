/**
 * Case live T4 (bổ sung): write-report — các event còn thiếu.
 * Payload mirror `payload`/`kafkaData` trong `src/modules/write-report/**`.
 */

import {
  AI_OUTLINE,
  AI_PROPOSAL,
  AI_VARIABLES,
  DATA,
  LiveCase,
  PROPOSAL,
  VARIABLES,
  basePayload,
  hasFinished,
  objectId,
  textOf,
} from './live-cases';
import {
  cleanupProposeMethodFixture,
  seedProposalTitleAndConfig,
} from './mongo-fixtures';

export const WRITE_EXTRA_CASES: LiveCase[] = [
  {
    id: 'chuyen_de_1',
    eventType: 'chuyen_de_1',
    tier: 'core',
    topic: 'write_section_request',
    // mirror write-content-ai.service.ts (startWriteContent)
    buildPayload: (ctx) =>
      basePayload(ctx, 'chuyen_de_1', {
        document_id: objectId(ctx.documentId),
        field: 'Kinh tế học',
        domain: 'Quản trị kinh doanh',
        references_style: 'APA',
        final_proposal: AI_PROPOSAL,
        papers_search: [
          {
            title: 'Nghiên cứu về Livestream bán hàng và hành vi mua sắm',
            abstract:
              'Livestream bán hàng là hình thức thương mại điện tử mới nổi tác động mạnh đến người tiêu dùng.',
            source: 'USER',
            ref_id: 'ref-paper-1',
          },
        ],
        outline: AI_OUTLINE,
        outline_code: ['chapter_1'],
        heading: 'chapter_1',
        thread_id: `thread-${ctx.sessionId}`,
        db: 'redis',
        retry: 2,
        use_web_search: false,
        resume: false,
        resume_error: false,
        resume_method: false,
        analyze_log: null,
        detailed_logs: null,
        generated_files: null,
        data: DATA,
        variables: VARIABLES,
      }),
    assert: (response) =>
      hasFinished(response) ? 'chuyen_de_1 finished' : 'chunk nhận được',
  },
  {
    id: 'chuyen_de_3',
    eventType: 'chuyen_de_3',
    tier: 'core',
    topic: 'write_section_request',
    buildPayload: (ctx) =>
      basePayload(ctx, 'chuyen_de_3', {
        document_id: objectId(ctx.documentId),
        field: 'Kinh tế học',
        domain: 'Quản trị kinh doanh',
        references_style: 'APA',
        final_proposal: AI_PROPOSAL,
        papers_search: [
          {
            title: 'Nghiên cứu về Livestream bán hàng và hành vi mua sắm',
            abstract:
              'Livestream bán hàng là hình thức thương mại điện tử mới nổi tác động mạnh đến người tiêu dùng.',
            source: 'USER',
            ref_id: 'ref-paper-1',
          },
        ],
        outline: AI_OUTLINE,
        outline_code: ['chapter_1'],
        heading: 'chapter_1',
        thread_id: `thread-${ctx.sessionId}`,
        db: 'redis',
        retry: 2,
        use_web_search: false,
        resume: false,
        resume_error: false,
        resume_method: false,
        analyze_log: null,
        detailed_logs: null,
        generated_files: null,
        data: DATA,
        variables: VARIABLES,
      }),
    assert: (response) =>
      hasFinished(response) ? 'chuyen_de_3 finished' : 'chunk nhận được',
  },
  {
    id: 'analyzer_tool',
    eventType: 'analyzer_tool',
    tier: 'full',
    topic: 'write_section_request',
    // mirror analyze-ai.service.ts (analyzeTool) — is_tool: true
    buildPayload: (ctx) =>
      basePayload(ctx, 'analyzer_tool', {
        conv_id: ctx.sessionId,
        thread_id: `thread-${ctx.sessionId}`,
        final_proposal: PROPOSAL,
        final_report: PROPOSAL,
        data: DATA,
        variables: VARIABLES,
        is_tool: true,
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
      return html ? `result_html=${html.slice(0, 40)}` : 'analyzer_tool finished';
    },
  },
  {
    id: 'check_logic_data',
    eventType: 'check_logic_data',
    tier: 'full',
    topic: 'write_section_request',
    // mirror data-analysis-ai.service.ts (checkLogicData)
    buildPayload: (ctx) =>
      basePayload(ctx, 'check_logic_data', {
        // AI shape: {name,type,description}
        variables: AI_VARIABLES,
      }),
    assert: (response) => {
      const data = (response.data ?? response) as Record<string, unknown>;
      return data.is_valid !== undefined || data.result !== undefined
        ? 'check_logic_data OK'
        : 'check_logic_data finished';
    },
  },
  {
    id: 'parsed_value',
    eventType: 'parsed_value',
    tier: 'full',
    topic: 'write_section_request',
    // mirror parsed-value-ai.service.ts
    buildPayload: (ctx) =>
      basePayload(ctx, 'parsed_value', {
        document_id: objectId(ctx.documentId),
        // §1.4: parsed_value thuộc nhóm tài liệu ⇒ gửi data_id
        data_id: objectId(ctx.documentId),
        // AI shape: mỗi survey_question là object (payloads/parsed_value.json)
        survey_questions: [
          {
            variable_name: 'Sự hài lòng',
            variable_code: 'HL',
            variable_type: 'Phụ thuộc',
            measurement_type: 'Tiềm ẩn',
            questions: 'Bạn hài lòng với dịch vụ?',
            answer_content: 'Thang Likert 1-5',
          },
          {
            variable_name: 'Tuổi',
            variable_code: 'TUOI',
            variable_type: 'Độc lập',
            measurement_type: 'Quan sát',
            questions: 'Bạn bao nhiêu tuổi?',
            answer_content: 'Số nguyên',
          },
        ],
        variables: [],
      }),
    assert: (response) =>
      response.variables || response.parsed_value || hasFinished(response)
        ? 'parsed_value OK'
        : 'chunk nhận được',
  },
  {
    id: 'edit_report',
    eventType: 'edit_report',
    tier: 'core',
    topic: 'write_section_request',
    // mirror edit-report-ai.service.ts
    buildPayload: (ctx) =>
      basePayload(ctx, 'edit_report', {
        document_id: objectId(ctx.documentId),
        section_name: '1.1 Tính cấp thiết của đề tài',
        content:
          'Chuyển đổi số đang trở thành xu hướng tất yếu của doanh nghiệp Việt Nam.',
        instruction: 'Viết lại học thuật hơn, giữ nguyên ý nghĩa.',
        context: 'Chương 1: Tổng quan nghiên cứu',
      }),
    assert: (response) => {
      const text = textOf(response);
      if (!text) throw new Error('không có nội dung đã chỉnh sửa');
      return text.slice(0, 60);
    },
  },
  {
    id: 'translate_report',
    eventType: 'translate_report',
    tier: 'core',
    topic: 'write_section_request',
    // mirror translate-ai.service.ts — AI đọc/ghi admin.document_configurations
    fixture: 'mongo',
    setup: async (ctx) => {
      await seedProposalTitleAndConfig(ctx.documentId);
    },
    teardown: async (ctx) => {
      await cleanupProposeMethodFixture(ctx.documentId);
    },
    buildPayload: (ctx) =>
      basePayload(ctx, 'translate_report', {
        document_id: objectId(ctx.documentId),
        language: 'Vietnamese',
        target_language: 'English',
        is_outline: false,
      }),
    assert: (response) =>
      hasFinished(response) ? 'translate_report OK' : 'chunk nhận được',
  },
  {
    id: 'delete_report',
    eventType: 'delete_report',
    tier: 'full',
    topic: 'write_section_request',
    // mirror delete-report-ai.service.ts
    buildPayload: (ctx) =>
      basePayload(ctx, 'delete_report', {
        // AI `payloads/delete_report.json`: document_id là LIST, không phải string
        document_id: [ctx.documentId],
        thread_id: [`thread-${ctx.sessionId}`],
      }),
    assert: (response) =>
      hasFinished(response) ? 'delete_report OK' : 'chunk nhận được',
  },
  {
    id: 'enhance_all',
    eventType: 'enhance_all',
    tier: 'full',
    topic: 'enhancement_request',
    // mirror enhancement-ai.service.ts (enhanceAll)
    buildPayload: (ctx) =>
      basePayload(ctx, 'enhance_all', {
        section: '1.1 - Tính cấp thiết của đề tài',
        // AI: mỗi suggested_paragraph là dict {paragraphs, comment, paragraph_id}
        suggested_paragraphs: [
          {
            paragraphs:
              'Chuyển đổi số giúp doanh nghiệp tăng năng suất và giảm chi phí.',
            comment: 'làm rõ và học thuật hơn',
            paragraph_id: 'p-1',
          },
        ],
      }),
    assert: (response) => {
      const updated = (response.updated_paragraphs ?? []) as unknown[];
      return updated.length > 0
        ? `${updated.length} paragraphs`
        : 'enhance_all OK';
    },
  },
];
