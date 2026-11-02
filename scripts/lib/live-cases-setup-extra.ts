/**
 * Case live T4 (bổ sung): document-setup — các event còn thiếu so với
 * `EVENT_TIMEOUT_CONFIG`. Payload mirror `kafkaData` của dispatcher/service
 * tương ứng trong `src/modules/document-setup/**`.
 */

import { AI_DOCUMENTS, LiveCase, PROPOSAL, basePayload, objectId } from './live-cases';
import {
  cleanupProposeMethodFixture,
  seedOutlineAndConfig,
} from './mongo-fixtures';

function firstArray(response: Record<string, unknown>, keys: string[]): unknown[] {
  for (const key of keys) {
    const value = response[key];
    if (Array.isArray(value) && value.length > 0) return value;
  }
  const data = response.data as Record<string, unknown> | undefined;
  return data ? firstArray(data, keys) : [];
}

export const SETUP_EXTRA_CASES: LiveCase[] = [
  {
    id: 'generate_subdomains',
    eventType: 'generate_subdomains',
    tier: 'core',
    topic: 'document_setup_request',
    // mirror proposal-ideation.service.ts (generateSubdomains)
    buildPayload: (ctx) =>
      basePayload(ctx, 'generate_subdomains', {
        field: 'Kinh tế học',
        domain: 'Quản trị kinh doanh',
        subdomains_num: 3,
        number_of_subdomains: 3,
        is_tool: false,
      }),
    assert: (response) => {
      const items = firstArray(response, ['subdomains', 'sub_domains']);
      if (items.length === 0) throw new Error('không có subdomains');
      return `${items.length} subdomains`;
    },
  },
  {
    id: 'generate_keywords',
    eventType: 'generate_keywords',
    tier: 'core',
    topic: 'document_setup_request',
    // mirror proposal-ideation.service.ts (generateKeywords)
    buildPayload: (ctx) =>
      basePayload(ctx, 'generate_keywords', {
        field: 'Kinh tế học',
        domain: 'Quản trị kinh doanh',
        subdomains: ['Chuyển đổi số', 'Hiệu quả doanh nghiệp'],
        proposal: PROPOSAL,
      }),
    assert: (response) => {
      const keywords = response.keywords;
      if (keywords && typeof keywords === 'object') return 'keywords object';
      const items = firstArray(response, [
        'keywords_main',
        'keywordsMain',
        'main_keywords',
      ]);
      if (items.length === 0) throw new Error('không có keywords');
      return `${items.length} keywords`;
    },
  },
  {
    id: 'generate_title_description',
    eventType: 'generate_title_description',
    tier: 'core',
    topic: 'document_setup_request',
    // mirror proposal-details.service.ts (generateTitleDescription)
    buildPayload: (ctx) =>
      basePayload(ctx, 'generate_title_description', {
        title: PROPOSAL.title,
        field: 'Kinh tế học',
        domain: 'Quản trị kinh doanh',
        subdomains: ['Chuyển đổi số'],
        keywords: ['chuyển đổi số', 'SMEs'],
        level: 'KHOA_LUAN',
        knowledge_base: [
          {
            title: PROPOSAL.title,
            content: 'Tài liệu tham khảo cơ sở đề tài',
          },
        ],
      }),
    assert: (response) =>
      response.problem_statement || response.problemStatement
        ? 'has problem_statement'
        : 'title description OK',
  },
  {
    id: 'generate_title_description_no_info',
    eventType: 'generate_title_description_no_info',
    tier: 'full',
    topic: 'document_setup_request',
    buildPayload: (ctx) =>
      basePayload(ctx, 'generate_title_description_no_info', {
        title: PROPOSAL.title,
        level: 'KHOA_LUAN',
        use_web_search: false,
      }),
    assert: (response) => (response.field ? 'has field' : 'no_info OK'),
  },
  {
    id: 'mix_titles',
    eventType: 'mix_titles',
    tier: 'full',
    topic: 'document_setup_request',
    // mirror proposal-details.service.ts (mixTitles)
    buildPayload: (ctx) =>
      basePayload(ctx, 'mix_titles', {
        field: 'Kinh tế học',
        domain: 'Quản trị kinh doanh',
        level: 'KHOA_LUAN',
        proposals: [PROPOSAL],
        titles: [PROPOSAL.title],
      }),
    assert: (response) => {
      const proposal = (response.proposal ?? response) as Record<string, unknown>;
      return proposal.title
        ? `title=${String(proposal.title).slice(0, 40)}`
        : 'mix_titles OK';
    },
  },
  {
    id: 'get_research_type',
    eventType: 'get_research_type',
    tier: 'full',
    topic: 'document_setup_request',
    buildPayload: (ctx) =>
      basePayload(ctx, 'get_research_type', {
        field: 'Kinh tế học',
        domain: 'Quản trị kinh doanh',
        proposal: {
          title: PROPOSAL.title,
          problem_statement: PROPOSAL.problem_statement,
          motivation: PROPOSAL.motivation,
          research_gap: PROPOSAL.research_gap,
        },
      }),
    assert: (response) => {
      const type =
        response.research_type ?? response.researchType ?? response.type;
      if (type === undefined || type === null) {
        throw new Error('không có research_type');
      }
      return `research_type=${JSON.stringify(type).slice(0, 40)}`;
    },
  },
  {
    id: 'generate_outline_with_refs',
    eventType: 'generate_outline_with_refs',
    tier: 'full',
    topic: 'document_setup_request',
    // mirror outline-ai.service.ts (generateOutlineWithRefs)
    buildPayload: (ctx) =>
      basePayload(ctx, 'generate_outline_with_refs', {
        outline: {
          outline: [
            {
              chapter: 'chapter_1',
              heading: 'Chương 1: Tổng quan',
              title: 'Chương 1: Tổng quan',
              overview: 'Tổng quan chuyển đổi số',
              word_count: '5000-6000',
              // AI yêu cầu mỗi heading có `subheadings` (626 nếu thiếu)
              subheadings: [{ subheading: '1.1 Tính cấp thiết' }],
              // AI Q1: percent thang 0..1
              percent: 0.5,
            },
            {
              chapter: 'chapter_2',
              heading: 'Chương 2: Phương pháp',
              title: 'Chương 2: Phương pháp',
              overview: 'Phương pháp nghiên cứu',
              word_count: '4000-5000',
              subheadings: [{ subheading: '2.1 Quy trình nghiên cứu' }],
              percent: 0.5,
            },
          ],
        },
        outline_code: ['chapter_1', 'chapter_2'],
        final_proposal: PROPOSAL,
        processed_docs: [
          {
            id: 'ref-1',
            title: 'Digital transformation in SMEs',
            authors: 'Nguyen Van A',
            journal: 'Journal of Business',
            year: 2024,
            url: 'https://example.com/paper-1',
            open_access: true,
            q: 'Q1',
            abstract: 'Tổng quan chuyển đổi số trong SMEs.',
            user_language_title: 'Chuyển đổi số trong SMEs',
          },
        ],
      }),
    assert: (response) => {
      const outline = response.outline ?? response.new_outline ?? response.data;
      if (!outline) throw new Error('không có outline');
      return 'outline with refs OK';
    },
  },
  {
    id: 'update_user_refs',
    eventType: 'update_user_refs',
    tier: 'full',
    topic: 'document_setup_request',
    // AI đọc outline + document_configuration trong Mongo (thiếu ⇒ 628 NoneType)
    fixture: 'mongo',
    setup: async (ctx) => {
      await seedOutlineAndConfig(ctx.documentId);
    },
    teardown: async (ctx) => {
      await cleanupProposeMethodFixture(ctx.documentId);
    },
    // Mirror `payloads/update_user_refs.json`: documents là list phẳng
    // {title,url,source,authors,year,journal} — KHÔNG phải map heading_id.
    buildPayload: (ctx) =>
      basePayload(ctx, 'update_user_refs', {
        document_id: objectId(ctx.documentId),
        documents: AI_DOCUMENTS,
      }),
    assert: (response) =>
      response.has_finished === false ? 'chunk nhận được' : 'update refs OK',
  },
];
