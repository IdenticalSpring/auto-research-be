/** Case live T4: document-setup (paper, ideation, outline, journals). */

import {
  AI_PROPOSAL,
  LiveCase,
  OUTLINE,
  PROPOSAL,
  basePayload,
} from './live-cases';

export const SETUP_CASES: LiveCase[] = [
  {
    id: 'search_papers',
    eventType: 'search_papers',
    tier: 'smoke',
    topic: 'document_setup_request',
    buildPayload: (ctx) =>
      basePayload(ctx, 'search_papers', {
        field: 'Kinh tế học',
        domain: 'Quản trị kinh doanh',
        // AI payload mẫu có subdomains/proposal/use_web_search (parity §1.8)
        subdomains: ['Hành vi người tiêu dùng'],
        proposal: PROPOSAL,
        query: 'Digital transformation SMEs Vietnam',
        // AI contract: string enum both|in|out
        country_filter: 'both',
        oa_filter: false,
        is_tool: false,
        is_oa: false,
        use_web_search: false,
        search_range: '',
        downloaded_papers: [],
        cut_off_year_low: 2018,
        cut_off_year_high: 2026,
      }),
    assert: (response) => {
      const papers = (response.papers as unknown[]) || [];
      if (papers.length === 0) throw new Error('không có papers trả về');
      return `${papers.length} papers`;
    },
  },
  {
    id: 'generate_outline',
    eventType: 'generate_outline',
    // core: AI yêu cầu bộ field đầy đủ (research_type/word_count_str/headings…)
    // → cần mirror payload của outline-ai.dispatcher, chưa đủ ở mức smoke
    tier: 'core',
    topic: 'document_setup_request',
    buildPayload: (ctx) =>
      // Mirror outline-ai.service.ts: research_type là object, headings là map
      basePayload(ctx, 'generate_outline', {
        field: 'Kinh tế học',
        domain: 'Quản trị kinh doanh',
        // AI payload mẫu dùng final_proposal kèm description (§1.8)
        final_proposal: AI_PROPOSAL,
        research_type: { type: 0, reason: 'quantitative' },
        word_count_str: '10000',
        // AI đọc key `heading` (617 nếu thiếu) + `subheadings` của từng heading.
        headings: [
          {
            heading: 'Chương 1: Tổng quan',
            title: 'Chương 1: Tổng quan',
            overview: 'Tổng quan chuyển đổi số',
            word_count: '5000-6000',
            subheadings: [{ subheading: 'Tính cấp thiết' }],
            // AI Q1: percent thang 0..1
            percent: 0.5,
          },
          {
            heading: 'Chương 2: Phương pháp',
            title: 'Chương 2: Phương pháp',
            overview: 'Phương pháp nghiên cứu',
            word_count: '4000-5000',
            subheadings: [{ subheading: 'Quy trình nghiên cứu' }],
            percent: 0.5,
          },
        ],
        // AI Q1: tổng percent = 1.0
        headings_percent: [0.5, 0.5],
        outline_code: ['chapter_1', 'chapter_2'],
        references_style: 'APA 7th',
      }),
    assert: (response) => {
      const outline = response.outline ?? response.data;
      if (!outline) throw new Error('không có outline');
      return 'outline OK';
    },
  },
  {
    id: 'generate_domains',
    eventType: 'generate_domains',
    tier: 'core',
    topic: 'document_setup_request',
    buildPayload: (ctx) =>
      basePayload(ctx, 'generate_domains', {
        field: 'Kinh tế học',
        domains_num: 3,
        is_tool: false,
      }),
    assert: (response) => {
      const domains = (response.domains as unknown[]) || [];
      if (domains.length === 0) throw new Error('không có domains');
      return `${domains.length} domains`;
    },
  },
  {
    id: 'generate_titles',
    eventType: 'generate_titles',
    tier: 'core',
    topic: 'document_setup_request',
    buildPayload: (ctx) =>
      basePayload(ctx, 'generate_titles', {
        field: 'Kinh tế học',
        domain: 'Quản trị kinh doanh',
        subdomains: ['Tài chính doanh nghiệp'],
        level: 'KHOA_LUAN',
        titles_num: 3,
      }),
    assert: (response) => {
      const data = (response.data ?? {}) as Record<string, unknown>;
      // AI trả danh sách đề tài trong key `proposals`
      const titles = (response.proposals ??
        response.titles ??
        response.titles_list ??
        data.titles ??
        []) as unknown[];
      if (!Array.isArray(titles) || titles.length === 0) {
        throw new Error(
          `không có titles trong response (keys=${Object.keys(response).join(',')})`,
        );
      }
      return `${titles.length} titles`;
    },
  },
  {
    id: 'modify_outline',
    eventType: 'modify_outline',
    tier: 'core',
    topic: 'document_setup_request',
    buildPayload: (ctx) =>
      // Mirror outline-ai.service.ts#modifyOutline
      basePayload(ctx, 'modify_outline', {
        field: 'Kinh tế học',
        domain: 'Quản trị kinh doanh',
        research_type: { type: 0, reason: 'quantitative' },
        final_proposal: PROPOSAL,
        // modify_outline validator yêu cầu list[str] (khác generate_outline)
        headings: ['Chương 1: Tổng quan'],
        // AI Q1: tổng percent = 1.0
        headings_percent: [1],
        new_headings: ['Chương 1: Tổng quan'],
        new_headings_percent: [1],
        outline: OUTLINE,
        instruction: 'Thêm mục phương pháp nghiên cứu',
        language: 'vi',
      }),
    assert: (response) => (response.outline ? 'outline modified' : 'outline OK'),
  },
  {
    id: 'get_journals',
    eventType: 'get_journals',
    tier: 'core',
    topic: 'document_setup_request',
    buildPayload: (ctx) =>
      basePayload(ctx, 'get_journals', {
        field: 'Kinh tế học',
        // AI yêu cầu number (không phải string)
        last_updated: 2024,
      }),
    assert: (response) => (response.journals ? 'journals OK' : 'response OK'),
  },
];
