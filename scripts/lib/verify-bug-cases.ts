/**
 * Dữ liệu 3 test case cho `scripts/verify-3-fixed-bugs.ts`.
 * Tách riêng để script chính gọn (< 250 dòng) và dễ thêm case mới.
 */

export type Json = Record<string, unknown>;

/** Timeout tối thiểu 400s cho harness; mỗi event có ngưỡng riêng. */
export const HARNESS_TIMEOUTS = {
  default: 400_000,
  searchPapers: 300_000,
  aiInDoc: 240_000,
  chuyenDe2: 1_800_000,
} as const;

export interface TestCase {
  name: string;
  topic: string;
  eventType: string;
  docTag: string;
  timeoutMs: number;
  buildPayload: (documentId: string, modelId: string) => Json;
  assert: (response: Json) => string;
}

const TIMEOUT_OVERRIDE = Number(process.env.HARNESS_TIMEOUT_MS) || 0;

const withOverride = (value: number): number =>
  TIMEOUT_OVERRIDE > 0 ? TIMEOUT_OVERRIDE : value;

export const RESPONSE_TOPICS = [
  'document_setup_response',
  'enhancement_response',
  'write_section_response',
];

export const DOC_ID = process.env.DOC_ID || '';

/** document_id riêng cho từng test → tránh nhận nhầm response của instance khác. */
export function docIdFor(tag: string, modelId: string, runId: string): string {
  return DOC_ID || `${runId}-${tag}-${modelId}`;
}

const USER_ID = '00000000-0000-0000-0000-000000000001';

export const TEST_CASES: TestCase[] = [
  {
    name: 'TEST 1: AI-BUG-11 — search_papers (parse OpenAlex e-location)',
    topic: 'document_setup_request',
    eventType: 'search_papers',
    docTag: 'search-papers',
    timeoutMs: withOverride(HARNESS_TIMEOUTS.searchPapers),
    buildPayload: (documentId, modelId) => ({
      event_type: 'search_papers',
      document_id: documentId,
      model_id: modelId,
      user_id: USER_ID,
      language: 'vi',
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      query: 'Digital transformation SMEs Vietnam',
      country_filter: 'both',
      oa_filter: false,
      is_tool: false,
      is_oa: false,
      cut_off_year_low: 2018,
      cut_off_year_high: 2026,
      search_range: 'all',
      downloaded_papers: [],
    }),
    assert: (response) => {
      const papers = (response.papers as unknown[]) || [];
      return `Tìm thấy ${papers.length} bài báo OpenAlex (không còn ValueError int('e...'))`;
    },
  },
  {
    name: 'TEST 2: AI-BUG-10 — ai_in_doc (fix_grammar, UnboundLocalError)',
    topic: 'enhancement_request',
    eventType: 'ai_in_doc',
    docTag: 'ai-in-doc',
    timeoutMs: withOverride(HARNESS_TIMEOUTS.aiInDoc),
    buildPayload: (documentId, modelId) => ({
      event_type: 'ai_in_doc',
      document_id: documentId,
      model_id: modelId,
      user_id: USER_ID,
      language: 'vi',
      section_id: 'sec_1',
      paragraphs:
        'Chuyển đổi số giúp nâng cao năng xuất và giảm thiểu các chi phí lãng phí của doanh nghiệp.',
      paragraph_context:
        'Bối cảnh nghiên cứu hiệu quả hoạt động doanh nghiệp SMEs.',
      action: 'fix_grammar',
      proposal: {
        title:
          'Nghiên cứu tác động của Chuyển đổi số đến Hiệu quả hoạt động SMEs',
      },
    }),
    assert: (response) => {
      const text = String(
        response.response ?? response.improved_text ?? response.content ?? '',
      );
      if (text.trim().length === 0) {
        throw new Error(
          'AI trả về rỗng (không có response/improved_text/content)',
        );
      }
      return `AI đã sửa ngữ pháp: "${text.slice(0, 70)}..."`;
    },
  },
  {
    name: 'TEST 3: AI-BUG-12 — chuyen_de_2 (subsections NoneType)',
    topic: 'write_section_request',
    eventType: 'chuyen_de_2',
    docTag: 'chuyen-de-2',
    timeoutMs: withOverride(HARNESS_TIMEOUTS.chuyenDe2),
    buildPayload: (documentId, modelId) => ({
      event_type: 'chuyen_de_2',
      document_id: documentId,
      model_id: modelId,
      user_id: USER_ID,
      language: 'vi',
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      final_proposal: {
        title:
          'Nghiên cứu tác động của Chuyển đổi số đến Hiệu quả hoạt động SMEs',
        level: 'KHOA_LUAN',
      },
      outline: {
        title: 'Đề cương',
        outline: [
          {
            chapter: 'chapter_1',
            heading: 'Chương 1: Tổng quan nghiên cứu',
            name: 'Chương 1: Tổng quan nghiên cứu',
            overview: 'Tổng quan chuyển đổi số',
            sections: [
              { section_code: '1.1', subheading: 'Tính cấp thiết của đề tài' },
            ],
          },
        ],
      },
      data: {
        doanh_thu: [120, 145, 110, 160, 200],
        chi_phi_cntt: [15, 20, 12, 25, 35],
        nhan_su_so: [3, 4, 2, 5, 8],
        roa: [8.5, 9.2, 7.8, 10.1, 12.4],
      },
      references_style: 'APA 7th',
      papers_search: [
        {
          id: 'p1',
          title: 'Digital Transformation in Vietnamese SMEs',
          authors: 'Nguyen Van A',
          year: 2023,
        },
      ],
      outline_code: ['chapter_1'],
      heading: 'chapter_1',
    }),
    assert: (response) =>
      `chuyen_de_2 hoàn tất (has_finished=${String(response.has_finished ?? true)}, section="${String(response.section_name ?? 'chapter_1')}")`,
  },
];
