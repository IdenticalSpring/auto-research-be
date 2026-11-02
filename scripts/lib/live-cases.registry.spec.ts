import { jest } from '@jest/globals';
import { LIVE_CASES, selectLiveCases } from './live-cases';
import { resolveEventTimeout } from './e2e-timeouts';
import { objectId } from './object-id';
import { mongoDocumentId } from './mongo-fixtures';

const TOPICS = new Set([
  'document_setup_request',
  'write_section_request',
  'chatbot_request',
  'enhancement_request',
]);

const TIERS = new Set(['smoke', 'core', 'full']);

/** Event chưa thể chạy vì thiếu fixture file trên MinIO — phải khai báo lý do. */
const ALLOWED_SKIP_EVENTS = new Set([
  'user_documents',
  'embed_user_documents',
  'search_user_documents',
  'delete_documents',
  'admin_documents',
  'user_guide_documents',
]);

const ctx = {
  documentId: 'e2e-live-123-registry',
  sessionId: 'sess-registry',
  userId: '00000000-0000-0000-0000-000000000001',
  modelId: 'deepseek-flash',
};

describe('LIVE_CASES registry', () => {
  it('id và eventType không trùng nhau', () => {
    const ids = LIVE_CASES.map((item) => item.id);
    const events = LIVE_CASES.map((item) => item.eventType);
    expect(new Set(ids).size).toBe(ids.length);
    expect(new Set(events).size).toBe(events.length);
  });

  it('mọi case dùng tier và topic hợp lệ', () => {
    for (const item of LIVE_CASES) {
      expect(TIERS.has(item.tier)).toBe(true);
      expect(TOPICS.has(item.topic)).toBe(true);
    }
  });

  it('payload có envelope tối thiểu và document_id|conv_id', () => {
    for (const item of LIVE_CASES) {
      const payload = item.buildPayload(ctx);
      expect(payload.event_type).toBe(item.eventType);
      expect(payload.user_id).toBe(ctx.userId);
      expect(payload.model_id).toBe(ctx.modelId);
      expect(typeof payload.session_id).toBe('string');
      expect(payload.document_id ?? payload.conv_id).toBeTruthy();
    }
  });

  it('payload build lại nhiều lần cho cùng kết quả (không state ẩn)', () => {
    for (const item of LIVE_CASES) {
      expect(item.buildPayload(ctx)).toEqual(item.buildPayload(ctx));
    }
  });

  it('mỗi case có timeout > 0', () => {
    for (const item of LIVE_CASES) {
      expect(resolveEventTimeout(item.eventType)).toBeGreaterThan(0);
    }
  });

  it('skipReason chỉ dùng cho event thiếu fixture file và phải có nội dung', () => {
    for (const item of LIVE_CASES) {
      if (!item.skipReason) continue;
      expect(ALLOWED_SKIP_EVENTS.has(item.eventType)).toBe(true);
      expect(item.skipReason.length).toBeGreaterThan(10);
    }
  });

  it('AI Q3: nhóm tài liệu gửi data_id, 2 event bắt buộc không được thiếu', () => {
    const DOCUMENT_EVENTS = [
      'user_documents',
      'embed_user_documents',
      'search_user_documents',
      'delete_documents',
      'admin_documents',
      'user_guide_documents',
    ];
    const MANDATORY = ['user_guide_documents', 'delete_documents'];

    for (const eventType of DOCUMENT_EVENTS) {
      const liveCase = LIVE_CASES.find((item) => item.eventType === eventType);
      expect(liveCase).toBeDefined();
      const payload = liveCase!.buildPayload(ctx);
      expect({ eventType, hasDataId: Boolean(payload.data_id) }).toEqual({
        eventType,
        hasDataId: true,
      });
    }

    // 2 event bắt buộc: kiểm tường minh để thông điệp lỗi rõ ràng
    for (const eventType of MANDATORY) {
      const liveCase = LIVE_CASES.find((item) => item.eventType === eventType);
      const payload = liveCase!.buildPayload(ctx);
      expect(String(payload.data_id).length).toBeGreaterThan(0);
    }
  });

  it('AI §2: documents gửi url dạng bucket/key và source ADMIN|USER', () => {
    for (const eventType of [
      'admin_documents',
      'user_documents',
      'search_user_documents',
    ]) {
      const liveCase = LIVE_CASES.find((item) => item.eventType === eventType);
      const payload = liveCase!.buildPayload(ctx);
      const documents = payload.documents as Array<Record<string, unknown>>;
      expect(documents.length).toBeGreaterThan(0);
      for (const doc of documents) {
        expect(String(doc.url)).toMatch(/^[\w.-]+\/.+/);
        expect(['ADMIN', 'USER']).toContain(doc.source);
      }
    }

    const guide = LIVE_CASES.find(
      (item) => item.eventType === 'user_guide_documents',
    )!.buildPayload(ctx);
    for (const url of guide.documents_urls as string[]) {
      expect(url).toMatch(/^[\w.-]+\/.+/);
    }
  });

  it('case cần Mongo phải khai báo fixture để tier4 biết đường skip', () => {
    for (const item of LIVE_CASES) {
      if (item.setup) {
        expect(item.fixture).toBe('mongo');
        expect(typeof item.teardown).toBe('function');
      }
    }
  });

  it('selectLiveCases lọc theo tier và --only', () => {
    const smoke = selectLiveCases('smoke');
    expect(smoke.length).toBeGreaterThan(0);
    expect(smoke.every((item) => item.tier === 'smoke')).toBe(true);

    const onlyOne = selectLiveCases('core', ['search_papers']);
    expect(onlyOne).toHaveLength(1);

    const byEvent = selectLiveCases('full', ['chuyen_de_3']);
    expect(byEvent.map((item) => item.id)).toEqual(['chuyen_de_3']);
  });
});

describe('objectId helper', () => {
  it('luôn trả 24 ký tự hex hợp lệ', () => {
    for (const seed of ['abc', 'e2e-live-123', 'ZZZ-999', '']) {
      const value = objectId(seed);
      expect(value).toHaveLength(24);
      expect(/^[0-9a-f]{24}$/.test(value)).toBe(true);
    }
  });

  it('mongo-fixtures dùng cùng giá trị với payload', () => {
    const documentId = 'e2e-live-999-x';
    expect(mongoDocumentId(documentId)).toBe(objectId(documentId));
  });
});

describe('phủ event theo kế hoạch T4', () => {
  const events = new Set(LIVE_CASES.map((item) => item.eventType));

  // Danh sách event bắt buộc phải có case (mirror EVENT_TIMEOUT_CONFIG + contract AI).
  const REQUIRED = [
    'search_papers',
    'generate_domains',
    'generate_subdomains',
    'generate_keywords',
    'generate_titles',
    'generate_title_description',
    'generate_title_description_no_info',
    'mix_titles',
    'get_research_type',
    'get_journals',
    'generate_outline',
    'generate_outline_with_refs',
    'modify_outline',
    'update_user_refs',
    'admin_documents',
    'user_guide_documents',
    'user_documents',
    'embed_user_documents',
    'search_user_documents',
    'delete_documents',
    'write_content',
    'chuyen_de_1',
    'chuyen_de_2',
    'chuyen_de_3',
    'analyzer',
    'analyzer_tool',
    'comment_data',
    'check_logic_data',
    'parsed_value',
    'propose_method',
    'gen_slides',
    'edit_report',
    'translate_report',
    'delete_report',
    'outer_chatbot',
    'inner_chatbot',
    'writer',
    'writer_chatbot',
    'ai_in_doc',
    'get_suggestions',
    'enhance',
    'enhance_all',
  ];

  it.each(REQUIRED)('có case cho %s', (eventType) => {
    expect(events.has(eventType)).toBe(true);
  });
});
