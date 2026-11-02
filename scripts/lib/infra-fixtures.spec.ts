import { jest } from '@jest/globals';
import {
  infraPayload,
  isInfraEnabled,
  loadInfraFixture,
  type InfraFixture,
} from './infra-fixtures';

const CTX = {
  documentId: 'e2e-case-1',
  sessionId: 'sess-case-1',
  userId: '00000000-0000-0000-0000-000000000001',
  modelId: 'deepseek-flash',
};

describe('loadInfraFixture', () => {
  it('should đọc fixture đã vendor trong scripts/fixtures', () => {
    const fixture = loadInfraFixture();

    expect(fixture.document_id).toBeTruthy();
    expect(fixture.session_id).toBeTruthy();
    expect(fixture.data_id).toBeTruthy();
    expect(Array.isArray(fixture.uuids)).toBe(true);
    expect(Object.keys(fixture.payloads).length).toBeGreaterThanOrEqual(6);
  });

  it('should có đủ 6 event nhóm tài liệu', () => {
    const fixture = loadInfraFixture();
    for (const event of [
      'admin_documents',
      'user_guide_documents',
      'user_documents',
      'search_user_documents',
      'embed_user_documents',
      'delete_documents',
    ]) {
      expect(fixture.payloads[event]).toBeDefined();
    }
  });
});

describe('infraPayload', () => {
  const fixture = loadInfraFixture();

  it('should override model_id/user_id nhưng giữ ids đã seed', () => {
    const payload = infraPayload('user_documents', CTX, fixture)!;

    // Model/user do BE quyết định
    expect(payload.model_id).toBe('deepseek-flash');
    expect(payload.user_id).toBe(CTX.userId);
    // document_id/session_id/data_id lấy từ fixture (đã seed thật)
    expect(payload.document_id).toBe(fixture.document_id);
    expect(payload.session_id).toBe(fixture.session_id);
    expect(payload.data_id).toBe(fixture.data_id);
  });

  it('should không mang model gemini từ fixture', () => {
    const payload = infraPayload('admin_documents', CTX, fixture)!;
    expect(payload.model_id).toBe('deepseek-flash');
    expect(JSON.stringify(payload).toLowerCase()).not.toContain('gemini');
  });

  it('delete_documents dùng đúng 6 uuids trong fixture', () => {
    const payload = infraPayload('delete_documents', CTX, fixture)!;
    const documents = payload.documents as Array<{ uuids: string[] }>;
    expect(documents[0].uuids).toEqual(fixture.uuids);
    expect(payload.data_id).toBe(fixture.data_id);
    expect(payload.collection).toBe(fixture.document_id);
  });

  it('user_guide_documents có data_id bắt buộc', () => {
    const payload = infraPayload('user_guide_documents', CTX, fixture)!;
    expect(payload.data_id).toBeTruthy();
    expect(Array.isArray(payload.documents_urls)).toBe(true);
  });

  it('documents dùng url bucket/key + source ADMIN|USER', () => {
    for (const event of [
      'admin_documents',
      'user_documents',
      'search_user_documents',
    ]) {
      const payload = infraPayload(event, CTX, fixture)!;
      const documents = payload.documents as Array<Record<string, unknown>>;
      expect(documents.length).toBeGreaterThan(0);
      for (const doc of documents) {
        expect(String(doc.url)).toMatch(/^[\w.-]+\/.+/);
        expect(['ADMIN', 'USER']).toContain(doc.source);
      }
    }
  });

  it('search_user_documents được bổ sung ref_id từ fixture (AI đang cần)', () => {
    const payload = infraPayload('search_user_documents', CTX, fixture)!;
    const documents = payload.documents as Array<Record<string, unknown>>;
    const expected = fixture.payloads.user_documents.documents as Array<
      Record<string, unknown>
    >;
    expect(documents[0].ref_id).toBe(expected[0].ref_id);
  });

  it('should trả undefined cho event không có trong fixture', () => {
    expect(infraPayload('generate_domains', CTX, fixture)).toBeUndefined();
  });
});

describe('isInfraEnabled', () => {
  const original = process.env.E2E_WITH_INFRA;
  afterEach(() => {
    if (original === undefined) delete process.env.E2E_WITH_INFRA;
    else process.env.E2E_WITH_INFRA = original;
  });

  it('chỉ bật khi E2E_WITH_INFRA=1', () => {
    process.env.E2E_WITH_INFRA = '1';
    expect(isInfraEnabled()).toBe(true);
    process.env.E2E_WITH_INFRA = '0';
    expect(isInfraEnabled()).toBe(false);
    delete process.env.E2E_WITH_INFRA;
    expect(isInfraEnabled()).toBe(false);
  });
});
