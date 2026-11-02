/**
 * Case live T4 (bổ sung): các event phụ thuộc file thật trên MinIO/S3.
 *
 * Chưa bật vì cần fixture file (PDF) upload sẵn trong bucket. Giữ khai báo để
 * report luôn liệt kê đủ event và để khi có fixture chỉ cần bỏ `skipReason`.
 * Xem `plans/todo-next-steps-2026-11-01.md` mục task 3.
 */

import { LiveCase, basePayload } from './live-cases';
import { infraPayload, isInfraEnabled } from './infra-fixtures';

const MINIO_SKIP =
  'Cần hạ tầng AI: chạy `npm run e2e:seed-infra` rồi `E2E_WITH_INFRA=1` (dùng reports/infra-fixtures.json)';

function fileCase(
  id: string,
  extra: Record<string, unknown>,
): LiveCase {
  return {
    id,
    eventType: id,
    tier: 'full',
    topic: 'document_setup_request',
    infra: true,
    skipReason: MINIO_SKIP,
    buildPayload: (ctx) => {
      // Khi có hạ tầng: dùng đúng payload/ids AI đã seed (không tự đoán uuids).
      if (isInfraEnabled()) {
        const payload = infraPayload(id, ctx);
        if (payload) return payload;
      }
      return basePayload(ctx, id, extra);
    },
    assert: (response) =>
      response.has_finished === false ? 'chunk nhận được' : `${id} OK`,
  };
}

export const SETUP_FILE_CASES: LiveCase[] = [
  fileCase('user_documents', {
    // AI §2 fixture: url là bucket/key, source là vai trò USER
    // AI Q3: data_id optional nhưng nên gửi
    data_id: 'e2e-data-001',
    documents: [
      {
        title: 'AI fixture sample',
        url: 'autoresearching/sample/sample.pdf',
        source: 'USER',
        authors: 'AI fixture',
        year: '2024',
        journal: 'AI fixture journal',
        ref_id: 'ai-fixture-ref-1',
      },
    ],
  }),
  fileCase('embed_user_documents', {
    data_id: 'e2e-data-001',
    document_id: 'e2e-live-user-documents-e70459',
    processed_docs: [
      {
        title: 'AI fixture sample',
        url: 'autoresearching/sample/sample.pdf',
        source: 'USER',
        authors: 'AI fixture',
        year: '2024',
        journal: 'AI fixture journal',
        ref_id: 'ai-fixture-ref-1',
      },
    ],
  }),
  fileCase('search_user_documents', {
    data_id: 'e2e-data-001',
    document_id: 'e2e-live-user-documents-e70459',
    query: 'chuyển đổi số',
    top_k: 5,
    documents: [
      {
        url: 'autoresearching/sample/sample.pdf',
        title: 'AI fixture sample',
        source: 'USER',
      },
    ],
  }),
  fileCase('delete_documents', {
    // AI Q3: data_id BẮT BUỘC; uuids lấy từ reports/infra-fixtures.json (không tự đoán)
    data_id: 'e2e-data-001',
    collection: 'e2e-live-user-documents-e70459',
    documents: [
      {
        uuids: [
          '43a32029-73a7-4e89-abb5-3e93a77a7b17',
          '768546ec-fadd-4d43-9779-d7f6f6a30a42',
          '7f9e8a55-6484-49da-9e10-09e475a1132a',
          '8c8d941f-bd6f-49b1-b2f9-76b2ec4f6132',
          'd3dc158d-34ff-4886-9c5a-deb620ac4a75',
          'f419db3a-0674-4dc5-a72f-5a83a894a361',
        ],
      },
    ],
  }),
  fileCase('admin_documents', {
    // AI §2: url là bucket/key, source là vai trò ADMIN/USER
    data_id: 'admin_documents',
    documents: [
      {
        url: 'autoresearching/sample/sample.pdf',
        title: 'AI fixture sample',
        source: 'ADMIN',
      },
    ],
  }),
  fileCase('user_guide_documents', {
    // AI Q3: data_id BẮT BUỘC cho event này
    data_id: 'e2e-data-001',
    documents_urls: ['autoresearching/sample/sample.pdf'],
  }),
];
