/**
 * Hạ tầng fixture dùng chung cho T4 (Mongo/MinIO/Qdrant của AI server).
 *
 * Nguồn seed: `autoresearching/scripts/seed_p2_fixtures.py` (do AI cung cấp) —
 * seed `admin.document_configurations/articles/proposal_titles`,
 * `user_documents.reports_refs`, `admin.reports_refs_chunks`, upload CSV lên
 * MinIO và tạo collection Qdrant cho `DOC_ID`.
 *
 * Cách dùng (chạy trên HOST, không phải trong container BE vì cần docker CLI):
 *   npm run e2e:seed-infra        # seed
 *   npm run e2e:cleanup-infra     # dọn
 *   npm run e2e:smoke             # smoke runner của AI
 *
 * Sau khi seed, đặt `E2E_WITH_INFRA=1` khi chạy T4 để các case cần hạ tầng
 * dùng đúng `document_id` đã seed thay vì SKIP.
 */

import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { MongoClient } from 'mongodb';
import { basePayload, LiveContext } from './live-cases';

/**
 * Hạ tầng fixture dùng chung cho T4 (Mongo/MinIO/Qdrant của AI server).
 *
 * Nguồn: `reports/infra-fixtures.json` do AI sinh bằng
 * `autoresearching/scripts/seed_infra_fixtures.py` (2026-11-01). Bản commit sẵn ở
 * `scripts/fixtures/infra-fixtures.json` để CI chạy được mà không cần repo AI.
 *
 * Quy tắc: **không tự đoán `uuids`/`document_id`** — đọc đúng file fixture.
 * Harness chỉ override `model_id` (deepseek-flash) và `user_id` từ context.
 */

/** `DOC_ID` mà `seed_p2_fixtures.py` của AI seed sẵn (fallback cho case Mongo). */
export const DEFAULT_INFRA_DOC_ID = '650000000000000000000001';

export const INFRA_DOC_ID =
  process.env.E2E_INFRA_DOC_ID?.trim() || DEFAULT_INFRA_DOC_ID;

export function isInfraEnabled(): boolean {
  return process.env.E2E_WITH_INFRA === '1';
}

export interface InfraFixture {
  document_id: string;
  session_id: string;
  data_id: string;
  uuids: string[];
  minio: { bucket?: string; key?: string; url: string; size?: number };
  payloads: Record<string, Record<string, unknown>>;
  [key: string]: unknown;
}

function candidatePaths(): string[] {
  const custom = process.env.INFRA_FIXTURES_PATH?.trim();
  const paths = [
    custom,
    join(process.cwd(), 'reports', 'infra-fixtures.json'),
    join(process.cwd(), 'scripts', 'fixtures', 'infra-fixtures.json'),
  ].filter((item): item is string => Boolean(item));
  return paths;
}

export function loadInfraFixture(): InfraFixture {
  for (const path of candidatePaths()) {
    if (!existsSync(path)) continue;
    const parsed = JSON.parse(readFileSync(path, 'utf8')) as InfraFixture;
    if (!parsed.document_id || !parsed.payloads) {
      throw new Error(`Fixture hạ tầng không hợp lệ: ${path}`);
    }
    return parsed;
  }
  throw new Error(
    'Không tìm thấy infra-fixtures.json. Chạy `npm run e2e:seed-infra` hoặc đặt INFRA_FIXTURES_PATH.',
  );
}

/**
 * Payload cho event nhóm tài liệu, lấy từ fixture và override envelope:
 * `model_id` = ctx.modelId (deepseek-flash), `user_id` = ctx.userId.
 * `document_id`/`session_id`/`data_id`/`documents`/`collection` giữ nguyên fixture
 * (đây là dữ liệu đã seed thật, không được đoán).
 */
export function infraPayload(
  eventType: string,
  ctx: LiveContext,
  fixture: InfraFixture = loadInfraFixture(),
): Record<string, unknown> | undefined {
  const raw = fixture.payloads[eventType];
  if (!raw) return undefined;

  const { model_id: _ignoredModel, user_id: _ignoredUser, ...rest } = raw as Record<
    string,
    unknown
  >;

  const payload: Record<string, unknown> = {
    ...basePayload(ctx, eventType),
    ...rest,
    // Envelope do BE quyết định, không lấy từ fixture
    model_id: ctx.modelId,
    user_id: ctx.userId,
    document_id: (rest.document_id as string) ?? fixture.document_id,
    session_id: (rest.session_id as string) ?? fixture.session_id,
    // AI Q3: nhóm tài liệu phải gửi `data_id` — fixture của AI thiếu ở
    // `search_user_documents`/`admin_documents` nên fallback về data_id chung.
    data_id: (rest.data_id as string) ?? fixture.data_id,
  };

  // AI hiện vẫn đọc `ref_id` ở `search_user_documents` (lỗi 625 khi thiếu).
  // Bổ sung từ chính fixture `user_documents` theo `url` — không tự sinh giá trị.
  if (eventType === 'search_user_documents') {
    payload.documents = attachRefIds(payload.documents, fixture);
  }

  return payload;
}

/**
 * Gắn `ref_id` cho documents dựa trên mapping url → ref_id của
 * `payloads.user_documents` trong cùng fixture (AI cung cấp).
 */
function attachRefIds(
  documents: unknown,
  fixture: InfraFixture,
): unknown {
  if (!Array.isArray(documents)) return documents;
  const sourceDocs = (fixture.payloads.user_documents?.documents ?? []) as Array<
    Record<string, unknown>
  >;
  const refByUrl = new Map<string, unknown>();
  for (const doc of sourceDocs) {
    if (typeof doc.url === 'string' && doc.ref_id !== undefined) {
      refByUrl.set(doc.url, doc.ref_id);
    }
  }

  return documents.map((doc) => {
    const item = doc as Record<string, unknown>;
    if (item.ref_id !== undefined) return item;
    const refId = typeof item.url === 'string' ? refByUrl.get(item.url) : undefined;
    // AI `ingest_docs.py` đọc `document["outline_id"]` (default "") — thiếu key ⇒ 625.
    return {
      ...item,
      ...(refId === undefined ? {} : { ref_id: refId }),
      ...(item.outline_id === undefined ? { outline_id: '' } : {}),
    };
  });
}

export interface InfraSeedStatus {
  enabled: boolean;
  fixtureFound: boolean;
  documentId: string;
  mongoUriPresent: boolean;
  collections: Record<string, boolean>;
  ready: boolean;
}

/**
 * Kiểm tra seed đã sẵn sàng chưa (chỉ đọc, không ghi).
 * Dùng trong T4 để quyết định SKIP có lý do thay vì chạy case thiếu dữ liệu.
 */
export async function verifyInfraSeed(): Promise<InfraSeedStatus> {
  const uri = process.env.MONGO_URI?.trim();
  const collections: Record<string, boolean> = {
    'admin.articles': false,
    'admin.proposal_titles': false,
    'admin.document_configurations': false,
    'user_documents.reports_refs': false,
  };

  const base = {
    enabled: isInfraEnabled(),
    documentId: INFRA_DOC_ID,
    fixtureFound: candidatePaths().some((path) => existsSync(path)),
    mongoUriPresent: Boolean(uri),
    collections,
  };

  if (!uri) {
    return { ...base, ready: false };
  }

  const client = new MongoClient(uri, { serverSelectionTimeoutMS: 5000 });
  try {
    await client.connect();
    const admin = client.db('admin');
    const userDocs = client.db('user_documents');

    collections['admin.articles'] = Boolean(
      await admin.collection('articles').findOne({ _id: INFRA_DOC_ID as never }),
    );
    collections['admin.proposal_titles'] = Boolean(
      await admin.collection('proposal_titles').findOne({}),
    );
    collections['admin.document_configurations'] = Boolean(
      await admin
        .collection('document_configurations')
        .findOne({ documentId: INFRA_DOC_ID }),
    );
    collections['user_documents.reports_refs'] = Boolean(
      await userDocs
        .collection('reports_refs')
        .findOne({ _id: INFRA_DOC_ID as never }),
    );
  } finally {
    await client.close().catch(() => undefined);
  }

  const ready = Object.values(collections).every(Boolean);
  return { ...base, ready };
}

