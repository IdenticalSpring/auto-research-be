/**
 * Seed Mongo fixture cho các case T4 mà AI worker đọc trực tiếp Mongo.
 *
 * Vì sao cần: `propose_method` đọc `admin.articles[_id=document_id]` rồi
 * `article["title"]` → `admin.proposal_titles` + `admin.document_configurations`
 * (xem `autoresearching/write_listener.py:1594-1615`). Thiếu record ⇒ AI trả
 * `637 NoneType` thay vì lỗi nghiệp vụ.
 *
 * Toàn bộ id phải là ObjectId 24-hex ⇒ dùng chung `objectId()` của `live-cases`
 * để payload gửi AI và fixture luôn trùng nhau.
 */

import { MongoClient, ObjectId } from 'mongodb';
import { objectId } from './object-id';

let cachedClient: MongoClient | null = null;

export interface ProposalFixtureOptions {
  field?: string;
  domain?: string;
  subDomain?: string;
  /** 0 = định tính, 1 = định lượng, 2 = hỗn hợp (AI map RESEARCH_TYPE_ENG). */
  researchType?: { type: 0 | 1 | 2; reason: string };
  referencesStyle?: string;
  keywords?: { mainKeywords: string[]; subKeywords: string[] };
}

export interface ProposalFixture {
  mongoDocumentId: string;
  titleId: string;
}

export function isMongoFixtureEnabled(): boolean {
  return Boolean(process.env.MONGO_URI?.trim());
}

async function getClient(): Promise<MongoClient> {
  if (cachedClient) return cachedClient;
  const uri = process.env.MONGO_URI?.trim();
  if (!uri) {
    throw new Error(
      'Thiếu biến môi trường MONGO_URI — không seed được fixture (xem .env.example).',
    );
  }
  const client = new MongoClient(uri, { serverSelectionTimeoutMS: 5000 });
  await client.connect();
  cachedClient = client;
  return client;
}

export async function closeMongoFixtureClient(): Promise<void> {
  if (!cachedClient) return;
  await cachedClient.close().catch(() => undefined);
  cachedClient = null;
}

/** ObjectId (24-hex) tương ứng với documentId của case live. */
export function mongoDocumentId(documentId: string): string {
  return objectId(documentId);
}

function mongoId(hex: string): ObjectId {
  return new ObjectId(hex);
}

/** Tạo proposal_titles + document_configurations (dùng lại cho translate/delete). */
export async function seedProposalTitleAndConfig(
  documentId: string,
  options: ProposalFixtureOptions = {},
): Promise<ProposalFixture> {
  const client = await getClient();
  const admin = client.db('admin');
  const mongoDocumentIdValue = mongoDocumentId(documentId);
  const titleId = objectId(`title-${documentId}`);

  await admin.collection('proposal_titles').updateOne(
    { _id: mongoId(titleId) },
    {
      $set: {
        _id: mongoId(titleId),
        title:
          'Nghiên cứu tác động của chuyển đổi số đến hiệu quả doanh nghiệp SMEs',
        problemStatement: 'SMEs gặp khó khăn khi chuyển đổi số.',
        motivation: 'Làm rõ nhân tố công nghệ ảnh hưởng năng suất.',
        researchGap: 'Thiếu nghiên cứu định lượng tại Việt Nam.',
        webSearch: '',
        hypothesis: '',
      },
    },
    { upsert: true },
  );

  await admin.collection('document_configurations').updateOne(
    { documentId: mongoDocumentIdValue },
    {
      $set: {
        documentId: mongoDocumentIdValue,
        createdBy: 'e2e-user',
        finalProposal: mongoId(titleId),
        keywords: options.keywords ?? {
          mainKeywords: ['chuyển đổi số', 'hiệu quả doanh nghiệp'],
          subKeywords: ['SMEs', 'năng suất'],
        },
        referencesStyle: options.referencesStyle ?? 'APA 7th',
        researchType: options.researchType ?? {
          type: 1,
          reason: 'Nghiên cứu định lượng',
        },
      },
    },
    { upsert: true },
  );

  return { mongoDocumentId: mongoDocumentIdValue, titleId };
}

/** Seed đầy đủ dữ liệu cho `propose_method`. */
export async function seedProposeMethodFixture(
  documentId: string,
  options: ProposalFixtureOptions = {},
): Promise<ProposalFixture> {
  const fixture = await seedProposalTitleAndConfig(documentId, options);
  const client = await getClient();
  const admin = client.db('admin');

  await admin.collection('articles').updateOne(
    { _id: mongoId(fixture.mongoDocumentId) },
    {
      $set: {
        _id: mongoId(fixture.mongoDocumentId),
        title: fixture.titleId,
        category: 1,
        topicRefs: [],
        docsPrepare: {
          field: options.field ?? 'Kinh tế học',
          domain: options.domain ?? 'Quản trị kinh doanh',
          subDomain: options.subDomain ?? 'Chuyển đổi số',
        },
      },
    },
    { upsert: true },
  );

  // proposed_method_v2 đọc nội dung đã viết + danh sách tài liệu đã dùng
  // (yêu cầu đủ title/authors/year/key_points.{objectives,…} — thiếu ⇒ 637).
  await client.db('user_documents').collection('reports_refs').updateOne(
    { _id: fixture.mongoDocumentId as unknown as object },
    {
      $set: {
        documents: [
          {
            title: 'Nghiên cứu về chuyển đổi số trong SMEs',
            authors: 'Nguyen Van A, Tran Thi B',
            year: 2024,
            key_points: {
              objectives: 'Đánh giá tác động của chuyển đổi số đến hiệu quả SMEs.',
              results: 'Chuyển đổi số tác động tích cực đến hiệu quả hoạt động.',
              variables: ['chuyển đổi số', 'hiệu quả doanh nghiệp'],
              hypotheses: ['H1: Chuyển đổi số làm tăng hiệu quả doanh nghiệp'],
              measurement_items: ['Mức độ số hoá quy trình', 'ROA'],
              model_design: 'Hồi quy tuyến tính đa biến',
            },
          },
        ],
      },
    },
    { upsert: true },
  );

  const outlineUpdates = Array.from({ length: 5 }, (_, index) => ({
    updateOne: {
      filter: { documentId: fixture.mongoDocumentId, index: index + 1 },
      update: {
        $setOnInsert: {
          documentId: fixture.mongoDocumentId,
          index: index + 1,
          refTable: [],
          content: '',
          contentArr: [],
        },
      },
      upsert: true,
    },
  }));
  await admin.collection('outlines').bulkWrite(outlineUpdates);

  return fixture;
}

/**
 * Seed outline + document_configuration để `writer` / `update_user_refs` chạy được.
 * AI đọc `admin.document_configurations.processedOutline` + `admin.outlines` theo
 * `document_id` (outline_generation.update_user_refs_v2) — thiếu ⇒ `NoneType`.
 */
export async function seedOutlineAndConfig(
  documentId: string,
  options: ProposalFixtureOptions = {},
): Promise<ProposalFixture> {
  const fixture = await seedProposalTitleAndConfig(documentId, options);
  const client = await getClient();
  const admin = client.db('admin');

  const outlineItem = {
    code: 'chapter_1',
    chapter: 'chapter_1',
    heading: '1. Mở đầu',
    title: '1. Mở đầu',
    word_count: '500-1000',
    overview: 'Tổng quan bối cảnh nghiên cứu',
    subheadings: [
      {
        subheading: '1.1. Tính cấp thiết của đề tài',
        detail_description: 'Trình bày lý do chọn đề tài',
      },
    ],
  };

  await admin.collection('document_configurations').updateOne(
    { documentId: fixture.mongoDocumentId },
    {
      $set: {
        processedOutline: { title: 'Đề cương nghiên cứu', outline: [outlineItem] },
        finalProposal: mongoId(fixture.titleId),
      },
    },
    { upsert: true },
  );

  const outlineUpdates = Array.from({ length: 3 }, (_, index) => ({
    updateOne: {
      filter: { documentId: fixture.mongoDocumentId, index: index + 1 },
      update: {
        $setOnInsert: {
          documentId: fixture.mongoDocumentId,
          index: index + 1,
          // AI đọc `code` của từng outline (thiếu ⇒ 653 KeyError 'code')
          code: `chapter_${index + 1}`,
          refTable: [],
          content: '',
          contentArr: [],
        },
      },
      upsert: true,
    },
  }));
  await admin.collection('outlines').bulkWrite(outlineUpdates);

  return fixture;
}

/** Xoá sạch fixture đã seed (gọi trong `finally` của case live). */
export async function cleanupProposeMethodFixture(
  documentId: string,
): Promise<void> {
  const client = await getClient();
  const mongoDocumentIdValue = mongoDocumentId(documentId);
  const titleId = mongoDocumentId(`title-${documentId}`);

  await Promise.all([
    client
      .db('admin')
      .collection('articles')
      .deleteOne({ _id: mongoId(mongoDocumentIdValue) }),
    client
      .db('admin')
      .collection('proposal_titles')
      .deleteOne({ _id: mongoId(titleId) }),
    client
      .db('admin')
      .collection('document_configurations')
      .deleteOne({ documentId: mongoDocumentIdValue }),
    client
      .db('user_documents')
      .collection('reports_refs')
      .deleteOne({ _id: mongoDocumentIdValue as unknown as object }),
    client
      .db('admin')
      .collection('outlines')
      .deleteMany({ documentId: mongoDocumentIdValue }),
  ]);
}

