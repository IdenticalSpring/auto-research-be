/**
 * Registry case live T4 (payload gửi thẳng qua Kafka tới AI server thật).
 * Case được chia theo module: setup / write / chat để mỗi file ≤ 250 dòng.
 */

import { SETUP_CASES } from './live-cases-setup';
import { SETUP_EXTRA_CASES } from './live-cases-setup-extra';
import { SETUP_FILE_CASES } from './live-cases-setup-files';
import { WRITE_CASES } from './live-cases-write';
import { WRITE_EXTRA_CASES } from './live-cases-write-extra';
import { CHAT_CASES } from './live-cases-chat';

export type LiveTier = 'smoke' | 'core' | 'full';

export interface LiveContext {
  documentId: string;
  sessionId: string;
  userId: string;
  modelId: string;
}

export interface LiveCase {
  id: string;
  eventType: string;
  tier: LiveTier;
  topic: string;
  buildPayload: (ctx: LiveContext) => Record<string, unknown>;
  assert: (response: Record<string, unknown>) => string;
  /** Lý do SKIP (thiếu fixture/hạ tầng) — case vẫn hiển thị trong report. */
  skipReason?: string;
  /** Fixture hạ tầng cần có trước khi chạy (vd `mongo` ⇒ cần `MONGO_URI`). */
  fixture?: 'mongo';
  /** Chuẩn bị dữ liệu trước khi gửi request (seed Mongo, upload file…). */
  setup?: (ctx: LiveContext) => Promise<void>;
  /** Dọn dữ liệu sau khi case kết thúc — luôn chạy trong `finally`. */
  teardown?: (ctx: LiveContext) => Promise<void>;
  /**
   * Case cần hạ tầng AI (MinIO/Mongo/Qdrant) — khi `E2E_WITH_INFRA=1` sẽ dùng
   * payload + document_id/session_id lấy từ `reports/infra-fixtures.json`.
   */
  infra?: boolean;
  /**
   * Case nhiều lượt (hỏi lại người dùng) — AI trả `has_finished:false` +
   * `ai_message` được coi là kết quả hợp lệ thay vì FAIL.
   */
  allowSuspend?: boolean;
}

export const DEFAULT_USER_ID = '00000000-0000-0000-0000-000000000001';

export const PROPOSAL = {
  title: 'Nghiên cứu tác động của Chuyển đổi số đến hiệu quả SMEs',
  problem_statement: 'SMEs gặp khó khăn khi chuyển đổi số.',
  motivation: 'Làm rõ nhân tố công nghệ ảnh hưởng năng suất.',
  research_gap: 'Thiếu nghiên cứu định lượng tại Việt Nam.',
  level: 'KHOA_LUAN',
  research_type: 'quantitative',
};

export const DATA = {
  doanh_thu: [120, 145, 110, 160, 200],
  chi_phi_cntt: [15, 20, 12, 25, 35],
  nhan_su_so: [3, 4, 2, 5, 8],
  roa: [8.5, 9.2, 7.8, 10.1, 12.4],
};

export const VARIABLES = [
  { name: 'Hiệu quả', code: 'roa', scale: 'ratio', role: 'dependent' },
  {
    name: 'Chi phí CNTT',
    code: 'chi_phi_cntt',
    scale: 'ratio',
    role: 'independent',
  },
  { name: 'Nhân sự số', code: 'nhan_su_so', scale: 'ratio', role: 'independent' },
];

export const OUTLINE = {
  title: 'Đề cương',
  outline: [
    {
      chapter: 'chapter_1',
      heading: 'Chương 1: Tổng quan',
      name: 'Chương 1: Tổng quan',
      overview: 'Tổng quan chuyển đổi số',
      sections: [{ section_code: '1.1', subheading: 'Tính cấp thiết' }],
    },
  ],
};

/**
 * Các hằng mirror **đúng shape** trong `autoresearching/scripts/payloads/*.json`
 * (quyết định 2026-11-01 §1.8). Parity test `payload-parity.spec.ts` sẽ fail nếu
 * case live lệch shape so với snapshot.
 */
export const AI_PROPOSAL = {
  title: 'Tác động của livestream bán hàng đến quyết định mua sắm',
  description:
    'Phân tích tác động của livestream bán hàng đến quyết định mua sắm của người tiêu dùng',
};

export const AI_DATA = {
  tuoi: [20, 22, 25, 30],
  thu_nhap: [5, 10, 15, 20],
  chi_tieu: [2, 5, 8, 12],
};

/** Biến theo shape AI cho `comment_data` / `check_logic_data`. */
export const AI_VARIABLES = [
  { name: 'tuoi', type: 'int', description: 'Độ tuổi của người tham gia khảo sát' },
  {
    name: 'thu_nhap',
    type: 'int',
    description: 'Thu nhập hàng tháng (triệu VNĐ)',
  },
];

/** Outline theo shape AI (`chuyen_de_*`): heading + word_count + overview + subheadings. */
export const AI_OUTLINE = {
  title: 'Tác động của livestream bán hàng đến quyết định mua sắm',
  outline: [
    {
      heading: '1. Mở đầu',
      word_count: '500-1000',
      overview: 'Tổng quan bối cảnh nghiên cứu',
      subheadings: [
        {
          subheading: '1.1. Tính cấp thiết của đề tài',
          detail_description: 'Trình bày lý do chọn đề tài livestream bán hàng',
        },
      ],
    },
  ],
};

/** Tài liệu theo shape AI cho `user_documents` / `update_user_refs` / `papers_search`. */
export const AI_DOCUMENTS = [
  {
    title: 'Nghiên cứu về Livestream bán hàng và hành vi mua sắm',
    url: 'users/sample/sample.pdf',
    source: 'USER',
    authors: 'Nguyen Van A',
    year: '2024',
    journal: 'Tap chi Thuong mai Dien tu',
  },
];

/**
 * `write_content` (và một số event Mongo) yêu cầu document_id là ObjectId
 * 24 ký tự hex. Sinh từ seed để mỗi run khác nhau nhưng vẫn hợp lệ.
 * (Định nghĩa nằm ở `object-id.ts` để `mongo-fixtures` dùng lại không vòng import.)
 */
export { objectId } from './object-id';

/** Base envelope dùng chung cho mọi case live. */
export function basePayload(
  ctx: LiveContext,
  eventType: string,
  extra: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    event_type: eventType,
    document_id: ctx.documentId,
    session_id: ctx.sessionId,
    user_id: ctx.userId || DEFAULT_USER_ID,
    model_id: ctx.modelId,
    language: 'vi',
    ...extra,
  };
}

/** Base cho chatbot: contract dùng `conv_id`, không phải `document_id`. */
export function baseChatPayload(
  ctx: LiveContext,
  eventType: string,
  extra: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    event_type: eventType,
    conv_id: ctx.sessionId,
    session_id: ctx.sessionId,
    user_id: ctx.userId || DEFAULT_USER_ID,
    model_id: ctx.modelId,
    language: 'vi',
    ...extra,
  };
}

export const hasFinished = (response: Record<string, unknown>): boolean =>
  response.has_finished !== false;

export const textOf = (response: Record<string, unknown>): string =>
  String(
    response.response ??
      response.content ??
      response.improved_text ??
      response.text ??
      '',
  );

export const LIVE_CASES: LiveCase[] = [
  ...SETUP_CASES,
  ...SETUP_EXTRA_CASES,
  ...SETUP_FILE_CASES,
  ...WRITE_CASES,
  ...WRITE_EXTRA_CASES,
  ...CHAT_CASES,
];

export function selectLiveCases(tier: LiveTier, only?: string[]): LiveCase[] {
  const order: Record<LiveTier, number> = { smoke: 0, core: 1, full: 2 };
  const limit = order[tier];
  return LIVE_CASES.filter((item) => order[item.tier] <= limit).filter((item) =>
    only && only.length > 0
      ? only.includes(item.id) || only.includes(item.eventType)
      : true,
  );
}
