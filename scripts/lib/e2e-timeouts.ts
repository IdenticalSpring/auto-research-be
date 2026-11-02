/**
 * Timeout cho E2E live test.
 *
 * Bám theo `EVENT_TIMEOUT_CONFIG` của BE (`src/common/services/timeout-manager.service.ts`)
 * cộng biên an toàn, với sàn tối thiểu để tránh cắt oan job dài.
 *
 * **Quyết định của AI server (2026-11-01):** harness BE phải chờ **≥ `TIME_OUT_DICT`
 * của AI** (`autoresearching/error_type.py`). Đặt timeout thấp hơn rồi kết luận
 * "AI chậm/treo" là **sai thiết kế** (AI đã nêu rõ trong handoff §1.7).
 *
 * Parity với BE được kiểm bởi `e2e-timeouts.spec.ts`.
 */

export type Tier = 'smoke' | 'core' | 'full';

/** Copy của EVENT_TIMEOUT_CONFIG — parity test sẽ fail nếu BE đổi mà file này không đổi. */
export const EVENT_TIMEOUT_CONFIG: Record<string, number> = {
  write_content: 1_800_000,
  chuyen_de_1: 1_800_000,
  chuyen_de_2: 1_800_000,
  chuyen_de_3: 1_800_000,
  writer: 1_800_000,
  generate_outline: 1_500_000,
  get_journals: 900_000,
  analyzer: 420_000,
  analyzer_tool: 420_000,
  gen_slides: 420_000,
  comment_data: 420_000,
  propose_method: 420_000,
  parsed_value: 420_000,
  edit_report: 420_000,
  // Nhóm tài liệu: AI khai marker 0 nhưng listener chạy 3600s
  admin_documents: 3_600_000,
  user_guide_documents: 3_600_000,
  check_logic_data: 300_000,
  search_papers: 300_000,
  generate_titles: 420_000,
  generate_title_description: 420_000,
  generate_title_description_no_info: 420_000,
  mix_titles: 420_000,
  generate_outline_with_refs: 180_000,
  modify_outline: 180_000,
  // Nhóm tài liệu: AI khai marker 0 nhưng listener chạy 3600s
  user_documents: 3_600_000,
  embed_user_documents: 3_600_000,
  outer_chatbot: 240_000,
  inner_chatbot: 240_000,
  writer_chatbot: 240_000,
  ai_in_doc: 240_000,
  get_suggestions: 240_000,
  enhance: 240_000,
  enhance_all: 240_000,
  generate_domains: 120_000,
  generate_subdomains: 120_000,
  generate_keywords: 120_000,
  get_research_type: 120_000,
  search_user_documents: 120_000,
  delete_documents: 120_000,
  delete_report: 120_000,
};

/**
 * `TIME_OUT_DICT` của AI server (giây) — nguồn: `autoresearching/error_type.py`.
 * Sàn tuyệt đối: harness PHẢI chờ ≥ giá trị này (× 1000 ms) cho từng event.
 * **Lưu ý:** `TIME_OUT_DICT = 0` ở AI là **marker**, không phải 0 giây — 4 event
 * nhóm tài liệu được listener chạy với 3600s (`document_setup_listener.py:55-58`).
 */
export const AI_TIMEOUT_DICT_SECONDS: Record<string, number> = {
  search_papers: 300,
  generate_domains: 120,
  generate_subdomains: 120,
  generate_keywords: 120,
  generate_titles: 420,
  generate_title_description: 420,
  mix_titles: 420,
  generate_outline: 1500,
  // AI khai `TIME_OUT_DICT = 0` là MARKER ⇒ thực tế listener chạy 3600s
  // (document_setup_listener.py:55-58). Harness phải chờ ≥ 3600s.
  admin_documents: 3600,
  user_guide_documents: 3600,
  user_documents: 3600,
  embed_user_documents: 3600,
  get_journals: 900,
  get_research_type: 120,
  write_content: 1800,
  analyzer: 420,
  analyzer_tool: 420,
  chuyen_de_1: 1800,
  chuyen_de_2: 1800,
  chuyen_de_3: 1800,
  gen_slides: 420,
  comment_data: 420,
  propose_method: 420,
  parsed_value: 420,
  edit_report: 420,
  outer_chatbot: 240,
  inner_chatbot: 240,
  writer_chatbot: 240,
  writer: 1800,
  ai_in_doc: 240,
  get_suggestions: 240,
  enhance: 240,
  enhance_all: 240,
};

export const DEFAULT_EVENT_TIMEOUT_MS = 120_000;

/** Sàn timeout cho E2E: không chờ ít hơn 4 phút. */
export const MIN_E2E_TIMEOUT_MS = 240_000;

/** Biên an toàn 20% so với timeout BE dùng để cắt request. */
export const TIMEOUT_MARGIN = 1.2;

export interface ResolveTimeoutOptions {
  /** Override thủ công (ms) — thắng mọi cấu hình khác. */
  override?: number;
  /** Đọc thêm env `HARNESS_TIMEOUT_MS` nếu không truyền override. */
  env?: Record<string, string | undefined>;
}

export function resolveEventTimeout(
  eventType: string | undefined,
  options: ResolveTimeoutOptions = {},
): number {
  // Mặc định đọc process.env để `HARNESS_TIMEOUT_MS` thực sự có tác dụng khi chạy CLI
  const env =
    options.env ?? (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env;
  const envValue = Number(env?.HARNESS_TIMEOUT_MS || 0);
  const override = options.override ?? (envValue > 0 ? envValue : undefined);
  if (override && override > 0) {
    return Math.trunc(override);
  }

  const base = eventType
    ? (EVENT_TIMEOUT_CONFIG[eventType] ?? DEFAULT_EVENT_TIMEOUT_MS)
    : DEFAULT_EVENT_TIMEOUT_MS;

  // Sàn AI (quyết định 2026-11-01): không bao giờ chờ ít hơn TIME_OUT_DICT của AI.
  const aiFloorMs = aiTimeoutFloorMs(eventType);

  return Math.max(Math.round(base * TIMEOUT_MARGIN), aiFloorMs, MIN_E2E_TIMEOUT_MS);
}

/** Sàn timeout theo `TIME_OUT_DICT` của AI (ms); 0 nếu AI không đặt timeout. */
export function aiTimeoutFloorMs(eventType: string | undefined): number {
  if (!eventType) return 0;
  const seconds = AI_TIMEOUT_DICT_SECONDS[eventType] ?? 0;
  return seconds > 0 ? seconds * 1000 : 0;
}

export function formatTimeout(ms: number): string {
  return ms >= 60_000 ? `${Math.round(ms / 1000)}s (${(ms / 60_000).toFixed(1)}m)` : `${ms}ms`;
}
