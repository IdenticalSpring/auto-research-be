/**
 * Chuẩn hoá `headings_percent` cho contract AI server.
 *
 * **Quyết định cuối của AI (2026-11-01, Q1):** thang là **0..1** và tổng = `1.0`
 * (`outline_generation.py:372` → `int(low_count * percent)`; `:759` kiểm `sum != 1`;
 * `:771` → `100 * percent`). AI có guard: nhận thang 0..100 sẽ tự chia 100 và log
 * `WARN [headings_percent] nhận thang 0..100` ⇒ WARN còn = BE chưa sửa.
 *
 * BE lưu DB/FE theo thang 0..100 (không đổi để tránh phá FE), nên chỉ chuyển đổi
 * **ở biên payload** gửi AI bằng các helper dưới đây.
 */

export type PercentList = number[] | undefined | null;

/** Làm tròn 4 chữ số, tránh sai số floating-point (0.15000000000000002). */
function round4(value: number): number {
  return Math.round(value * 10_000) / 10_000;
}

function isNumericList(list: unknown): list is number[] {
  return (
    Array.isArray(list) &&
    list.length > 0 &&
    list.every((item) => typeof item === 'number' && Number.isFinite(item))
  );
}

function fixSum(percents: number[]): number[] {
  if (percents.length === 0) return percents;
  const total = percents.reduce((sum, value) => sum + value, 0);
  const diff = round4(1 - total);
  if (Math.abs(diff) < 0.0001) return percents;
  const fixed = [...percents];
  const lastIndex = fixed.length - 1;
  fixed[lastIndex] = round4(fixed[lastIndex] + diff);
  return fixed;
}

/**
 * 0..100 → 0..1 (tổng = 1). Nếu input đã là 0..1 thì chỉ fix sai số làm tròn.
 * Mảng rỗng/không hợp lệ ⇒ `[]` (AI sẽ tự chia đều).
 */
export function toAiPercents(list: PercentList): number[] {
  if (!isNumericList(list)) return [];
  const total = list.reduce((sum, value) => sum + value, 0);
  const scaled =
    total > 1.0001
      ? list.map((value) => round4(value / 100))
      : list.map((value) => round4(value));
  return fixSum(scaled);
}

/** Ngược lại: 0..1 → 0..100 (dùng khi cần lưu DB/FE từ dữ liệu AI). */
export function toBePercents(list: PercentList): number[] {
  if (!isNumericList(list)) return [];
  const total = list.reduce((sum, value) => sum + value, 0);
  if (total <= 1.0001) {
    return list.map((value) => Math.round(value * 100));
  }
  return list.map((value) => Math.round(value));
}

/**
 * Chuyển `percent` của từng heading sang 0..1 (nếu heading có `percent`).
 * Giữ nguyên mọi field khác (`heading`, `subheadings`, `references`…).
 */
export function toAiHeadingPercents<T extends Record<string, unknown>>(
  headings?: T[] | null,
): T[] {
  if (!Array.isArray(headings) || headings.length === 0) return [];
  const percents = toAiPercents(
    headings.map((item) =>
      typeof item.percent === 'number' ? (item.percent as number) : undefined,
    ) as PercentList,
  );

  return headings.map((item, index) => {
    if (typeof item.percent !== 'number') return { ...item };
    return {
      ...item,
      percent: percents.length === headings.length ? percents[index] : item.percent,
    };
  });
}

/** Chuyển heading trong `outline.outline` / `outline.headings` sang 0..1. */
export function toAiOutlinePercents(
  outline: Record<string, unknown> | null | undefined,
): Record<string, unknown> {
  if (!outline || typeof outline !== 'object') return {};
  const result: Record<string, unknown> = { ...outline };
  for (const key of ['outline', 'headings'] as const) {
    const list = result[key];
    if (Array.isArray(list) && list.length > 0) {
      result[key] = toAiHeadingPercents(list as Record<string, unknown>[]);
    }
  }
  return result;
}
