/**
 * Phép tính thời gian cho harness.
 *
 * Tách riêng để test được lớp lỗi `TimeoutNegativeWarning`: mọi delay đưa vào
 * `setTimeout` phải >= MIN_DELAY_MS, kể cả khi đồng hồ lệch hoặc deadline đã qua.
 */

export const MIN_DELAY_MS = 1;

function sanitize(value: number): number {
  if (!Number.isFinite(value)) {
    return MIN_DELAY_MS;
  }
  return Math.max(Math.trunc(value), MIN_DELAY_MS);
}

/** Delay cho idle timer: mỗi lần nhận chunk sẽ reset. */
export function computeIdleDelay(timeoutMs: number): number {
  return sanitize(timeoutMs);
}

/**
 * Delay còn lại cho chặn trên tổng thời gian.
 * Nếu `now` đã vượt deadline (hoặc đồng hồ lệch) → trả MIN_DELAY_MS thay vì số âm.
 */
export function computeTotalDelay(
  startedAt: number,
  maxTotalMs: number,
  now = Date.now(),
): number {
  const remaining = startedAt + maxTotalMs - now;
  return sanitize(remaining);
}
