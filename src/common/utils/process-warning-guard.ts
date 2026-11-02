/**
 * Guard cho warning `TimeoutNegativeWarning` phát sinh **bên trong kafkajs 2.2.4**.
 *
 * Nguyên nhân đã xác định (2026-11-01, `--trace-warnings`):
 * `node_modules/kafkajs/src/network/requestQueue/index.js:317`
 *
 * ```js
 * let scheduleAt = this.throttledUntil - Date.now()   // 0 - now ⇒ số âm
 * if (this.pending.length > 0) scheduleAt = scheduleAt > 0 ? scheduleAt : INTERVAL
 * this.throttleCheckTimeoutId = setTimeout(..., scheduleAt)
 * ```
 *
 * Khi hàng đợi rỗng, `scheduleAt` giữ nguyên giá trị âm ⇒ Node in cảnh báo và
 * clamp về 1ms ở **mỗi lần khởi động** producer/consumer. Đây là lỗi upstream,
 * không phải logic BE nên không thể clamp từ phía mình.
 *
 * Node vẫn in warning kể cả khi có listener `process.on('warning')`, nên cách
 * duy nhất để log sạch mà không tắt toàn bộ cảnh báo là chặn tại
 * `process.emitWarning`. Guard dưới đây chỉ bỏ đúng type này, mọi warning khác
 * (DeprecationWarning…) vẫn đi qua nguyên vẹn.
 *
 * Cách thay thế không monkey-patch: chạy process với
 * `NODE_OPTIONS=--disable-warning=TimeoutNegativeWarning` (Node ≥ 21.3).
 */

/** Type warning do Node phát ra khi `setTimeout` nhận số âm. */
export const TIMEOUT_NEGATIVE_WARNING_TYPE = 'TimeoutNegativeWarning';

export function shouldSuppressWarning(type?: string): boolean {
  return type === TIMEOUT_NEGATIVE_WARNING_TYPE;
}

function resolveWarningType(warning: unknown, args: unknown[]): string | undefined {
  if (warning instanceof Error) {
    return warning.name;
  }
  const first = args[0];
  if (typeof first === 'string') {
    return first;
  }
  if (first && typeof first === 'object') {
    const type = (first as { type?: unknown }).type;
    if (typeof type === 'string') {
      return type;
    }
  }
  return undefined;
}

let installed = false;

export interface SilencerOptions {
  /** Gọi lại mỗi khi một warning bị chặn (mặc định: im lặng). */
  onSuppressed?: (type: string) => void;
}

/**
 * Cài silencer 1 lần cho cả process. Gọi trước khi tạo Kafka client.
 * Các warning khác vẫn được Node in như mặc định (ta patch `emitWarning`,
 * không dùng listener nên không "nuốt" warning khác).
 */
export function installTimeoutNegativeWarningSilencer(
  options: SilencerOptions = {},
): void {
  if (installed) return;
  installed = true;

  const original = process.emitWarning.bind(process);

  const patched = (warning: unknown, ...args: unknown[]) => {
    const type = resolveWarningType(warning, args);
    if (shouldSuppressWarning(type)) {
      options.onSuppressed?.(type as string);
      return;
    }
    return (original as (...a: unknown[]) => void)(warning, ...args);
  };

  process.emitWarning = patched as typeof process.emitWarning;
}

/** Chỉ dùng cho test. */
export function resetTimeoutNegativeWarningSilencerForTest(): void {
  installed = false;
}
