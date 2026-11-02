import {
  MIN_DELAY_MS,
  computeIdleDelay,
  computeTotalDelay,
} from './harness-timing';

describe('computeIdleDelay', () => {
  it('giữ nguyên timeout hợp lệ', () => {
    expect(computeIdleDelay(240_000)).toBe(240_000);
  });

  it('clamp khi timeout âm (lớp lỗi TimeoutNegativeWarning)', () => {
    expect(computeIdleDelay(-1_793_526_986_183)).toBe(MIN_DELAY_MS);
  });

  it('clamp khi timeout = 0 hoặc NaN', () => {
    expect(computeIdleDelay(0)).toBe(MIN_DELAY_MS);
    expect(computeIdleDelay(Number.NaN)).toBe(MIN_DELAY_MS);
    expect(computeIdleDelay(Number.POSITIVE_INFINITY)).toBe(MIN_DELAY_MS);
  });

  it('cắt phần thập phân', () => {
    expect(computeIdleDelay(1500.9)).toBe(1500);
  });
});

describe('computeTotalDelay', () => {
  it('trả phần còn lại khi chưa tới deadline', () => {
    expect(computeTotalDelay(1_000, 10_000, 4_000)).toBe(7_000);
  });

  it('clamp khi đã vượt deadline', () => {
    expect(computeTotalDelay(1_000, 10_000, 99_000)).toBe(MIN_DELAY_MS);
  });

  it('clamp khi đồng hồ lệch (now < startedAt)', () => {
    expect(computeTotalDelay(1_000, 0, 1_000)).toBe(MIN_DELAY_MS);
  });

  it('không bao giờ trả số âm với tham số bất thường', () => {
    expect(computeTotalDelay(0, -1, 0)).toBe(MIN_DELAY_MS);
    expect(computeTotalDelay(Number.NaN, 1_000, 0)).toBe(MIN_DELAY_MS);
  });
});
