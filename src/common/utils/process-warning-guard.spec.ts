import { jest } from '@jest/globals';
import {
  installTimeoutNegativeWarningSilencer,
  resetTimeoutNegativeWarningSilencerForTest,
  shouldSuppressWarning,
  TIMEOUT_NEGATIVE_WARNING_TYPE,
} from './process-warning-guard';

describe('shouldSuppressWarning', () => {
  it('should chỉ chặn TimeoutNegativeWarning', () => {
    expect(shouldSuppressWarning(TIMEOUT_NEGATIVE_WARNING_TYPE)).toBe(true);
    expect(shouldSuppressWarning('DeprecationWarning')).toBe(false);
    expect(shouldSuppressWarning('ExperimentalWarning')).toBe(false);
    expect(shouldSuppressWarning(undefined)).toBe(false);
  });
});

describe('installTimeoutNegativeWarningSilencer', () => {
  const originalEmit = process.emitWarning;

  afterEach(() => {
    resetTimeoutNegativeWarningSilencerForTest();
    process.emitWarning = originalEmit;
  });

  it('should chặn warning timeout âm (string + type)', () => {
    const onSuppressed = jest.fn();
    installTimeoutNegativeWarningSilencer({ onSuppressed });

    process.emitWarning('-1793 is a negative number.', 'TimeoutNegativeWarning');

    expect(onSuppressed).toHaveBeenCalledWith(TIMEOUT_NEGATIVE_WARNING_TYPE);
  });

  it('should chặn warning khi truyền options object', () => {
    const onSuppressed = jest.fn();
    installTimeoutNegativeWarningSilencer({ onSuppressed });

    process.emitWarning('-5 is a negative number.', {
      type: 'TimeoutNegativeWarning',
    });

    expect(onSuppressed).toHaveBeenCalledTimes(1);
  });

  it('should chặn warning dạng Error cùng tên', () => {
    const onSuppressed = jest.fn();
    installTimeoutNegativeWarningSilencer({ onSuppressed });
    const error = new Error('-1 is a negative number.');
    error.name = 'TimeoutNegativeWarning';

    process.emitWarning(error);

    expect(onSuppressed).toHaveBeenCalledTimes(1);
  });

  it('should chuyển tiếp warning khác cho Node in như mặc định', () => {
    const onSuppressed = jest.fn();
    const forwarded: unknown[] = [];
    process.emitWarning = ((warning: unknown, ...args: unknown[]) => {
      forwarded.push([warning, ...args]);
    }) as typeof process.emitWarning;

    installTimeoutNegativeWarningSilencer({ onSuppressed });
    process.emitWarning('api cũ', 'DeprecationWarning');

    expect(onSuppressed).not.toHaveBeenCalled();
    expect(forwarded).toHaveLength(1);
  });

  it('should chỉ cài 1 lần (idempotent)', () => {
    const first = jest.fn();
    const second = jest.fn();
    installTimeoutNegativeWarningSilencer({ onSuppressed: first });
    installTimeoutNegativeWarningSilencer({ onSuppressed: second });

    process.emitWarning('-9 is a negative number.', 'TimeoutNegativeWarning');

    expect(first).toHaveBeenCalledTimes(1);
    expect(second).not.toHaveBeenCalled();
  });
});
