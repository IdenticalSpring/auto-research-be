import {
  toAiHeadingPercents,
  toAiOutlinePercents,
  toAiPercents,
  toBePercents,
} from './outline-percent.helper';

describe('toAiPercents', () => {
  it('should chuyển thang 0..100 sang 0..1 (tổng = 1)', () => {
    expect(toAiPercents([15, 25, 25, 25, 10])).toEqual([
      0.15, 0.25, 0.25, 0.25, 0.1,
    ]);
    expect(toAiPercents([15, 25, 60])).toEqual([0.15, 0.25, 0.6]);
  });

  it('should giữ nguyên khi đã là 0..1', () => {
    expect(toAiPercents([0.5, 0.5])).toEqual([0.5, 0.5]);
    expect(toAiPercents([1])).toEqual([1]);
  });

  it('should fix sai số để tổng đúng bằng 1', () => {
    const result = toAiPercents([33, 33, 34]);
    expect(result.reduce((sum, value) => sum + value, 0)).toBeCloseTo(1, 6);

    const fraction = toAiPercents([0.3333, 0.3333, 0.3333]);
    expect(fraction.reduce((sum, value) => sum + value, 0)).toBeCloseTo(1, 6);
  });

  it('should trả [] cho input rỗng/không hợp lệ', () => {
    expect(toAiPercents(undefined)).toEqual([]);
    expect(toAiPercents([])).toEqual([]);
    expect(toAiPercents([Number.NaN] as number[])).toEqual([]);
  });
});

describe('toBePercents', () => {
  it('should chuyển 0..1 sang 0..100', () => {
    expect(toBePercents([0.15, 0.25, 0.6])).toEqual([15, 25, 60]);
    expect(toBePercents([0.5, 0.5])).toEqual([50, 50]);
  });

  it('should giữ nguyên khi đã là 0..100', () => {
    expect(toBePercents([15, 25, 60])).toEqual([15, 25, 60]);
  });
});

describe('toAiHeadingPercents', () => {
  it('should chỉ đổi percent, giữ heading/subheadings', () => {
    const result = toAiHeadingPercents([
      { heading: 'Chương 1', percent: 60, subheadings: [{ subheading: 'A' }] },
      { heading: 'Chương 2', percent: 40 },
    ]);

    expect(result[0].percent).toBe(0.6);
    expect(result[1].percent).toBe(0.4);
    expect(result[0].heading).toBe('Chương 1');
    expect(result[0].subheadings).toEqual([{ subheading: 'A' }]);
  });

  it('should không thêm percent khi heading không có', () => {
    const result = toAiHeadingPercents([{ heading: 'Chương 1' }]);
    expect(result[0]).toEqual({ heading: 'Chương 1' });
  });

  it('should trả [] cho input rỗng', () => {
    expect(toAiHeadingPercents(undefined)).toEqual([]);
    expect(toAiHeadingPercents([])).toEqual([]);
  });
});

describe('toAiOutlinePercents', () => {
  it('should đổi percent trong outline.outline và outline.headings', () => {
    const result = toAiOutlinePercents({
      title: 'Đề cương',
      outline: [{ heading: 'A', percent: 100 }],
      headings: [{ heading: 'B', percent: 100 }],
    });

    expect((result.outline as any)[0].percent).toBe(1);
    expect((result.headings as any)[0].percent).toBe(1);
    expect(result.title).toBe('Đề cương');
  });

  it('should trả {} cho input không hợp lệ', () => {
    expect(toAiOutlinePercents(undefined)).toEqual({});
    expect(toAiOutlinePercents(null)).toEqual({});
  });

  it('should giữ nguyên khi không có outline/headings array', () => {
    const result = toAiOutlinePercents({ title: 'x' });
    expect(result).toEqual({ title: 'x' });
  });
});
