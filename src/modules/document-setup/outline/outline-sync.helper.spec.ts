import { OutlineSyncHelper } from './outline-sync.helper';

/**
 * Regression cho contract heading vs title (F4/F9).
 *
 * AI server đọc key `heading` trong từng heading (thiếu ⇒ 617 KeyError), trong khi
 * DB/FE dùng `title`. Vì vậy mọi phần tử do BE sinh ra **bắt buộc** có đủ 3 key
 * `heading` + `title` + `percent`.
 */
function expectHeadingShape(heading: Record<string, any>) {
  expect(heading).toEqual(
    expect.objectContaining({
      heading: expect.any(String),
      title: expect.any(String),
      // AI đọc thêm overview + word_count của từng heading (quyết định 2026-11-01)
      overview: expect.any(String),
      word_count: expect.any(String),
      percent: expect.any(Number),
    }),
  );
  expect(heading.heading.length).toBeGreaterThan(0);
  expect(heading.title.length).toBeGreaterThan(0);
}

describe('OutlineSyncHelper.resolveFinalProposal', () => {
  it('should normalize customProposal và giữ field lạ', () => {
    const res = OutlineSyncHelper.resolveFinalProposal(null, {
      title: 'Đề tài A',
      level: 'Thạc sĩ',
      extra: 1,
    });

    expect(res.title).toBe('Đề tài A');
    expect(res.level).toBe('LUAN_VAN_THAC_SI');
    expect(res.extra).toBe(1);
  });

  it('should trả {} khi không có proposal', () => {
    expect(OutlineSyncHelper.resolveFinalProposal(undefined)).toEqual({});
    expect(OutlineSyncHelper.resolveFinalProposal(null, null)).toEqual({});
  });

  it('should map camelCase của proposal trong DB', () => {
    const res = OutlineSyncHelper.resolveFinalProposal({
      title: 'Đề tài B',
      problemStatement: 'Vấn đề',
      motivation: 'Động lực',
      researchGap: 'Khoảng trống',
      level: 'Tiến sĩ',
      finalModel: { m: 1 },
      hypothesis: 'H1',
      variables: [{ name: 'x' }],
      surveyQuestions: ['q1'],
      questions: ['q2'],
    });

    expect(res).toEqual({
      title: 'Đề tài B',
      problem_statement: 'Vấn đề',
      motivation: 'Động lực',
      research_gap: 'Khoảng trống',
      level: 'LUAN_AN_TIEN_SI',
      final_model: { m: 1 },
      hypothesis: 'H1',
      variables: [{ name: 'x' }],
      survey_questions: ['q1'],
      questions: ['q2'],
    });
  });

  it('should default field thiếu thành chuỗi rỗng / null', () => {
    const res = OutlineSyncHelper.resolveFinalProposal({ level: undefined });

    expect(res.title).toBe('');
    expect(res.problem_statement).toBe('');
    expect(res.motivation).toBe('');
    expect(res.research_gap).toBe('');
    expect(res.level).toBe('KHOA_LUAN');
    expect(res.final_model).toBeNull();
    expect(res.hypothesis).toBeNull();
    expect(res.variables).toBeNull();
    expect(res.survey_questions).toBeNull();
    expect(res.questions).toBeNull();
  });
});

describe('OutlineSyncHelper.resolveHeadingsAndPercents', () => {
  it('should dùng default 5 chương với percent cố định', () => {
    const res = OutlineSyncHelper.resolveHeadingsAndPercents();

    expect(res.headings).toHaveLength(5);
    expect(res.headingsPercent).toEqual([15, 25, 25, 25, 10]);
    res.headings.forEach(expectHeadingShape);
    expect(res.outlineCode).toEqual([
      'chapter_1',
      'chapter_2',
      'chapter_3',
      'chapter_4',
      'chapter_5',
    ]);
  });

  it('should coi mảng rỗng là không truyền headings', () => {
    const res = OutlineSyncHelper.resolveHeadingsAndPercents([], []);

    expect(res.headings).toHaveLength(5);
    expect(res.headingsPercent).toEqual([15, 25, 25, 25, 10]);
  });

  it('should giữ percents hợp lệ (cùng độ dài)', () => {
    const res = OutlineSyncHelper.resolveHeadingsAndPercents(
      [{ title: 'A' }, { title: 'B' }],
      [30, 70],
    );

    expect(res.headingsPercent).toEqual([30, 70]);
    expect(res.headings.map((h: any) => h.percent)).toEqual([30, 70]);
  });

  it('should chia đều và dồn phần lẻ vào chương cuối (không phải 5)', () => {
    const res = OutlineSyncHelper.resolveHeadingsAndPercents([
      { title: 'A' },
      { title: 'B' },
      { title: 'C' },
    ]);

    // floor(100/3) = 33 ⇒ [33, 33, 34]
    expect(res.headingsPercent).toEqual([33, 33, 34]);
    const sum = res.headingsPercent.reduce((a: number, b: number) => a + b, 0);
    expect(sum).toBe(100);
    res.headings.forEach(expectHeadingShape);
  });

  it('should tính lại khi percents lệch độ dài', () => {
    const res = OutlineSyncHelper.resolveHeadingsAndPercents(
      [{ title: 'A' }, { title: 'B' }],
      [10, 20, 30],
    );

    expect(res.headingsPercent).toEqual([50, 50]);
  });

  it('should hỗ trợ heading dạng string', () => {
    const res = OutlineSyncHelper.resolveHeadingsAndPercents(['Mở đầu', 'Kết luận']);

    expect(res.headings[0]).toEqual(
      expect.objectContaining({
        heading: 'Mở đầu',
        title: 'Mở đầu',
        percent: 50,
      }),
    );
    res.headings.forEach(expectHeadingShape);
  });

  it('should fallback title từ heading/name và ưu tiên percent của item', () => {
    const res = OutlineSyncHelper.resolveHeadingsAndPercents([
      { heading: 'Chỉ có heading' },
      { name: 'Chỉ có name' },
      { title: 'Có title', heading: 'heading khác', percent: 99 },
      {},
    ]);

    expect(res.headings[0]).toEqual(
      expect.objectContaining({
        heading: 'Chỉ có heading',
        title: 'Chỉ có heading',
      }),
    );
    expect(res.headings[1].title).toBe('Chỉ có name');
    expect(res.headings[2]).toEqual(
      expect.objectContaining({
        heading: 'heading khác',
        title: 'Có title',
        percent: 99,
      }),
    );
    expect(res.headings[3].title).toBe('Chương 4');
    res.headings.forEach(expectHeadingShape);
  });

  it('should fallback khi heading không phải string/object', () => {
    const res = OutlineSyncHelper.resolveHeadingsAndPercents([null, 42] as any[]);

    expect(res.headings[0]).toEqual(
      expect.objectContaining({ heading: 'Chương 1', title: 'Chương 1' }),
    );
    expect(res.headings[1]).toEqual(
      expect.objectContaining({ heading: 'Chương 2', title: 'Chương 2' }),
    );
    res.headings.forEach(expectHeadingShape);
  });

  it('should giữ field lạ của heading (spread ...h)', () => {
    const res = OutlineSyncHelper.resolveHeadingsAndPercents([
      { title: 'A', code: 'c1', references: [{ id: 'r1' }] },
    ]);

    expect(res.headings[0].code).toBe('c1');
    expect(res.headings[0].references).toEqual([{ id: 'r1' }]);
  });

  it('should giữ subheadings của heading khi AI/DB đã có', () => {
    const res = OutlineSyncHelper.resolveHeadingsAndPercents([
      {
        heading: 'Chương 1',
        subheadings: [{ subheading: 'Tính cấp thiết' }],
      },
    ]);

    expect(res.headings[0].subheadings).toEqual([
      { subheading: 'Tính cấp thiết' },
    ]);
    expect(res.headings[0].overview).toBe('Tổng quan nội dung chương.');
  });

  it('should ưu tiên overview/word_count có sẵn, fallback description', () => {
    const res = OutlineSyncHelper.resolveHeadingsAndPercents([
      { heading: 'A', overview: 'Tóm tắt A', word_count: '500-800' },
      { heading: 'B', description: 'Mô tả B' },
    ]);

    expect(res.headings[0].overview).toBe('Tóm tắt A');
    expect(res.headings[0].word_count).toBe('500-800');
    expect(res.headings[1].overview).toBe('Mô tả B');
    expect(res.headings[1].word_count).toBe('1000-1500');
  });

  it('should giữ outlineCode khi khớp độ dài, ngược lại sinh chapter_i', () => {
    const match = OutlineSyncHelper.resolveHeadingsAndPercents(
      [{ title: 'A' }, { title: 'B' }],
      undefined,
      ['code_a', 'code_b'],
    );
    expect(match.outlineCode).toEqual(['code_a', 'code_b']);

    const mismatch = OutlineSyncHelper.resolveHeadingsAndPercents(
      [{ title: 'A' }, { title: 'B' }],
      undefined,
      ['only_one'],
    );
    expect(mismatch.outlineCode).toEqual(['chapter_1', 'chapter_2']);
  });

  it('should luôn trả percent là number cho mọi heading', () => {
    const res = OutlineSyncHelper.resolveHeadingsAndPercents(
      [{ title: 'A' }, { title: 'B', percent: 0 }, { title: 'C' }],
      [10, 20, 70],
    );

    res.headings.forEach((h: any) => expect(typeof h.percent).toBe('number'));
    expect(res.headings[1].percent).toBe(0);
  });
});
