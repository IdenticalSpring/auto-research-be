import { jest } from '@jest/globals';
import { PrismaService } from '../../../database/prisma.service';
import { OutlineSyncHelper } from './outline-sync.helper';

type AnyMock = Record<string, any>;

function buildPrisma(overrides: AnyMock = {}) {
  return {
    documentReference: { findMany: jest.fn(async () => []) },
    document: { findFirst: jest.fn(async () => null) },
    outline: {
      upsert: jest.fn(async () => ({})),
      findUnique: jest.fn(async () => null),
      update: jest.fn(async () => ({})),
    },
    ...overrides,
  };
}

const asPrisma = (prisma: AnyMock) => prisma as unknown as PrismaService;

describe('OutlineSyncHelper.loadProcessedDocs', () => {
  it('should lọc theo referenceIds khi có truyền vào', async () => {
    const prisma = buildPrisma();

    await OutlineSyncHelper.loadProcessedDocs(asPrisma(prisma), 'doc-1', [
      'r1',
      'r2',
    ]);

    expect(prisma.documentReference.findMany).toHaveBeenCalledWith({
      where: { documentId: 'doc-1', referenceId: { in: ['r1', 'r2'] } },
      include: { reference: true },
    });
  });

  it('should không thêm điều kiện referenceId khi mảng rỗng', async () => {
    const prisma = buildPrisma();

    await OutlineSyncHelper.loadProcessedDocs(asPrisma(prisma), 'doc-1', []);

    expect(prisma.documentReference.findMany).toHaveBeenCalledWith({
      where: { documentId: 'doc-1' },
      include: { reference: true },
    });
  });

  it('should trả reference mặc định lấy title của document khi không có refs', async () => {
    const prisma = buildPrisma();
    prisma.document.findFirst.mockResolvedValue({ id: 'doc-1', title: 'Đề tài X' });

    const res = await OutlineSyncHelper.loadProcessedDocs(
      asPrisma(prisma),
      'doc-1',
    );

    expect(prisma.document.findFirst).toHaveBeenCalledWith({
      where: { id: 'doc-1' },
    });
    expect(res).toHaveLength(1);
    expect(res[0]).toEqual(
      expect.objectContaining({
        id: 'ref-default',
        title: 'Đề tài X',
        user_language_title: 'Đề tài X',
        open_access: true,
        q: 'Q1',
      }),
    );
  });

  it('should dùng title mặc định khi document không tồn tại', async () => {
    const prisma = buildPrisma();

    const res = await OutlineSyncHelper.loadProcessedDocs(
      asPrisma(prisma),
      'doc-1',
    );

    expect(res[0].title).toBe('Tài liệu tham khảo cơ sở');
  });

  it('should chịu được prisma không có model document', async () => {
    const prisma = buildPrisma();
    delete prisma.document;

    const res = await OutlineSyncHelper.loadProcessedDocs(
      asPrisma(prisma),
      'doc-1',
    );

    expect(res[0].title).toBe('Tài liệu tham khảo cơ sở');
  });

  it('should map camelCase của reference sang snake_case', async () => {
    const prisma = buildPrisma();
    prisma.documentReference.findMany.mockResolvedValue([
      {
        reference: {
          id: 'r1',
          title: 'Paper 1',
          authors: 'A, B',
          journal: 'J',
          year: 2024,
          url: 'https://x',
          openAccess: true,
          q: 'Q2',
          abstract: 'Abs',
          userLanguageTitle: 'Bài 1',
        },
      },
    ]);

    const res = await OutlineSyncHelper.loadProcessedDocs(
      asPrisma(prisma),
      'doc-1',
      ['r1'],
    );

    expect(res[0]).toEqual({
      id: 'r1',
      title: 'Paper 1',
      authors: 'A, B',
      journal: 'J',
      year: 2024,
      url: 'https://x',
      open_access: true,
      q: 'Q2',
      abstract: 'Abs',
      user_language_title: 'Bài 1',
    });
  });
});

describe('OutlineSyncHelper.syncOutlineDb', () => {
  it('should ưu tiên data.outline và truyền meta vào cả update/create', async () => {
    const prisma = buildPrisma();
    const structure = { outline: [{ title: 'A' }] };

    await OutlineSyncHelper.syncOutlineDb(
      asPrisma(prisma),
      'doc-1',
      { outline: structure, total_word_count: 5000 },
      {
        headings: [{ heading: 'A', title: 'A', percent: 100 }],
        headingsPercent: [100],
        outlineCode: ['chapter_1'],
        wordCountStr: '5000 words',
      },
    );

    const arg = prisma.outline.upsert.mock.calls[0][0] as Record<string, any>;
    expect(arg.where).toEqual({ documentId: 'doc-1' });
    expect(arg.update.structure).toEqual(structure);
    expect(arg.update.totalWordCount).toBe(5000);
    expect(arg.update.wordCountStr).toBe('5000 words');
    expect(arg.update.headings).toEqual([
      { heading: 'A', title: 'A', percent: 100 },
    ]);
    expect(arg.update.headingsPercent).toEqual([100]);
    expect(arg.update.outlineCode).toEqual(['chapter_1']);
    expect(arg.create.documentId).toBe('doc-1');
  });

  it('should fallback structure theo thứ tự structure > new_outline > data', async () => {
    const prisma = buildPrisma();

    await OutlineSyncHelper.syncOutlineDb(asPrisma(prisma), 'd1', {
      structure: 'from-structure',
    });
    await OutlineSyncHelper.syncOutlineDb(asPrisma(prisma), 'd2', {
      new_outline: 'from-new-outline',
    });
    await OutlineSyncHelper.syncOutlineDb(asPrisma(prisma), 'd3', {
      foo: 'raw-data',
    });

    const calls = prisma.outline.upsert.mock.calls as any[];
    expect(calls[0][0].update.structure).toBe('from-structure');
    expect(calls[1][0].update.structure).toBe('from-new-outline');
    expect(calls[2][0].update.structure).toEqual({ foo: 'raw-data' });
  });

  it('should default word count 8000 và không ghi meta khi thiếu', async () => {
    const prisma = buildPrisma();

    await OutlineSyncHelper.syncOutlineDb(asPrisma(prisma), 'doc-1', {});

    const arg = prisma.outline.upsert.mock.calls[0][0] as Record<string, any>;
    expect(arg.update.totalWordCount).toBe(8000);
    expect(arg.update.wordCountStr).toBe('8000 words');
    expect(arg.update.headings).toBeUndefined();
    expect(arg.update.headingsPercent).toBeUndefined();
    expect(arg.update.outlineCode).toBeUndefined();
  });

  it('should đọc totalWordCount camelCase và word_count_str của data', async () => {
    const prisma = buildPrisma();

    await OutlineSyncHelper.syncOutlineDb(asPrisma(prisma), 'doc-1', {
      totalWordCount: 1234,
      word_count_str: '1234 từ',
    });

    const arg = prisma.outline.upsert.mock.calls[0][0] as Record<string, any>;
    expect(arg.update.totalWordCount).toBe(1234);
    expect(arg.update.wordCountStr).toBe('1234 từ');
  });
});

describe('OutlineSyncHelper.applyHeadingRefs', () => {
  it('should bỏ qua khi outline/structure không tồn tại', async () => {
    const prisma = buildPrisma();

    await OutlineSyncHelper.applyHeadingRefs(asPrisma(prisma), 'doc-1', [
      { heading_id: 'h1', references: ['r1'] },
    ]);

    expect(prisma.outline.update).not.toHaveBeenCalled();
  });

  it('should bỏ qua khi structure là JSON hỏng', async () => {
    const prisma = buildPrisma();
    prisma.outline.findUnique.mockResolvedValue({ structure: '{broken' });

    await OutlineSyncHelper.applyHeadingRefs(asPrisma(prisma), 'doc-1', [
      { heading_id: 'h1', references: ['r1'] },
    ]);

    expect(prisma.outline.update).not.toHaveBeenCalled();
  });

  it('should parse structure dạng string và gán refs theo id/code/title', async () => {
    const prisma = buildPrisma();
    prisma.outline.findUnique.mockResolvedValue({
      structure: JSON.stringify({
        outline: [
          { id: 'h1', references: [] },
          { code: 'h2', references: [] },
          { title: 'h3', references: [] },
        ],
      }),
    });

    await OutlineSyncHelper.applyHeadingRefs(asPrisma(prisma), 'doc-1', [
      { heading_id: 'h1', documents: ['r1'] },
      { headingId: 'h2', references: ['r2'] },
      { id: 'h3', references: ['r3'] },
    ]);

    const arg = prisma.outline.update.mock.calls[0][0] as Record<string, any>;
    expect(arg.where).toEqual({ documentId: 'doc-1' });
    expect(arg.data.structure.outline[0].references).toEqual(['r1']);
    expect(arg.data.structure.outline[1].references).toEqual(['r2']);
    expect(arg.data.structure.outline[2].references).toEqual(['r3']);
  });

  it('should hỗ trợ structure là array trực tiếp', async () => {
    const prisma = buildPrisma();
    prisma.outline.findUnique.mockResolvedValue({
      structure: [{ id: 'h1', references: [] }],
    });

    await OutlineSyncHelper.applyHeadingRefs(asPrisma(prisma), 'doc-1', [
      { heading_id: 'h1', references: ['r1'] },
    ]);

    expect(prisma.outline.update).toHaveBeenCalled();
  });

  it('should không update khi structure không chứa outline array', async () => {
    const prisma = buildPrisma();
    prisma.outline.findUnique.mockResolvedValue({ structure: { foo: 'bar' } });

    await OutlineSyncHelper.applyHeadingRefs(asPrisma(prisma), 'doc-1', [
      { heading_id: 'h1', references: ['r1'] },
    ]);

    expect(prisma.outline.update).not.toHaveBeenCalled();
  });

  it('should bỏ qua mapping thiếu heading id và mặc định refs = []', async () => {
    const prisma = buildPrisma();
    prisma.outline.findUnique.mockResolvedValue({
      structure: { outline: [{ id: 'h1', references: ['old'] }] },
    });

    await OutlineSyncHelper.applyHeadingRefs(asPrisma(prisma), 'doc-1', [
      { references: ['no-id'] },
      { heading_id: 'h1' },
    ]);

    const arg = prisma.outline.update.mock.calls[0][0] as Record<string, any>;
    expect(arg.data.structure.outline[0].references).toEqual([]);
  });
});

