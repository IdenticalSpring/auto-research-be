import { jest } from '@jest/globals';
import { PrismaService } from '../../../database/prisma.service';
import { WriteContentHelper } from './write-content.helper';

type AnyMock = Record<string, any>;
const asPrisma = (p: AnyMock) => p as unknown as PrismaService;
const asDto = (dto: AnyMock) => dto as any;

function buildPrisma(overrides: AnyMock = {}) {
  return {
    proposal: { findFirst: jest.fn(async () => null) as any },
    outline: { findUnique: jest.fn(async () => null) as any },
    documentReference: { findMany: jest.fn(async () => []) as any },
    ...overrides,
  };
}

describe('WriteContentHelper.resolveWritePayload — outline', () => {
  it('should dùng dto.outline và normalize chapter/section', async () => {
    const prisma = buildPrisma();

    const res = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma),
      asDto({
        documentId: 'doc-1',
        finalProposal: { title: 'T' },
        outline: {
          title: 'Outline title',
          outline: [
            {
              chapter: 'chapter_1',
              name: 'Chương 1',
              sections: [
                { subheading: 'Tiểu mục', description: 'Mô tả', refs: ['r1'] },
              ],
            },
          ],
        },
        outlineCode: ['chapter_1'],
      }),
      {},
    );

    const outline = res.outline as AnyMock;
    expect(outline.title).toBe('Outline title');
    expect(outline.outline).toHaveLength(1);
    expect(outline.headings).toEqual(outline.outline);
    const chapter = outline.outline[0];
    expect(chapter).toEqual(
      expect.objectContaining({
        code: 'chapter_1',
        chapter: 'chapter_1',
        heading: 'Chương 1',
        name: 'Chương 1',
        overview: 'Tổng quan nội dung chương.',
        word_count: '1000-1500',
      }),
    );
    expect(chapter.sections[0]).toEqual(
      expect.objectContaining({
        section_code: '1.1',
        subheading: 'Tiểu mục',
        description: 'Mô tả',
        refs: ['r1'],
      }),
    );
    expect(res.outlineCode).toEqual(['chapter_1']);
  });

  it('should dùng outline trong DB và outlineCode của DB khi dto thiếu', async () => {
    const prisma = buildPrisma();
    prisma.outline.findUnique.mockResolvedValue({
      structure: {
        title: 'DB outline',
        headings: [{ chapter: 'chapter_1', title: 'Chương 1' }],
      },
      outlineCode: ['chapter_1'],
    });

    const res = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma),
      asDto({ documentId: 'doc-1', finalProposal: { title: 'T' } }),
      {},
    );

    const outline = res.outline as AnyMock;
    expect(outline.title).toBe('DB outline');
    expect(outline.outline[0].sections[0].subheading).toContain('Tổng quan');
    expect(res.outlineCode).toEqual(['chapter_1']);
  });

  it('should sinh 5 chương mặc định khi không có outline', async () => {
    const prisma = buildPrisma();

    const res = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma),
      asDto({ documentId: 'doc-1', finalProposal: { title: 'T' } }),
      { title: 'Doc' },
    );

    const outline = res.outline as AnyMock;
    expect(outline.outline).toHaveLength(5);
    expect(outline.title).toBe('Doc');
    expect(outline.outline[0].chapter).toBe('chapter_1');
  });

  it('should map outlineCode dạng số và dạng lạ', async () => {
    const prisma = buildPrisma();
    const outline = {
      outline: [
        { chapter: 'chapter_1', title: 'A' },
        { chapter: 'chapter_2', title: 'B' },
      ],
    };

    const numeric = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma),
      asDto({
        documentId: 'doc-1',
        finalProposal: {},
        outline,
        outlineCode: ['2'],
      }),
      {},
    );
    expect(numeric.outlineCode).toEqual(['chapter_2']);

    const unknown = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma),
      asDto({
        documentId: 'doc-1',
        finalProposal: {},
        outline,
        outlineCode: ['xx9'],
      }),
      {},
    );
    expect(unknown.outlineCode).toEqual(['chapter_9']);
  });

  it('should hỗ trợ outlineCode = "all" và string đơn', async () => {
    const prisma = buildPrisma();
    const outline = {
      outline: [
        { chapter: 'chapter_1', title: 'A' },
        { chapter: 'chapter_2', title: 'B' },
      ],
    };

    const all = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma),
      asDto({
        documentId: 'doc-1',
        finalProposal: {},
        outline,
        outlineCode: 'ALL',
      }),
      {},
    );
    expect(all.outlineCode).toEqual(['chapter_1', 'chapter_2']);

    const single = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma),
      asDto({
        documentId: 'doc-1',
        finalProposal: {},
        outline,
        outlineCode: 'chapter_1',
      }),
      {},
    );
    expect(single.outlineCode).toEqual(['chapter_1']);
  });

  it('should đọc subheadings và structure.headings', async () => {
    const prisma = buildPrisma();

    const viaSubheadings = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma),
      asDto({
        documentId: 'doc-1',
        finalProposal: {},
        outline: {
          outline: [
            { chapter: 'chapter_1', title: 'A', subheadings: [{ name: 'S' }] },
          ],
        },
      }),
      {},
    );
    expect((viaSubheadings.outline as AnyMock).outline[0].sections[0].name).toBe(
      'S',
    );

    const viaStructure = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma),
      asDto({
        documentId: 'doc-1',
        finalProposal: {},
        outline: {
          structure: { headings: [{ chapter: 'chapter_1', title: 'A' }] },
        },
      }),
      {},
    );
    expect((viaStructure.outline as AnyMock).outline).toHaveLength(1);
  });
});
