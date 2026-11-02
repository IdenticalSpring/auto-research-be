import { jest } from '@jest/globals';
import { PrismaService } from '../../../database/prisma.service';
import { ReferenceSyncHelper } from './reference-sync.helper';

type AnyMock = Record<string, any>;

function buildPrisma(overrides: AnyMock = {}) {
  const prisma = {
    reference: {
      findFirst: jest.fn(async () => null) as any,
      create: jest.fn(async () => ({ id: 'ref-new' })) as any,
    },
    documentReference: { upsert: jest.fn(async () => ({})) },
    proposal: {
      findUnique: jest.fn(async () => null) as any,
      findFirst: jest.fn(async () => null) as any,
    },
    ...overrides,
  };
  return prisma;
}

const asPrisma = (prisma: AnyMock) => prisma as unknown as PrismaService;
const asDto = (dto: AnyMock) => dto as any;

describe('ReferenceSyncHelper.syncFoundPapers', () => {
  it('should bỏ qua paper không có title', async () => {
    const prisma = buildPrisma();

    await ReferenceSyncHelper.syncFoundPapers(asPrisma(prisma), 'doc-1', [
      { title: '' },
    ] as any);

    expect(prisma.reference.findFirst).not.toHaveBeenCalled();
  });

  it('should tìm reference theo url trước, rồi theo title', async () => {
    const prisma = buildPrisma();
    prisma.reference.findFirst
      .mockResolvedValueOnce(null)
      .mockResolvedValueOnce({ id: 'ref-by-title' });

    await ReferenceSyncHelper.syncFoundPapers(asPrisma(prisma), 'doc-1', [
      { title: 'Paper A', url: 'https://a' },
    ] as any);

    expect(prisma.reference.findFirst).toHaveBeenNthCalledWith(1, {
      where: { url: 'https://a' },
    });
    expect(prisma.reference.findFirst).toHaveBeenNthCalledWith(2, {
      where: { title: 'Paper A' },
    });
    expect(prisma.reference.create).not.toHaveBeenCalled();
    expect(prisma.documentReference.upsert).toHaveBeenCalledWith({
      where: {
        documentId_referenceId: {
          documentId: 'doc-1',
          referenceId: 'ref-by-title',
        },
      },
      create: { documentId: 'doc-1', referenceId: 'ref-by-title' },
      update: {},
    });
  });

  it('should tạo reference mới với dữ liệu đã chuẩn hoá', async () => {
    const prisma = buildPrisma();

    await ReferenceSyncHelper.syncFoundPapers(asPrisma(prisma), 'doc-1', [
      {
        title: 'Paper B',
        authors: ['A', 'B'],
        year: '2023',
        open_access: true,
        user_language_title: 'Bài B',
        q: 'Q1',
        url: 'https://b',
        citation: 12,
      },
    ] as any);

    expect(prisma.reference.create).toHaveBeenCalledWith({
      data: {
        title: 'Paper B',
        userLanguageTitle: 'Bài B',
        authors: 'A, B',
        journal: null,
        year: 2023,
        openAccess: true,
        q: 'Q1',
        url: 'https://b',
        citation: 12,
      },
    });
  });

  it('should xử lý authors string, year lỗi, citation không phải number', async () => {
    const prisma = buildPrisma();

    await ReferenceSyncHelper.syncFoundPapers(asPrisma(prisma), 'doc-1', [
      { title: 'C', authors: 'Solo', year: 'không phải số', citation: 'x' },
    ] as any);

    const arg = prisma.reference.create.mock.calls[0][0].data;
    expect(arg.authors).toBe('Solo');
    expect(arg.year).toBeNull();
    expect(arg.citation).toBe(0);
    expect(arg.openAccess).toBe(false);
  });

  it('should dùng year number và userLanguageTitle camelCase', async () => {
    const prisma = buildPrisma();

    await ReferenceSyncHelper.syncFoundPapers(asPrisma(prisma), 'doc-1', [
      { title: 'D', year: 2020, userLanguageTitle: 'Bài D', citation: 5 },
    ] as any);

    const arg = prisma.reference.create.mock.calls[0][0].data;
    expect(arg.year).toBe(2020);
    expect(arg.userLanguageTitle).toBe('Bài D');
    expect(arg.citation).toBe(5);
  });
});

describe('ReferenceSyncHelper.resolveProposal', () => {
  it('should ưu tiên dto.proposal và map snake_case', async () => {
    const prisma = buildPrisma();

    const res = await ReferenceSyncHelper.resolveProposal(
      asPrisma(prisma),
      asDto({
        documentId: 'doc-1',
        proposal: {
          title: 'T',
          problem_statement: 'P',
          motivation: 'M',
          research_gap: 'G',
        },
      }),
    );

    expect(res).toEqual({
      title: 'T',
      problem_statement: 'P',
      motivation: 'M',
      research_gap: 'G',
    });
    expect(prisma.proposal.findFirst).not.toHaveBeenCalled();
  });

  it('should fallback camelCase trong dto.proposal', async () => {
    const prisma = buildPrisma();

    const res = await ReferenceSyncHelper.resolveProposal(
      asPrisma(prisma),
      asDto({
        documentId: 'doc-1',
        proposal: { title: 'T', problemStatement: 'P', researchGap: 'G' },
      }),
    );

    expect(res?.problem_statement).toBe('P');
    expect(res?.research_gap).toBe('G');
    expect(res?.motivation).toBe('');
  });

  it('should tìm theo proposalId khi không có dto.proposal', async () => {
    const prisma = buildPrisma();
    prisma.proposal.findUnique.mockResolvedValue({
      title: 'T',
      problemStatement: 'P',
      motivation: null,
      researchGap: null,
    });

    const res = await ReferenceSyncHelper.resolveProposal(
      asPrisma(prisma),
      asDto({ documentId: 'doc-1', proposalId: 'p-1' }),
    );

    expect(prisma.proposal.findUnique).toHaveBeenCalledWith({
      where: { id: 'p-1' },
    });
    expect(res).toEqual({
      title: 'T',
      problem_statement: 'P',
      motivation: '',
      research_gap: '',
    });
  });

  it('should fallback proposal đang chọn khi không có proposalId', async () => {
    const prisma = buildPrisma();
    prisma.proposal.findFirst.mockResolvedValueOnce({
      title: 'Selected',
      problemStatement: null,
      motivation: null,
      researchGap: null,
    });

    const res = await ReferenceSyncHelper.resolveProposal(
      asPrisma(prisma),
      asDto({ documentId: 'doc-1' }),
    );

    expect(prisma.proposal.findFirst).toHaveBeenCalledWith({
      where: { documentId: 'doc-1', isSelected: true },
    });
    expect(res?.title).toBe('Selected');
  });

  it('should trả undefined khi không có proposal nào', async () => {
    const prisma = buildPrisma();

    const res = await ReferenceSyncHelper.resolveProposal(
      asPrisma(prisma),
      asDto({ documentId: 'doc-1' }),
    );

    expect(res).toBeUndefined();
  });
});
