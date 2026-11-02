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

describe('WriteContentHelper.resolveWritePayload — finalProposal', () => {
  it('should dùng dto.finalProposal khi có', async () => {
    const prisma = buildPrisma();

    const res = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma),
      asDto({ documentId: 'doc-1', finalProposal: { title: 'T' } }),
      { title: 'Doc' },
    );

    expect(res.finalProposal).toEqual({ title: 'T' });
    expect(prisma.proposal.findFirst).not.toHaveBeenCalled();
  });

  it('should lấy proposal đang chọn khi dto không truyền', async () => {
    const prisma = buildPrisma();
    prisma.proposal.findFirst.mockResolvedValueOnce({
      title: 'Selected',
      problemStatement: 'P',
      motivation: 'M',
      researchGap: 'G',
      finalModel: { m: 1 },
    });

    const res = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma),
      asDto({ documentId: 'doc-1' }),
      {},
    );

    expect(res.finalProposal).toEqual({
      title: 'Selected',
      problem_statement: 'P',
      motivation: 'M',
      research_gap: 'G',
      final_model: { m: 1 },
    });
  });

  it('should fallback proposal bất kỳ rồi tới title của document', async () => {
    const prisma = buildPrisma();
    prisma.proposal.findFirst
      .mockResolvedValueOnce(null)
      .mockResolvedValueOnce({ title: 'Any' });

    const anyRes = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma),
      asDto({ documentId: 'doc-1' }),
      {},
    );
    expect((anyRes.finalProposal as AnyMock).title).toBe('Any');

    const prisma2 = buildPrisma();
    const docRes = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma2),
      asDto({ documentId: 'doc-1' }),
      { title: 'Doc title' },
    );
    expect(docRes.finalProposal).toEqual({ title: 'Doc title' });

    const prisma3 = buildPrisma();
    const defaultRes = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma3),
      asDto({ documentId: 'doc-1' }),
      {},
    );
    expect((defaultRes.finalProposal as AnyMock).title).toBe('Research Report');
  });

  it('should merge practical_data vào finalProposal', async () => {
    const prisma = buildPrisma();

    const res = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma),
      asDto({
        documentId: 'doc-1',
        finalProposal: { title: 'T' },
        practicalData: { survey: 1 },
      }),
      {},
    );

    expect(res.finalProposal).toEqual({
      title: 'T',
      practical_data: { survey: 1 },
    });
  });
});

describe('WriteContentHelper.resolveWritePayload — papers/field/domain', () => {
  it('should lấy papers từ documentReference khi dto không có', async () => {
    const prisma = buildPrisma();
    prisma.documentReference.findMany.mockResolvedValue([
      {
        reference: {
          id: 'r1',
          title: 'Paper',
          authors: 'A',
          journal: 'J',
          year: 2024,
          url: 'https://x',
        },
      },
    ]);

    const res = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma),
      asDto({ documentId: 'doc-1', finalProposal: {} }),
      {},
    );

    expect(prisma.documentReference.findMany).toHaveBeenCalledWith({
      where: { documentId: 'doc-1' },
      include: { reference: true },
      take: 30,
    });
    expect(res.papersSearch).toEqual([
      {
        id: 'r1',
        title: 'Paper',
        authors: 'A',
        journal: 'J',
        year: 2024,
        url: 'https://x',
      },
    ]);
  });

  it('should giữ dto.papersSearch khi có', async () => {
    const prisma = buildPrisma();

    const res = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma),
      asDto({
        documentId: 'doc-1',
        finalProposal: {},
        papersSearch: [{ title: 'x' }],
      }),
      {},
    );

    expect(res.papersSearch).toEqual([{ title: 'x' }]);
    expect(prisma.documentReference.findMany).not.toHaveBeenCalled();
  });

  it('should resolve field/domain theo dto → doc → default', async () => {
    const prisma = buildPrisma();

    const fromDto = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma),
      asDto({ documentId: 'd', finalProposal: {}, field: 'F', domain: 'D' }),
      { field: 'docF', domains: ['docD'] },
    );
    expect(fromDto.field).toBe('F');
    expect(fromDto.domain).toBe('D');

    const fromDoc = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma),
      asDto({ documentId: 'd', finalProposal: {} }),
      { field: 'docF', domains: ['docD'] },
    );
    expect(fromDoc.field).toBe('docF');
    expect(fromDoc.domain).toBe('docD');

    const stringDomain = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma),
      asDto({ documentId: 'd', finalProposal: {} }),
      { domains: 'stringD' },
    );
    expect(stringDomain.domain).toBe('stringD');

    const fallback = await WriteContentHelper.resolveWritePayload(
      asPrisma(prisma),
      asDto({ documentId: 'd', finalProposal: {} }),
      {},
    );
    expect(fallback.field).toBe('General');
    expect(fallback.domain).toBe('General');
  });
});

