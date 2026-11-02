import { jest } from '@jest/globals';
import { BadRequestException, NotFoundException } from '@nestjs/common';
import { JobStatus } from '@prisma/client';
import { PrismaService } from '../../database/prisma.service';
import { EnhancementAiHelper } from './enhancement-ai.helper';

type AnyMock = Record<string, any>;
const asPrisma = (p: AnyMock) => p as unknown as PrismaService;

function buildPrisma(overrides: AnyMock = {}) {
  return {
    document: { findUnique: jest.fn(async () => ({ id: 'doc-1' })) as any },
    proposal: { findFirst: jest.fn(async () => null) as any },
    enhancementSuggestion: {
      create: jest.fn(async () => ({ id: 'sug-1' })) as any,
    },
    job: {
      create: jest.fn(async () => ({ id: 'job-1' })) as any,
      update: jest.fn(async () => ({})) as any,
    },
    ...overrides,
  };
}

describe('EnhancementAiHelper.ensureOwnedDocument', () => {
  it('should trả document khi thuộc user', async () => {
    const prisma = buildPrisma();

    await expect(
      EnhancementAiHelper.ensureOwnedDocument(asPrisma(prisma), 'u-1', 'doc-1'),
    ).resolves.toEqual({ id: 'doc-1' });
  });

  it('should throw NotFound khi không tìm thấy', async () => {
    const prisma = buildPrisma();
    prisma.document.findUnique.mockResolvedValue(null);

    await expect(
      EnhancementAiHelper.ensureOwnedDocument(asPrisma(prisma), 'u-1', 'doc-1'),
    ).rejects.toThrow(NotFoundException);
  });
});

describe('EnhancementAiHelper.resolveProposal', () => {
  it('should dùng override khi có dữ liệu', async () => {
    const prisma = buildPrisma();

    const res = await EnhancementAiHelper.resolveProposal(
      asPrisma(prisma),
      'doc-1',
      { title: 'Override' },
    );

    expect(res).toEqual({ title: 'Override' });
    expect(prisma.proposal.findFirst).not.toHaveBeenCalled();
  });

  it('should bỏ qua override rỗng và lấy proposal đang chọn', async () => {
    const prisma = buildPrisma();
    prisma.proposal.findFirst.mockResolvedValueOnce({
      title: 'Selected',
      problemStatement: 'P',
    });

    const res = await EnhancementAiHelper.resolveProposal(
      asPrisma(prisma),
      'doc-1',
      {},
    );

    expect(res).toEqual({ title: 'Selected', problem_statement: 'P' });
    expect(prisma.proposal.findFirst).toHaveBeenCalledTimes(1);
  });

  it('should fallback proposal bất kỳ khi không có proposal chọn', async () => {
    const prisma = buildPrisma();
    prisma.proposal.findFirst
      .mockResolvedValueOnce(null)
      .mockResolvedValueOnce({ title: 'Any' });

    const res = await EnhancementAiHelper.resolveProposal(
      asPrisma(prisma),
      'doc-1',
    );

    expect(prisma.proposal.findFirst).toHaveBeenCalledTimes(2);
    expect(res.title).toBe('Any');
  });

  it('should trả chuỗi rỗng khi không có proposal', async () => {
    const prisma = buildPrisma();

    const res = await EnhancementAiHelper.resolveProposal(
      asPrisma(prisma),
      'doc-1',
    );

    expect(res).toEqual({ title: '', problem_statement: '' });
  });
});

describe('EnhancementAiHelper.saveSuggestions', () => {
  it('should trả [] khi rawList không phải array', async () => {
    const prisma = buildPrisma();

    await expect(
      EnhancementAiHelper.saveSuggestions(asPrisma(prisma), 's-1', undefined),
    ).resolves.toEqual([]);
    expect(prisma.enhancementSuggestion.create).not.toHaveBeenCalled();
  });

  it('should map suggestion/reason và fallback field khác', async () => {
    const prisma = buildPrisma();

    const res = await EnhancementAiHelper.saveSuggestions(
      asPrisma(prisma),
      's-1',
      [
        { suggestion: 'Text 1', reason: 'Vì lý do' },
        { suggestedText: 'Text 2', explanation: 'Giải thích' },
        { original: 'Text 3' },
      ],
    );

    expect(res).toHaveLength(3);
    const first = prisma.enhancementSuggestion.create.mock.calls[0][0].data;
    expect(first).toEqual({
      sessionId: 's-1',
      suggestedText: 'Text 1',
      explanation: 'Vì lý do',
      status: 'pending',
    });
    const third = prisma.enhancementSuggestion.create.mock.calls[2][0].data;
    expect(third.suggestedText).toBe('Text 3');
    expect(third.explanation).toBe('');
  });
});

describe('EnhancementAiHelper.executeAiFlow', () => {
  function buildDeps(resolved: Record<string, unknown> = { ok: true }) {
    const prisma = buildPrisma();
    const producer = { publish: jest.fn(async () => undefined) as any };
    const correlation = {
      buildCorrelationKey: jest.fn(() => 'key-1') as any,
      registerPending: jest.fn(async () => resolved) as any,
      clearPending: jest.fn() as any,
    };
    return { prisma, producer, correlation };
  }

  const meta = {
    userId: 'u-1',
    documentId: 'doc-1',
    sessionId: 's-1',
    eventType: 'enhance',
    payloadDto: { a: 1 },
  };

  it('should tạo job, publish kèm job_id, set processing rồi trả kết quả', async () => {
    const { prisma, producer, correlation } = buildDeps();

    const res = await EnhancementAiHelper.executeAiFlow(
      asPrisma(prisma),
      producer,
      correlation,
      'enhancement_request',
      meta,
      { event_type: 'enhance' },
    );

    expect(res.jobId).toBe('job-1');
    expect(res.res).toEqual({ ok: true });
    expect(producer.publish).toHaveBeenCalledWith('enhancement_request', {
      event_type: 'enhance',
      job_id: 'job-1',
    });
    expect(prisma.job.update).toHaveBeenCalledWith({
      where: { id: 'job-1' },
      data: { status: JobStatus.processing },
    });
  });

  it('should clearPending, set job error và ném BadRequest khi lỗi', async () => {
    const { prisma, producer, correlation } = buildDeps();
    correlation.registerPending.mockRejectedValue({ error: 'AI 637' });

    await expect(
      EnhancementAiHelper.executeAiFlow(
        asPrisma(prisma),
        producer,
        correlation,
        'enhancement_request',
        meta,
        {},
      ),
    ).rejects.toThrow(BadRequestException);

    expect(correlation.clearPending).toHaveBeenCalledWith('key-1', undefined);
    expect(prisma.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          status: JobStatus.error,
          errorCode: '603',
        }),
      }),
    );
  });

  it('should fallback err.message khi lỗi không có field error', async () => {
    const { prisma, producer, correlation } = buildDeps();
    correlation.registerPending.mockRejectedValue(new Error('plain error'));

    await expect(
      EnhancementAiHelper.executeAiFlow(
        asPrisma(prisma),
        producer,
        correlation,
        'enhancement_request',
        meta,
        {},
      ),
    ).rejects.toThrow('plain error');
  });

  it('should không làm hỏng flow khi cập nhật job error thất bại', async () => {
    const { prisma, producer, correlation } = buildDeps();
    correlation.registerPending.mockRejectedValue(new Error('boom'));
    // Lần 1 (processing) thành công để pendingPromise được await; lần 2 (error) reject.
    prisma.job.update
      .mockResolvedValueOnce({})
      .mockRejectedValue(new Error('db down'));

    await expect(
      EnhancementAiHelper.executeAiFlow(
        asPrisma(prisma),
        producer,
        correlation,
        'enhancement_request',
        meta,
        {},
      ),
    ).rejects.toThrow(BadRequestException);
  });
});

describe('EnhancementAiHelper.finishJob', () => {
  it('should cập nhật job finished với result', async () => {
    const prisma = buildPrisma();

    await EnhancementAiHelper.finishJob(asPrisma(prisma), 'job-1', {
      improved_text: 'x',
    });

    expect(prisma.job.update).toHaveBeenCalledWith({
      where: { id: 'job-1' },
      data: { status: JobStatus.finished, result: { improved_text: 'x' } },
    });
  });
});

