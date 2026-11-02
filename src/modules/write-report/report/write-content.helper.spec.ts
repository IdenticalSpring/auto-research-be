import { jest } from '@jest/globals';
import { BadRequestException } from '@nestjs/common';
import { JobStatus } from '@prisma/client';
import { PrismaService } from '../../../database/prisma.service';
import { WriteContentHelper } from './write-content.helper';

describe('WriteContentHelper', () => {
  describe('extractMarkdown', () => {
    it('should accumulate section objects and return combined markdown', () => {
      const sectionsMap = new Map<string, string>();
      const chunk1 = {
        section: { name: 'Chương 1', content: 'Nội dung chương 1' },
      };
      const res1 = WriteContentHelper.extractMarkdown(sectionsMap, chunk1);
      expect(res1).toBe('## Chương 1\n\nNội dung chương 1');

      const chunk2 = {
        section: { name: 'Chương 2', content: 'Nội dung chương 2' },
      };
      const res2 = WriteContentHelper.extractMarkdown(sectionsMap, chunk2);
      expect(res2).toBe(
        '## Chương 1\n\nNội dung chương 1\n\n## Chương 2\n\nNội dung chương 2',
      );
    });

    it('should extract content from content or full_content string', () => {
      const sectionsMap = new Map<string, string>();
      expect(
        WriteContentHelper.extractMarkdown(sectionsMap, {
          content: 'Direct content',
        }),
      ).toBe('Direct content');
      expect(
        WriteContentHelper.extractMarkdown(sectionsMap, {
          full_content: 'Full text',
        }),
      ).toBe('Full text');
      expect(WriteContentHelper.extractMarkdown(sectionsMap, {})).toBeNull();
    });
  });

  describe('failJob', () => {
    it('should update job status to error and throw BadRequestException', async () => {
      const mockPrisma = {
        job: { update: jest.fn<any>().mockResolvedValue({}) },
      };

      await expect(
        WriteContentHelper.failJob(
          mockPrisma as unknown as PrismaService,
          'j1',
          630,
          'Write content failed',
        ),
      ).rejects.toThrow(BadRequestException);

      expect(mockPrisma.job.update).toHaveBeenCalledWith({
        where: { id: 'j1' },
        data: {
          status: JobStatus.error,
          errorCode: '630',
          errorMessage: 'Write content failed',
        },
      });
    });
  });
});

describe('WriteContentHelper.extractMarkdown — nhánh biên', () => {
  it('should bỏ qua section không có content và trả null khi map rỗng', () => {
    const sectionsMap = new Map<string, string>();

    const res = WriteContentHelper.extractMarkdown(sectionsMap, {
      section: { name: 'Chương 1', content: '   ' },
    });

    expect(res).toBeNull();
  });

  it('should không thêm tiêu đề khi content đã bắt đầu bằng "#"', () => {
    const sectionsMap = new Map<string, string>();

    const res = WriteContentHelper.extractMarkdown(sectionsMap, {
      section: { section_code: '1.1', content: '# Tiêu đề sẵn' },
    });

    expect(res).toBe('# Tiêu đề sẵn');
  });

  it('should dùng "Phần" khi section thiếu name và section_code', () => {
    const sectionsMap = new Map<string, string>();

    const res = WriteContentHelper.extractMarkdown(sectionsMap, {
      section: { content: 'Nội dung' },
    });

    expect(res).toBe('## Phần\n\nNội dung');
  });
});

describe('WriteContentHelper.handleChunk', () => {
  function buildPrisma() {
    return {
      report: { upsert: jest.fn(async () => ({})) as any },
      job: { update: jest.fn(async () => ({})) as any },
    };
  }
  type MockPrisma = ReturnType<typeof buildPrisma>;
  const asPrisma = (p: MockPrisma) => p as unknown as PrismaService;
  let prisma: MockPrisma;

  beforeEach(() => {
    prisma = buildPrisma();
  });

  it('should upsert report khi có markdown', async () => {
    const res = await WriteContentHelper.handleChunk(
      asPrisma(prisma),
      'job-1',
      { content: 'Nội dung', thread_id: 't-1' },
      new Map(),
      'doc-1',
    );

    expect(prisma.report.upsert).toHaveBeenCalledWith({
      where: { documentId: 'doc-1' },
      update: { contentMarkdown: 'Nội dung' },
      create: { documentId: 'doc-1', contentMarkdown: 'Nội dung' },
    });
    expect(res).toEqual({ inProgress: true });
  });

  it('should suspend ở chapter_break / __interrupt__', async () => {
    const res = await WriteContentHelper.handleChunk(
      asPrisma(prisma),
      'job-1',
      { chapter_break: true, thread_id: 't-1' },
      new Map(),
      'doc-1',
    );

    expect(res).toEqual({ isSuspended: true, reason: 'chapter_break' });
    expect(prisma.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          status: JobStatus.suspended,
          resumeStep: 'chapter_break',
          threadId: 't-1',
        }),
      }),
    );

    const interrupt = await WriteContentHelper.handleChunk(
      asPrisma(prisma),
      'job-2',
      { __interrupt__: true },
      new Map(),
      'doc-1',
    );
    expect(interrupt).toEqual({ isSuspended: true, reason: 'chapter_break' });
  });

  it('should suspend ở need_data_analyze', async () => {
    const res = await WriteContentHelper.handleChunk(
      asPrisma(prisma),
      'job-1',
      { need_data_analyze: true },
      new Map(),
      'doc-1',
    );

    expect(res).toEqual({ isSuspended: true, reason: 'need_data_analyze' });
  });

  it('should suspend ở need_propose_method', async () => {
    const res = await WriteContentHelper.handleChunk(
      asPrisma(prisma),
      'job-1',
      { need_propose_method: true },
      new Map(),
      'doc-1',
    );

    expect(res).toEqual({ isSuspended: true, reason: 'need_propose_method' });
  });

  it('should finish khi has_finished=true', async () => {
    const res = await WriteContentHelper.handleChunk(
      asPrisma(prisma),
      'job-1',
      { has_finished: true },
      new Map(),
      'doc-1',
    );

    expect(res).toEqual({ isFinished: true });
    expect(prisma.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({ status: JobStatus.finished }),
      }),
    );
  });
});
