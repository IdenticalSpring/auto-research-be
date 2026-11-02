import { jest } from '@jest/globals';
import { Test, TestingModule } from '@nestjs/testing';
import { JobFallbackService } from './job-fallback.service';
import { PrismaService } from '../../../database/prisma.service';

describe('JobFallbackService', () => {
  let service: JobFallbackService;
  let prismaMock: Record<string, any>;
  const originalNodeEnv = process.env.NODE_ENV;

  const buildJob = (module: string, action: string, extra: any = {}) => ({
    id: 'job-1',
    module,
    action,
    documentId: 'doc-1',
    payload: { field: 'Finance', ...(extra.payload || {}) },
    ...extra,
  });

  beforeEach(async () => {
    prismaMock = {
      job: {
        update: jest.fn((args: any) =>
          Promise.resolve({ id: args.where.id, status: args.data.status }),
        ),
      },
      document: {
        update: jest.fn(() => Promise.resolve({ id: 'doc-1' })),
        findUnique: jest.fn(() => Promise.resolve({ domains: [] })),
      },
      proposal: {
        create: jest.fn((args: any) =>
          Promise.resolve({ id: `prop-${args.data.title}`, ...args.data }),
        ),
      },
      documentKeyword: {
        findFirst: jest.fn(() => Promise.resolve(null)),
        create: jest.fn(() => Promise.resolve({ id: 'kw-1' })),
        update: jest.fn(() => Promise.resolve({ id: 'kw-1' })),
      },
      reference: {
        create: jest.fn((args: any) =>
          Promise.resolve({ id: 'ref-1', ...args.data }),
        ),
      },
      outline: {
        findFirst: jest.fn(() => Promise.resolve(null)),
        create: jest.fn(() => Promise.resolve({ id: 'outline-1' })),
        update: jest.fn(() => Promise.resolve({ id: 'outline-1' })),
      },
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        JobFallbackService,
        { provide: PrismaService, useValue: prismaMock },
      ],
    }).compile();

    service = module.get<JobFallbackService>(JobFallbackService);
  });

  afterEach(() => {
    process.env.NODE_ENV = originalNodeEnv;
    jest.clearAllMocks();
  });

  it('ném lỗi và KHÔNG ghi DB khi NODE_ENV=production', async () => {
    process.env.NODE_ENV = 'production';

    await expect(
      service.processJobFallback({ id: 'job-1', module: 'unknown_action' }),
    ).rejects.toThrow(/bị chặn ở production/);

    expect(prismaMock.job.update).not.toHaveBeenCalled();
  });

  it('vẫn chạy fallback ở môi trường dev/test', async () => {
    process.env.NODE_ENV = 'test';

    const result = await service.processJobFallback({
      id: 'job-1',
      module: 'chatbot',
      action: 'outer_chatbot',
    });

    expect(prismaMock.job.update).toHaveBeenCalledTimes(1);
    expect(result).toEqual({ id: 'job-1', status: 'finished' });
  });

  describe('document_setup actions', () => {
    beforeEach(() => {
      process.env.NODE_ENV = 'test';
    });

    it('generate_domains: cập nhật document.domains', async () => {
      const result = await service.processJobFallback(
        buildJob('document_setup', 'generate_domains'),
      );

      expect(prismaMock.document.update).toHaveBeenCalledWith({
        where: { id: 'doc-1' },
        data: { domains: expect.any(Array) },
      });
      expect((result as any).status).toBe('finished');
    });

    it('generate_domains: không update khi không có documentId', async () => {
      await service.processJobFallback(
        buildJob('document_setup', 'generate_domains', { documentId: null }),
      );

      expect(prismaMock.document.update).not.toHaveBeenCalled();
    });

    it('generate_subdomains: đọc domains hiện tại rồi cập nhật', async () => {
      prismaMock.document.findUnique.mockResolvedValueOnce({
        domains: [{ id: 'd1', subdomains: ['A'] }],
      });

      await service.processJobFallback(
        buildJob('document_setup', 'generate_subdomains'),
      );

      expect(prismaMock.document.findUnique).toHaveBeenCalledWith({
        where: { id: 'doc-1' },
      });
      expect(prismaMock.document.update).toHaveBeenCalled();
    });

    it('generate_titles: tạo proposal cho từng title', async () => {
      const result = await service.processJobFallback(
        buildJob('document_setup', 'generate_titles'),
      );

      expect(prismaMock.proposal.create).toHaveBeenCalled();
      expect((result as any).status).toBe('finished');
    });

    it('generate_titles: không tạo proposal khi thiếu documentId', async () => {
      await service.processJobFallback(
        buildJob('document_setup', 'generate_titles', { documentId: null }),
      );

      expect(prismaMock.proposal.create).not.toHaveBeenCalled();
    });

    it('generate_keywords: tạo mới khi chưa có bản ghi', async () => {
      await service.processJobFallback(
        buildJob('document_setup', 'generate_keywords'),
      );

      expect(prismaMock.documentKeyword.create).toHaveBeenCalled();
      expect(prismaMock.documentKeyword.update).not.toHaveBeenCalled();
    });

    it('generate_keywords: cập nhật khi đã có bản ghi', async () => {
      prismaMock.documentKeyword.findFirst.mockResolvedValueOnce({
        id: 'kw-9',
      });

      await service.processJobFallback(
        buildJob('document_setup', 'generate_keywords'),
      );

      expect(prismaMock.documentKeyword.update).toHaveBeenCalledWith({
        where: { id: 'kw-9' },
        data: expect.any(Object),
      });
    });

    it('search_papers: tạo reference cho từng paper', async () => {
      const result = await service.processJobFallback(
        buildJob('document_setup', 'search_papers'),
      );

      expect(prismaMock.reference.create).toHaveBeenCalled();
      expect((result as any).status).toBe('finished');
    });

    it('generate_outline: tạo mới khi chưa có outline', async () => {
      await service.processJobFallback(
        buildJob('document_setup', 'generate_outline'),
      );

      expect(prismaMock.outline.create).toHaveBeenCalled();
      expect(prismaMock.outline.update).not.toHaveBeenCalled();
    });

    it('modify_outline: cập nhật outline đã có', async () => {
      prismaMock.outline.findFirst.mockResolvedValueOnce({ id: 'outline-9' });

      await service.processJobFallback(
        buildJob('document_setup', 'modify_outline'),
      );

      expect(prismaMock.outline.update).toHaveBeenCalledWith({
        where: { id: 'outline-9' },
        data: expect.any(Object),
      });
    });

    it('action không xác định: trả message mặc định', async () => {
      const result = await service.processJobFallback(
        buildJob('document_setup', 'unknown_action'),
      );

      expect((result as any).status).toBe('finished');
      const updateArgs = prismaMock.job.update.mock.calls[0][0];
      expect(updateArgs.data.result.message).toContain('Executed');
    });

    it('lỗi trong xử lý ⇒ job chuyển error kèm message', async () => {
      prismaMock.document.update.mockRejectedValueOnce(new Error('DB down'));

      const result = await service.processJobFallback(
        buildJob('document_setup', 'generate_domains'),
      );

      expect(result).toEqual({ id: 'job-1', status: 'error' });
      const calls = prismaMock.job.update.mock.calls;
      expect(calls.at(-1)![0].data.errorMessage).toBe('DB down');
    });
  });
});
