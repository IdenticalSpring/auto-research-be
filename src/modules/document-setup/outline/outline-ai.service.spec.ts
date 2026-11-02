import { jest } from '@jest/globals';
import { BadRequestException, NotFoundException } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { PrismaService } from '../../../database/prisma.service';
import { OutlineAiDispatcher } from './outline-ai.dispatcher';
import { OutlineAiService } from './outline-ai.service';

describe('OutlineAiService', () => {
  let service: OutlineAiService;
  let mockPrisma: Record<string, any>;
  let mockDispatcher: Record<string, any>;

  const userId = '00000000-0000-0000-0000-000000000001';
  const documentId = '00000000-0000-0000-0000-000000000002';

  beforeEach(async () => {
    mockPrisma = {
      document: { findFirst: jest.fn<any>() },
      proposal: { findFirst: jest.fn<any>() },
      outline: { findUnique: jest.fn<any>(), upsert: jest.fn<any>() },
      documentReference: {
        findMany: jest.fn<any>(),
        upsert: jest.fn<any>(),
      },
    };
    mockDispatcher = { dispatch: jest.fn<any>() };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OutlineAiService,
        { provide: PrismaService, useValue: mockPrisma },
        { provide: OutlineAiDispatcher, useValue: mockDispatcher },
      ],
    }).compile();

    service = module.get<OutlineAiService>(OutlineAiService);
  });

  describe('ensureDocument', () => {
    it('should throw NotFoundException if document not found', async () => {
      mockPrisma.document.findFirst.mockResolvedValue(null);
      await expect(service.ensureDocument(userId, documentId)).rejects.toThrow(NotFoundException);
    });
  });

  describe('generateOutline', () => {
    it('should dispatch generate_outline with code 617 and full schema fields', async () => {
      mockPrisma.document.findFirst.mockResolvedValue({ id: documentId, field: 'CS', domain: 'AI' });
      mockPrisma.proposal.findFirst.mockResolvedValue({ title: 'AI Prop', problemStatement: 'Problem X' });
      mockPrisma.outline.upsert.mockResolvedValue({});

      mockDispatcher.dispatch.mockImplementation(async (opt: any) => {
        if (opt.onSuccess) {
          await opt.onSuccess({
            outline: { sections: [{ title: 'Intro' }] },
            total_word_count: 5000,
          });
        }
        return { jobId: 'job-outline-1' };
      });

      const res = await service.generateOutline(userId, { documentId, totalWordCount: 5000 });
      expect(res.jobId).toBe('job-outline-1');
      expect(mockDispatcher.dispatch).toHaveBeenCalledWith(
        expect.objectContaining({
          eventType: 'generate_outline',
          errorCode: 617,
          kafkaData: expect.objectContaining({
            field: 'CS',
            domain: 'AI',
            word_count_str: '5000',
            headings: expect.any(Array),
            headings_percent: expect.any(Array),
            outline_code: expect.any(Array),
            final_proposal: expect.objectContaining({ title: 'AI Prop' }),
          }),
        }),
      );
      expect(mockPrisma.outline.upsert).toHaveBeenCalled();
    });
  });

  describe('modifyOutline', () => {
    it('should dispatch modify_outline with code 627 and updated headings percent', async () => {
      mockPrisma.document.findFirst.mockResolvedValue({ id: documentId });
      mockPrisma.proposal.findFirst.mockResolvedValue(null);
      mockPrisma.outline.findUnique.mockResolvedValue({
        structure: { chapters: ['Old'] },
        headings: [{ title: 'Old' }],
        headingsPercent: [100],
      });
      mockPrisma.outline.upsert.mockResolvedValue({});
      mockDispatcher.dispatch.mockResolvedValue({ jobId: 'job-modify-1' });

      const res = await service.modifyOutline(userId, {
        documentId,
        newHeadings: [{ title: 'New 1' }, { title: 'New 2' }],
        newHeadingsPercent: [40, 60],
      });
      expect(res.jobId).toBe('job-modify-1');
      expect(mockDispatcher.dispatch).toHaveBeenCalledWith(
        expect.objectContaining({
          eventType: 'modify_outline',
          errorCode: 627,
          kafkaData: expect.objectContaining({
            new_headings: [{ title: 'New 1' }, { title: 'New 2' }],
            // AI Q1: percent gửi đi là thang 0..1 (không phải 40/60)
            new_headings_percent: [0.4, 0.6],
            headings_percent: [1],
          }),
        }),
      );
    });
  });

  describe('generateOutlineWithRefs', () => {
    it('should dispatch generate_outline_with_refs with code 626 and processed_docs', async () => {
      mockPrisma.document.findFirst.mockResolvedValue({ id: documentId });
      mockPrisma.proposal.findFirst.mockResolvedValue(null);
      mockPrisma.outline.findUnique.mockResolvedValue({ structure: { title: 'Doc' } });
      mockPrisma.documentReference.findMany.mockResolvedValue([
        {
          reference: {
            id: 'ref-1',
            title: 'Ref Paper',
            url: 'https://doi.org/1',
            authors: 'Author A',
            year: 2024,
          },
        },
      ]);
      mockPrisma.outline.upsert.mockResolvedValue({});
      mockDispatcher.dispatch.mockResolvedValue({ jobId: 'job-refs-1' });

      const res = await service.generateOutlineWithRefs(userId, {
        documentId,
        referenceIds: ['ref-1'],
      });
      expect(res.jobId).toBe('job-refs-1');
      expect(mockDispatcher.dispatch).toHaveBeenCalledWith(
        expect.objectContaining({
          eventType: 'generate_outline_with_refs',
          errorCode: 626,
          kafkaData: expect.objectContaining({
            processed_docs: [expect.objectContaining({ id: 'ref-1', title: 'Ref Paper' })],
          }),
        }),
      );
    });
  });

  describe('updateUserRefs', () => {
    it('should dispatch update_user_refs with code 628 and persist document references', async () => {
      mockPrisma.document.findFirst.mockResolvedValue({ id: documentId });
      mockPrisma.documentReference.findMany.mockResolvedValue([]);
      mockPrisma.documentReference.upsert.mockResolvedValue({});

      mockDispatcher.dispatch.mockImplementation(async (opt: any) => {
        if (opt.onSuccess) await opt.onSuccess({});
        return { jobId: 'job-update-refs' };
      });

      const res = await service.updateUserRefs(userId, {
        documentId,
        referenceIds: ['ref-a'],
        documents: [{ id: 'ref-a', title: 'Manual Ref' }],
      });
      expect(res.jobId).toBe('job-update-refs');
      expect(mockDispatcher.dispatch).toHaveBeenCalledWith(
        expect.objectContaining({
          eventType: 'update_user_refs',
          errorCode: 628,
          kafkaData: { documents: [{ id: 'ref-a', title: 'Manual Ref' }] },
        }),
      );
      expect(mockPrisma.documentReference.upsert).toHaveBeenCalledWith({
        where: { documentId_referenceId: { documentId, referenceId: 'ref-a' } },
        update: {},
        create: { documentId, referenceId: 'ref-a' },
      });
    });

    it('should throw error when dispatcher rejects with errorCode 628', async () => {
      mockPrisma.document.findFirst.mockResolvedValue({ id: documentId });
      mockPrisma.documentReference.findMany.mockResolvedValue([]);
      mockDispatcher.dispatch.mockRejectedValue(
        new BadRequestException({ errorCode: 628, message: 'Update refs failed' }),
      );

      await expect(
        service.updateUserRefs(userId, { documentId, referenceIds: ['ref-a'] }),
      ).rejects.toThrow(BadRequestException);
    });
  });
});
