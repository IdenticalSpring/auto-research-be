import { jest } from '@jest/globals';
import { BadRequestException, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { JobStatus, SuggestionStatus } from '@prisma/client';
import { PrismaService } from '../../database/prisma.service';
import { KafkaProducerService } from '../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../messaging/request-correlation.service';
import { AiInDocAction } from './dto/enhancement-ai.dto';
import { EnhancementAiService } from './enhancement-ai.service';

describe('EnhancementAiService', () => {
  let service: EnhancementAiService;
  let mockPrisma: any;
  let mockKafkaProducer: any;
  let mockCorrelation: any;

  const userId = '00000000-0000-0000-0000-000000000001';
  const documentId = '00000000-0000-0000-0000-000000000002';
  const sessionId = '00000000-0000-0000-0000-000000000003';

  beforeEach(async () => {
    mockPrisma = {
      document: { findUnique: jest.fn<any>().mockResolvedValue({ id: documentId }) },
      proposal: {
        findFirst: jest.fn<any>().mockResolvedValue({
          title: 'Proposal title',
          problemStatement: 'Problem statement text',
        }),
      },
      job: {
        create: jest.fn<any>().mockResolvedValue({ id: 'job-enh-1' }),
        update: jest.fn<any>().mockResolvedValue({}),
      },
      enhancementSession: {
        create: jest.fn<any>().mockResolvedValue({ id: sessionId }),
      },
      enhancementSuggestion: {
        create: jest.fn<any>().mockResolvedValue({ id: 'sug-1', status: SuggestionStatus.pending }),
        update: jest.fn<any>().mockResolvedValue({ id: 'sug-1', status: SuggestionStatus.applied }),
        updateMany: jest.fn<any>().mockResolvedValue({ count: 2 }),
      },
    };

    mockKafkaProducer = {
      publish: jest.fn<any>().mockResolvedValue(undefined),
    };

    mockCorrelation = {
      buildCorrelationKey: jest.fn<any>().mockReturnValue('corr-enhancement-key'),
      registerPending: jest.fn<any>(),
      clearPending: jest.fn<any>(),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        EnhancementAiService,
        { provide: PrismaService, useValue: mockPrisma },
        { provide: KafkaProducerService, useValue: mockKafkaProducer },
        { provide: RequestCorrelationService, useValue: mockCorrelation },
        {
          provide: ConfigService,
          useValue: {
            get: jest.fn<any>().mockReturnValue('enhancement_request'),
          },
        },
      ],
    }).compile();

    service = module.get<EnhancementAiService>(EnhancementAiService);
  });

  describe('aiInDoc', () => {
    const dto = {
      documentId,
      sectionId: 'sec-1',
      paragraphs: 'Original text to improve',
      action: AiInDocAction.make_academic,
    };

    it('should throw NotFoundException if document is not found or not owned', async () => {
      mockPrisma.document.findUnique.mockResolvedValue(null);
      await expect(service.aiInDoc(userId, dto)).rejects.toThrow(NotFoundException);
    });

    it('should publish to kafka and return enhanced paragraph', async () => {
      mockCorrelation.registerPending.mockResolvedValue({
        event_type: 'ai_in_doc',
        response: 'Academic rewritten text',
      });

      const res = await service.aiInDoc(userId, dto);
      expect(res.jobId).toBe('job-enh-1');
      expect(res.response).toBe('Academic rewritten text');
      expect(mockKafkaProducer.publish).toHaveBeenCalledWith(
        'enhancement_request',
        expect.objectContaining({
          event_type: 'ai_in_doc',
          action: 'make_academic',
          paragraphs: 'Original text to improve',
        }),
      );
      expect(mockPrisma.enhancementSession.create).toHaveBeenCalled();
    });

    it('should join response theo document_id+session_id dù AI KHÔNG echo section_id', async () => {
      // Quyết định 2026-11-01 §1.3: AI không echo section_id — BE tự join.
      mockCorrelation.registerPending.mockResolvedValue({
        event_type: 'ai_in_doc',
        user_id: userId,
        document_id: documentId,
        session_id: 'sess-ai-in-doc',
        response: 'Bản viết lại',
      });

      const res = await service.aiInDoc(userId, dto);

      expect(res.response).toBe('Bản viết lại');
      // Join dựa trên key do buildCorrelationKey(documentId, sessionId) tạo —
      // response thiếu section_id vẫn về đúng pending request.
      expect(mockCorrelation.buildCorrelationKey).toHaveBeenCalledWith(
        expect.objectContaining({
          documentId,
          eventType: 'ai_in_doc',
        }),
      );
      expect(mockCorrelation.registerPending).toHaveBeenCalledWith(
        'corr-enhancement-key',
        120_000,
        'ai_in_doc',
        'job-enh-1',
      );
    });

    it('should handle error when correlation rejects', async () => {
      mockCorrelation.registerPending.mockRejectedValue(new Error('AI failed'));

      await expect(service.aiInDoc(userId, dto)).rejects.toThrow(BadRequestException);
      expect(mockCorrelation.clearPending).toHaveBeenCalled();
      expect(mockPrisma.job.update).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: 'job-enh-1' },
          data: expect.objectContaining({ status: JobStatus.error }),
        }),
      );
    });
  });

  describe('getSuggestions', () => {
    const dto = {
      documentId,
      section: 'Section content',
      sectionContext: 'Chapter 1',
    };

    it('should request suggestions and persist them to enhancement_suggestions table', async () => {
      mockCorrelation.registerPending.mockResolvedValue({
        event_type: 'get_suggestions',
        suggested_paragraphs: [
          { original: 'Old sentence', suggestion: 'New sentence', reason: 'Better flow' },
        ],
      });

      const res = await service.getSuggestions(userId, dto);
      expect(res.jobId).toBe('job-enh-1');
      expect(res.suggestedParagraphs).toHaveLength(1);
      expect(mockPrisma.enhancementSuggestion.create).toHaveBeenCalledWith({
        data: {
          sessionId,
          suggestedText: 'New sentence',
          explanation: 'Better flow',
          status: SuggestionStatus.pending,
        },
      });
    });
  });

  describe('enhance', () => {
    const dto = {
      documentId,
      section: 'Full section content',
      suggestedParagraph: { original: 'Old', suggestion: 'New' },
      suggestionId: 'sug-1',
    };

    it('should apply single suggestion and mark suggestion as applied', async () => {
      mockCorrelation.registerPending.mockResolvedValue({
        event_type: 'enhance',
        updated_paragraphs: 'Full updated section content',
      });

      const res = await service.enhance(userId, dto);
      expect(res.updatedParagraphs).toBe('Full updated section content');
      expect(mockPrisma.enhancementSuggestion.update).toHaveBeenCalledWith({
        where: { id: 'sug-1' },
        data: { status: SuggestionStatus.applied },
      });
    });
  });

  describe('enhanceAll', () => {
    const dto = {
      documentId,
      section: 'Full section content',
      suggestedParagraphs: [{ original: 'Old', suggestion: 'New' }],
      sessionId,
    };

    it('should apply all suggestions and mark all pending suggestions as applied', async () => {
      mockCorrelation.registerPending.mockResolvedValue({
        event_type: 'enhance_all',
        updated_paragraphs: 'Full section with all changes applied',
      });

      const res = await service.enhanceAll(userId, dto);
      expect(res.updatedParagraphs).toBe('Full section with all changes applied');
      expect(mockPrisma.enhancementSuggestion.updateMany).toHaveBeenCalledWith({
        where: { sessionId, status: SuggestionStatus.pending },
        data: { status: SuggestionStatus.applied },
      });
    });
  });
});
