import { jest } from '@jest/globals';
import { BadRequestException, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { JobStatus, ModuleType } from '@prisma/client';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { ReferenceService } from './reference.service';
import { SearchPapersDto } from './dto/search-papers.dto';

describe('ReferenceService - SearchPapers & Journals', () => {
  let service: ReferenceService;
  let mockPrisma: Record<string, any>;
  let mockKafkaProducer: Record<string, any>;
  let mockCorrelationService: Record<string, any>;
  let mockConfigService: Record<string, any>;

  const userId = '00000000-0000-0000-0000-000000000001';
  const documentId = '00000000-0000-0000-0000-000000000002';

  const sampleSearchDto: SearchPapersDto = {
    documentId,
    field: 'Computer Science',
    domain: 'Artificial Intelligence',
    subdomains: ['Machine Learning'],
    query: 'AI in Higher Education',
    countryFilter: 'both',
    oaFilter: true,
    isOa: true,
    isTool: false,
    downloadedPapers: [],
    cutOffYearLow: 2020,
    cutOffYearHigh: 2026,
    searchRange: 'all',
    useWebSearch: false,
    language: 'vi',
  };

  beforeEach(async () => {
    mockPrisma = {
      reference: {
        create: jest.fn<any>(),
        findMany: jest.fn<any>(),
        findFirst: jest.fn<any>(),
        findUnique: jest.fn<any>(),
        update: jest.fn<any>(),
        delete: jest.fn<any>(),
      },
      documentReference: {
        upsert: jest.fn<any>(),
      },
      document: { findFirst: jest.fn<any>() },
      proposal: {
        findUnique: jest.fn<any>(),
        findFirst: jest.fn<any>(),
      },
      job: { create: jest.fn<any>(), update: jest.fn<any>() },
    };

    mockKafkaProducer = {
      publish: jest.fn<any>().mockResolvedValue(undefined),
    };

    mockCorrelationService = {
      buildCorrelationKey: jest
        .fn<any>()
        .mockReturnValue(`${userId}:${documentId}:sess-123:search_papers`),
      registerPending: jest.fn<any>(),
      clearPending: jest.fn<any>(),
    };

    mockConfigService = {
      get: jest.fn<any>((key: string) => {
        if (key === 'kafka.topics.document_setup.request') {
          return 'document_setup_request';
        }
        return undefined;
      }),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ReferenceService,
        { provide: PrismaService, useValue: mockPrisma },
        { provide: KafkaProducerService, useValue: mockKafkaProducer },
        {
          provide: RequestCorrelationService,
          useValue: mockCorrelationService,
        },
        { provide: ConfigService, useValue: mockConfigService },
      ],
    }).compile();

    service = module.get<ReferenceService>(ReferenceService);
  });

  describe('searchPapers', () => {
    it('should throw NotFoundException if document does not belong to user', async () => {
      mockPrisma.document.findFirst.mockResolvedValue(null);
      await expect(
        service.searchPapers(userId, sampleSearchDto),
      ).rejects.toThrow(NotFoundException);
    });

    it('should publish to Kafka and return papers on successful response', async () => {
      mockPrisma.document.findFirst.mockResolvedValue({ id: documentId });
      const mockJob = {
        id: 'job-uuid-1',
        module: ModuleType.document_setup,
        action: 'search_papers',
        status: JobStatus.pending,
      };
      mockPrisma.job.create.mockResolvedValue(mockJob);
      mockPrisma.job.update.mockResolvedValue(mockJob);
      mockPrisma.reference.findFirst.mockResolvedValue(null);
      mockPrisma.reference.create.mockResolvedValue({ id: 'ref-1' });
      mockPrisma.documentReference.upsert.mockResolvedValue({});

      const mockAiResponse = {
        event_type: 'search_papers',
        session_id: 'sess-123',
        papers: [
          {
            title: 'Deep Learning in Education',
            user_language_title: 'Học sâu trong Giáo dục',
            year: 2024,
            authors: 'Le, V. A.',
            journal: 'Computers & Education',
            q: 'Q1',
            url: 'https://example.com/paper1',
            open_access: true,
            status: 'downloaded',
            citation: 42,
          },
        ],
      };

      mockCorrelationService.registerPending.mockResolvedValue(mockAiResponse);

      const result = await service.searchPapers(userId, sampleSearchDto);

      expect(result).toBeDefined();
      expect(result.jobId).toBe('job-uuid-1');
      expect(result.papers).toHaveLength(1);
      expect(result.papers[0].title).toBe('Deep Learning in Education');
      expect(mockKafkaProducer.publish).toHaveBeenCalledWith(
        'document_setup_request',
        expect.objectContaining({ event_type: 'search_papers' }),
      );
    });

    it('should handle alternative data field in AI response', async () => {
      mockPrisma.document.findFirst.mockResolvedValue({ id: documentId });
      mockPrisma.job.create.mockResolvedValue({ id: 'job-uuid-2' });
      mockPrisma.job.update.mockResolvedValue({});
      mockPrisma.reference.findFirst.mockResolvedValue({ id: 'ref-exist' });
      mockPrisma.documentReference.upsert.mockResolvedValue({});

      mockCorrelationService.registerPending.mockResolvedValue({
        data: [{ title: 'Paper via Data' }],
      });

      const result = await service.searchPapers(userId, sampleSearchDto);
      expect(result.papers).toHaveLength(1);
    });

    it('should handle Kafka publish error, clear pending and throw error 610', async () => {
      mockPrisma.document.findFirst.mockResolvedValue({ id: documentId });
      mockPrisma.job.create.mockResolvedValue({ id: 'job-uuid-3' });
      mockPrisma.job.update.mockResolvedValue({});
      mockKafkaProducer.publish.mockRejectedValue(
        new Error('Broker unreachable'),
      );

      await expect(
        service.searchPapers(userId, sampleSearchDto),
      ).rejects.toThrow(BadRequestException);
      expect(mockCorrelationService.clearPending).toHaveBeenCalled();
    });

    it('should handle AI error 610 returned from AI Engine', async () => {
      mockPrisma.document.findFirst.mockResolvedValue({ id: documentId });
      mockPrisma.job.create.mockResolvedValue({ id: 'job-uuid-4' });
      mockPrisma.job.update.mockResolvedValue({});
      mockCorrelationService.registerPending.mockRejectedValue({
        error_code: 610,
        error: 'SEARCH_PAPERS_ERROR',
      });

      await expect(
        service.searchPapers(userId, sampleSearchDto),
      ).rejects.toThrow(BadRequestException);
    });

    it('should handle generic error message rejection from correlation service', async () => {
      mockPrisma.document.findFirst.mockResolvedValue({ id: documentId });
      mockPrisma.job.create.mockResolvedValue({ id: 'job-uuid-5' });
      mockPrisma.job.update.mockResolvedValue({});
      mockCorrelationService.registerPending.mockRejectedValue(
        new Error('Network drop'),
      );

      await expect(
        service.searchPapers(userId, sampleSearchDto),
      ).rejects.toThrow(BadRequestException);
    });
  });

  describe('getJournals', () => {
    it('should publish get_journals to Kafka', async () => {
      mockCorrelationService.registerPending.mockResolvedValue({
        message: 'Finish crawling',
      });
      const result = await service.getJournals(1700000000);
      expect(result).toBeDefined();
      expect(mockKafkaProducer.publish).toHaveBeenCalledWith(
        'document_setup_request',
        expect.objectContaining({ event_type: 'get_journals' }),
      );
    });
  });
});
