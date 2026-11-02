import { jest } from '@jest/globals';
import { BadRequestException, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { JobStatus, SlideStatus } from '@prisma/client';
import { Subject } from 'rxjs';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { StreamingManagerService } from '../../../common/streaming/streaming-manager.service';
import { GeneratedSlideAiService } from './generated-slide-ai.service';
import { GenerateSlideRequestDto } from './dto/generate-slide-request.dto';

describe('GeneratedSlideAiService', () => {
  let service: GeneratedSlideAiService;
  let mockPrisma: any;
  let mockKafkaProducer: any;
  let mockCorrelation: any;
  let mockStreamingManager: any;
  let streamSubject: Subject<any>;

  const userId = '00000000-0000-0000-0000-000000000001';
  const documentId = '00000000-0000-0000-0000-000000000002';
  const slideId = '00000000-0000-0000-0000-000000000003';

  const validDto: GenerateSlideRequestDto = {
    documentId,
    slideStyle: 'modern',
    userPreference: { numSlides: 10, theme: 'dark' },
    manualMode: false,
    language: 'vi',
  };

  beforeEach(async () => {
    streamSubject = new Subject();

    mockPrisma = {
      document: { findUnique: jest.fn<any>() },
      report: { findUnique: jest.fn<any>() },
      generatedSlide: {
        create: jest.fn<any>().mockResolvedValue({ id: slideId }),
        update: jest.fn<any>().mockResolvedValue({}),
        findFirst: jest.fn<any>(),
      },
      job: {
        create: jest.fn<any>().mockResolvedValue({ id: 'job-slide-1' }),
        update: jest.fn<any>().mockResolvedValue({}),
      },
    };

    mockKafkaProducer = {
      publish: jest.fn<any>().mockResolvedValue(undefined),
    };

    mockCorrelation = {
      buildCorrelationKey: jest.fn<any>().mockReturnValue('corr-key-slide'),
      getStreamObservable: jest
        .fn<any>()
        .mockReturnValue(streamSubject.asObservable()),
      registerPending: jest.fn<any>(),
      clearPending: jest.fn<any>(),
    };

    mockStreamingManager = {
      buildStreamKey: jest.fn<any>().mockReturnValue('stream-key-slide'),
      createStream: jest.fn<any>().mockReturnValue('mock-sse-stream'),
      completeStream: jest.fn<any>(),
      destroyStream: jest.fn<any>(),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        GeneratedSlideAiService,
        { provide: PrismaService, useValue: mockPrisma },
        { provide: KafkaProducerService, useValue: mockKafkaProducer },
        { provide: RequestCorrelationService, useValue: mockCorrelation },
        { provide: StreamingManagerService, useValue: mockStreamingManager },
        {
          provide: ConfigService,
          useValue: {
            get: jest.fn<any>().mockReturnValue('write_section_request'),
          },
        },
      ],
    }).compile();

    service = module.get<GeneratedSlideAiService>(GeneratedSlideAiService);
  });

  describe('generateSlides', () => {
    it('should throw NotFoundException if document does not exist or belong to user', async () => {
      mockPrisma.document.findUnique.mockResolvedValue(null);

      await expect(service.generateSlides(userId, validDto)).rejects.toThrow(
        NotFoundException,
      );
    });

    it('should throw BadRequestException if rawReport is missing and report content not found in DB', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId });
      mockPrisma.report.findUnique.mockResolvedValue(null);

      await expect(service.generateSlides(userId, validDto)).rejects.toThrow(
        BadRequestException,
      );
    });

    it('should publish to Kafka and process slide progress chunks and completion successfully', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId });
      mockPrisma.report.findUnique.mockResolvedValue({
        contentMarkdown: '# Report Content\nDetailed analysis',
      });
      const pendingPromise = new Promise(() => {});
      mockCorrelation.registerPending.mockReturnValue(pendingPromise);

      const res = await service.generateSlides(userId, validDto);
      expect(res.status).toBe('started');
      expect(res.jobId).toBe('job-slide-1');
      expect(res.slideId).toBe(slideId);

      expect(mockKafkaProducer.publish).toHaveBeenCalledWith(
        'write_section_request',
        expect.objectContaining({
          event_type: 'gen_slides',
          document_id: documentId,
          raw_report: '# Report Content\nDetailed analysis',
        }),
      );

      // Phát chunk tiến trình (progress: 50)
      streamSubject.next({ progress: 50 });
      await new Promise((r) => setTimeout(r, 10));

      expect(mockPrisma.generatedSlide.update).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: slideId },
          data: { progressStep: '50%' },
        }),
      );

      // Phát chunk hoàn tất (has_finished: true, pptx_url)
      streamSubject.next({
        has_finished: true,
        pptx_url: 'http://minio/thesis-slides.pptx',
      });
      await new Promise((r) => setTimeout(r, 10));

      expect(mockPrisma.generatedSlide.update).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: slideId },
          data: {
            status: SlideStatus.completed,
            pptxUrl: 'http://minio/thesis-slides.pptx',
            progressStep: '100%',
          },
        }),
      );
      expect(mockPrisma.job.update).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: 'job-slide-1' },
          data: expect.objectContaining({ status: JobStatus.finished }),
        }),
      );
      expect(mockStreamingManager.completeStream).toHaveBeenCalledWith(
        'stream-key-slide',
      );
    });

    it('should throw BadRequestException with code 634 if Kafka publish fails', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId });
      mockKafkaProducer.publish.mockRejectedValue(new Error('Broker down'));

      await expect(
        service.generateSlides(userId, {
          ...validDto,
          rawReport: 'direct text',
        }),
      ).rejects.toThrow(BadRequestException);

      expect(mockCorrelation.clearPending).toHaveBeenCalled();
      expect(mockPrisma.generatedSlide.update).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: slideId },
          data: { status: SlideStatus.error },
        }),
      );
    });

    it('should handle stream error by updating records and destroying stream', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId });
      const pendingPromise = new Promise(() => {});
      mockCorrelation.registerPending.mockReturnValue(pendingPromise);

      await service.generateSlides(userId, {
        ...validDto,
        rawReport: 'report text',
      });

      streamSubject.error(new Error('MinIO connection failed'));
      await new Promise((r) => setTimeout(r, 10));

      expect(mockPrisma.generatedSlide.update).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: slideId },
          data: { status: SlideStatus.error },
        }),
      );
      expect(mockPrisma.job.update).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: 'job-slide-1' },
          data: expect.objectContaining({
            status: JobStatus.error,
            errorCode: '634',
          }),
        }),
      );
      expect(mockStreamingManager.destroyStream).toHaveBeenCalledWith(
        'stream-key-slide',
      );
    });

    it('should handle pending promise timeout/rejection', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId });
      const pendingPromise = Promise.reject({
        error_code: 604,
        error: 'Timed out 300s',
      });
      mockCorrelation.registerPending.mockReturnValue(pendingPromise);

      await service.generateSlides(userId, {
        ...validDto,
        rawReport: 'report text',
      });
      await new Promise((r) => setTimeout(r, 20));

      expect(mockPrisma.generatedSlide.update).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: slideId },
          data: { status: SlideStatus.error },
        }),
      );
      expect(mockStreamingManager.destroyStream).toHaveBeenCalledWith(
        'stream-key-slide',
      );
    });
  });

  describe('getSlideStream', () => {
    it('should return stream when document is owned', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId });

      const res = await service.getSlideStream(userId, documentId);
      expect(res).toBe('mock-sse-stream');
      expect(mockStreamingManager.createStream).toHaveBeenCalledWith(
        'stream-key-slide',
        300000,
      );
    });
  });

  describe('getSlideStatus', () => {
    it('should return latest slide record for document', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId });
      mockPrisma.generatedSlide.findFirst.mockResolvedValue({
        id: slideId,
        status: SlideStatus.completed,
      });

      const res = await service.getSlideStatus(userId, documentId);
      expect(res).toEqual({ id: slideId, status: SlideStatus.completed });
    });
  });
});
