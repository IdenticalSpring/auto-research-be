import { jest } from '@jest/globals';
import { BadRequestException, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { JobStatus } from '@prisma/client';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { StreamingManagerService } from '../../../common/streaming/streaming-manager.service';
import { InterruptResumeService } from '../../../common/services/interrupt-resume.service';
import { WriterAgentService } from './writer-agent.service';
import {
  ResumeWriterAgentDto,
  StartWriterAgentDto,
} from './dto/writer-chatbot.dto';

describe('WriterAgentService', () => {
  let service: WriterAgentService;
  let mockPrisma: any;
  let mockKafkaProducer: any;
  let mockCorrelation: any;
  let mockStreamingManager: any;
  let mockInterruptResume: any;

  const userId = '00000000-0000-0000-0000-000000000001';
  const sessionId = '00000000-0000-0000-0000-000000000002';
  const documentId = '00000000-0000-0000-0000-000000000003';

  const mockSession = {
    id: sessionId,
    userId,
    documentId,
    webSearchEnabled: false,
    language: 'vi',
    model: 'localhost',
  };

  beforeEach(async () => {
    mockPrisma = {
      chatSession: { findFirst: jest.fn<any>() },
      chatMessage: { findMany: jest.fn<any>().mockResolvedValue([]) },
      outline: { findUnique: jest.fn<any>().mockResolvedValue(null) },
      document: { findUnique: jest.fn<any>().mockResolvedValue(null) },
      job: {
        create: jest.fn<any>().mockResolvedValue({ id: 'job-w-1' }),
        update: jest.fn<any>().mockResolvedValue({}),
        findFirst: jest.fn<any>(),
      },
      report: { upsert: jest.fn<any>().mockResolvedValue({}) },
    };

    mockKafkaProducer = {
      publish: jest.fn<any>().mockResolvedValue(undefined),
    };

    mockCorrelation = {
      buildCorrelationKey: jest.fn<any>().mockReturnValue('corr-writer'),
      registerPending: jest.fn<any>(),
      clearPending: jest.fn<any>(),
    };

    mockStreamingManager = {
      buildStreamKey: jest.fn<any>().mockReturnValue('stream-writer-key'),
      createStream: jest.fn<any>().mockReturnValue('mock-stream'),
      completeStream: jest.fn<any>(),
      destroyStream: jest.fn<any>(),
    };

    mockInterruptResume = {
      suspendJob: jest.fn<any>().mockResolvedValue(undefined),
      resumeJob: jest.fn<any>().mockResolvedValue({ success: true }),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        WriterAgentService,
        { provide: PrismaService, useValue: mockPrisma },
        { provide: KafkaProducerService, useValue: mockKafkaProducer },
        { provide: RequestCorrelationService, useValue: mockCorrelation },
        { provide: StreamingManagerService, useValue: mockStreamingManager },
        { provide: InterruptResumeService, useValue: mockInterruptResume },
        {
          provide: ConfigService,
          useValue: {
            get: jest.fn<any>().mockReturnValue('chatbot_request'),
          },
        },
      ],
    }).compile();

    service = module.get<WriterAgentService>(WriterAgentService);
  });

  describe('startWriterAgent', () => {
    const dto: StartWriterAgentDto = { instruction: 'Write chapter 1' };

    it('should throw BadRequestException if documentId is missing', async () => {
      mockPrisma.chatSession.findFirst.mockResolvedValue({
        ...mockSession,
        documentId: null,
      });

      await expect(
        service.startWriterAgent(userId, sessionId, dto),
      ).rejects.toThrow(BadRequestException);
    });

    it('should initiate stream and register pending correlation with 1500s timeout', async () => {
      mockPrisma.chatSession.findFirst.mockResolvedValue(mockSession);
      const pendingPromise = Promise.resolve({
        has_finished: true,
        content: '# Chapter 1\nIntro text',
      });
      mockCorrelation.registerPending.mockReturnValue(pendingPromise);

      const res = await service.startWriterAgent(userId, sessionId, dto);
      expect(res.status).toBe('started');
      expect(res.jobId).toBe('job-w-1');
      expect(mockCorrelation.registerPending).toHaveBeenCalledWith(
        'corr-writer',
        1500000,
        'writer',
        'job-w-1',
      );

      await new Promise((r) => setTimeout(r, 20));
      expect(mockPrisma.report.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { documentId },
          update: { contentMarkdown: '# Chapter 1\nIntro text' },
        }),
      );
      expect(mockStreamingManager.completeStream).toHaveBeenCalledWith(
        'stream-writer-key',
      );
    });

    it('should suspend job if writer returns has_finished false', async () => {
      mockPrisma.chatSession.findFirst.mockResolvedValue(mockSession);
      const pendingPromise = Promise.resolve({
        has_finished: false,
        action_required: 'user_input',
      });
      mockCorrelation.registerPending.mockReturnValue(pendingPromise);

      await service.startWriterAgent(userId, sessionId, dto);
      await new Promise((r) => setTimeout(r, 20));
      expect(mockInterruptResume.suspendJob).toHaveBeenCalledWith(
        'job-w-1',
        expect.anything(),
        'writer_input_required',
      );
    });

    it('should throw BadRequestException with code 653 if kafka publish fails', async () => {
      mockPrisma.chatSession.findFirst.mockResolvedValue(mockSession);
      mockKafkaProducer.publish.mockRejectedValue(new Error('Broker failed'));

      await expect(
        service.startWriterAgent(userId, sessionId, dto),
      ).rejects.toThrow(BadRequestException);
      expect(mockCorrelation.clearPending).toHaveBeenCalled();
    });

    it('should handle background rejection and destroy stream', async () => {
      mockPrisma.chatSession.findFirst.mockResolvedValue(mockSession);
      const pendingPromise = Promise.reject(
        new Error('Writer process crashed'),
      );
      mockCorrelation.registerPending.mockReturnValue(pendingPromise);

      await service.startWriterAgent(userId, sessionId, dto);
      await new Promise((r) => setTimeout(r, 20));

      expect(mockPrisma.job.update).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({
            status: JobStatus.error,
            errorCode: '653',
          }),
        }),
      );
      expect(mockStreamingManager.destroyStream).toHaveBeenCalledWith(
        'stream-writer-key',
      );
    });
  });

  describe('getWriterStream', () => {
    it('should return stream from streamingManager', () => {
      const res = service.getWriterStream(userId, sessionId);
      expect(res).toBe('mock-stream');
      expect(mockStreamingManager.createStream).toHaveBeenCalledWith(
        'stream-writer-key',
        1500000,
      );
    });
  });

  describe('resumeWriterAgent', () => {
    const dto: ResumeWriterAgentDto = { userInput: 'Proceed with table 1' };

    it('should resume suspended writer job', async () => {
      mockPrisma.chatSession.findFirst.mockResolvedValue(mockSession);
      mockPrisma.job.findFirst.mockResolvedValue({ id: 'job-suspended-1' });

      const res = await service.resumeWriterAgent(userId, sessionId, dto);
      expect(res.success).toBe(true);
      expect(mockInterruptResume.resumeJob).toHaveBeenCalledWith(
        'job-suspended-1',
        'Proceed with table 1',
        undefined,
      );
    });

    it('should throw NotFoundException if no suspended job found', async () => {
      mockPrisma.chatSession.findFirst.mockResolvedValue(mockSession);
      mockPrisma.job.findFirst.mockResolvedValue(null);

      await expect(
        service.resumeWriterAgent(userId, sessionId, dto),
      ).rejects.toThrow(NotFoundException);
    });
  });
});
