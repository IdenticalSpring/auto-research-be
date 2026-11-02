import { jest } from '@jest/globals';
import { BadRequestException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { JobStatus } from '@prisma/client';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { InterruptResumeService } from '../../../common/services/interrupt-resume.service';
import { WriterChatbotService } from './writer-chatbot.service';
import { WriterChatbotDto } from './dto/writer-chatbot.dto';

describe('WriterChatbotService', () => {
  let service: WriterChatbotService;
  let mockPrisma: any;
  let mockKafkaProducer: any;
  let mockCorrelation: any;
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
      chatSession: {
        findFirst: jest.fn<any>(),
        update: jest.fn<any>().mockResolvedValue({}),
      },
      chatMessage: {
        findMany: jest.fn<any>().mockResolvedValue([]),
        create: jest.fn<any>().mockResolvedValue({ id: 'msg-1' }),
      },
      job: {
        create: jest.fn<any>().mockResolvedValue({ id: 'job-w-1' }),
        update: jest.fn<any>().mockResolvedValue({}),
        findFirst: jest.fn<any>(),
      },
      outline: { upsert: jest.fn<any>().mockResolvedValue({}) },
    };

    mockKafkaProducer = {
      publish: jest.fn<any>().mockResolvedValue(undefined),
    };

    mockCorrelation = {
      buildCorrelationKey: jest.fn<any>().mockReturnValue('corr-writer'),
      registerPending: jest.fn<any>(),
      clearPending: jest.fn<any>(),
    };

    mockInterruptResume = {
      suspendJob: jest.fn<any>().mockResolvedValue(undefined),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        WriterChatbotService,
        { provide: PrismaService, useValue: mockPrisma },
        { provide: KafkaProducerService, useValue: mockKafkaProducer },
        { provide: RequestCorrelationService, useValue: mockCorrelation },
        { provide: InterruptResumeService, useValue: mockInterruptResume },
        {
          provide: ConfigService,
          useValue: {
            get: jest.fn<any>().mockReturnValue('chatbot_request'),
          },
        },
      ],
    }).compile();

    service = module.get<WriterChatbotService>(WriterChatbotService);
  });

  describe('interactWriterChatbot', () => {
    const dto: WriterChatbotDto = { question: 'Let us outline section 1' };

    it('should suspend job when done_chat is false', async () => {
      mockPrisma.chatSession.findFirst.mockResolvedValue(mockSession);
      mockPrisma.job.findFirst.mockResolvedValue(null);
      mockCorrelation.registerPending.mockResolvedValue({
        done_chat: false,
        ai_message: 'What method do you prefer?',
        template_key: 'method_choice',
      });

      const res = await service.interactWriterChatbot(userId, sessionId, dto);
      expect(res.doneChat).toBe(false);
      expect(res.aiMessage).toBe('What method do you prefer?');
      expect(mockInterruptResume.suspendJob).toHaveBeenCalledWith(
        'job-w-1',
        expect.anything(),
        'method_choice',
      );
    });

    it('should finish job and save outline when done_chat is true', async () => {
      mockPrisma.chatSession.findFirst.mockResolvedValue(mockSession);
      mockPrisma.job.findFirst.mockResolvedValue(null);
      mockCorrelation.registerPending.mockResolvedValue({
        done_chat: true,
        summary: 'Thesis Summary',
        outline: { sections: ['Intro', 'Lit Review'] },
      });

      const res = await service.interactWriterChatbot(userId, sessionId, dto);
      expect(res.doneChat).toBe(true);
      expect(res.summary).toBe('Thesis Summary');
      expect(mockPrisma.outline.upsert).toHaveBeenCalled();
      expect(mockPrisma.job.update).toHaveBeenCalledWith({
        where: { id: 'job-w-1' },
        data: expect.objectContaining({ status: JobStatus.finished }),
      });
    });

    it('should throw BadRequestException with code 652 if publish fails', async () => {
      mockPrisma.chatSession.findFirst.mockResolvedValue(mockSession);
      mockPrisma.job.findFirst.mockResolvedValue(null);
      mockKafkaProducer.publish.mockRejectedValue(
        new Error('Broker unreachable'),
      );

      await expect(
        service.interactWriterChatbot(userId, sessionId, dto),
      ).rejects.toThrow(BadRequestException);
    });

    it('should throw BadRequestException with code 652 if pending promise rejects', async () => {
      mockPrisma.chatSession.findFirst.mockResolvedValue(mockSession);
      mockPrisma.job.findFirst.mockResolvedValue(null);
      mockCorrelation.registerPending.mockRejectedValue(
        new Error('Timeout 120s'),
      );

      await expect(
        service.interactWriterChatbot(userId, sessionId, dto),
      ).rejects.toThrow(BadRequestException);
    });
  });
});
