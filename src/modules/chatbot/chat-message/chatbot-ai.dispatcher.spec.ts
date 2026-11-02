import { jest } from '@jest/globals';
import { BadRequestException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { JobStatus, ModuleType } from '@prisma/client';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { ChatbotAiDispatcher } from './chatbot-ai.dispatcher';

describe('ChatbotAiDispatcher', () => {
  let dispatcher: ChatbotAiDispatcher;
  let mockPrisma: Record<string, any>;
  let mockKafkaProducer: Record<string, any>;
  let mockCorrelation: Record<string, any>;
  let mockConfigService: Record<string, any>;

  const userId = '00000000-0000-0000-0000-000000000001';
  const sessionId = '00000000-0000-0000-0000-000000000002';
  const documentId = '00000000-0000-0000-0000-000000000003';

  beforeEach(async () => {
    mockPrisma = {
      job: {
        create: jest.fn<any>(),
        update: jest.fn<any>(),
      },
    };

    mockKafkaProducer = {
      publish: jest.fn<any>().mockResolvedValue(undefined),
    };

    mockCorrelation = {
      buildCorrelationKey: jest.fn<any>().mockReturnValue('corr-chat-key'),
      registerPending: jest.fn<any>(),
      clearPending: jest.fn<any>(),
    };

    mockConfigService = {
      get: jest.fn<any>().mockReturnValue('chatbot_request'),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ChatbotAiDispatcher,
        { provide: PrismaService, useValue: mockPrisma },
        { provide: KafkaProducerService, useValue: mockKafkaProducer },
        { provide: RequestCorrelationService, useValue: mockCorrelation },
        { provide: ConfigService, useValue: mockConfigService },
      ],
    }).compile();

    dispatcher = module.get<ChatbotAiDispatcher>(ChatbotAiDispatcher);
  });

  it('should dispatch successfully and update job to finished', async () => {
    mockPrisma.job.create.mockResolvedValue({ id: 'job-cb-1' });
    mockPrisma.job.update.mockResolvedValue({});
    mockCorrelation.registerPending.mockResolvedValue({
      response: 'Hello user',
    });
    const onSuccess = jest.fn<any>().mockResolvedValue(undefined);

    const res = await dispatcher.dispatch({
      userId,
      sessionId,
      documentId,
      eventType: 'outer_chatbot',
      errorCode: 650,
      dto: { question: 'What is AI?' },
      kafkaData: { question: 'What is AI?' },
      onSuccess,
    });

    expect(res.jobId).toBe('job-cb-1');
    expect(res.response).toBe('Hello user');
    expect(onSuccess).toHaveBeenCalledWith({ response: 'Hello user' });
    expect(mockPrisma.job.create).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          module: ModuleType.chatbot,
          action: 'outer_chatbot',
          sessionId,
        }),
      }),
    );
    expect(mockPrisma.job.update).toHaveBeenCalledWith({
      where: { id: 'job-cb-1' },
      data: { status: JobStatus.finished, result: { response: 'Hello user' } },
    });
  });

  it('should handle kafka publish failure and update job to error', async () => {
    mockPrisma.job.create.mockResolvedValue({ id: 'job-cb-fail' });
    mockPrisma.job.update.mockResolvedValue({});
    mockKafkaProducer.publish.mockRejectedValue(
      new Error('Broker unreachable'),
    );
    const onError = jest.fn<any>().mockResolvedValue(undefined);

    await expect(
      dispatcher.dispatch({
        userId,
        sessionId,
        eventType: 'outer_chatbot',
        errorCode: 650,
        dto: { question: 'Q' },
        kafkaData: {},
        onError,
      }),
    ).rejects.toThrow(BadRequestException);

    expect(onError).toHaveBeenCalled();
    expect(mockCorrelation.clearPending).toHaveBeenCalledWith(
      'corr-chat-key',
      'Broker unreachable',
    );
    expect(mockPrisma.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'job-cb-fail' },
        data: expect.objectContaining({
          status: JobStatus.error,
          errorCode: '650',
        }),
      }),
    );
  });

  it('should handle pending timeout/error with custom error_code', async () => {
    mockPrisma.job.create.mockResolvedValue({ id: 'job-cb-err' });
    mockPrisma.job.update.mockResolvedValue({});
    mockCorrelation.registerPending.mockRejectedValue({
      error_code: 651,
      error: 'Inner chatbot context error',
    });
    const onError = jest.fn<any>().mockResolvedValue(undefined);

    try {
      await dispatcher.dispatch({
        userId,
        sessionId,
        documentId,
        eventType: 'inner_chatbot',
        errorCode: 651,
        dto: { question: 'Q' },
        kafkaData: {},
        onError,
      });
      fail('Should throw');
    } catch (err: any) {
      expect(err).toBeInstanceOf(BadRequestException);
      const resp = err.getResponse();
      expect(resp.errorCode).toBe(651);
      expect(resp.message).toBe('Inner chatbot context error');
    }

    expect(onError).toHaveBeenCalled();
    expect(mockPrisma.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'job-cb-err' },
        data: expect.objectContaining({
          status: JobStatus.error,
          errorCode: '651',
        }),
      }),
    );
  });
});
