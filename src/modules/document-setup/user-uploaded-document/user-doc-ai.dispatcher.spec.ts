import { jest } from '@jest/globals';
import { BadRequestException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { JobStatus, ModuleType } from '@prisma/client';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { UserDocAiDispatcher } from './user-doc-ai.dispatcher';

describe('UserDocAiDispatcher', () => {
  let dispatcher: UserDocAiDispatcher;
  let mockPrisma: Record<string, any>;
  let mockKafkaProducer: Record<string, any>;
  let mockCorrelation: Record<string, any>;
  let mockConfigService: Record<string, any>;

  const userId = '00000000-0000-0000-0000-000000000001';
  const documentId = '00000000-0000-0000-0000-000000000002';

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
      buildCorrelationKey: jest.fn<any>().mockReturnValue('corr-key-doc'),
      registerPending: jest.fn<any>(),
      clearPending: jest.fn<any>(),
    };

    mockConfigService = {
      get: jest.fn<any>().mockReturnValue('document_setup_request'),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        UserDocAiDispatcher,
        { provide: PrismaService, useValue: mockPrisma },
        { provide: KafkaProducerService, useValue: mockKafkaProducer },
        { provide: RequestCorrelationService, useValue: mockCorrelation },
        { provide: ConfigService, useValue: mockConfigService },
      ],
    }).compile();

    dispatcher = module.get<UserDocAiDispatcher>(UserDocAiDispatcher);
  });

  it('should dispatch successfully, trigger onSuccess and finish job', async () => {
    mockPrisma.job.create.mockResolvedValue({ id: 'job-doc-1' });
    mockPrisma.job.update.mockResolvedValue({});
    mockCorrelation.registerPending.mockResolvedValue({ parsed: true });
    const onSuccess = jest.fn<any>().mockResolvedValue(undefined);

    const res = await dispatcher.dispatch({
      userId,
      documentId,
      eventType: 'user_documents',
      errorCode: 620,
      dto: {},
      kafkaData: { file_id: 'f-1' },
      onSuccess,
    });

    expect(res.jobId).toBe('job-doc-1');
    expect(res.parsed).toBe(true);
    expect(onSuccess).toHaveBeenCalledWith({ parsed: true });
    expect(mockPrisma.job.create).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          module: ModuleType.document_setup,
          action: 'user_documents',
        }),
      }),
    );
    expect(mockPrisma.job.update).toHaveBeenCalledWith({
      where: { id: 'job-doc-1' },
      data: { status: JobStatus.finished, result: { parsed: true } },
    });
  });

  it('should handle kafka publish error, invoke onError and fail job', async () => {
    mockPrisma.job.create.mockResolvedValue({ id: 'job-fail-pub' });
    mockPrisma.job.update.mockResolvedValue({});
    mockKafkaProducer.publish.mockRejectedValue(
      new Error('Broker unreachable'),
    );
    const onError = jest.fn<any>().mockResolvedValue(undefined);

    await expect(
      dispatcher.dispatch({
        userId,
        documentId,
        eventType: 'user_documents',
        errorCode: 620,
        dto: {},
        kafkaData: {},
        onError,
      }),
    ).rejects.toThrow(BadRequestException);

    expect(onError).toHaveBeenCalled();
    expect(mockCorrelation.clearPending).toHaveBeenCalledWith(
      'corr-key-doc',
      'Broker unreachable',
    );
    expect(mockPrisma.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'job-fail-pub' },
        data: expect.objectContaining({
          status: JobStatus.error,
          errorCode: '620',
        }),
      }),
    );
  });

  it('should handle correlation pending error, invoke onError and fail job with custom code', async () => {
    mockPrisma.job.create.mockResolvedValue({ id: 'job-corr-fail' });
    mockPrisma.job.update.mockResolvedValue({});
    mockCorrelation.registerPending.mockRejectedValue({
      error_code: 621,
      error: 'Qdrant collection error',
    });
    const onError = jest.fn<any>().mockResolvedValue(undefined);

    try {
      await dispatcher.dispatch({
        userId,
        documentId,
        eventType: 'embed_user_documents',
        errorCode: 621,
        dto: {},
        kafkaData: {},
        onError,
      });
      fail('Should throw');
    } catch (err: any) {
      expect(err).toBeInstanceOf(BadRequestException);
      const resp = err.getResponse();
      expect(resp.errorCode).toBe(621);
      expect(resp.message).toBe('Qdrant collection error');
    }

    expect(onError).toHaveBeenCalled();
    expect(mockPrisma.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'job-corr-fail' },
        data: expect.objectContaining({
          status: JobStatus.error,
          errorCode: '621',
        }),
      }),
    );
  });
});
