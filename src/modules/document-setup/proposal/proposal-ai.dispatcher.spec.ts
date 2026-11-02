import { jest } from '@jest/globals';
import { BadRequestException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { JobStatus, ModuleType } from '@prisma/client';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { ProposalAiDispatcher } from './proposal-ai.dispatcher';

describe('ProposalAiDispatcher', () => {
  let dispatcher: ProposalAiDispatcher;
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
      buildCorrelationKey: jest.fn<any>().mockReturnValue('corr-key-1'),
      registerPending: jest.fn<any>(),
      clearPending: jest.fn<any>(),
    };

    mockConfigService = {
      get: jest.fn<any>().mockReturnValue('document_setup_request'),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ProposalAiDispatcher,
        { provide: PrismaService, useValue: mockPrisma },
        { provide: KafkaProducerService, useValue: mockKafkaProducer },
        { provide: RequestCorrelationService, useValue: mockCorrelation },
        { provide: ConfigService, useValue: mockConfigService },
      ],
    }).compile();

    dispatcher = module.get<ProposalAiDispatcher>(ProposalAiDispatcher);
  });

  it('should dispatch successfully and complete job', async () => {
    mockPrisma.job.create.mockResolvedValue({ id: 'job-1' });
    mockPrisma.job.update.mockResolvedValue({});
    mockCorrelation.registerPending.mockResolvedValue({ resultData: 123 });
    const onSuccess = jest.fn<any>().mockResolvedValue(undefined);

    const res = await dispatcher.dispatch({
      userId,
      documentId,
      eventType: 'generate_domains',
      errorCode: 611,
      dto: {},
      kafkaData: { field: 'CS' },
      onSuccess,
    });

    expect(res.jobId).toBe('job-1');
    expect(res.resultData).toBe(123);
    expect(onSuccess).toHaveBeenCalledWith({ resultData: 123 });
    expect(mockPrisma.job.create).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          module: ModuleType.document_setup,
          action: 'generate_domains',
        }),
      }),
    );
    expect(mockPrisma.job.update).toHaveBeenCalledWith({
      where: { id: 'job-1' },
      data: { status: JobStatus.finished, result: { resultData: 123 } },
    });
  });

  it('should handle kafka publish error and update job to error', async () => {
    mockPrisma.job.create.mockResolvedValue({ id: 'job-pub-fail' });
    mockPrisma.job.update.mockResolvedValue({});
    mockKafkaProducer.publish.mockRejectedValue(
      new Error('Broker unreachable'),
    );

    await expect(
      dispatcher.dispatch({
        userId,
        documentId,
        eventType: 'generate_domains',
        errorCode: 611,
        dto: {},
        kafkaData: {},
      }),
    ).rejects.toThrow(BadRequestException);

    expect(mockCorrelation.clearPending).toHaveBeenCalledWith(
      'corr-key-1',
      'Broker unreachable',
    );
    expect(mockPrisma.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'job-pub-fail' },
        data: expect.objectContaining({
          status: JobStatus.error,
          errorCode: '611',
        }),
      }),
    );
  });

  it('should handle correlation pending error and fail job with custom code', async () => {
    mockPrisma.job.create.mockResolvedValue({ id: 'job-corr-fail' });
    mockPrisma.job.update.mockResolvedValue({});
    mockCorrelation.registerPending.mockRejectedValue({
      error_code: 615,
      error: 'Custom AI Error',
    });

    try {
      await dispatcher.dispatch({
        userId,
        documentId,
        eventType: 'generate_title_description',
        errorCode: 615,
        dto: {},
        kafkaData: {},
      });
      fail('Should throw');
    } catch (err: any) {
      expect(err).toBeInstanceOf(BadRequestException);
      const resp = err.getResponse();
      expect(resp.errorCode).toBe(615);
      expect(resp.message).toBe('Custom AI Error');
    }

    expect(mockPrisma.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'job-corr-fail' },
        data: expect.objectContaining({
          status: JobStatus.error,
          errorCode: '615',
        }),
      }),
    );
  });
});
