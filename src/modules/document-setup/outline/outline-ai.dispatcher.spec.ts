import { jest } from '@jest/globals';
import { BadRequestException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { JobStatus, ModuleType } from '@prisma/client';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { OutlineAiDispatcher } from './outline-ai.dispatcher';

describe('OutlineAiDispatcher', () => {
  let dispatcher: OutlineAiDispatcher;
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
      buildCorrelationKey: jest.fn<any>().mockReturnValue('corr-key-outline'),
      registerPending: jest.fn<any>(),
      clearPending: jest.fn<any>(),
    };

    mockConfigService = {
      get: jest.fn<any>().mockReturnValue('document_setup_request'),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OutlineAiDispatcher,
        { provide: PrismaService, useValue: mockPrisma },
        { provide: KafkaProducerService, useValue: mockKafkaProducer },
        { provide: RequestCorrelationService, useValue: mockCorrelation },
        { provide: ConfigService, useValue: mockConfigService },
      ],
    }).compile();

    dispatcher = module.get<OutlineAiDispatcher>(OutlineAiDispatcher);
  });

  it('should dispatch successfully and complete job', async () => {
    mockPrisma.job.create.mockResolvedValue({ id: 'job-out-1' });
    mockPrisma.job.update.mockResolvedValue({});
    mockCorrelation.registerPending.mockResolvedValue({
      outline: { sections: [] },
    });
    const onSuccess = jest.fn<any>().mockResolvedValue(undefined);

    const res = await dispatcher.dispatch({
      userId,
      documentId,
      eventType: 'generate_outline',
      errorCode: 617,
      dto: {},
      kafkaData: { title: 'T' },
      onSuccess,
    });

    expect(res.jobId).toBe('job-out-1');
    expect(res.outline).toEqual({ sections: [] });
    expect(onSuccess).toHaveBeenCalledWith({ outline: { sections: [] } });
    expect(mockPrisma.job.create).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          module: ModuleType.document_setup,
          action: 'generate_outline',
        }),
      }),
    );
    expect(mockPrisma.job.update).toHaveBeenCalledWith({
      where: { id: 'job-out-1' },
      data: {
        status: JobStatus.finished,
        result: { outline: { sections: [] } },
      },
    });
  });

  it('should handle kafka publish error and update job to error', async () => {
    mockPrisma.job.create.mockResolvedValue({ id: 'job-out-pub-fail' });
    mockPrisma.job.update.mockResolvedValue({});
    mockKafkaProducer.publish.mockRejectedValue(
      new Error('Broker disconnected'),
    );

    await expect(
      dispatcher.dispatch({
        userId,
        documentId,
        eventType: 'generate_outline',
        errorCode: 617,
        dto: {},
        kafkaData: {},
      }),
    ).rejects.toThrow(BadRequestException);

    expect(mockCorrelation.clearPending).toHaveBeenCalledWith(
      'corr-key-outline',
      'Broker disconnected',
    );
    expect(mockPrisma.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'job-out-pub-fail' },
        data: expect.objectContaining({
          status: JobStatus.error,
          errorCode: '617',
        }),
      }),
    );
  });

  it('should handle correlation pending error and fail job with custom code', async () => {
    mockPrisma.job.create.mockResolvedValue({ id: 'job-out-corr-fail' });
    mockPrisma.job.update.mockResolvedValue({});
    mockCorrelation.registerPending.mockRejectedValue({
      error_code: 627,
      error: 'Outline error message',
    });

    try {
      await dispatcher.dispatch({
        userId,
        documentId,
        eventType: 'modify_outline',
        errorCode: 627,
        dto: {},
        kafkaData: {},
      });
      fail('Should throw');
    } catch (err: any) {
      expect(err).toBeInstanceOf(BadRequestException);
      const resp = err.getResponse();
      expect(resp.errorCode).toBe(627);
      expect(resp.message).toBe('Outline error message');
    }

    expect(mockPrisma.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'job-out-corr-fail' },
        data: expect.objectContaining({
          status: JobStatus.error,
          errorCode: '627',
        }),
      }),
    );
  });
});
