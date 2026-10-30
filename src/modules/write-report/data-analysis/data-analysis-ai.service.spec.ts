import { jest } from '@jest/globals';
import { BadRequestException, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { DataAnalysisAiService } from './data-analysis-ai.service';

const variables = [
  {
    name: 'Tuổi',
    code: 'age',
    variable_type: 'observed',
    role: 'independent',
    scale: 'ratio',
  },
];

const commentBody = {
  finalProposal: { title: 'Đề tài A' },
  data: { age: [22, 35, 41] },
  variables,
};

describe('DataAnalysisAiService', () => {
  let service: DataAnalysisAiService;
  let prismaMock: Record<string, any>;
  let producerMock: Record<string, any>;
  let correlationMock: Record<string, any>;

  beforeEach(async () => {
    prismaMock = {
      dataset: {
        findFirst: jest.fn(() =>
          Promise.resolve({ id: 'ds-1', documentId: 'doc-1' }),
        ),
        update: jest.fn(() => Promise.resolve({})),
      },
      job: {
        create: jest.fn(() => Promise.resolve({ id: 'job-1' })),
        update: jest.fn(() => Promise.resolve({})),
      },
    };
    producerMock = { publish: jest.fn(() => Promise.resolve()) };
    correlationMock = {
      buildCorrelationKey: jest.fn(() => 'key-1'),
      registerPending: jest.fn(() => Promise.resolve({ comments: ['Hợp lệ'] })),
      clearPending: jest.fn(),
    };
    const configMock = {
      get: jest.fn(() => 'write_section_request_local'),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        DataAnalysisAiService,
        { provide: PrismaService, useValue: prismaMock },
        { provide: KafkaProducerService, useValue: producerMock },
        { provide: RequestCorrelationService, useValue: correlationMock },
        { provide: ConfigService, useValue: configMock },
      ],
    }).compile();

    service = module.get<DataAnalysisAiService>(DataAnalysisAiService);
  });

  it('comment_data: gửi đúng event_type + luôn kèm 3 identifier + thread_id và lưu analyzeLog', async () => {
    const res = await service.commentData('user-1', 'ds-1', commentBody as any);

    const payload = producerMock.publish.mock.calls[0][1] as Record<
      string,
      unknown
    >;
    expect(payload.event_type).toBe('comment_data');
    expect(payload.user_id).toBe('user-1');
    expect(payload.document_id).toBe('doc-1');
    expect(payload.session_id).toBeTruthy();
    expect(payload.thread_id).toBeTruthy();
    expect(payload.variables).toHaveLength(1);
    expect(correlationMock.registerPending).toHaveBeenCalledWith(
      'key-1',
      300_000,
      'comment_data',
      'job-1',
    );
    expect(prismaMock.dataset.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'ds-1' },
        data: { analyzeLog: JSON.stringify(['Hợp lệ']) },
      }),
    );
    expect(res.job_id).toBe('job-1');
    expect(res.event_type).toBe('comment_data');
  });

  it('check_logic_data: chỉ gửi variables + identifier, không gửi thread_id', async () => {
    await service.checkLogicData('user-1', 'ds-1', { variables } as any);

    const payload = producerMock.publish.mock.calls[0][1] as Record<
      string,
      unknown
    >;
    expect(payload.event_type).toBe('check_logic_data');
    expect(payload.user_id).toBe('user-1');
    expect(payload.document_id).toBe('doc-1');
    expect(payload.variables).toHaveLength(1);
    expect(payload.thread_id).toBeUndefined();
  });

  it('AI trả rejection ⇒ cập nhật job error và ném BadRequestException', async () => {
    correlationMock.registerPending.mockRejectedValueOnce({
      error_code: 636,
      error: 'Data distribution error',
    });

    await expect(
      service.commentData('user-1', 'ds-1', commentBody as any),
    ).rejects.toThrow(BadRequestException);

    expect(prismaMock.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'job-1' },
        data: expect.objectContaining({
          status: 'error',
          errorCode: '636',
          errorMessage: 'Data distribution error',
        }),
      }),
    );
  });

  it('dataset của user khác ⇒ 404 và không publish', async () => {
    prismaMock.dataset.findFirst = jest.fn(() => Promise.resolve(null));

    await expect(
      service.commentData('user-2', 'ds-1', commentBody as any),
    ).rejects.toBeInstanceOf(NotFoundException);
    expect(producerMock.publish).not.toHaveBeenCalled();
  });

  it('publish lỗi ⇒ job chuyển error với mã 610', async () => {
    producerMock.publish = jest.fn(() =>
      Promise.reject(new Error('kafka down')),
    );

    await expect(
      service.checkLogicData('user-1', 'ds-1', { variables } as any),
    ).rejects.toThrow();

    expect(correlationMock.clearPending).toHaveBeenCalledWith(
      'key-1',
      'kafka down',
    );
    expect(prismaMock.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({ errorCode: '610' }),
      }),
    );
  });
});
