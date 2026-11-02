import { jest } from '@jest/globals';
import { BadRequestException, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { WriteContentMongoHelper } from '../report/write-content-mongo.helper';
import { ParsedValueAiService } from './parsed-value-ai.service';

const surveyQuestions = [
  {
    variable_name: 'Độ tin cậy',
    variable_code: 'REL',
    variable_type: 'latent',
    measurement_type: 'likert',
    questions: ['Hệ thống hoạt động ổn định', 'Hệ thống ít khi gặp lỗi'],
    answer_content: ['1 - Hoàn toàn không đồng ý', '5 - Hoàn toàn đồng ý'],
  },
];

describe('ParsedValueAiService', () => {
  let service: ParsedValueAiService;
  let prismaMock: Record<string, any>;
  let producerMock: Record<string, any>;
  let correlationMock: Record<string, any>;
  let pendingResponse: Record<string, unknown>;

  beforeEach(async () => {
    pendingResponse = {
      parsed_variables: [
        {
          variable_code: 'REL1',
          variable_name: 'Độ tin cậy 1',
          scale: 'interval',
        },
      ],
    };

    prismaMock = {
      document: { findFirst: jest.fn(() => Promise.resolve({ id: 'doc-1' })) },
      job: {
        create: jest.fn(() => Promise.resolve({ id: 'job-1' })),
        update: jest.fn(() => Promise.resolve({})),
      },
      proposedMethod: {
        findFirst: jest.fn(() =>
          Promise.resolve({ id: 'pm-1', variables: [{ name: 'REL' }] }),
        ),
        update: jest.fn(() => Promise.resolve({})),
      },
    };
    producerMock = { publish: jest.fn(() => Promise.resolve()) };
    correlationMock = {
      buildCorrelationKey: jest.fn(() => 'key-1'),
      registerPending: jest.fn(() => Promise.resolve(pendingResponse)),
      clearPending: jest.fn(),
    };
    const configMock = { get: jest.fn(() => 'write_section_request') };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ParsedValueAiService,
        { provide: PrismaService, useValue: prismaMock },
        { provide: KafkaProducerService, useValue: producerMock },
        { provide: RequestCorrelationService, useValue: correlationMock },
        { provide: ConfigService, useValue: configMock },
      ],
    }).compile();

    service = module.get<ParsedValueAiService>(ParsedValueAiService);
  });

  it('parseValue: gửi đúng event_type, survey_questions và cập nhật ProposedMethod variables', async () => {
    const res = await service.parseValue('user-1', 'doc-1', {
      survey_questions: surveyQuestions,
      variables: [{ name: 'REL' }],
    });

    const payload = producerMock.publish.mock.calls[0][1] as Record<
      string,
      unknown
    >;
    expect(payload.event_type).toBe('parsed_value');
    expect(payload.user_id).toBe('user-1');
    expect(payload.document_id).toBe(
      WriteContentMongoHelper.toMongoDocId('doc-1'),
    );
    expect(payload.survey_questions).toEqual(surveyQuestions);
    expect(payload.variables).toEqual([{ name: 'REL' }]);
    expect(prismaMock.proposedMethod.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'pm-1' },
        data: {
          variables: pendingResponse.parsed_variables,
        },
      }),
    );
    expect(res.job_id).toBe('job-1');
    expect(res.event_type).toBe('parsed_value');
  });

  it('parseValue: tự động lấy variables từ DB nếu client không truyền', async () => {
    await service.parseValue('user-1', 'doc-1', {
      survey_questions: surveyQuestions,
    });

    const payload = producerMock.publish.mock.calls[0][1] as Record<
      string,
      unknown
    >;
    expect(payload.variables).toEqual([{ name: 'REL' }]);
  });

  it('AI trả rejection ⇒ cập nhật job error với code 639 và ném BadRequestException', async () => {
    correlationMock.registerPending.mockRejectedValueOnce({
      error_code: 639,
      error: 'Invalid survey scale',
    });

    await expect(
      service.parseValue('user-1', 'doc-1', {
        survey_questions: surveyQuestions,
      }),
    ).rejects.toThrow(BadRequestException);

    expect(prismaMock.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'job-1' },
        data: expect.objectContaining({
          status: 'error',
          errorCode: '639',
          errorMessage: 'Invalid survey scale',
        }),
      }),
    );
  });

  it('document không thuộc user ⇒ 404, không publish', async () => {
    prismaMock.document.findFirst = jest.fn(() => Promise.resolve(null));

    await expect(
      service.parseValue('user-2', 'doc-1', {
        survey_questions: surveyQuestions,
      }),
    ).rejects.toBeInstanceOf(NotFoundException);
    expect(producerMock.publish).not.toHaveBeenCalled();
  });

  it('publish Kafka lỗi ⇒ cập nhật job error với code 610', async () => {
    producerMock.publish = jest.fn(() =>
      Promise.reject(new Error('Kafka broker unavailable')),
    );

    await expect(
      service.parseValue('user-1', 'doc-1', {
        survey_questions: surveyQuestions,
      }),
    ).rejects.toThrow();

    expect(correlationMock.clearPending).toHaveBeenCalledWith(
      'key-1',
      'Kafka broker unavailable',
    );
    expect(prismaMock.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({ errorCode: '610' }),
      }),
    );
  });
});
