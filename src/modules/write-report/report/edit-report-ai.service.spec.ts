import { jest } from '@jest/globals';
import { BadRequestException, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { EditReportAiService } from './edit-report-ai.service';
import { WriteContentMongoHelper } from './write-content-mongo.helper';

describe('EditReportAiService', () => {
  let service: EditReportAiService;
  let prismaMock: Record<string, any>;
  let producerMock: Record<string, any>;
  let correlationMock: Record<string, any>;
  let pendingResponse: Record<string, unknown>;

  beforeEach(async () => {
    pendingResponse = {
      section_name: 'Mở đầu',
      content: 'Nội dung đã được biên tập súc tích hơn.',
      has_finished: true,
    };

    prismaMock = {
      document: { findFirst: jest.fn(() => Promise.resolve({ id: 'doc-1' })) },
      job: {
        create: jest.fn(() => Promise.resolve({ id: 'job-1' })),
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
        EditReportAiService,
        { provide: PrismaService, useValue: prismaMock },
        { provide: KafkaProducerService, useValue: producerMock },
        { provide: RequestCorrelationService, useValue: correlationMock },
        { provide: ConfigService, useValue: configMock },
      ],
    }).compile();

    service = module.get<EditReportAiService>(EditReportAiService);
  });

  it('edit: gửi đúng event_type, section_name, content, instruction và trả về nội dung đã chỉnh sửa', async () => {
    const res = await service.edit('user-1', 'doc-1', {
      section_name: 'Mở đầu',
      content: 'Nội dung ban đầu còn dài dòng...',
      instruction: 'viết súc tích hơn',
      context: 'Nghiên cứu về chuyển đổi số',
    });

    const payload = producerMock.publish.mock.calls[0][1] as Record<
      string,
      unknown
    >;
    expect(payload.event_type).toBe('edit_report');
    expect(payload.user_id).toBe('user-1');
    expect(payload.document_id).toBe(
      WriteContentMongoHelper.toMongoDocId('doc-1'),
    );
    expect(payload.section_name).toBe('Mở đầu');
    expect(payload.content).toBe('Nội dung ban đầu còn dài dòng...');
    expect(payload.instruction).toBe('viết súc tích hơn');
    expect(payload.context).toBe('Nghiên cứu về chuyển đổi số');

    expect(prismaMock.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'job-1' },
        data: expect.objectContaining({ status: 'finished' }),
      }),
    );
    expect(res.job_id).toBe('job-1');
    expect(res.section_name).toBe('Mở đầu');
    expect(res.content).toBe('Nội dung đã được biên tập súc tích hơn.');
  });

  it('AI trả rejection ⇒ cập nhật job error với code 630 và ném BadRequestException', async () => {
    correlationMock.registerPending.mockRejectedValueOnce({
      error_code: 630,
      error: 'Cannot edit text',
    });

    await expect(
      service.edit('user-1', 'doc-1', {
        section_name: 'Mở đầu',
        content: 'Text',
        instruction: 'Fix it',
      }),
    ).rejects.toThrow(BadRequestException);

    expect(prismaMock.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'job-1' },
        data: expect.objectContaining({
          status: 'error',
          errorCode: '630',
          errorMessage: 'Cannot edit text',
        }),
      }),
    );
  });

  it('document không thuộc user ⇒ 404, không publish', async () => {
    prismaMock.document.findFirst = jest.fn(() => Promise.resolve(null));

    await expect(
      service.edit('user-2', 'doc-1', {
        section_name: 'Mở đầu',
        content: 'Text',
        instruction: 'Fix it',
      }),
    ).rejects.toBeInstanceOf(NotFoundException);
    expect(producerMock.publish).not.toHaveBeenCalled();
  });

  it('publish Kafka lỗi ⇒ cập nhật job error với code 610', async () => {
    producerMock.publish = jest.fn(() =>
      Promise.reject(new Error('Broker unreachable')),
    );

    await expect(
      service.edit('user-1', 'doc-1', {
        section_name: 'Mở đầu',
        content: 'Text',
        instruction: 'Fix it',
      }),
    ).rejects.toThrow();

    expect(correlationMock.clearPending).toHaveBeenCalledWith(
      'key-1',
      'Broker unreachable',
    );
    expect(prismaMock.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({ errorCode: '610' }),
      }),
    );
  });
});
