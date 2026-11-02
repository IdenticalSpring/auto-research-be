import { jest } from '@jest/globals';
import { NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { TranslateAiService } from './translate-ai.service';
import { WriteContentMongoHelper } from './write-content-mongo.helper';

describe('TranslateAiService', () => {
  let service: TranslateAiService;
  let prismaMock: Record<string, any>;
  let producerMock: Record<string, any>;
  let correlationMock: Record<string, any>;

  beforeEach(async () => {
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
      registerPending: jest.fn(() =>
        Promise.resolve({ event_type: 'translate_report' }),
      ),
      clearPending: jest.fn(),
    };
    const configMock = { get: jest.fn(() => 'write_section_request') };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        TranslateAiService,
        { provide: PrismaService, useValue: prismaMock },
        { provide: KafkaProducerService, useValue: producerMock },
        { provide: RequestCorrelationService, useValue: correlationMock },
        { provide: ConfigService, useValue: configMock },
      ],
    }).compile();

    service = module.get<TranslateAiService>(TranslateAiService);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('toàn văn: gửi is_outline=false và đọc translatedContent từ Mongo', async () => {
    jest
      .spyOn(WriteContentMongoHelper, 'readDocumentConfiguration')
      .mockResolvedValue({ translatedContent: 'Bản dịch' } as any);

    const res = await service.translate('user-1', 'doc-1', {
      targetLanguage: 'English',
    } as any);

    const payload = producerMock.publish.mock.calls[0][1] as Record<
      string,
      unknown
    >;
    expect(payload.event_type).toBe('translate_report');
    expect(payload.target_language).toBe('English');
    expect(payload.is_outline).toBe(false);
    expect(payload.user_id).toBe('user-1');
    expect(payload.document_id).toBe(
      WriteContentMongoHelper.toMongoDocId('doc-1'),
    );
    expect(res.translated_field).toBe('translatedContent');
    expect(res.content).toBe('Bản dịch');
  });

  it('đề cương: is_outline=true ⇒ đọc translatedOutline', async () => {
    const spy = jest
      .spyOn(WriteContentMongoHelper, 'readDocumentConfiguration')
      .mockResolvedValue({ translatedOutline: ['Chương 1'] } as any);

    const res = await service.translate('user-1', 'doc-1', {
      targetLanguage: 'Tiếng Việt',
      isOutline: true,
    } as any);

    expect(spy).toHaveBeenCalledWith(
      WriteContentMongoHelper.toMongoDocId('doc-1'),
      ['translatedOutline'],
    );
    expect(res.translated_field).toBe('translatedOutline');
    expect(res.content).toEqual(['Chương 1']);
  });

  it('Mongo không đọc được ⇒ content=null nhưng job vẫn finished', async () => {
    jest
      .spyOn(WriteContentMongoHelper, 'readDocumentConfiguration')
      .mockRejectedValue(new Error('mongo down'));

    const res = await service.translate('user-1', 'doc-1', {
      targetLanguage: 'English',
    } as any);

    expect(res.content).toBeNull();
    expect(prismaMock.job.update).toHaveBeenLastCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({ status: 'finished' }),
      }),
    );
  });

  it('document không thuộc user ⇒ 404, không publish', async () => {
    prismaMock.document.findFirst = jest.fn(() => Promise.resolve(null));

    await expect(
      service.translate('user-2', 'doc-1', { targetLanguage: 'English' } as any),
    ).rejects.toBeInstanceOf(NotFoundException);
    expect(producerMock.publish).not.toHaveBeenCalled();
  });

  it('publish lỗi ⇒ job error 610', async () => {
    producerMock.publish = jest.fn(() =>
      Promise.reject(new Error('kafka down')),
    );

    await expect(
      service.translate('user-1', 'doc-1', { targetLanguage: 'English' } as any),
    ).rejects.toThrow();

    expect(prismaMock.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({ errorCode: '610' }),
      }),
    );
  });

  it('AI trả rejection ⇒ cập nhật job error với code 641 và ném BadRequestException', async () => {
    correlationMock.registerPending.mockRejectedValueOnce({
      error_code: 641,
      error: 'Translation service unavailable',
    });

    await expect(
      service.translate('user-1', 'doc-1', {
        targetLanguage: 'English',
      } as any),
    ).rejects.toThrow();

    expect(prismaMock.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'job-1' },
        data: expect.objectContaining({
          status: 'error',
          errorCode: '641',
          errorMessage: 'Translation service unavailable',
        }),
      }),
    );
  });
});
