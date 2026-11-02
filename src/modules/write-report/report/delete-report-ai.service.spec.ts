import { jest } from '@jest/globals';
import { NotFoundException } from '@nestjs/common';
import { JobStatus } from '@prisma/client';
import { DeleteReportAiService } from './delete-report-ai.service';
import { WriteContentMongoHelper } from './write-content-mongo.helper';

const USER_ID = 'user-1';
const DOC_A = '11111111-1111-4111-8111-111111111111';
const DOC_B = '22222222-2222-4222-8222-222222222222';

describe('DeleteReportAiService', () => {
  let service: DeleteReportAiService;
  let prisma: Record<string, any>;
  let kafkaProducer: Record<string, any>;
  let correlation: Record<string, any>;
  let config: Record<string, any>;
  let responses: Array<Record<string, unknown>>;

  beforeEach(() => {
    responses = [{}, {}];
    prisma = {
      document: { findMany: jest.fn(() => Promise.resolve([{ id: DOC_A }, { id: DOC_B }])) },
      job: {
        create: jest.fn(() => Promise.resolve({ id: 'job-1' })),
        update: jest.fn(() => Promise.resolve({ id: 'job-1' })),
      },
    };
    kafkaProducer = { publish: jest.fn(() => Promise.resolve(undefined)) };
    correlation = {
      buildCorrelationKey: jest.fn(() => 'corr-key'),
      registerPending: jest.fn(() =>
        Promise.resolve(responses.shift() ?? ({} as Record<string, unknown>)),
      ),
      clearPending: jest.fn(),
    };
    config = { get: jest.fn(() => 'write_section_request') };

    service = new DeleteReportAiService(
      prisma as never,
      kafkaProducer as never,
      correlation as never,
      config as never,
    );
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('xoá thành công nhiều document: gửi 1 message mỗi doc và job finished', async () => {
    jest
      .spyOn(WriteContentMongoHelper, 'findRemainingReportRefs')
      .mockResolvedValue([]);

    const result = await service.delete(USER_ID, { documentIds: [DOC_A, DOC_B] });

    expect(kafkaProducer.publish).toHaveBeenCalledTimes(2);
    expect(correlation.registerPending).toHaveBeenCalledTimes(2);
    expect(prisma.job.update).toHaveBeenLastCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({ status: JobStatus.finished }),
      }),
    );
    expect(result.deleted_ids).toEqual([DOC_A, DOC_B]);
    expect(result.failed_ids).toEqual([]);
    expect(result.verified).toBe(true);
  });

  it('payload mỗi message chỉ chứa 1 document_id và đủ identifier', async () => {
    jest
      .spyOn(WriteContentMongoHelper, 'findRemainingReportRefs')
      .mockResolvedValue([]);

    await service.delete(USER_ID, {
      documentIds: [DOC_A],
      threadIds: ['thread-1'],
    });

    const payload = kafkaProducer.publish.mock.calls[0][1] as Record<
      string,
      unknown
    >;
    expect(payload.event_type).toBe('delete_report');
    expect(payload.document_id).toBe(DOC_A);
    expect(payload.user_id).toBe(USER_ID);
    expect(payload.session_id).toEqual(expect.any(String));
    expect(payload.model_id).toEqual(expect.any(String));
    expect(payload.thread_id).toEqual(['thread-1']);
  });

  it('AI trả error_code 640 cho 1 doc: job error 640 và giữ đúng failed_ids', async () => {
    responses = [{ error_code: 640, failed_ids: [DOC_A] }, {}];
    jest
      .spyOn(WriteContentMongoHelper, 'findRemainingReportRefs')
      .mockResolvedValue([]);

    const result = await service.delete(USER_ID, { documentIds: [DOC_A, DOC_B] });

    expect(result.failed_ids).toEqual([DOC_A]);
    expect(result.deleted_ids).toEqual([DOC_B]);
    expect(prisma.job.update).toHaveBeenLastCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          status: JobStatus.error,
          errorCode: '640',
        }),
      }),
    );
  });

  it('AI báo thành công nhưng Mongo còn record: vẫn coi là lỗi 640 (tự xác minh)', async () => {
    jest
      .spyOn(WriteContentMongoHelper, 'findRemainingReportRefs')
      .mockResolvedValue([DOC_B]);

    const result = await service.delete(USER_ID, { documentIds: [DOC_A, DOC_B] });

    expect(result.verified_remaining_ids).toEqual([DOC_B]);
    expect(result.failed_ids).toEqual([DOC_B]);
    expect(result.deleted_ids).toEqual([DOC_A]);
  });

  it('publish lỗi: không throw, clearPending và đánh dấu doc thất bại', async () => {
    kafkaProducer.publish = jest.fn(() => Promise.reject(new Error('broker down')));
    jest
      .spyOn(WriteContentMongoHelper, 'findRemainingReportRefs')
      .mockResolvedValue([]);

    const result = await service.delete(USER_ID, { documentIds: [DOC_A] });

    expect(correlation.clearPending).toHaveBeenCalled();
    expect(result.failed_ids).toEqual([DOC_A]);
    expect(prisma.job.update).toHaveBeenLastCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({ errorCode: '640' }),
      }),
    );
  });

  it('không xác minh được Mongo: verified=false và giữ kết quả AI báo', async () => {
    jest
      .spyOn(WriteContentMongoHelper, 'findRemainingReportRefs')
      .mockRejectedValue(new Error('Thiếu biến môi trường MONGO_URI'));

    const result = await service.delete(USER_ID, { documentIds: [DOC_A, DOC_B] });

    expect(result.verified).toBe(false);
    expect(result.failed_ids).toEqual([]);
    expect(result.deleted_ids).toEqual([DOC_A, DOC_B]);
  });

  it('document không thuộc user: ném NotFoundException và không publish', async () => {
    prisma.document.findMany = jest.fn(() => Promise.resolve([{ id: DOC_A }]));

    await expect(
      service.delete(USER_ID, { documentIds: [DOC_A, DOC_B] }),
    ).rejects.toBeInstanceOf(NotFoundException);
    expect(kafkaProducer.publish).not.toHaveBeenCalled();
    expect(prisma.job.create).not.toHaveBeenCalled();
  });
});
