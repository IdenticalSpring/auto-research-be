import { jest } from '@jest/globals';
import { BadRequestException, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { ProposedMethodAiService } from './proposed-method-ai.service';
import { WriteContentMongoHelper } from '../report/write-content-mongo.helper';

describe('ProposedMethodAiService', () => {
  let service: ProposedMethodAiService;
  let prismaMock: Record<string, any>;
  let producerMock: Record<string, any>;
  let correlationMock: Record<string, any>;
  let pendingResponse: Record<string, unknown>;

  beforeEach(async () => {
    pendingResponse = { has_finished: true, final_proposal: {} };

    prismaMock = {
      document: { findFirst: jest.fn(() => Promise.resolve({ id: 'doc-1' })) },
      job: {
        create: jest.fn(() => Promise.resolve({ id: 'job-1' })),
        update: jest.fn(() => Promise.resolve({})),
      },
      proposedMethod: {
        findFirst: jest.fn(() => Promise.resolve(null)),
        create: jest.fn(() => Promise.resolve({ id: 'pm-1' })),
        update: jest.fn(() => Promise.resolve({ id: 'pm-1' })),
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
        ProposedMethodAiService,
        { provide: PrismaService, useValue: prismaMock },
        { provide: KafkaProducerService, useValue: producerMock },
        { provide: RequestCorrelationService, useValue: correlationMock },
        { provide: ConfigService, useValue: configMock },
      ],
    }).compile();

    service = module.get<ProposedMethodAiService>(ProposedMethodAiService);
  });

  it('start: gửi đúng payload với default auto_mode=false và resume_method=false', async () => {
    await service.start('user-1', 'doc-1', {
      finalProposal: { title: 'Đề tài' },
      researchType: { type: 1 },
    } as any);

    const payload = producerMock.publish.mock.calls[0][1] as Record<
      string,
      unknown
    >;
    expect(payload.event_type).toBe('propose_method');
    expect(payload.user_id).toBe('user-1');
    expect(payload.document_id).toBe(
      WriteContentMongoHelper.toMongoDocId('doc-1'),
    );
    expect(payload.session_id).toBeTruthy();
    expect(payload.thread_id).toBeTruthy();
    expect(payload.auto_mode).toBe(false);
    expect(payload.inherit_mode).toBe(false);
    expect(payload.resume_method).toBe(false);
    expect(payload.research_type).toEqual({ type: 1 });
    expect(payload.db).toBe('redis');
  });

  it('resume: bật resume_method và giữ nguyên thread_id', async () => {
    await service.resume('user-1', 'doc-1', {
      threadId: 'thread-9',
      userInput: 'Chọn mô hình 2 biến trung gian',
    } as any);

    const payload = producerMock.publish.mock.calls[0][1] as Record<
      string,
      unknown
    >;
    expect(payload.resume_method).toBe(true);
    expect(payload.thread_id).toBe('thread-9');
    expect(payload.user_input).toBe('Chọn mô hình 2 biến trung gian');
  });

  it('resume: tự động khôi phục finalProposal từ ProposedMethod nếu client không truyền', async () => {
    prismaMock.proposedMethod.findFirst = jest.fn(() =>
      Promise.resolve({
        id: 'pm-1',
        researchModel: { iv: ['X1'], dv: 'Y' },
        hypotheses: [{ code: 'H1' }],
        variables: [{ name: 'X1' }],
        surveyQuestions: [{ q: 'Q1' }],
        questions: ['RQ1'],
      }),
    );

    await service.resume('user-1', 'doc-1', {
      threadId: 'thread-9',
      userInput: 'Đồng ý với các biến trên',
    } as any);

    const payload = producerMock.publish.mock.calls[0][1] as Record<
      string,
      unknown
    >;
    expect(payload.final_proposal).toEqual({
      final_model: { iv: ['X1'], dv: 'Y' },
      hypothesis: [{ code: 'H1' }],
      variables: [{ name: 'X1' }],
      survey_questions: [{ q: 'Q1' }],
      questions: ['RQ1'],
    });
  });

  it('AI hỏi lại (has_finished=false) ⇒ job suspended + trả ai_message', async () => {
    pendingResponse = {
      has_finished: false,
      ai_message: 'Bạn chọn thang đo nào?',
    };

    const res = await service.start('user-1', 'doc-1', {
      finalProposal: { title: 'X' },
    } as any);

    expect(res.status).toBe('suspended');
    expect(res.ai_message).toBe('Bạn chọn thang đo nào?');
    expect(prismaMock.job.update).toHaveBeenLastCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          status: 'suspended',
          resumeStep: 'need_propose_method',
        }),
      }),
    );
    expect(prismaMock.proposedMethod.create).not.toHaveBeenCalled();
  });

  it('hoàn tất ⇒ lưu ProposedMethod + job finished', async () => {
    pendingResponse = {
      has_finished: true,
      final_proposal: {
        final_model: { iv: ['X1'] },
        hypothesis: [{ code: 'H1' }],
        variables: [{ name: 'X1' }],
        survey_questions: [{ q: 'Câu 1' }],
        questions: ['RQ1'],
        parsed_variables: [{ name: 'X1', scale: 'interval' }],
      },
    };

    const res = await service.start('user-1', 'doc-1', {
      finalProposal: { title: 'X' },
    } as any);

    expect(res.status).toBe('finished');
    expect(prismaMock.proposedMethod.create).toHaveBeenCalledWith({
      data: expect.objectContaining({
        documentId: 'doc-1',
        researchModel: { iv: ['X1'] },
        hypotheses: [{ code: 'H1' }],
        questions: ['RQ1'],
      }),
    });
    expect(prismaMock.job.update).toHaveBeenLastCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({ status: 'finished' }),
      }),
    );
  });

  it('AI trả lỗi rejection ⇒ cập nhật job error 637 và ném BadRequestException', async () => {
    correlationMock.registerPending.mockReturnValueOnce(
      Promise.reject({
        error_code: 637,
        error: 'Hypothesis invalid',
      }),
    );

    await expect(
      service.start('user-1', 'doc-1', { finalProposal: { title: 'X' } } as any),
    ).rejects.toThrow(BadRequestException);

    expect(prismaMock.job.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'job-1' },
        data: expect.objectContaining({
          status: 'error',
          errorCode: '637',
          errorMessage: 'Hypothesis invalid',
        }),
      }),
    );
  });

  it('document không thuộc user ⇒ 404, không publish', async () => {
    prismaMock.document.findFirst = jest.fn(() => Promise.resolve(null));

    await expect(
      service.start('user-2', 'doc-1', { finalProposal: { title: 'X' } } as any),
    ).rejects.toBeInstanceOf(NotFoundException);
    expect(producerMock.publish).not.toHaveBeenCalled();
  });
});
