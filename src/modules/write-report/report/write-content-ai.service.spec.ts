import { jest } from '@jest/globals';
import { BadRequestException, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { JobStatus } from '@prisma/client';
import { Subject } from 'rxjs';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { StreamingManagerService } from '../../../common/streaming/streaming-manager.service';
import { WriteContentAiService } from './write-content-ai.service';
import { WriteContentRequestDto } from './dto/write-content-request.dto';

describe('WriteContentAiService', () => {
  let service: WriteContentAiService;
  let mockPrisma: any;
  let mockKafkaProducer: any;
  let mockCorrelation: any;
  let mockStreamingManager: any;
  let streamSubject: Subject<any>;

  const userId = '00000000-0000-0000-0000-000000000001';
  const documentId = '00000000-0000-0000-0000-000000000002';

  const validDto: WriteContentRequestDto = {
    documentId,
    field: 'Khoa học máy tính',
    domain: 'Trí tuệ nhân tạo',
    referencesStyle: 'APA',
    finalProposal: { title: 'AI in Healthcare', problemStatement: 'Issues' },
    papersSearch: [{ title: 'Paper 1' }],
    outline: { sections: ['Mở đầu', 'Nội dung'] },
    outlineCode: ['chapter_1'],
    language: 'vi',
  };

  beforeEach(async () => {
    streamSubject = new Subject();
    mockPrisma = {
      document: { findUnique: jest.fn<any>() },
      proposal: { findFirst: jest.fn<any>() },
      outline: { findUnique: jest.fn<any>() },
      documentReference: { findMany: jest.fn<any>().mockResolvedValue([]) },
      job: {
        create: jest.fn<any>().mockResolvedValue({ id: 'job-wc-1' }),
        update: jest.fn<any>().mockResolvedValue({}),
        findFirst: jest.fn<any>(),
      },
      report: { upsert: jest.fn<any>().mockResolvedValue({}) },
    };

    mockKafkaProducer = { publish: jest.fn<any>().mockResolvedValue(undefined) };
    mockCorrelation = {
      buildCorrelationKey: jest.fn<any>().mockReturnValue('corr-key-wc'),
      getStreamObservable: jest.fn<any>().mockReturnValue(streamSubject.asObservable()),
      registerPending: jest.fn<any>(),
      clearPending: jest.fn<any>(),
    };
    mockStreamingManager = {
      buildStreamKey: jest.fn<any>().mockReturnValue('stream-key-wc'),
      createStream: jest.fn<any>().mockReturnValue('mock-sse-stream'),
      emitChunk: jest.fn<any>(),
      completeStream: jest.fn<any>(),
      destroyStream: jest.fn<any>(),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        WriteContentAiService,
        { provide: PrismaService, useValue: mockPrisma },
        { provide: KafkaProducerService, useValue: mockKafkaProducer },
        { provide: RequestCorrelationService, useValue: mockCorrelation },
        { provide: StreamingManagerService, useValue: mockStreamingManager },
        {
          provide: ConfigService,
          useValue: { get: jest.fn<any>().mockReturnValue('write_section_request_local') },
        },
      ],
    }).compile();

    service = module.get<WriteContentAiService>(WriteContentAiService);
  });

  describe('startWriteContent', () => {
    it('should throw NotFoundException if document does not exist', async () => {
      mockPrisma.document.findUnique.mockResolvedValue(null);
      await expect(service.startWriteContent(userId, validDto)).rejects.toThrow(NotFoundException);
    });

    it('should publish to Kafka and process section chunks & finish successfully', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId, field: 'CS' });
      mockCorrelation.registerPending.mockReturnValue(new Promise(() => {}));

      const res = await service.startWriteContent(userId, validDto);
      expect(res.status).toBe('started');
      expect(res.jobId).toBe('job-wc-1');
      expect(mockKafkaProducer.publish).toHaveBeenCalledWith(
        'write_section_request_local',
        expect.objectContaining({ event_type: 'write_content' }),
      );

      streamSubject.next({ section: { name: 'Chương 1', content: 'Mở đầu' } });
      await new Promise((r) => setTimeout(r, 10));
      expect(mockPrisma.report.upsert).toHaveBeenCalledWith(
        expect.objectContaining({ update: { contentMarkdown: '## Chương 1\n\nMở đầu' } }),
      );

      streamSubject.next({ has_finished: true, section: { name: 'Chương 2', content: 'Kết' } });
      await new Promise((r) => setTimeout(r, 10));
      expect(mockPrisma.job.update).toHaveBeenCalledWith(
        expect.objectContaining({ data: expect.objectContaining({ status: JobStatus.finished }) }),
      );
      expect(mockStreamingManager.completeStream).toHaveBeenCalledWith('stream-key-wc');
    });

    it('should suspend job when need_data_analyze flag is received', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId });
      mockCorrelation.registerPending.mockReturnValue(new Promise(() => {}));

      await service.startWriteContent(userId, validDto);
      streamSubject.next({ need_data_analyze: true, thread_id: 'thread-123' });
      await new Promise((r) => setTimeout(r, 10));

      expect(mockPrisma.job.update).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: 'job-wc-1' },
          data: expect.objectContaining({
            status: JobStatus.suspended,
            resumeStep: 'need_data_analyze',
            threadId: 'thread-123',
          }),
        }),
      );
    });

    it('should suspend job when chapter_break or __interrupt__ flag is received', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId });
      mockCorrelation.registerPending.mockReturnValue(new Promise(() => {}));

      await service.startWriteContent(userId, validDto);
      streamSubject.next({ chapter_break: true, __interrupt__: true, thread_id: 'thread-break-1' });
      await new Promise((r) => setTimeout(r, 10));

      expect(mockPrisma.job.update).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: 'job-wc-1' },
          data: expect.objectContaining({
            status: JobStatus.suspended,
            resumeStep: 'chapter_break',
            threadId: 'thread-break-1',
          }),
        }),
      );
      expect(mockStreamingManager.completeStream).toHaveBeenCalledWith('stream-key-wc');
    });

    it('should suspend job when need_propose_method flag is received', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId });
      mockCorrelation.registerPending.mockReturnValue(new Promise(() => {}));

      await service.startWriteContent(userId, validDto);
      streamSubject.next({ need_propose_method: true, thread_id: 'thread-456' });
      await new Promise((r) => setTimeout(r, 10));

      expect(mockPrisma.job.update).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: 'job-wc-1' },
          data: expect.objectContaining({
            status: JobStatus.suspended,
            resumeStep: 'need_propose_method',
            threadId: 'thread-456',
          }),
        }),
      );
    });

    it('should support resume with analyzeLog and threadId', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId });
      mockCorrelation.registerPending.mockReturnValue(new Promise(() => {}));

      const res = await service.startWriteContent(userId, {
        ...validDto,
        threadId: 'thread-123',
        resume: true,
        analyzeLog: 'SPSS Analysis Log Output',
      });
      expect(res.threadId).toBe('thread-123');
      expect(mockKafkaProducer.publish).toHaveBeenCalledWith(
        'write_section_request_local',
        expect.objectContaining({
          thread_id: 'thread-123',
          resume: true,
          analyze_log: 'SPSS Analysis Log Output',
        }),
      );
    });

    it('should support chuyen_de_1 with error code 632', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId });
      mockKafkaProducer.publish.mockRejectedValue(new Error('Broker unreachable'));

      await expect(
        service.startWriteContent(userId, { ...validDto, eventType: 'chuyen_de_1' }),
      ).rejects.toThrow(BadRequestException);
      expect(mockPrisma.job.update).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({ errorCode: '632' }),
        }),
      );
    });

    it('should support chuyen_de_2 with practicalData and error code 633', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId });
      mockCorrelation.registerPending.mockReturnValue(new Promise(() => {}));

      const practicalData = { revenue_2023: 500000000, market_share: '15%' };
      const res = await service.startWriteContent(userId, {
        ...validDto,
        eventType: 'chuyen_de_2',
        practicalData,
      });

      expect(res.status).toBe('started');
      expect(mockKafkaProducer.publish).toHaveBeenCalledWith(
        'write_section_request_local',
        expect.objectContaining({
          event_type: 'chuyen_de_2',
          practical_data: practicalData,
          final_proposal: expect.objectContaining({ practical_data: practicalData }),
        }),
      );
    });
  });

  describe('resumeAfterDataAnalysis', () => {
    it('should resume suspended job with analyze_log and keep thread id', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId });
      mockPrisma.job.findFirst.mockResolvedValue({
        id: 'job-suspended-1',
        threadId: 'thread-abc',
        payload: { event_type: 'chuyen_de_3', outline_code: ['c1'] },
      });
      mockPrisma.job.update.mockResolvedValue({
        id: 'job-suspended-1',
        status: JobStatus.processing,
      });

      const result = await service.resumeAfterDataAnalysis(userId, documentId, {
        analyzeLog: '{"p_value":0.03}',
      });

      expect(mockPrisma.job.findFirst).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            documentId,
            status: JobStatus.suspended,
            resumeStep: 'need_data_analyze',
          }),
        }),
      );
      expect(mockKafkaProducer.publish).toHaveBeenCalledWith(
        'write_section_request_local',
        expect.objectContaining({
          resume: true,
          analyze_log: '{"p_value":0.03}',
          thread_id: 'thread-abc',
          event_type: 'chuyen_de_3',
        }),
      );
      expect(result.resume_step).toBe('writing_after_analysis');
      expect(result.thread_id).toBe('thread-abc');
    });

    it('should throw NotFoundException when no suspended job exists', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId });
      mockPrisma.job.findFirst.mockResolvedValue(null);

      await expect(
        service.resumeAfterDataAnalysis(userId, documentId, { analyzeLog: 'x' }),
      ).rejects.toBeInstanceOf(NotFoundException);
      expect(mockKafkaProducer.publish).not.toHaveBeenCalled();
    });

    it('should map publish failure to job error 610 and BadRequest', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId });
      mockPrisma.job.findFirst.mockResolvedValue({
        id: 'job-suspended-2',
        threadId: 'thread-xyz',
        payload: { event_type: 'write_content' },
      });
      mockPrisma.job.update.mockResolvedValue({ id: 'job-suspended-2' });
      mockKafkaProducer.publish.mockRejectedValue(new Error('broker down'));

      await expect(
        service.resumeAfterDataAnalysis(userId, documentId, { analyzeLog: 'x' }),
      ).rejects.toBeInstanceOf(BadRequestException);
      expect(mockPrisma.job.update).toHaveBeenLastCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({ errorCode: '610' }),
        }),
      );
    });
  });

  describe('resumeChapter', () => {
    it('should resume suspended job with resumeCharacter: true and keep/forward threadId', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId });
      mockPrisma.job.findFirst.mockResolvedValue({
        id: 'job-suspended-ch',
        threadId: 'thread-break-ch',
        payload: { event_type: 'chuyen_de_1' },
      });
      mockPrisma.job.update.mockResolvedValue({
        id: 'job-suspended-ch',
        status: JobStatus.processing,
      });
      mockCorrelation.registerPending.mockReturnValue(new Promise(() => {}));

      const result = await service.resumeChapter(userId, documentId, {
        resumeCharacter: true,
      });

      expect(mockPrisma.job.findFirst).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            documentId,
            status: JobStatus.suspended,
            resumeStep: 'chapter_break',
          }),
        }),
      );
      expect(mockKafkaProducer.publish).toHaveBeenCalledWith(
        'write_section_request_local',
        expect.objectContaining({
          resumeCharacter: true,
          thread_id: 'thread-break-ch',
          event_type: 'chuyen_de_1',
        }),
      );
      expect(result.status).toBe('resumed');
      expect(result.threadId).toBe('thread-break-ch');
    });

    it('should throw NotFoundException when no suspended job at chapter_break exists', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId });
      mockPrisma.job.findFirst.mockResolvedValue(null);

      await expect(
        service.resumeChapter(userId, documentId, { resumeCharacter: true }),
      ).rejects.toBeInstanceOf(NotFoundException);
      expect(mockKafkaProducer.publish).not.toHaveBeenCalled();
    });

    it('should map publish failure to job error 610 and BadRequest', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId });
      mockPrisma.job.findFirst.mockResolvedValue({
        id: 'job-suspended-ch2',
        threadId: 'thread-fail',
        payload: { event_type: 'chuyen_de_1' },
      });
      mockPrisma.job.update.mockResolvedValue({ id: 'job-suspended-ch2' });
      mockKafkaProducer.publish.mockRejectedValue(new Error('kafka write fail'));

      await expect(
        service.resumeChapter(userId, documentId, { resumeCharacter: true }),
      ).rejects.toBeInstanceOf(BadRequestException);
      expect(mockPrisma.job.update).toHaveBeenLastCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({ errorCode: '610' }),
        }),
      );
    });
  });

  describe('getWriteStream', () => {
    it('should return stream from streamingManager when document is owned', async () => {
      mockPrisma.document.findUnique.mockResolvedValue({ id: documentId });
      const stream = await service.getWriteStream(userId, documentId);
      expect(stream).toBe('mock-sse-stream');
      expect(mockStreamingManager.createStream).toHaveBeenCalledWith('stream-key-wc', 1500000);
    });
  });
});
