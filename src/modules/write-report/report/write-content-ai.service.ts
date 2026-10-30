import {
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { JobStatus, ModuleType, Prisma } from '@prisma/client';
import { randomUUID } from 'node:crypto';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { StreamingManagerService } from '../../../common/streaming/streaming-manager.service';
import { ResumeDataAnalysisDto } from './dto/resume-data-analysis.dto';
import { ResumeChapterDto } from './dto/resume-chapter.dto';
import { WriteContentRequestDto } from './dto/write-content-request.dto';
import { WriteContentHelper } from './write-content.helper';
import { WriteContentMongoHelper } from './write-content-mongo.helper';
import { WriteContentStreamHelper } from './write-content-stream.helper';
import {
  ResumeServiceContext,
  WriteContentResumeHelper,
} from './write-content-resume.helper';
import { getAiErrorMessage } from '../../../common/constants/ai-error-codes';
import { AI_DEFAULT_MODEL } from '../../../common/kafka/kafka.constants';

@Injectable()
export class WriteContentAiService {
  private readonly logger = new Logger(WriteContentAiService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly correlation: RequestCorrelationService,
    private readonly streamingManager: StreamingManagerService,
    private readonly configService: ConfigService,
  ) {}

  private get resumeContext(): ResumeServiceContext {
    return {
      prisma: this.prisma,
      kafkaProducer: this.kafkaProducer,
      correlation: this.correlation,
      streamingManager: this.streamingManager,
      configService: this.configService,
      logger: this.logger,
    };
  }

  async startWriteContent(userId: string, dto: WriteContentRequestDto) {
    const doc = await this.ensureOwnedDocument(userId, dto.documentId);
    const eventType = dto.eventType || 'write_content';
    const errorCode =
      eventType === 'chuyen_de_1'
        ? 632
        : eventType === 'chuyen_de_2'
          ? 633
          : eventType === 'chuyen_de_3'
            ? 635
            : 630;

    const threadId = dto.threadId || randomUUID();
    const sessionId = dto.sessionId || randomUUID();
    const resolved = await WriteContentHelper.resolveWritePayload(this.prisma, dto, doc);
    const mongoDocId = WriteContentMongoHelper.toMongoDocId(dto.documentId);
    await WriteContentMongoHelper.ensureMongoReady(
      mongoDocId,
      (resolved.papersSearch || []) as Array<{ title?: string; abstract?: string; [key: string]: unknown }>,
      resolved.outline,
    );

    const job = await this.prisma.job.create({
      data: {
        module: ModuleType.write_report,
        action: eventType,
        documentId: dto.documentId,
        threadId,
        status: JobStatus.pending,
        payload: dto as unknown as Prisma.InputJsonValue,
      },
    });

    const streamKey = this.streamingManager.buildStreamKey({
      userId,
      documentId: dto.documentId,
      eventType,
    });
    const corrKey = this.correlation.buildCorrelationKey({
      userId,
      documentId: mongoDocId,
      sessionId,
      eventType,
    });

    const streamObservable = this.correlation.getStreamObservable<Record<string, unknown>>(corrKey);
    const pendingPromise = this.correlation.registerPending<Record<string, unknown>>(
      corrKey,
      undefined,
      eventType,
      job.id,
    );

    const payload = {
      event_type: eventType,
      model_id: AI_DEFAULT_MODEL,
      user_id: userId,
      document_id: mongoDocId,
      session_id: sessionId,
      language: dto.language || 'vi',
      field: resolved.field,
      domain: resolved.domain,
      references_style: dto.referencesStyle || 'APA',
      final_proposal: resolved.finalProposal,
      papers_search: resolved.papersSearch,
      outline: resolved.outline,
      outline_code: resolved.outlineCode,
      thread_id: threadId,
      db: 'redis',
      retry: 2,
      use_web_search: dto.useWebSearch ?? false,
      resume: dto.resume ?? false,
      resume_error: dto.resumeError ?? false,
      resume_method: dto.resumeMethod ?? false,
      resumeCharacter: dto.resumeCharacter ?? false,
      analyze_log: dto.analyzeLog,
      detailed_logs: dto.detailedLogs,
      generated_files: dto.generatedFiles,
      practical_data: dto.practicalData ?? (resolved.finalProposal as Record<string, unknown>)?.practical_data,
      job_id: job.id,
    };

    const topic =
      this.configService.get<string>('kafka.topics.write_report.request') ||
      'write_section_request_local';

    try {
      await this.kafkaProducer.publish(topic, payload);
      await this.prisma.job.update({
        where: { id: job.id },
        data: { status: JobStatus.processing },
      });
    } catch (publishErr) {
      this.correlation.clearPending(corrKey, (publishErr as Error).message);
      return WriteContentHelper.failJob(this.prisma, job.id, errorCode, getAiErrorMessage(errorCode));
    }

    WriteContentStreamHelper.attachStreamHandlers({
      streamObservable,
      pendingPromise,
      streamKey,
      jobId: job.id,
      documentId: dto.documentId,
      errorCode,
      prisma: this.prisma,
      streamingManager: this.streamingManager,
      logger: this.logger,
    });

    return { jobId: job.id, streamKey, threadId, status: 'started' };
  }

  /**
   * BE-030: Multi-turn resume cho chuyen_de_1 (sau khi ngắt chapter_break)
   */
  async resumeChapter(userId: string, documentId: string, dto: ResumeChapterDto) {
    await this.ensureOwnedDocument(userId, documentId);
    return WriteContentResumeHelper.resumeChapter(this.resumeContext, userId, documentId, dto);
  }

  /**
   * T-11: hoàn tất job đang treo ở bước `need_data_analyze`.
   */
  async resumeAfterDataAnalysis(
    userId: string,
    documentId: string,
    dto: ResumeDataAnalysisDto,
  ) {
    await this.ensureOwnedDocument(userId, documentId);
    return WriteContentResumeHelper.resumeAfterDataAnalysis(
      this.resumeContext,
      userId,
      documentId,
      dto,
    );
  }

  async getWriteStream(userId: string, documentId: string, eventType = 'write_content') {
    await this.ensureOwnedDocument(userId, documentId);
    const streamKey = this.streamingManager.buildStreamKey({ userId, documentId, eventType });
    return this.streamingManager.createStream(streamKey, 1500000);
  }

  async ensureOwnedDocument(userId: string, documentId: string) {
    const doc = await this.prisma.document.findUnique({
      where: { id: documentId, userId },
      select: { id: true, title: true, field: true, domains: true },
    });
    if (!doc) throw new NotFoundException('Document not found');
    return doc;
  }
}
