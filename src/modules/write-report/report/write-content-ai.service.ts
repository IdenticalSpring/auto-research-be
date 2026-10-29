import {
  BadRequestException,
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
import { WriteContentRequestDto } from './dto/write-content-request.dto';
import { WriteContentHelper } from './write-content.helper';
import { WriteContentMongoHelper } from './write-content-mongo.helper';
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

  async startWriteContent(userId: string, dto: WriteContentRequestDto) {
    const doc = await this.ensureOwnedDocument(userId, dto.documentId);
    const eventType = dto.eventType || 'write_content';
    // Map mã lỗi thật theo từng event (bỏ workaround 631 dùng chung cho chuyen_de_3 — B-3/B4).
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

    const sectionsMap = new Map<string, string>();
    let hasResumed = false;
    const subscription = streamObservable.subscribe({
      next: async (chunk: Record<string, unknown>) => {
        try {
          if (chunk.disable_edit !== undefined && chunk.section && !hasResumed) {
            hasResumed = true;
            this.logger.log(`Reached chapter breakpoint for ${(chunk.section as Record<string, unknown>).section_code}, auto-resuming...`);
            const resumePayload = {
              ...payload,
              session_id: randomUUID(),
              resume: false,
              resumeCharacter: true,
            };
            void this.kafkaProducer.publish(topic, resumePayload).catch((err: Error) => {
              this.logger.error(`Failed to publish auto-resume: ${err.message}`);
            });
            return;
          }

          this.streamingManager.emitChunk(streamKey, chunk);
          const res = await WriteContentHelper.handleChunk(
            this.prisma,
            job.id,
            chunk,
            sectionsMap,
            dto.documentId,
          );
          if (res.isFinished || res.isSuspended) {
            this.streamingManager.completeStream(streamKey);
            subscription.unsubscribe();
          }
        } catch (err) {
          this.logger.error(`Error handling streaming chunk: ${(err as Error).message}`);
        }
      },
      error: async (err: unknown) => {
        const errorMsg = (err as Error)?.message || getAiErrorMessage(errorCode);
        await this.prisma.job.update({
          where: { id: job.id },
          data: { status: JobStatus.error, errorCode: String(errorCode), errorMessage: errorMsg },
        });
        this.streamingManager.destroyStream(streamKey);
        subscription.unsubscribe();
      },
    });

    void pendingPromise.catch(async (err: unknown) => {
      const code = (err as Record<string, unknown>)?.error_code || errorCode;
      const msg =
        (err as Record<string, unknown>)?.error ||
        (err as Error)?.message ||
        getAiErrorMessage(errorCode);

      await this.prisma.job.update({
        where: { id: job.id },
        data: { status: JobStatus.error, errorCode: String(code), errorMessage: String(msg) },
      });
      this.streamingManager.destroyStream(streamKey);
      subscription.unsubscribe();
    });

    return { jobId: job.id, streamKey, threadId, status: 'started' };
  }

  /**
   * T-11: hoàn tất job đang treo ở bước `need_data_analyze`.
   * FE chạy `analyzer` xong → gửi `analyzeLog` để AI viết tiếp từ checkpoint Redis
   * (`resume: true` + `analyze_log`, giữ nguyên `thread_id` của job đang treo).
   */
  async resumeAfterDataAnalysis(
    userId: string,
    documentId: string,
    dto: ResumeDataAnalysisDto,
  ) {
    await this.ensureOwnedDocument(userId, documentId);

    const job = await this.prisma.job.findFirst({
      where: {
        documentId,
        status: JobStatus.suspended,
        resumeStep: 'need_data_analyze',
      },
      orderBy: { createdAt: 'desc' },
    });

    if (!job) {
      throw new NotFoundException(
        'Không có job nào đang chờ dữ liệu phân tích (need_data_analyze) cho tài liệu này',
      );
    }

    const previousPayload =
      typeof job.payload === 'object' && job.payload !== null
        ? (job.payload as Record<string, unknown>)
        : {};

    const threadId = dto.threadId || job.threadId || randomUUID();
    const payload: Record<string, unknown> = {
      ...previousPayload,
      user_id: userId,
      document_id: WriteContentMongoHelper.toMongoDocId(documentId),
      thread_id: threadId,
      job_id: job.id,
      resume: true,
      analyze_log: dto.analyzeLog,
      detailed_logs: dto.detailedLogs ?? previousPayload.detailed_logs,
      generated_files: dto.generatedFiles ?? previousPayload.generated_files,
    };

    const updatedJob = await this.prisma.job.update({
      where: { id: job.id },
      data: {
        status: JobStatus.processing,
        resumeStep: 'writing_after_analysis',
        threadId,
        payload: payload as Prisma.InputJsonValue,
        errorCode: null,
        errorMessage: null,
      },
    });

    const topic =
      this.configService.get<string>('kafka.topics.write_report.request') ||
      'write_section_request_local';

    try {
      await this.kafkaProducer.publish(topic, payload);
    } catch (error: unknown) {
      const message =
        error instanceof Error ? error.message : 'Publish to Kafka failed';
      await this.prisma.job.update({
        where: { id: job.id },
        data: {
          status: JobStatus.error,
          errorCode: '610',
          errorMessage: message,
        },
      });
      throw new BadRequestException(
        `Không gửi được yêu cầu viết tiếp sau phân tích: ${message}`,
      );
    }

    this.logger.log(
      `[resume-data-analysis] job ${job.id} viết tiếp cho ${documentId} (thread ${threadId})`,
    );

    return {
      job_id: updatedJob.id,
      event_type: String(previousPayload.event_type ?? 'write_content'),
      document_id: documentId,
      thread_id: threadId,
      status: updatedJob.status,
      resume_step: 'writing_after_analysis',
      stream: `/reports/${documentId}/write-stream`,
    };
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
