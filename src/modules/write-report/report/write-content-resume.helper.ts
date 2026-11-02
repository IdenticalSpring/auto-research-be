import { BadRequestException, Logger, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { JobStatus, Prisma } from '@prisma/client';
import { randomUUID } from 'node:crypto';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { StreamingManagerService } from '../../../common/streaming/streaming-manager.service';
import { ResumeDataAnalysisDto } from './dto/resume-data-analysis.dto';
import { ResumeChapterDto } from './dto/resume-chapter.dto';
import { WriteContentHelper } from './write-content.helper';
import { WriteContentMongoHelper } from './write-content-mongo.helper';
import { WriteContentStreamHelper } from './write-content-stream.helper';

export interface ResumeServiceContext {
  prisma: PrismaService;
  kafkaProducer: KafkaProducerService;
  correlation: RequestCorrelationService;
  streamingManager: StreamingManagerService;
  configService: ConfigService;
  logger: Logger;
}

export class WriteContentResumeHelper {
  static async resumeChapter(
    ctx: ResumeServiceContext,
    userId: string,
    documentId: string,
    dto: ResumeChapterDto,
  ) {
    const job = await ctx.prisma.job.findFirst({
      where: {
        documentId,
        status: JobStatus.suspended,
        resumeStep: 'chapter_break',
      },
      orderBy: { createdAt: 'desc' },
    });

    if (!job) {
      throw new NotFoundException(
        'Không có job nào đang tạm dừng tại chapter_break cho tài liệu này',
      );
    }

    const previousPayload =
      typeof job.payload === 'object' && job.payload !== null
        ? (job.payload as Record<string, unknown>)
        : {};

    const mongoDocId = WriteContentMongoHelper.toMongoDocId(documentId);
    const threadId = dto.threadId || job.threadId || mongoDocId;
    const sessionId = randomUUID();
    const eventType = String(previousPayload.event_type || 'chuyen_de_1');

    const payload: Record<string, unknown> = {
      ...previousPayload,
      user_id: userId,
      document_id: mongoDocId,
      session_id: sessionId,
      thread_id: threadId,
      job_id: job.id,
      resumeCharacter: true,
      resume: false,
      resume_error: false,
      resume_method: false,
    };

    const updatedJob = await ctx.prisma.job.update({
      where: { id: job.id },
      data: {
        status: JobStatus.processing,
        resumeStep: 'resuming_chapter',
        threadId,
        payload: payload as Prisma.InputJsonValue,
        errorCode: null,
        errorMessage: null,
      },
    });

    const streamKey = ctx.streamingManager.buildStreamKey({ userId, documentId, eventType });
    const corrKey = ctx.correlation.buildCorrelationKey({
      userId,
      documentId: mongoDocId,
      sessionId,
      eventType,
    });

    const streamObservable = ctx.correlation.getStreamObservable<Record<string, unknown>>(corrKey);
    const pendingPromise = ctx.correlation.registerPending<Record<string, unknown>>(
      corrKey,
      undefined,
      eventType,
      job.id,
    );

    const topic =
      ctx.configService.get<string>('kafka.topics.write_report.request') ||
      'write_section_request';

    try {
      await ctx.kafkaProducer.publish(topic, payload);
    } catch (publishErr) {
      ctx.correlation.clearPending(corrKey, (publishErr as Error).message);
      return WriteContentHelper.failJob(ctx.prisma, job.id, 610, (publishErr as Error).message);
    }

    WriteContentStreamHelper.attachStreamHandlers({
      streamObservable,
      pendingPromise,
      streamKey,
      jobId: job.id,
      documentId,
      errorCode: 632,
      prisma: ctx.prisma,
      streamingManager: ctx.streamingManager,
      logger: ctx.logger,
    });

    return {
      jobId: updatedJob.id,
      streamKey,
      threadId,
      status: 'resumed',
      stream: `/reports/${documentId}/write-stream`,
    };
  }

  static async resumeAfterDataAnalysis(
    ctx: ResumeServiceContext,
    userId: string,
    documentId: string,
    dto: ResumeDataAnalysisDto,
  ) {
    const job = await ctx.prisma.job.findFirst({
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

    const updatedJob = await ctx.prisma.job.update({
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
      ctx.configService.get<string>('kafka.topics.write_report.request') ||
      'write_section_request';

    try {
      await ctx.kafkaProducer.publish(topic, payload);
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : 'Publish to Kafka failed';
      await ctx.prisma.job.update({
        where: { id: job.id },
        data: { status: JobStatus.error, errorCode: '610', errorMessage: message },
      });
      throw new BadRequestException(`Không gửi được yêu cầu viết tiếp sau phân tích: ${message}`);
    }

    ctx.logger.log(`[resume-data-analysis] job ${job.id} viết tiếp cho ${documentId} (thread ${threadId})`);

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
}
