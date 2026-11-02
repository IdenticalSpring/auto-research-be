import { BadRequestException, Injectable, Logger, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { JobStatus, ModuleType, Prisma, SlideStatus } from '@prisma/client';
import { randomUUID } from 'node:crypto';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { StreamingManagerService } from '../../../common/streaming/streaming-manager.service';
import { GenerateSlideRequestDto } from './dto/generate-slide-request.dto';
import { GeneratedSlideHelper } from './generated-slide.helper';
import { getAiErrorMessage } from '../../../common/constants/ai-error-codes';
import { getAiModelConfig } from '../../../common/kafka/kafka.constants';

@Injectable()
export class GeneratedSlideAiService {
  private readonly logger = new Logger(GeneratedSlideAiService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly correlation: RequestCorrelationService,
    private readonly streamingManager: StreamingManagerService,
    private readonly configService: ConfigService,
  ) {}

  async generateSlides(userId: string, dto: GenerateSlideRequestDto) {
    await this.ensureOwnedDocument(userId, dto.documentId);

    let rawReport = dto.rawReport;
    if (!rawReport) {
      const report = await this.prisma.report.findUnique({
        where: { documentId: dto.documentId },
        select: { contentMarkdown: true },
      });
      if (!report || !report.contentMarkdown) {
        throw new BadRequestException(
          'Report content not found for this document',
        );
      }
      rawReport = report.contentMarkdown;
    }

    const slideRecord = await this.prisma.generatedSlide.create({
      data: {
        documentId: dto.documentId,
        slideStyle: dto.slideStyle || dto.userPreference?.theme || 'default',
        manualMode: dto.manualMode ?? false,
        language: dto.language || 'vi',
        status: SlideStatus.generating,
        progressStep: '0%',
      },
    });

    const job = await this.prisma.job.create({
      data: {
        module: ModuleType.write_report,
        action: 'gen_slides',
        documentId: dto.documentId,
        status: JobStatus.pending,
        payload: dto as unknown as Prisma.InputJsonValue,
      },
    });

    const sessionId = dto.sessionId || randomUUID();
    const numSlides =
      typeof dto.userPreference === 'number'
        ? dto.userPreference
        : (dto.userPreference?.numSlides ?? 15);

    const streamKey = this.streamingManager.buildStreamKey({
      userId,
      documentId: dto.documentId,
      sessionId,
      eventType: 'gen_slides',
    });

    const corrKey = this.correlation.buildCorrelationKey({
      userId,
      documentId: dto.documentId,
      sessionId,
      eventType: 'gen_slides',
    });

    const streamObservable =
      this.correlation.getStreamObservable<Record<string, unknown>>(corrKey);
    const pendingPromise = this.correlation.registerPending<
      Record<string, unknown>
    >(corrKey, 300000, 'gen_slides', job.id);

    const payload = {
      event_type: 'gen_slides',
      user_id: userId,
      document_id: dto.documentId,
      session_id: sessionId,
      raw_report: rawReport,
      user_preference: numSlides,
      manual_mode: dto.manualMode ?? false,
      slide_style: dto.slideStyle || dto.userPreference?.theme || 'default',
      model_id: getAiModelConfig().defaultModel,
      language: dto.language || 'vi',
      job_id: job.id,
      slide_id: slideRecord.id,
    };

    const topic =
      this.configService.get<string>('kafka.topics.write_report.request') ||
      'write_section_request';

    try {
      await this.kafkaProducer.publish(topic, payload);
      await this.prisma.job.update({
        where: { id: job.id },
        data: { status: JobStatus.processing },
      });
    } catch (publishErr) {
      this.correlation.clearPending(corrKey, (publishErr as Error).message);
      return GeneratedSlideHelper.failSlideJob(
        this.prisma,
        job.id,
        slideRecord.id,
        634,
        getAiErrorMessage(634),
      );
    }

    const subscription = streamObservable.subscribe({
      next: async (chunk) => {
        try {
          if (chunk.progress !== undefined) {
            await this.prisma.generatedSlide.update({
              where: { id: slideRecord.id },
              data: { progressStep: `${chunk.progress}%` },
            });
          }

          if (chunk.has_finished === true) {
            const pptxUrl = GeneratedSlideHelper.extractPptxUrl(chunk) || '';
            await this.prisma.generatedSlide.update({
              where: { id: slideRecord.id },
              data: {
                status: SlideStatus.completed,
                pptxUrl,
                progressStep: '100%',
              },
            });

            await this.prisma.job.update({
              where: { id: job.id },
              data: {
                status: JobStatus.finished,
                result: chunk as unknown as Prisma.InputJsonValue,
              },
            });

            this.streamingManager.completeStream(streamKey);
            subscription.unsubscribe();
          }
        } catch (err) {
          this.logger.error(
            `Error processing slide chunk: ${(err as Error).message}`,
          );
        }
      },
      error: async (err: unknown) => {
        const errorMsg = (err as Error)?.message || getAiErrorMessage(634);
        await this.prisma.generatedSlide.update({
          where: { id: slideRecord.id },
          data: { status: SlideStatus.error },
        });
        await this.prisma.job.update({
          where: { id: job.id },
          data: {
            status: JobStatus.error,
            errorCode: '634',
            errorMessage: errorMsg,
          },
        });
        this.streamingManager.destroyStream(streamKey);
        subscription.unsubscribe();
      },
    });

    void pendingPromise.catch(async (err: unknown) => {
      const code = (err as Record<string, unknown>)?.error_code || '634';
      const msg =
        (err as Record<string, unknown>)?.error ||
        (err as Error)?.message ||
        getAiErrorMessage(634);

      await this.prisma.generatedSlide.update({
        where: { id: slideRecord.id },
        data: { status: SlideStatus.error },
      });
      await this.prisma.job.update({
        where: { id: job.id },
        data: {
          status: JobStatus.error,
          errorCode: String(code),
          errorMessage: String(msg),
        },
      });
      this.streamingManager.destroyStream(streamKey);
      subscription.unsubscribe();
    });

    return {
      jobId: job.id,
      slideId: slideRecord.id,
      sessionId,
      streamKey,
      status: 'started',
    };
  }

  async getSlideStream(
    userId: string,
    documentId: string,
    sessionId?: string,
  ) {
    await this.ensureOwnedDocument(userId, documentId);
    const streamKey = this.streamingManager.buildStreamKey({
      userId,
      documentId,
      sessionId,
      eventType: 'gen_slides',
    });
    return this.streamingManager.createStream(streamKey, 300000);
  }

  async getSlideStatus(userId: string, documentId: string) {
    await this.ensureOwnedDocument(userId, documentId);
    return this.prisma.generatedSlide.findFirst({
      where: { documentId, document: { userId } },
      orderBy: { createdAt: 'desc' },
    });
  }

  async ensureOwnedDocument(userId: string, documentId: string) {
    const doc = await this.prisma.document.findUnique({
      where: { id: documentId, userId },
      select: { id: true },
    });
    if (!doc) throw new NotFoundException('Document not found');
    return doc;
  }
}
