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
import { AI_DEFAULT_MODEL } from '../../../common/kafka/kafka.constants';
import { TranslateReportDto } from './dto/translate-report.dto';
import { WriteContentMongoHelper } from './write-content-mongo.helper';

const EVENT_TYPE = 'translate_report';
const TIMEOUT_MS = 300_000;
const PUBLISH_ERROR_CODE = 610;

@Injectable()
export class TranslateAiService {
  private readonly logger = new Logger(TranslateAiService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly correlationService: RequestCorrelationService,
    private readonly configService: ConfigService,
  ) {}

  /**
   * `translate_report`: AI dịch rồi **ghi thẳng** vào Mongo
   * `admin.document_configurations.translatedContent` (toàn văn) hoặc
   * `.translatedOutline` (đề cương). Response Kafka chỉ có identifier + token,
   * nên BE phải đọc lại Mongo để trả nội dung cho FE.
   */
  async translate(userId: string, documentId: string, dto: TranslateReportDto) {
    const document = await this.prisma.document.findFirst({
      where: { id: documentId, userId },
      select: { id: true },
    });
    if (!document) {
      throw new NotFoundException('Document not found');
    }

    const mongoDocId = WriteContentMongoHelper.toMongoDocId(documentId);
    const sessionId = dto.sessionId || `sess-${EVENT_TYPE}-${randomUUID()}`;
    const isOutline = dto.isOutline ?? false;
    const field = isOutline ? 'translatedOutline' : 'translatedContent';

    const payload: Record<string, unknown> = {
      event_type: EVENT_TYPE,
      model_id: AI_DEFAULT_MODEL,
      user_id: userId,
      document_id: mongoDocId,
      session_id: sessionId,
      language: 'Vietnamese',
      target_language: dto.targetLanguage,
      is_outline: isOutline,
    };

    const job = await this.prisma.job.create({
      data: {
        module: ModuleType.write_report,
        action: EVENT_TYPE,
        documentId,
        sessionId,
        status: JobStatus.pending,
        payload: payload as Prisma.InputJsonValue,
      },
    });

    const correlationKey = this.correlationService.buildCorrelationKey({
      userId,
      documentId: mongoDocId,
      sessionId,
      eventType: EVENT_TYPE,
    });

    const pendingPromise = this.correlationService.registerPending<
      Record<string, unknown>
    >(correlationKey, TIMEOUT_MS, EVENT_TYPE, job.id);

    const topic =
      this.configService.get<string>('kafka.topics.write_report.request') ||
      'write_section_request_local';

    try {
      await this.kafkaProducer.publish(topic, payload);
      await this.prisma.job.update({
        where: { id: job.id },
        data: { status: JobStatus.processing },
      });
    } catch (publishError: unknown) {
      const message =
        publishError instanceof Error
          ? publishError.message
          : 'Publish to Kafka failed';
      this.correlationService.clearPending(correlationKey, message);
      await this.prisma.job.update({
        where: { id: job.id },
        data: {
          status: JobStatus.error,
          errorCode: String(PUBLISH_ERROR_CODE),
          errorMessage: message,
        },
      });
      throw publishError;
    }

    let response: Record<string, unknown>;
    try {
      response = (await pendingPromise) as Record<string, unknown>;
    } catch (aiError: unknown) {
      const err = aiError as Record<string, unknown>;
      const code = String(err?.error_code || err?.errorCode || 641);
      const msg = String(err?.error || err?.message || 'Translate report failed');
      await this.prisma.job.update({
        where: { id: job.id },
        data: { status: JobStatus.error, errorCode: code, errorMessage: msg },
      });
      throw new BadRequestException({
        statusCode: 400,
        errorCode: code,
        message: msg,
      });
    }

    let content = await this.readTranslatedContent(mongoDocId, field);
    if (!content && response.translated_content) {
      content = response.translated_content;
    }

    await this.prisma.job.update({
      where: { id: job.id },
      data: {
        status: JobStatus.finished,
        result: {
          ...response,
          translated_field: field,
          has_translated_content: content !== null,
        } as Prisma.InputJsonValue,
      },
    });

    this.logger.log(
      `[${EVENT_TYPE}] xong job ${job.id} (${field}) cho ${documentId}`,
    );

    return {
      job_id: job.id,
      event_type: EVENT_TYPE,
      document_id: documentId,
      session_id: sessionId,
      target_language: dto.targetLanguage,
      is_outline: isOutline,
      translated_field: field,
      content,
    };
  }

  /**
   * Đọc bản dịch sau khi AI xong. Lỗi/không có dữ liệu ⇒ trả `null`
   * (job vẫn `finished` vì AI đã dịch xong; FE hiển thị cảnh báo).
   */
  private async readTranslatedContent(
    documentId: string,
    field: string,
  ): Promise<unknown> {
    try {
      const config = await WriteContentMongoHelper.readDocumentConfiguration(
        documentId,
        [field],
      );
      return config?.[field] ?? null;
    } catch (error: unknown) {
      this.logger.warn(
        `Không đọc được ${field} của ${documentId}: ${(error as Error).message}`,
      );
      return null;
    }
  }
}
