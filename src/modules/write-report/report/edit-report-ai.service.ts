import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { JobStatus, ModuleType, Prisma } from '@prisma/client';
import { randomUUID } from 'node:crypto';
import { getAiErrorMessage } from '../../../common/constants/ai-error-codes';
import { getAiModelConfig } from '../../../common/kafka/kafka.constants';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { EditReportDto } from './dto/edit-report.dto';
import { WriteContentMongoHelper } from './write-content-mongo.helper';

const EVENT_TYPE = 'edit_report';
const TIMEOUT_MS = 300_000;
const EDIT_REPORT_ERROR_CODE = 630;
const PUBLISH_ERROR_CODE = 610;

export interface EditReportResponse {
  section_name?: string;
  content?: string;
  has_finished?: boolean;
  input_tokens?: number;
  output_tokens?: number;
}

@Injectable()
export class EditReportAiService {
  private readonly logger = new Logger(EditReportAiService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly correlationService: RequestCorrelationService,
    private readonly configService: ConfigService,
  ) {}

  async edit(userId: string, documentId: string, dto: EditReportDto) {
    const document = await this.prisma.document.findFirst({
      where: { id: documentId, userId },
      select: { id: true },
    });
    if (!document) {
      throw new NotFoundException('Document not found');
    }

    const mongoDocId = WriteContentMongoHelper.toMongoDocId(documentId);
    const sessionId = dto.sessionId || `sess-${EVENT_TYPE}-${randomUUID()}`;

    const payload = {
      section_name: dto.section_name,
      content: dto.content,
      instruction: dto.instruction,
      context: dto.context ?? '',
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

    const kafkaPayload: Record<string, unknown> = {
      event_type: EVENT_TYPE,
      model_id: getAiModelConfig().defaultModel,
      user_id: userId,
      document_id: mongoDocId,
      session_id: sessionId,
      language: 'Vietnamese',
      ...payload,
    };

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
      'write_section_request';

    try {
      await this.kafkaProducer.publish(topic, kafkaPayload);
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
          errorMessage: getAiErrorMessage(PUBLISH_ERROR_CODE),
        },
      });
      throw publishError;
    }

    let response: EditReportResponse;
    try {
      response = (await pendingPromise) as EditReportResponse;
    } catch (aiError: unknown) {
      const err = aiError as Record<string, unknown>;
      const code = String(
        err?.error_code || err?.errorCode || EDIT_REPORT_ERROR_CODE,
      );
      const msg = String(
        err?.error ||
          err?.message ||
          getAiErrorMessage(Number(code)) ||
          'Edit report failed',
      );
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

    await this.prisma.job.update({
      where: { id: job.id },
      data: {
        status: JobStatus.finished,
        result: response as unknown as Prisma.InputJsonValue,
      },
    });

    this.logger.log(`[${EVENT_TYPE}] hoàn tất job ${job.id} cho ${documentId}`);

    return {
      job_id: job.id,
      event_type: EVENT_TYPE,
      document_id: documentId,
      session_id: sessionId,
      section_name: response.section_name ?? dto.section_name,
      content: response.content ?? dto.content,
      data: response,
    };
  }
}
