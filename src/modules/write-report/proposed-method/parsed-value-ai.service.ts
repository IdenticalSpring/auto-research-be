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
import { AI_DEFAULT_MODEL } from '../../../common/kafka/kafka.constants';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { WriteContentMongoHelper } from '../report/write-content-mongo.helper';
import { ParsedValueDto } from './dto/parsed-value-ai.dto';

const EVENT_TYPE = 'parsed_value';
const PARSED_VALUE_ERROR_CODE = 639;
const PUBLISH_ERROR_CODE = 610;
const TIMEOUT_MS = 300_000;

@Injectable()
export class ParsedValueAiService {
  private readonly logger = new Logger(ParsedValueAiService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly correlationService: RequestCorrelationService,
    private readonly configService: ConfigService,
  ) {}

  async parseValue(userId: string, documentId: string, dto: ParsedValueDto) {
    const document = await this.prisma.document.findFirst({
      where: { id: documentId, userId },
      select: { id: true },
    });
    if (!document) {
      throw new NotFoundException('Document not found');
    }

    let variables = dto.variables;
    if (!variables || variables.length === 0) {
      const pm = await this.prisma.proposedMethod.findFirst({
        where: { documentId },
        select: { variables: true },
      });
      if (pm?.variables && Array.isArray(pm.variables)) {
        variables = pm.variables as Array<Record<string, unknown>>;
      } else {
        variables = [];
      }
    }

    const sessionId = dto.sessionId || `sess-${EVENT_TYPE}-${randomUUID()}`;
    const payload = {
      survey_questions: dto.survey_questions,
      variables,
    };

    const job = await this.prisma.job.create({
      data: {
        module: ModuleType.write_report,
        action: EVENT_TYPE,
        documentId,
        sessionId,
        status: JobStatus.pending,
        payload: payload as unknown as Prisma.InputJsonValue,
      },
    });

    const mongoDocId = WriteContentMongoHelper.toMongoDocId(documentId);
    const kafkaPayload: Record<string, unknown> = {
      event_type: EVENT_TYPE,
      model_id: AI_DEFAULT_MODEL,
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
      'write_section_request_local';

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

    let result: Record<string, unknown>;
    try {
      result = (await pendingPromise) as Record<string, unknown>;
    } catch (aiError: unknown) {
      const err = aiError as Record<string, unknown>;
      const code = String(
        err?.error_code || err?.errorCode || PARSED_VALUE_ERROR_CODE,
      );
      const msg = String(
        err?.error ||
          err?.message ||
          getAiErrorMessage(Number(code)) ||
          'Parse value failed',
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
        result: result as unknown as Prisma.InputJsonValue,
      },
    });

    if (result.parsed_variables && Array.isArray(result.parsed_variables)) {
      const existingPm = await this.prisma.proposedMethod.findFirst({
        where: { documentId },
        select: { id: true },
      });
      if (existingPm) {
        await this.prisma.proposedMethod.update({
          where: { id: existingPm.id },
          data: {
            variables: result.parsed_variables as unknown as Prisma.InputJsonValue,
          },
        });
      }
    }

    this.logger.log(`[${EVENT_TYPE}] hoàn tất job ${job.id} cho ${documentId}`);

    return {
      job_id: job.id,
      event_type: EVENT_TYPE,
      document_id: documentId,
      session_id: sessionId,
      data: result,
    };
  }
}
