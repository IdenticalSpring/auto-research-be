import {
  BadRequestException,
  ConflictException,
  HttpStatus,
  Injectable,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { randomUUID } from 'node:crypto';
import { getAiModelConfig } from '../../../common/kafka/kafka.constants';
import {
  getAiErrorMessage,
  getAiErrorName,
} from '../../../common/constants/ai-error-codes';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import {
  AdminSampleDocumentDto,
  AdminUserGuideDto,
  GetJournalsQueryDto,
} from './dto/admin-documents.dto';

@Injectable()
export class AdminDocumentsAiService {
  constructor(
    private readonly kafkaProducer: KafkaProducerService,
    private readonly correlationService: RequestCorrelationService,
    private readonly configService: ConfigService,
  ) {}

  async uploadSampleDocument(userId: string, dto: AdminSampleDocumentDto) {
    const payload = {
      event_type: 'admin_documents',
      model_id: getAiModelConfig().defaultModel,
      user_id: userId,
      document_id: 'admin-doc',
      session_id: randomUUID(),
      language: dto.language || 'vi',
      // AI Q3 (optional nhưng nên gửi): data_id để AI echo lại và BE ghép job.
      data_id: dto.refId || 'admin_documents',
      documents: [
        {
          // `url` là bucket/key — AI validate `bucket/key` (§2 fixture).
          url: dto.source,
          title: dto.title,
          // `source` theo AI là vai trò người gửi (ADMIN/USER), KHÔNG phải path.
          source: 'ADMIN',
          ref_id: dto.refId,
        },
      ],
    };

    return this.sendAndCorrelate(payload, 618);
  }

  async uploadUserGuide(userId: string, dto: AdminUserGuideDto) {
    const payload = {
      event_type: 'user_guide_documents',
      model_id: getAiModelConfig().defaultModel,
      user_id: userId,
      document_id: 'admin-user-guide',
      session_id: randomUUID(),
      language: 'vi',
      documents_urls: dto.documents_urls,
      data_id: dto.data_id,
      title: dto.title,
      guide_type: dto.guideType,
      content: dto.content,
      version: dto.version || 'v1.0',
    };

    return this.sendAndCorrelate(payload, 619);
  }

  async getJournals(userId: string, query: GetJournalsQueryDto) {
    const payload = {
      event_type: 'get_journals',
      model_id: getAiModelConfig().defaultModel,
      user_id: userId,
      document_id: 'admin-journals',
      session_id: randomUUID(),
      language: 'vi',
      last_updated: String(query.last_updated ?? new Date().getFullYear()),
    };

    return this.sendAndCorrelate(payload);
  }

  private async sendAndCorrelate(
    payload: Record<string, unknown> & {
      user_id: string;
      document_id: string;
      session_id: string;
      event_type: string;
    },
    fallbackErrorCode = 604,
  ): Promise<Record<string, unknown>> {
    const correlationKey = this.correlationService.buildCorrelationKey({
      userId: payload.user_id,
      documentId: payload.document_id,
      sessionId: payload.session_id,
      eventType: payload.event_type,
    });

    if (this.correlationService.hasPending(correlationKey)) {
      throw new ConflictException(
        'An admin request with this correlation key is already pending',
      );
    }

    // Handle rejection immediately, even while Kafka publish is still pending.
    const pendingResult = this.correlationService
      .registerPending<Record<string, unknown>>(
        correlationKey,
        undefined,
        payload.event_type,
      )
      .then(
        (response) => ({ ok: true as const, response }),
        (error: unknown) => ({ ok: false as const, error }),
      );

    try {
      await this.kafkaProducer.publish(this.getTopic(), payload);
    } catch (error: unknown) {
      this.correlationService.clearPending(
        correlationKey,
        error instanceof Error ? error.message : 'Publish to Kafka failed',
      );
      throw this.createAiException(undefined, fallbackErrorCode);
    }

    const result = await pendingResult;
    if (!result.ok) {
      throw this.createAiException(result.error, fallbackErrorCode);
    }

    if ('error_code' in result.response || result.response.error) {
      throw this.createAiException(result.response, fallbackErrorCode);
    }

    return result.response;
  }

  private createAiException(error: unknown, fallbackErrorCode: number) {
    const detail =
      error && typeof error === 'object'
        ? (error as Record<string, unknown>)
        : undefined;
    const code = Number(detail?.error_code);
    const errorCode =
      Number.isInteger(code) && code > 0 ? code : fallbackErrorCode;
    const message =
      typeof detail?.message === 'string'
        ? detail.message
        : typeof detail?.error === 'string'
          ? detail.error
          : getAiErrorMessage(errorCode);

    return new BadRequestException({
      statusCode: HttpStatus.BAD_REQUEST,
      errorCode,
      error: getAiErrorName(errorCode),
      message,
    });
  }

  private getTopic(): string {
    return (
      this.configService.get<string>('kafka.topics.document_setup.request') ||
      'document_setup_request'
    );
  }
}
