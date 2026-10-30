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
import { CheckLogicDataDto, CommentDataDto } from './dto/data-analysis.dto';

/** Lỗi publish Kafka dùng chung với các service AI khác trong dự án. */
const PUBLISH_ERROR_CODE = 610;
/** AI default timeout cho phân tích dữ liệu là 300s (error_type.py:144). */
const ANALYSIS_TIMEOUT_MS = 300_000;

type DataAnalysisEvent = 'comment_data' | 'check_logic_data';

interface DispatchParams {
  userId: string;
  datasetId: string;
  eventType: DataAnalysisEvent;
  errorCode: number;
  payload: Record<string, unknown>;
  sessionId?: string;
}

@Injectable()
export class DataAnalysisAiService {
  private readonly logger = new Logger(DataAnalysisAiService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly correlationService: RequestCorrelationService,
    private readonly configService: ConfigService,
  ) {}

  /**
   * `comment_data`: AI nhận xét tính hợp lệ của dữ liệu theo từng biến.
   * Không có stream — AI trả 1 message khi xong (timeout 300s).
   */
  commentData(userId: string, datasetId: string, dto: CommentDataDto) {
    return this.dispatch({
      userId,
      datasetId,
      eventType: 'comment_data',
      errorCode: 636,
      sessionId: dto.sessionId,
      payload: {
        final_proposal: dto.finalProposal,
        data: dto.data,
        variables: dto.variables,
        thread_id: dto.threadId ?? randomUUID(),
      },
    });
  }

  /** `check_logic_data`: kiểm tra logic biến/thang đo trước khi chạy `analyzer`. */
  checkLogicData(userId: string, datasetId: string, dto: CheckLogicDataDto) {
    return this.dispatch({
      userId,
      datasetId,
      eventType: 'check_logic_data',
      errorCode: 642,
      sessionId: dto.sessionId,
      payload: {
        variables: dto.variables,
      },
    });
  }

  private async dispatch(params: DispatchParams) {
    const { userId, datasetId, eventType, errorCode, payload, sessionId } =
      params;

    const dataset = await this.prisma.dataset.findFirst({
      where: { id: datasetId, document: { userId } },
      select: { id: true, documentId: true },
    });
    if (!dataset) {
      throw new NotFoundException('Dataset not found');
    }

    const resolvedSessionId = sessionId || `sess-${eventType}-${randomUUID()}`;

    const job = await this.prisma.job.create({
      data: {
        module: ModuleType.write_report,
        action: eventType,
        documentId: dataset.documentId,
        sessionId: resolvedSessionId,
        status: JobStatus.pending,
        payload: payload as Prisma.InputJsonValue,
      },
    });

    // Identifier luôn gửi kèm để AI echo lại → BE correlate được (Q-06/D1/D5).
    const kafkaPayload: Record<string, unknown> = {
      event_type: eventType,
      model_id: AI_DEFAULT_MODEL,
      user_id: userId,
      document_id: dataset.documentId,
      session_id: resolvedSessionId,
      language: 'Vietnamese',
      ...payload,
    };

    const correlationKey = this.correlationService.buildCorrelationKey({
      userId,
      documentId: dataset.documentId,
      sessionId: resolvedSessionId,
      eventType,
    });

    const pendingPromise = this.correlationService.registerPending<
      Record<string, unknown>
    >(correlationKey, ANALYSIS_TIMEOUT_MS, eventType, job.id);

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
      throw new BadRequestException(getAiErrorMessage(PUBLISH_ERROR_CODE));
    }

    let result: Record<string, unknown>;
    try {
      result = (await pendingPromise) as Record<string, unknown>;
    } catch (aiError: unknown) {
      const err = aiError as Record<string, unknown>;
      const code = String(err?.error_code || err?.errorCode || errorCode);
      const msg = String(
        err?.error ||
          err?.message ||
          getAiErrorMessage(Number(code)) ||
          'AI processing failed',
      );
      await this.prisma.job.update({
        where: { id: job.id },
        data: {
          status: JobStatus.error,
          errorCode: code,
          errorMessage: msg,
        },
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
        result: result as Prisma.InputJsonValue,
      },
    });

    if (eventType === 'comment_data' && result) {
      const logValue =
        typeof result.comments === 'string'
          ? result.comments
          : JSON.stringify(result.comments ?? result);
      await this.prisma.dataset.update({
        where: { id: dataset.id },
        data: {
          analyzeLog: logValue,
        },
      });
    }

    this.logger.log(
      `[${eventType}] Xong job ${job.id} cho document ${dataset.documentId}`,
    );

    return {
      job_id: job.id,
      event_type: eventType,
      document_id: dataset.documentId,
      session_id: resolvedSessionId,
      data: result,
    };
  }
}
