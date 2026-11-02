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
import { getAiModelConfig } from '../../../common/kafka/kafka.constants';
import {
  ProposeMethodResumeDto,
  ProposeMethodStartDto,
} from './dto/propose-method-ai.dto';
import { WriteContentMongoHelper } from '../report/write-content-mongo.helper';
import { ProposedMethodHelper } from './proposed-method.helper';

const EVENT_TYPE = 'propose_method';
const TIMEOUT_MS = 300_000;
const PUBLISH_ERROR_CODE = 610;
const PROPOSE_ERROR_CODE = 637;

export interface ProposeResponse {
  has_finished?: boolean;
  ai_message?: string;
  final_proposal?: Record<string, unknown>;
  input_tokens?: number;
  output_tokens?: number;
  embed_tokens?: number;
}

interface DispatchParams {
  userId: string;
  documentId: string;
  sessionId: string;
  threadId: string;
  payload: Record<string, unknown>;
}

@Injectable()
export class ProposedMethodAiService {
  private readonly logger = new Logger(ProposedMethodAiService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly correlationService: RequestCorrelationService,
    private readonly configService: ConfigService,
  ) {}

  /** Lượt đầu: AI đề xuất mô hình nghiên cứu / giả thuyết / bảng câu hỏi. */
  start(userId: string, documentId: string, dto: ProposeMethodStartDto) {
    const threadId = dto.threadId || randomUUID();
    const sessionId = dto.sessionId || `sess-${EVENT_TYPE}-${randomUUID()}`;

    return this.dispatch({
      userId,
      documentId,
      sessionId,
      threadId,
      payload: {
        thread_id: threadId,
        user_input: dto.userInput || '',
        final_proposal: dto.finalProposal,
        research_type: dto.researchType ?? { type: 0 },
        auto_mode: dto.autoMode ?? false,
        inherit_mode: dto.inheritMode ?? false,
        resume_method: false,
        resume_error: false,
        db: 'redis',
        model_id: getAiModelConfig().defaultModel,
      },
    });
  }

  /** Lượt tiếp: người dùng trả lời câu hỏi của AI (giữ nguyên `thread_id`). */
  async resume(
    userId: string,
    documentId: string,
    dto: ProposeMethodResumeDto,
  ) {
    const sessionId = dto.sessionId || `sess-${EVENT_TYPE}-${randomUUID()}`;
    const finalProposal = await ProposedMethodHelper.resolveProposal(
      this.prisma,
      documentId,
      dto.finalProposal,
    );

    return this.dispatch({
      userId,
      documentId,
      sessionId,
      threadId: dto.threadId,
      payload: {
        thread_id: dto.threadId,
        user_input: dto.userInput,
        final_proposal: finalProposal,
        research_type: dto.researchType ?? { type: 0 },
        resume_method: true,
        resume_error: false,
        db: 'redis',
        model_id: getAiModelConfig().defaultModel,
      },
    });
  }

  private async dispatch(params: DispatchParams) {
    const { userId, documentId, sessionId, threadId, payload } = params;

    const document = await this.prisma.document.findFirst({
      where: { id: documentId, userId },
      select: { id: true },
    });
    if (!document) {
      throw new NotFoundException('Document not found');
    }

    const job = await this.prisma.job.create({
      data: {
        module: ModuleType.write_report,
        action: EVENT_TYPE,
        documentId,
        sessionId,
        threadId,
        status: JobStatus.pending,
        payload: payload as Prisma.InputJsonValue,
      },
    });

    const mongoDocId = WriteContentMongoHelper.toMongoDocId(documentId);
    const kafkaPayload: Record<string, unknown> = {
      event_type: EVENT_TYPE,
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
          errorMessage: message,
        },
      });
      throw publishError;
    }

    let response: ProposeResponse;
    try {
      response = (await pendingPromise) as ProposeResponse;
    } catch (aiError: unknown) {
      const err = aiError as Record<string, unknown>;
      const code = String(
        err?.error_code || err?.errorCode || PROPOSE_ERROR_CODE,
      );
      const msg = String(err?.error || err?.message || 'Propose method failed');
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

    if (response.has_finished === false) {
      await this.prisma.job.update({
        where: { id: job.id },
        data: {
          status: JobStatus.suspended,
          resumeStep: 'need_propose_method',
          threadId,
          result: response as unknown as Prisma.InputJsonValue,
        },
      });

      return {
        job_id: job.id,
        event_type: EVENT_TYPE,
        document_id: documentId,
        session_id: sessionId,
        thread_id: threadId,
        status: 'suspended',
        ai_message: response.ai_message ?? null,
        data: response,
      };
    }

    const proposal = (response.final_proposal ?? {}) as Record<string, unknown>;
    await ProposedMethodHelper.saveProposedMethod(
      this.prisma,
      documentId,
      proposal,
      this.logger,
    );
    await this.prisma.job.update({
      where: { id: job.id },
      data: {
        status: JobStatus.finished,
        threadId,
        result: response as unknown as Prisma.InputJsonValue,
      },
    });

    this.logger.log(`[${EVENT_TYPE}] hoàn tất job ${job.id} cho ${documentId}`);

    return {
      job_id: job.id,
      event_type: EVENT_TYPE,
      document_id: documentId,
      session_id: sessionId,
      thread_id: threadId,
      status: 'finished',
      data: response,
    };
  }
}
