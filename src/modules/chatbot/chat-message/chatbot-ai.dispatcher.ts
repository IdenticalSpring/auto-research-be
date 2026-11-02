import { BadRequestException, HttpStatus, Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { JobStatus, ModuleType, Prisma } from '@prisma/client';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import {
  getAiErrorMessage,
  getAiErrorName,
} from '../../../common/constants/ai-error-codes';
import { getAiModelConfig } from '../../../common/kafka/kafka.constants';
import { SendAiMessageDto } from './dto/send-ai-message.dto';

export interface ChatbotAiDispatchOptions {
  userId: string;
  sessionId: string;
  documentId?: string;
  eventType: string;
  errorCode: number;
  dto: SendAiMessageDto;
  kafkaData: Record<string, unknown>;
  onSuccess?: (data: Record<string, unknown>) => Promise<void>;
  onError?: (err: unknown) => Promise<void>;
}

@Injectable()
export class ChatbotAiDispatcher {
  constructor(
    private readonly prisma: PrismaService,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly correlation: RequestCorrelationService,
    private readonly configService: ConfigService,
  ) {}

  async dispatch(opt: ChatbotAiDispatchOptions) {
    const job = await this.prisma.job.create({
      data: {
        module: ModuleType.chatbot,
        action: opt.eventType,
        documentId: opt.documentId,
        sessionId: opt.sessionId,
        status: JobStatus.pending,
        payload: opt.dto as unknown as Prisma.InputJsonValue,
      },
    });

    const payload = {
      event_type: opt.eventType,
      model_id: getAiModelConfig().defaultModel,
      user_id: opt.userId,
      conv_id: opt.sessionId,
      session_id: opt.sessionId,
      document_id: opt.documentId,
      language: opt.dto.language || 'vi',
      job_id: job.id,
      ...opt.kafkaData,
    };

    const corrKey = this.correlation.buildCorrelationKey({
      userId: opt.userId,
      convId: opt.sessionId,
      sessionId: opt.sessionId,
      eventType: opt.eventType,
    });
    const pendingPromise = this.correlation.registerPending<
      Record<string, unknown>
    >(corrKey, undefined, opt.eventType, job.id);
    const topic =
      this.configService.get<string>('kafka.topics.chatbot.request') ||
      'chatbot_request';

    try {
      await this.kafkaProducer.publish(topic, payload);
      await this.prisma.job.update({
        where: { id: job.id },
        data: { status: JobStatus.processing },
      });
    } catch (publishErr: unknown) {
      const msg =
        publishErr instanceof Error ? publishErr.message : 'Publish failed';
      this.correlation.clearPending(corrKey, msg);
      if (opt.onError) await opt.onError(publishErr);
      return this.failJob(
        job.id,
        opt.errorCode,
        getAiErrorMessage(opt.errorCode),
      );
    }

    try {
      const res = await pendingPromise;
      if (opt.onSuccess) await opt.onSuccess(res);
      await this.prisma.job.update({
        where: { id: job.id },
        data: {
          status: JobStatus.finished,
          result: res as unknown as Prisma.InputJsonValue,
        },
      });
      return { jobId: job.id, sessionId: opt.sessionId, ...res };
    } catch (err: unknown) {
      if (opt.onError) await opt.onError(err);
      let code = opt.errorCode;
      let msg = getAiErrorMessage(opt.errorCode);
      if (err && typeof err === 'object') {
        const obj = err as Record<string, unknown>;
        if (obj.error_code) {
          code = Number(obj.error_code);
          msg =
            typeof obj.error === 'string' ? obj.error : getAiErrorMessage(code);
        } else if (typeof obj.message === 'string') {
          msg = obj.message;
        }
      }
      return this.failJob(job.id, code, msg);
    }
  }

  private async failJob(
    jobId: string,
    code: number,
    msg: string,
  ): Promise<never> {
    await this.prisma.job.update({
      where: { id: jobId },
      data: {
        status: JobStatus.error,
        errorCode: String(code),
        errorMessage: msg,
      },
    });
    throw new BadRequestException({
      statusCode: HttpStatus.BAD_REQUEST,
      errorCode: code,
      error: getAiErrorName(code),
      message: msg,
    });
  }
}
