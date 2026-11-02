import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { JobStatus, ModuleType, Prisma } from '@prisma/client';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { StreamingManagerService } from '../../../common/streaming/streaming-manager.service';
import { InterruptResumeService } from '../../../common/services/interrupt-resume.service';
import {
  ResumeWriterAgentDto,
  StartWriterAgentDto,
} from './dto/writer-chatbot.dto';
import { WriterInteractiveHelper } from './writer-interactive.helper';
import { getAiErrorMessage } from '../../../common/constants/ai-error-codes';
import { getAiModelConfig } from '../../../common/kafka/kafka.constants';

@Injectable()
export class WriterAgentService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly correlation: RequestCorrelationService,
    private readonly streamingManager: StreamingManagerService,
    private readonly interruptResume: InterruptResumeService,
    private readonly configService: ConfigService,
  ) {}

  async startWriterAgent(
    userId: string,
    sessionId: string,
    dto: StartWriterAgentDto,
  ) {
    const session = await WriterInteractiveHelper.ensureOwnedSession(
      this.prisma,
      userId,
      sessionId,
    );
    const documentId = dto.documentId || session.documentId;
    if (!documentId) {
      throw new BadRequestException('Writer agent requires a valid documentId');
    }

    const job = await this.prisma.job.create({
      data: {
        module: ModuleType.chatbot,
        action: 'writer',
        documentId,
        sessionId: session.id,
        status: JobStatus.pending,
        payload: dto as unknown as Prisma.InputJsonValue,
      },
    });

    const streamKey = this.streamingManager.buildStreamKey({
      userId,
      convId: session.id,
      sessionId: session.id,
      eventType: 'writer',
    });

    let chatHistoryStr = dto.chatHistory || '';
    if (!chatHistoryStr) {
      const historyMessages = await this.prisma.chatMessage.findMany({
        where: { sessionId: session.id },
        orderBy: { createdAt: 'asc' },
      });
      chatHistoryStr = historyMessages
        .map((m: { sender: string; message: string }) => `${m.sender}: ${m.message}`)
        .join('\n');
    }

    const summaryObj =
      dto.summary ||
      (session.summary as Record<string, unknown> | null) ||
      {};

    let outlineStr = '';
    if (dto.outline) {
      outlineStr =
        typeof dto.outline === 'string'
          ? dto.outline
          : JSON.stringify(dto.outline);
    } else {
      const outlineRecord = await this.prisma.outline.findUnique({
        where: { documentId },
      });
      if (outlineRecord?.structure) {
        outlineStr =
          typeof outlineRecord.structure === 'string'
            ? outlineRecord.structure
            : JSON.stringify(outlineRecord.structure);
      }
    }

    let domainStr = dto.domain || '';
    let fieldStr = dto.field || '';
    if (!domainStr || !fieldStr) {
      const doc = await this.prisma.document.findUnique({
        where: { id: documentId },
        select: { field: true, domains: true },
      });
      if (doc) {
        if (!fieldStr && doc.field) fieldStr = doc.field;
        if (!domainStr && Array.isArray(doc.domains) && doc.domains.length > 0) {
          domainStr = String(doc.domains[0]);
        }
      }
    }
    if (!domainStr) domainStr = 'Tổng quan';
    if (!fieldStr) fieldStr = 'Nghiên cứu khoa học';

    const payload = {
      event_type: 'writer',
      user_id: userId,
      conv_id: session.id,
      session_id: session.id,
      document_id: documentId,
      model_id: getAiModelConfig().defaultModel,
      language: dto.language || session.language || 'vi',
      instruction: dto.instruction || '',
      chat_history: chatHistoryStr,
      summary: summaryObj,
      outline: outlineStr,
      domain: domainStr,
      field: fieldStr,
      notes: dto.notes || [],
      use_web_search: dto.useWebSearch ?? session.webSearchEnabled ?? true,
      max_research_iterations: dto.maxResearchIterations || 3,
      resume_error: dto.resumeError ?? false,
      job_id: job.id,
    };

    const corrKey = this.correlation.buildCorrelationKey({
      userId,
      convId: session.id,
      sessionId: session.id,
      eventType: 'writer',
    });
    const pendingPromise = this.correlation.registerPending<
      Record<string, unknown>
    >(corrKey, 1500000, 'writer', job.id);

    const topic =
      this.configService.get<string>('kafka.topics.chatbot.request') ||
      'chatbot_request';

    try {
      await this.kafkaProducer.publish(topic, payload);
      await this.prisma.job.update({
        where: { id: job.id },
        data: { status: JobStatus.processing },
      });
    } catch (err) {
      this.correlation.clearPending(corrKey, (err as Error).message);
      return WriterInteractiveHelper.failJob(
        this.prisma,
        job.id,
        653,
        getAiErrorMessage(653),
      );
    }

    void pendingPromise
      .then(async (res) => {
        if (res.has_finished === false) {
          await this.interruptResume.suspendJob(
            job.id,
            res,
            'writer_input_required',
          );
          return;
        }
        const text = WriterInteractiveHelper.extractText(res);
        if (text) {
          await this.prisma.report.upsert({
            where: { documentId },
            update: { contentMarkdown: text },
            create: { documentId, contentMarkdown: text },
          });
        }
        await this.prisma.job.update({
          where: { id: job.id },
          data: {
            status: JobStatus.finished,
            result: res as unknown as Prisma.InputJsonValue,
          },
        });
        this.streamingManager.completeStream(streamKey);
      })
      .catch(async (err) => {
        await this.prisma.job.update({
          where: { id: job.id },
          data: {
            status: JobStatus.error,
            errorCode: '653',
            errorMessage: (err as Error).message,
          },
        });
        this.streamingManager.destroyStream(streamKey);
      });

    return { jobId: job.id, streamKey, status: 'started' };
  }

  getWriterStream(userId: string, sessionId: string) {
    const streamKey = this.streamingManager.buildStreamKey({
      userId,
      convId: sessionId,
      sessionId,
      eventType: 'writer',
    });
    return this.streamingManager.createStream(streamKey, 1500000);
  }

  async resumeWriterAgent(
    userId: string,
    sessionId: string,
    dto: ResumeWriterAgentDto,
  ) {
    await WriterInteractiveHelper.ensureOwnedSession(
      this.prisma,
      userId,
      sessionId,
    );
    const job = await this.prisma.job.findFirst({
      where: { sessionId, action: 'writer', status: JobStatus.suspended },
      orderBy: { createdAt: 'desc' },
    });
    if (!job) throw new NotFoundException('No suspended writer job found');

    await this.interruptResume.resumeJob(job.id, dto.userInput, dto.resumeStep);
    await this.prisma.job.update({
      where: { id: job.id },
      data: { status: JobStatus.processing },
    });
    return { success: true, jobId: job.id };
  }
}
