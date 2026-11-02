import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ChatSender, JobStatus, ModuleType, Prisma } from '@prisma/client';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { InterruptResumeService } from '../../../common/services/interrupt-resume.service';
import { WriterChatbotDto } from './dto/writer-chatbot.dto';
import { WriterInteractiveHelper } from './writer-interactive.helper';
import { getAiErrorMessage } from '../../../common/constants/ai-error-codes';
import { getAiModelConfig } from '../../../common/kafka/kafka.constants';

@Injectable()
export class WriterChatbotService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly correlation: RequestCorrelationService,
    private readonly interruptResume: InterruptResumeService,
    private readonly configService: ConfigService,
  ) {}

  async interactWriterChatbot(
    userId: string,
    sessionId: string,
    dto: WriterChatbotDto,
  ) {
    const session = await WriterInteractiveHelper.ensureOwnedSession(
      this.prisma,
      userId,
      sessionId,
    );

    const lastSuspended = await this.prisma.job.findFirst({
      where: {
        sessionId,
        action: 'writer_chatbot',
        status: JobStatus.suspended,
      },
      orderBy: { createdAt: 'desc' },
    });
    const isResume = dto.resume ?? lastSuspended !== null;

    const chatHistory = await WriterInteractiveHelper.getChatHistory(
      this.prisma,
      session.id,
    );
    const userMsg = await WriterInteractiveHelper.saveMessage(
      this.prisma,
      session.id,
      ChatSender.user,
      dto.question,
    );

    const job = await this.prisma.job.create({
      data: {
        module: ModuleType.chatbot,
        action: 'writer_chatbot',
        documentId: session.documentId,
        sessionId: session.id,
        status: JobStatus.pending,
        payload: dto as unknown as Prisma.InputJsonValue,
      },
    });

    const payload = {
      event_type: 'writer_chatbot',
      user_id: userId,
      conv_id: session.id,
      session_id: session.id,
      document_id: session.documentId,
      model_id: getAiModelConfig().defaultModel,
      language: dto.language || session.language || 'vi',
      question: dto.question,
      resume: isResume,
      template_key: dto.templateKey,
      chat_history: chatHistory,
      short_answer: dto.shortAnswer ?? false,
      use_web_search_chatbot:
        dto.useWebSearchChatbot ?? session.webSearchEnabled ?? false,
      job_id: job.id,
    };

    const corrKey = this.correlation.buildCorrelationKey({
      userId,
      convId: session.id,
      sessionId: session.id,
      eventType: 'writer_chatbot',
    });
    const pendingPromise = this.correlation.registerPending<
      Record<string, unknown>
    >(corrKey, 120000, 'writer_chatbot', job.id);

    const topic =
      this.configService.get<string>('kafka.topics.chatbot.request') ||
      'chatbot_request';

    try {
      await this.kafkaProducer.publish(topic, payload);
      await this.prisma.job.update({
        where: { id: job.id },
        data: { status: JobStatus.processing },
      });
    } catch (publishErr) {
      this.correlation.clearPending(corrKey, (publishErr as Error).message);
      return WriterInteractiveHelper.failJob(
        this.prisma,
        job.id,
        652,
        getAiErrorMessage(652),
      );
    }

    try {
      const res = await pendingPromise;
      const isDone = res.done_chat === true;
      const responseText = WriterInteractiveHelper.extractText(res);

      const assistantMsg = await WriterInteractiveHelper.saveMessage(
        this.prisma,
        session.id,
        ChatSender.assistant,
        responseText,
        res,
      );

      if (!isDone) {
        await this.interruptResume.suspendJob(
          job.id,
          res,
          String(res.template_key || 'user_clarification'),
        );
        return {
          doneChat: false,
          jobId: job.id,
          userMessage: userMsg,
          assistantMessage: assistantMsg,
          aiMessage: responseText,
          templateKey: res.template_key,
          ...res,
        };
      }

      if (session.documentId && res.outline) {
        await this.prisma.outline.upsert({
          where: { documentId: session.documentId },
          update: { structure: res.outline as Prisma.InputJsonValue },
          create: {
            documentId: session.documentId,
            structure: res.outline as Prisma.InputJsonValue,
          },
        });
      }

      if (res.summary) {
        await this.prisma.chatSession.update({
          where: { id: session.id },
          data: { summary: res.summary as Prisma.InputJsonValue },
        });
      }

      await this.prisma.job.update({
        where: { id: job.id },
        data: {
          status: JobStatus.finished,
          result: res as unknown as Prisma.InputJsonValue,
        },
      });

      return {
        doneChat: true,
        jobId: job.id,
        userMessage: userMsg,
        assistantMessage: assistantMsg,
        summary: res.summary,
        outline: res.outline,
        ...res,
      };
    } catch (err: unknown) {
      return WriterInteractiveHelper.failJob(
        this.prisma,
        job.id,
        652,
        WriterInteractiveHelper.extractErrorMsg(err, 652),
      );
    }
  }
}
