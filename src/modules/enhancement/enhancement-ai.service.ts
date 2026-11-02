import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { EnhancementType, SuggestionStatus } from '@prisma/client';
import { randomUUID } from 'node:crypto';
import { PrismaService } from '../../database/prisma.service';
import { KafkaProducerService } from '../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../messaging/request-correlation.service';
import {
  AiInDocDto,
  EnhanceAllDto,
  EnhanceDto,
  GetSuggestionsDto,
} from './dto/enhancement-ai.dto';
import { EnhancementAiHelper } from './enhancement-ai.helper';
import { getAiModelConfig } from '../../common/kafka/kafka.constants';

@Injectable()
export class EnhancementAiService {
  private readonly logger = new Logger(EnhancementAiService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly correlation: RequestCorrelationService,
    private readonly configService: ConfigService,
  ) {}

  async aiInDoc(userId: string, dto: AiInDocDto) {
    await EnhancementAiHelper.ensureOwnedDocument(this.prisma, userId, dto.documentId);
    const sessionId = randomUUID();
    const proposal = await EnhancementAiHelper.resolveProposal(
      this.prisma,
      dto.documentId,
      dto.proposal,
    );

    const kafkaPayload = {
      event_type: 'ai_in_doc',
      model_id: getAiModelConfig().defaultModel,
      user_id: userId,
      document_id: dto.documentId,
      session_id: sessionId,
      language: dto.language || 'vi',
      section_id: dto.sectionId,
      paragraphs: dto.paragraphs,
      paragraph_context: dto.paragraphContext || '',
      action: dto.action,
      proposal,
    };

    const { jobId, res } = await EnhancementAiHelper.executeAiFlow(
      this.prisma,
      this.kafkaProducer,
      this.correlation,
      this.getTopic(),
      { userId, documentId: dto.documentId, sessionId, eventType: 'ai_in_doc', payloadDto: dto },
      kafkaPayload,
    );

    const session = await this.prisma.enhancementSession.create({
      data: {
        documentId: dto.documentId,
        selectedText: dto.paragraphs,
        contextText: dto.paragraphContext,
        userPrompt: `Action: ${dto.action}`,
        enhancementType: dto.action as unknown as EnhancementType,
      },
    });

    await EnhancementAiHelper.finishJob(this.prisma, jobId, res);
    return { jobId, sessionId: session.id, response: res.response, ...res };
  }

  async getSuggestions(userId: string, dto: GetSuggestionsDto) {
    await EnhancementAiHelper.ensureOwnedDocument(this.prisma, userId, dto.documentId);
    const sessionId = randomUUID();

    const kafkaPayload = {
      event_type: 'get_suggestions',
      model_id: getAiModelConfig().defaultModel,
      user_id: userId,
      document_id: dto.documentId,
      session_id: sessionId,
      language: dto.language || 'vi',
      section: dto.section,
      section_context: dto.sectionContext || '',
      processed_docs: dto.processedDocs || [],
    };

    const { jobId, res } = await EnhancementAiHelper.executeAiFlow(
      this.prisma,
      this.kafkaProducer,
      this.correlation,
      this.getTopic(),
      { userId, documentId: dto.documentId, sessionId, eventType: 'get_suggestions', payloadDto: dto },
      kafkaPayload,
    );

    const session = await this.prisma.enhancementSession.create({
      data: {
        documentId: dto.documentId,
        selectedText: dto.section,
        contextText: dto.sectionContext,
      },
    });

    const rawList = (res.suggested_paragraphs || res.suggestedParagraphs) as Array<Record<string, unknown>>;
    const suggestions = await EnhancementAiHelper.saveSuggestions(this.prisma, session.id, rawList);

    await EnhancementAiHelper.finishJob(this.prisma, jobId, res);
    return {
      jobId,
      sessionId: session.id,
      suggestedParagraphs: rawList || [],
      suggestions,
      disableEdit: res.disable_edit ?? false,
      ...res,
    };
  }

  async enhance(userId: string, dto: EnhanceDto) {
    await EnhancementAiHelper.ensureOwnedDocument(this.prisma, userId, dto.documentId);
    const sessionId = randomUUID();

    const kafkaPayload = {
      event_type: 'enhance',
      model_id: getAiModelConfig().defaultModel,
      user_id: userId,
      document_id: dto.documentId,
      session_id: sessionId,
      language: dto.language || 'vi',
      section: dto.section,
      suggested_paragraph: dto.suggestedParagraph,
    };

    const { jobId, res } = await EnhancementAiHelper.executeAiFlow(
      this.prisma,
      this.kafkaProducer,
      this.correlation,
      this.getTopic(),
      { userId, documentId: dto.documentId, sessionId, eventType: 'enhance', payloadDto: dto },
      kafkaPayload,
    );

    if (dto.suggestionId) {
      await this.prisma.enhancementSuggestion
        .update({
          where: { id: dto.suggestionId },
          data: { status: SuggestionStatus.applied },
        })
        .catch(() => null);
    }

    await EnhancementAiHelper.finishJob(this.prisma, jobId, res);
    return { jobId, updatedParagraphs: res.updated_paragraphs, ...res };
  }

  async enhanceAll(userId: string, dto: EnhanceAllDto) {
    await EnhancementAiHelper.ensureOwnedDocument(this.prisma, userId, dto.documentId);
    const sessionId = dto.sessionId || randomUUID();

    const kafkaPayload = {
      event_type: 'enhance_all',
      model_id: getAiModelConfig().defaultModel,
      user_id: userId,
      document_id: dto.documentId,
      session_id: sessionId,
      language: dto.language || 'vi',
      section: dto.section,
      suggested_paragraphs: dto.suggestedParagraphs,
    };

    const { jobId, res } = await EnhancementAiHelper.executeAiFlow(
      this.prisma,
      this.kafkaProducer,
      this.correlation,
      this.getTopic(),
      { userId, documentId: dto.documentId, sessionId, eventType: 'enhance_all', payloadDto: dto },
      kafkaPayload,
    );

    if (dto.sessionId) {
      await this.prisma.enhancementSuggestion
        .updateMany({
          where: { sessionId: dto.sessionId, status: SuggestionStatus.pending },
          data: { status: SuggestionStatus.applied },
        })
        .catch(() => null);
    }

    await EnhancementAiHelper.finishJob(this.prisma, jobId, res);
    return { jobId, updatedParagraphs: res.updated_paragraphs, ...res };
  }

  private getTopic(): string {
    return (
      this.configService.get<string>('kafka.topics.enhancement.request') ||
      'enhancement_request'
    );
  }
}
