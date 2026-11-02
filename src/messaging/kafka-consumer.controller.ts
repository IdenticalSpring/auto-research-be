import { Controller, Logger, Optional } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { RequestCorrelationService } from './request-correlation.service';
import { AiStatusMapperService } from '../common/services/ai-status-mapper.service';
import { TokenUsageService } from '../common/services/token-usage.service';

@Controller()
export class KafkaConsumerController {
  private readonly logger = new Logger(KafkaConsumerController.name);

  constructor(
    private readonly correlationService: RequestCorrelationService,
    @Optional() private readonly aiStatusMapper?: AiStatusMapperService,
    @Optional() private readonly tokenUsageService?: TokenUsageService,
  ) {}

  @EventPattern('document_setup_response')
  async handleDocumentSetupResponse(
    @Payload() rawData: unknown,
  ): Promise<void> {
    await this.processMessage('document_setup', rawData);
  }

  @EventPattern('write_section_response')
  async handleWriteSectionResponse(@Payload() rawData: unknown): Promise<void> {
    await this.processMessage('write_report', rawData);
  }

  @EventPattern('chatbot_response')
  async handleChatbotResponse(@Payload() rawData: unknown): Promise<void> {
    await this.processMessage('chatbot', rawData);
  }

  @EventPattern('enhancement_response')
  async handleEnhancementResponse(@Payload() rawData: unknown): Promise<void> {
    await this.processMessage('enhancement', rawData);
  }

  async processMessage(domain: string, rawData: unknown): Promise<void> {
    try {
      const payload =
        typeof rawData === 'string'
          ? (JSON.parse(rawData) as Record<string, unknown>)
          : (rawData as Record<string, unknown>);

      if (!payload || typeof payload !== 'object') {
        this.logger.warn(`[${domain}] Received invalid non-object payload`);
        return;
      }

      const userId = String(payload.user_id || payload.userId || '');
      const documentId = payload.document_id
        ? String(payload.document_id)
        : payload.documentId
          ? String(payload.documentId)
          : undefined;
      const convId = payload.conv_id
        ? String(payload.conv_id)
        : payload.convId
          ? String(payload.convId)
          : undefined;
      const sessionId = payload.session_id
        ? String(payload.session_id)
        : payload.sessionId
          ? String(payload.sessionId)
          : undefined;
      const eventType = String(
        payload.event_type || payload.eventType || 'unknown',
      );

      const correlationKey = this.correlationService.buildCorrelationKey({
        userId,
        documentId,
        convId,
        sessionId,
        eventType,
      });

      this.logger.log(
        `[${domain}] Routing Kafka response for event [${eventType}] with key [${correlationKey}]: ${JSON.stringify(payload)}`,
      );

      const jobId =
        payload.job_id !== undefined && payload.job_id !== null
          ? String(payload.job_id)
          : payload.jobId !== undefined && payload.jobId !== null
            ? String(payload.jobId)
            : undefined;

      if (jobId && this.aiStatusMapper) {
        const normalized = this.aiStatusMapper.normalizeResponse(payload);
        await this.aiStatusMapper.syncJobStatus(jobId, normalized);
      }

      if (this.tokenUsageService) {
        void this.tokenUsageService
          .recordUsageFromPayload(payload)
          .catch((err) => {
            this.logger.warn(
              `[${domain}] Failed to track token usage: ${(err as Error).message}`,
            );
          });
      }

      this.correlationService.dispatchResponse(correlationKey, payload);
    } catch (error) {
      this.logger.error(
        `[${domain}] Failed to process Kafka response: ${(error as Error).message}`,
      );
    }
  }
}
