import {
  Controller,
  Get,
  Logger,
  NotFoundException,
  Query,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { KafkaProducerService } from './kafka-producer.service';
import { RequestCorrelationService } from './request-correlation.service';
import { getAiModelConfig } from '../common/kafka/kafka.constants';

@Controller('dev/smoke-test')
export class SmokeTestController {
  private readonly logger = new Logger(SmokeTestController.name);

  constructor(
    private readonly producerService: KafkaProducerService,
    private readonly correlationService: RequestCorrelationService,
    private readonly configService: ConfigService,
  ) {}

  /**
   * Endpoint thực hiện smoke test gửi event search_papers qua Kafka và chờ response.
   * Chỉ kích hoạt ở môi trường local / development.
   */
  @Get('kafka')
  async runKafkaSmokeTest(@Query('timeout') customTimeout?: string) {
    const isLocal =
      this.configService.get<boolean>('kafka.isLocal') ||
      process.env.NODE_ENV !== 'production';

    if (!isLocal) {
      throw new NotFoundException(
        'Smoke test endpoint is disabled in production environment',
      );
    }

    const eventType = 'search_papers';
    const userId = '00000000-0000-0000-0000-000000000001';
    const documentId = '00000000-0000-0000-0000-000000000002';
    const sessionId = `test-smoke-${Date.now()}`;

    const payload = {
      event_type: eventType,
      model_id: getAiModelConfig().defaultModel,
      user_id: userId,
      document_id: documentId,
      session_id: sessionId,
      language: 'vi',
      field: 'Computer Science',
      domain: 'Artificial Intelligence',
      subdomains: ['Machine Learning'],
      query: 'Artificial Intelligence in Education',
      search_range: 'all',
      country_filter: 'both',
      oa_filter: false,
      is_oa: false,
      is_tool: false,
      downloaded_papers: [],
      cut_off_year_low: 2018,
      cut_off_year_high: 2026,
      use_web_search: false,
    };

    const correlationKey = this.correlationService.buildCorrelationKey({
      userId,
      documentId,
      sessionId,
      eventType,
    });

    const topic =
      this.configService.get<string>('kafka.topics.document_setup.request') ||
      'document_setup_request';

    const timeoutMs = customTimeout ? Number(customTimeout) : 30000;

    this.logger.log(
      `Triggering Kafka smoke test for event [${eventType}] on topic [${topic}], key [${correlationKey}]`,
    );

    const responsePromise = this.correlationService.registerPending<unknown>(
      correlationKey,
      timeoutMs,
      eventType,
    );

    await this.producerService.publish(topic, payload);

    try {
      const response = await responsePromise;

      return {
        success: true,
        correlationKey,
        request: payload,
        response,
      };
    } catch (error) {
      return {
        success: false,
        correlationKey,
        request: payload,
        response: error,
      };
    }
  }
}
