import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ClientKafka } from '@nestjs/microservices';
import { Job } from '@prisma/client';
import { KAFKA_CLIENT } from '../../../common/messaging.tokens';
import { getAiModelConfig } from '../../../common/kafka/kafka.constants';
import { normalizeAiLanguage } from '../../../common/helpers/academic-level.helper';

@Injectable()
export class JobsKafkaProducer implements OnModuleInit {
  private readonly logger = new Logger(JobsKafkaProducer.name);

  constructor(
    @Inject(KAFKA_CLIENT) private readonly clientKafka: ClientKafka,
    private readonly configService: ConfigService,
  ) {}

  async onModuleInit() {
    try {
      await this.clientKafka.connect();
      this.logger.log('Successfully connected Kafka Producer');
    } catch (error) {
      this.logger.warn(`Kafka producer connection deferred/failed: ${error}`);
    }
  }

  async publishJob(job: Job): Promise<void> {
    const topicsConfig =
      this.configService.get<Record<string, { request: string }>>(
        'kafka.topics',
      );
    const moduleConfig = topicsConfig?.[job.module];

    if (!moduleConfig?.request) {
      this.logger.error(`No request topic found for module: ${job.module}`);
      return;
    }

    const isLocal = this.configService.get<boolean>('app.isLocal', true);
    const localLlmBaseUrl = this.configService.get<string>(
      'app.localLlmBaseUrl',
    );

    const topic = moduleConfig.request;
    const rawPayload =
      job.payload && typeof job.payload === 'object'
        ? (job.payload as Record<string, any>)
        : {};

    const payload = {
      job_id: job.id,
      module: job.module,
      action: job.action,
      event_type: rawPayload.event_type || job.action,
      document_id: job.documentId || rawPayload.document_id || undefined,
      session_id:
        job.sessionId || rawPayload.session_id || `sess-job-${job.id}`,
      user_id: rawPayload.user_id || '00000000-0000-0000-0000-000000000001',
      resume_step: job.resumeStep || undefined,
      model_id: getAiModelConfig().defaultModel,
      localllm_base_url: isLocal ? localLlmBaseUrl : undefined,
      domains_num: rawPayload.domains_num || rawPayload.domainsNum || 3,
      subdomains_num:
        rawPayload.subdomains_num || rawPayload.subdomainsNum || 3,
      titles_num: rawPayload.titles_num || rawPayload.titlesNum || 3,
      keywords_num: rawPayload.keywords_num || rawPayload.keywordsNum || 5,
      is_tool: rawPayload.is_tool ?? false,
      language: normalizeAiLanguage(rawPayload.language),
      ...rawPayload,
      payload: rawPayload,
    };

    this.logger.log(
      `Publishing job ${job.id} [${job.module}:${job.action}] to topic: ${topic}`,
    );

    try {
      this.clientKafka.emit(topic, JSON.stringify(payload));
    } catch (error) {
      this.logger.error(
        `Failed to publish job ${job.id} to Kafka topic ${topic}: ${error}`,
      );
    }
  }
}
