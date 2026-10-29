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
import { AI_DEFAULT_MODEL } from '../../../common/kafka/kafka.constants';
import { AnalyzeRequestDto } from './dto/data-analysis.dto';

type AnalyzeEvent = 'analyzer' | 'analyzer_tool';
const PUBLISH_ERROR_CODE = 610;

export interface AnalyzeResponse {
  has_finished?: boolean;
  is_tool?: boolean;
  ai_message?: string;
  /** URL báo cáo HTML trên MinIO (rỗng nếu upload lỗi). */
  result_html?: string;
  analyze_log?: string;
  detailed_logs?: unknown;
  generated_files?: unknown;
  current_variables?: unknown;
  upload_failed?: boolean;
  analysis_results?: Record<string, unknown>;
  charts?: Array<Record<string, unknown>>;
  summary?: string;
  input_tokens?: number;
  output_tokens?: number;
  embed_tokens?: number;
  web_search_call?: number;
}

@Injectable()
export class AnalyzeAiService {
  private readonly logger = new Logger(AnalyzeAiService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly correlationService: RequestCorrelationService,
    private readonly configService: ConfigService,
  ) {}

  /** `analyzer` — báo cáo phân tích chuyên sâu. */
  analyze(userId: string, datasetId: string, dto: AnalyzeRequestDto) {
    return this.run('analyzer', userId, datasetId, dto);
  }

  /** `analyzer_tool` — cùng pipeline, AI chạy nhanh hơn. */
  analyzeTool(userId: string, datasetId: string, dto: AnalyzeRequestDto) {
    return this.run('analyzer_tool', userId, datasetId, dto);
  }

  private async run(
    eventType: AnalyzeEvent,
    userId: string,
    datasetId: string,
    dto: AnalyzeRequestDto,
  ) {
    if (eventType === 'analyzer') {
      if (!dto.variables?.some((v) => v.role === 'dependent')) {
        throw new BadRequestException('Hồi quy OLS (analyzer) yêu cầu tối thiểu 1 biến phụ thuộc (role: "dependent")');
      }
      if (!dto.variables?.some((v) => v.role === 'independent')) {
        throw new BadRequestException('Hồi quy OLS (analyzer) yêu cầu tối thiểu 1 biến độc lập (role: "independent")');
      }
    }

    const dataset = await this.prisma.dataset.findFirst({
      where: { id: datasetId, document: { userId } },
      select: { id: true, documentId: true },
    });
    if (!dataset) {
      throw new NotFoundException('Dataset not found');
    }

    const sessionId = dto.sessionId || `sess-${eventType}-${randomUUID()}`;
    const threadId = dto.threadId || randomUUID();

    const payload: Record<string, unknown> = {
      event_type: eventType,
      model_id: AI_DEFAULT_MODEL,
      user_id: userId,
      document_id: dataset.documentId,
      session_id: sessionId,
      language: 'Vietnamese',
      conv_id: dto.convId || sessionId,
      thread_id: threadId,
      final_proposal: dto.finalProposal,
      data: dto.data,
      variables: dto.variables,
      auto_plan_mode: dto.autoPlanMode ?? true,
      auto_action_mode: dto.autoActionMode ?? true,
      manual_plan_mode: dto.manualPlanMode ?? false,
      tool_configs: dto.toolConfigs ?? [],
      resume: dto.resume ?? false,
      resume_error: dto.resumeError ?? false,
      feedback: dto.feedback ?? '',
      db: dto.db ?? 'redis',
    };

    const job = await this.prisma.job.create({
      data: {
        module: ModuleType.write_report,
        action: eventType,
        documentId: dataset.documentId,
        sessionId,
        threadId,
        status: JobStatus.pending,
        payload: payload as Prisma.InputJsonValue,
      },
    });

    const correlationKey = this.correlationService.buildCorrelationKey({
      userId,
      documentId: dataset.documentId,
      sessionId,
      eventType,
    });

    const pendingPromise = this.correlationService.registerPending<
      Record<string, unknown>
    >(correlationKey, undefined, eventType, job.id);

    const topic =
      this.configService.get<string>('kafka.topics.write_report.request') ||
      'write_section_request_local';

    try {
      await this.kafkaProducer.publish(topic, payload);
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

    let response: AnalyzeResponse;
    try {
      response = (await pendingPromise) as AnalyzeResponse;
    } catch (aiError: unknown) {
      const err = aiError as Record<string, unknown>;
      const code = String(err?.error_code || err?.errorCode || '631');
      const msg = String(err?.error || err?.message || 'AI Analysis failed');
      await this.prisma.job.update({
        where: { id: job.id },
        data: { status: JobStatus.error, errorCode: code, errorMessage: msg },
      });
      throw new BadRequestException({ statusCode: 400, errorCode: code, message: msg });
    }

    if (response.has_finished === false) {
      await this.prisma.job.update({
        where: { id: job.id },
        data: {
          status: JobStatus.suspended,
          resumeStep: 'analyzer_feedback',
          threadId,
          result: response as unknown as Prisma.InputJsonValue,
        },
      });

      return {
        job_id: job.id,
        event_type: eventType,
        document_id: dataset.documentId,
        session_id: sessionId,
        thread_id: threadId,
        status: 'suspended',
        ai_message: response.ai_message ?? null,
        data: response,
      };
    }

    await Promise.all([
      this.prisma.job.update({
        where: { id: job.id },
        data: {
          status: JobStatus.finished,
          threadId,
          result: response as unknown as Prisma.InputJsonValue,
        },
      }),
      this.prisma.dataset.update({
        where: { id: datasetId },
        data: {
          analysisResults: (response.analysis_results ??
            response.detailed_logs ??
            response) as Prisma.InputJsonValue,
          analyzeLog: response.analyze_log ?? 'Done',
          detailedLogs: (response.detailed_logs ??
            null) as Prisma.InputJsonValue,
          generatedFiles: (response.generated_files ??
            null) as Prisma.InputJsonValue,
        },
      }),
    ]);

    const reportHtmlMissing = !response.result_html;
    this.logger.log(
      `[${eventType}] xong job ${job.id} cho ${dataset.documentId} (reportHtmlMissing=${reportHtmlMissing})`,
    );

    return {
      job_id: job.id,
      event_type: eventType,
      document_id: dataset.documentId,
      session_id: sessionId,
      thread_id: threadId,
      status: 'finished',
      is_tool: response.is_tool ?? eventType === 'analyzer_tool',
      result_html: response.result_html ?? '',
      report_html_missing: reportHtmlMissing,
      upload_failed: response.upload_failed ?? false,
      analysis_results: response.analysis_results ?? null,
      charts:
        response.charts ??
        (Array.isArray(response.generated_files)
          ? response.generated_files
          : []),
      summary: response.summary ?? response.analyze_log ?? null,
      detailed_logs: response.detailed_logs ?? null,
      generated_files: response.generated_files ?? null,
      has_finished: response.has_finished ?? true,
      data: response,
    };
  }
}
