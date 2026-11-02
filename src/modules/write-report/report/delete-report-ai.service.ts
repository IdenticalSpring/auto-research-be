import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { JobStatus, ModuleType, Prisma } from '@prisma/client';
import { randomUUID } from 'node:crypto';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { getAiModelConfig } from '../../../common/kafka/kafka.constants';
import { DeleteReportDto } from './dto/delete-report.dto';
import { WriteContentMongoHelper } from './write-content-mongo.helper';

const EVENT_TYPE = 'delete_report';
const PUBLISH_ERROR_CODE = 610;
const DELETE_REPORT_ERROR_CODE = 640;

export interface DeleteReportResult {
  job_id: string;
  event_type: string;
  document_ids: string[];
  deleted_ids: string[];
  failed_ids: string[];
  /** `documentId` vẫn còn trong `reports_refs` sau khi AI báo xong. */
  verified_remaining_ids: string[];
  /** `false` khi không đọc được Mongo để xác minh (vd: thiếu `MONGO_URI`). */
  verified: boolean;
}

@Injectable()
export class DeleteReportAiService {
  private readonly logger = new Logger(DeleteReportAiService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly correlationService: RequestCorrelationService,
    private readonly configService: ConfigService,
  ) {}

  /**
   * Event `delete_report` (hợp đồng §7.10 + B-4):
   * - Gửi **1 message / document_id** để correlation key luôn khớp
   *   (`document_id` là list trong hợp đồng nên gửi gộp sẽ sinh key `"id1,id2"`).
   * - AI xoá best-effort: không tin message thành công, BE **luôn** tự xác minh
   *   `user_documents.reports_refs` và chỉ set job `finished` khi không còn id lỗi.
   */
  async delete(
    userId: string,
    dto: DeleteReportDto,
  ): Promise<DeleteReportResult> {
    const documentIds = [...new Set(dto.documentIds)];
    await this.assertOwnership(userId, documentIds);

    const job = await this.prisma.job.create({
      data: {
        module: ModuleType.write_report,
        action: EVENT_TYPE,
        documentId: documentIds[0],
        status: JobStatus.pending,
        payload: {
          document_ids: documentIds,
          thread_ids: dto.threadIds ?? [],
        } as Prisma.InputJsonValue,
      },
    });

    const failedIds = new Set<string>();
    for (const documentId of documentIds) {
      const ok = await this.deleteOne(userId, documentId, dto, job.id);
      if (!ok) failedIds.add(documentId);
    }

    const verified = await this.verify(documentIds, failedIds);
    const remaining = new Set([
      ...failedIds,
      ...verified.remainingIds,
    ]);
    const failed = documentIds.filter((id) => remaining.has(id));
    const deleted = documentIds.filter((id) => !remaining.has(id));

    const result = {
      document_ids: documentIds,
      deleted_ids: deleted,
      failed_ids: failed,
      verified_remaining_ids: verified.remainingIds,
      verified: verified.ok,
    };

    if (failed.length > 0) {
      await this.prisma.job.update({
        where: { id: job.id },
        data: {
          status: JobStatus.error,
          errorCode: String(DELETE_REPORT_ERROR_CODE),
          errorMessage: `Xoá báo cáo chưa hoàn tất cho ${failed.length}/${documentIds.length} tài liệu`,
          result: result as Prisma.InputJsonValue,
        },
      });
      this.logger.warn(
        `[${EVENT_TYPE}] job ${job.id} lỗi ${failed.length}/${documentIds.length}: ${failed.join(', ')}`,
      );
    } else {
      await this.prisma.job.update({
        where: { id: job.id },
        data: { status: JobStatus.finished, result: result as Prisma.InputJsonValue },
      });
      this.logger.log(
        `[${EVENT_TYPE}] job ${job.id} xoá xong ${documentIds.length} tài liệu`,
      );
    }

    return { job_id: job.id, event_type: EVENT_TYPE, ...result };
  }

  /** Xoá 1 document; trả `true` nếu AI báo thành công và không có `failed_ids`. */
  private async deleteOne(
    userId: string,
    documentId: string,
    dto: DeleteReportDto,
    jobId: string,
  ): Promise<boolean> {
    const sessionId = `sess-${EVENT_TYPE}-${randomUUID()}`;
    const payload: Record<string, unknown> = {
      event_type: EVENT_TYPE,
      model_id: getAiModelConfig().defaultModel,
      user_id: userId,
      document_id: documentId,
      session_id: sessionId,
      language: 'Vietnamese',
      thread_id: dto.threadIds ?? [],
    };

    const correlationKey = this.correlationService.buildCorrelationKey({
      userId,
      documentId,
      sessionId,
      eventType: EVENT_TYPE,
    });

    const pending = this.correlationService.registerPending<
      Record<string, unknown>
    >(correlationKey, undefined, EVENT_TYPE, jobId);

    const topic =
      this.configService.get<string>('kafka.topics.write_report.request') ||
      'write_section_request';

    try {
      await this.kafkaProducer.publish(topic, payload);
    } catch (publishError: unknown) {
      const message =
        publishError instanceof Error
          ? publishError.message
          : 'Publish to Kafka failed';
      this.correlationService.clearPending(correlationKey, message);
      // Tránh unhandled rejection: pending vừa bị reject bởi clearPending.
      void pending.catch(() => undefined);
      this.logger.warn(
        `[${EVENT_TYPE}] publish lỗi cho ${documentId} (mã ${PUBLISH_ERROR_CODE}): ${message}`,
      );
      return false;
    }

    try {
      const response = await pending;
      const errorCode = response.error_code ?? response.errorCode;
      const failedIds = response.failed_ids;
      if (errorCode !== undefined && errorCode !== null) {
        this.logger.warn(
          `[${EVENT_TYPE}] AI trả error_code=${String(errorCode)} cho ${documentId}`,
        );
        return false;
      }
      if (Array.isArray(failedIds) && failedIds.length > 0) {
        this.logger.warn(
          `[${EVENT_TYPE}] AI báo failed_ids=${JSON.stringify(failedIds)} cho ${documentId}`,
        );
        return false;
      }
      return true;
    } catch (error: unknown) {
      this.logger.warn(
        `[${EVENT_TYPE}] không nhận được response cho ${documentId}: ${(error as Error).message}`,
      );
      return false;
    }
  }

  private async verify(
    documentIds: string[],
    failedIds: Set<string>,
  ): Promise<{ ok: boolean; remainingIds: string[] }> {
    try {
      const remaining = await WriteContentMongoHelper.findRemainingReportRefs(
        documentIds,
      );
      return { ok: true, remainingIds: remaining };
    } catch (error: unknown) {
      this.logger.warn(
        `[${EVENT_TYPE}] không xác minh được reports_refs: ${(error as Error).message}`,
      );
      // Không xác minh được ⇒ giữ nguyên kết quả AI báo (đã có trong failedIds).
      return { ok: false, remainingIds: [...failedIds] };
    }
  }

  private async assertOwnership(
    userId: string,
    documentIds: string[],
  ): Promise<void> {
    const owned = await this.prisma.document.findMany({
      where: { id: { in: documentIds }, userId },
      select: { id: true },
    });
    const ownedIds = new Set(owned.map((doc) => doc.id));
    const missing = documentIds.filter((id) => !ownedIds.has(id));
    if (missing.length > 0) {
      throw new NotFoundException(
        `Document not found: ${missing.join(', ')}`,
      );
    }
  }
}
