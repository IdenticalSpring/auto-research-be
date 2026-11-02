import { Injectable, Logger } from '@nestjs/common';
import { JobStatus } from '@prisma/client';
import { PrismaService } from '../../database/prisma.service';
import { RedisStateService } from '../../common/services/redis-state.service';
import { HealthCheckWebhookDto } from './dto/health-check-webhook.dto';

@Injectable()
export class HealthCheckService {
  private readonly logger = new Logger(HealthCheckService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly redisState: RedisStateService,
  ) {}

  async handleHeartbeat(
    id: string,
    dto: HealthCheckWebhookDto,
  ): Promise<{
    success: boolean;
    message: string;
    data: { id: string; eventType: string; processStatus: number };
  }> {
    let activeJob: any = null;
    try {
      activeJob = await this.prisma.job.findFirst({
        where: {
          OR: [{ documentId: id }, { sessionId: id }],
          status: { in: [JobStatus.pending, JobStatus.processing] },
        },
        orderBy: { createdAt: 'desc' },
      });
    } catch (dbErr: unknown) {
      this.logger.error(
        `Failed to find active Job for [${id}]: ${(dbErr as Error).message}`,
      );
    }

    const eventType = dto.eventType || activeJob?.action || 'unknown';

    this.logger.log(
      `Received health-check heartbeat for [${id}]: eventType=${eventType}, processStatus=${dto.processStatus}`,
    );

    // 1. Cập nhật AI_STATUS:{id} vào Redis với TTL 1 giờ
    await this.redisState.setAiStatus(id, { ...dto, eventType }, 3600);

    // 2. Cập nhật Job status nếu có
    if (activeJob) {
      try {
        if (dto.processStatus === 4) {
          // Trạng thái ERROR
          await this.prisma.job.update({
            where: { id: activeJob.id },
            data: {
              status: JobStatus.error,
              errorCode: dto.aiError?.errorCode
                ? String(dto.aiError.errorCode)
                : 'AI_ERROR',
              errorMessage:
                dto.aiError?.error || 'AI Service reported execution error',
            },
          });
          this.logger.warn(
            `Job [${activeJob.id}] marked as ERROR from health-check: code=${dto.aiError?.errorCode}`,
          );
        } else if (dto.processStatus === 5) {
          // Trạng thái SUSPENDED (chờ phản hồi của người dùng)
          await this.prisma.job.update({
            where: { id: activeJob.id },
            data: {
              status: JobStatus.suspended,
            },
          });
          this.logger.log(
            `Job [${activeJob.id}] marked as SUSPENDED (interrupt waiting user)`,
          );
        } else if (dto.processStatus === 3) {
          // Trạng thái FINISHED
          await this.prisma.job.update({
            where: { id: activeJob.id },
            data: {
              status: JobStatus.finished,
            },
          });
          this.logger.log(`Job [${activeJob.id}] marked as FINISHED`);
        } else if (
          dto.processStatus === 2 &&
          activeJob.status === JobStatus.pending
        ) {
          // Trạng thái PROCESSING
          await this.prisma.job.update({
            where: { id: activeJob.id },
            data: {
              status: JobStatus.processing,
            },
          });
        }
      } catch (dbErr: unknown) {
        this.logger.error(
          `Failed to sync Job status for [${id}]: ${(dbErr as Error).message}`,
        );
      }
    }

    return {
      success: true,
      message: 'Heartbeat and status synchronized successfully',
      data: {
        id,
        eventType,
        processStatus: dto.processStatus,
      },
    };
  }

  async getStatus(id: string) {
    return await this.redisState.getAiStatus(id);
  }
}
