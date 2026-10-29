import { Injectable, Logger, Optional } from '@nestjs/common';
import { JobStatus } from '@prisma/client';
import { PrismaService } from '../../database/prisma.service';
import { getAiErrorMessage } from '../constants/ai-error-codes';

export const EVENT_TIMEOUT_CONFIG: Record<string, number> = {
  // 1500s (25 minutes) - Long generation tasks
  write_content: 1500_000,
  chuyen_de_1: 1500_000,
  chuyen_de_2: 1500_000,
  chuyen_de_3: 1500_000,
  writer: 1500_000,

  // 900s (15 minutes) - Journal scraper
  get_journals: 900_000,

  // 300s (5 minutes) - Heavy analysis & presentation
  analyzer: 300_000,
  admin_documents: 300_000,
  user_guide_documents: 300_000,
  analyzer_tool: 300_000,
  gen_slides: 300_000,
  comment_data: 300_000,
  check_logic_data: 300_000,
  propose_method: 300_000,

  // 180s (3 minutes) - Proposal, Outline & Reference generation
  generate_titles: 180_000,
  generate_title_description: 180_000,
  generate_title_description_no_info: 180_000,
  mix_titles: 180_000,
  generate_outline: 180_000,
  generate_outline_with_refs: 180_000,
  modify_outline: 180_000,
  search_papers: 180_000,
  user_documents: 180_000,
  embed_user_documents: 180_000,

  // 120s (2 minutes) - Chatbots & Text enhancement & Ideation (and default)
  generate_domains: 120_000,
  generate_subdomains: 120_000,
  generate_keywords: 120_000,
  get_research_type: 120_000,
  search_user_documents: 120_000,
  delete_documents: 120_000,
  delete_report: 120_000,
  outer_chatbot: 120_000,
  inner_chatbot: 120_000,
  writer_chatbot: 120_000,
  ai_in_doc: 120_000,
  get_suggestions: 120_000,
  enhance: 120_000,
  enhance_all: 120_000,
};

export const DEFAULT_TIMEOUT_MS = 120_000;

export interface TimeoutEntry {
  timer: NodeJS.Timeout;
  eventType: string;
  jobId?: string;
  createdAt: number;
  durationMs: number;
}

@Injectable()
export class TimeoutManagerService {
  private readonly logger = new Logger(TimeoutManagerService.name);
  private readonly activeTimers = new Map<string, TimeoutEntry>();

  constructor(@Optional() private readonly prisma?: PrismaService) {}

  getTimeoutForEvent(eventType?: string): number {
    if (!eventType) return DEFAULT_TIMEOUT_MS;
    return EVENT_TIMEOUT_CONFIG[eventType] ?? DEFAULT_TIMEOUT_MS;
  }

  registerTimeout(
    key: string,
    eventType: string,
    onTimeout: (errorCode: number, errorMessage: string) => void,
    customTimeoutMs?: number,
    jobId?: string,
  ): NodeJS.Timeout {
    this.clearTimeout(key);

    const durationMs = customTimeoutMs ?? this.getTimeoutForEvent(eventType);

    const timer = setTimeout(async () => {
      this.logger.warn(
        `Timeout reached for [${key}] (event: ${eventType}) after ${durationMs}ms`,
      );
      this.activeTimers.delete(key);

      const errorCode = 604;
      const errorMessage = getAiErrorMessage(604);

      if (jobId) {
        await this.handleJobTimeout(jobId, errorMessage);
      }

      onTimeout(errorCode, errorMessage);
    }, durationMs);

    this.activeTimers.set(key, {
      timer,
      eventType,
      jobId,
      createdAt: Date.now(),
      durationMs,
    });

    this.logger.debug(
      `Registered timeout of ${durationMs}ms for [${key}] (event: ${eventType})`,
    );

    return timer;
  }

  clearTimeout(key: string): void {
    const entry = this.activeTimers.get(key);
    if (entry) {
      clearTimeout(entry.timer);
      this.activeTimers.delete(key);
      this.logger.debug(`Cleared timeout for [${key}]`);
    }
  }

  hasTimeout(key: string): boolean {
    return this.activeTimers.has(key);
  }

  getActiveTimeoutCount(): number {
    return this.activeTimers.size;
  }

  private async handleJobTimeout(
    jobId: string,
    errorMessage: string,
  ): Promise<void> {
    if (!this.prisma) {
      this.logger.debug(
        `Prisma not available, skipping DB update for timed-out job [${jobId}]`,
      );
      return;
    }

    try {
      await this.prisma.job.update({
        where: { id: jobId },
        data: {
          status: JobStatus.error,
          errorCode: '604',
          errorMessage,
        },
      });
      this.logger.debug(
        `Updated job [${jobId}] status to error with errorCode 604`,
      );
    } catch (err) {
      this.logger.warn(
        `Failed to update timed-out job [${jobId}] in DB: ${(err as Error).message}`,
      );
    }
  }
}
