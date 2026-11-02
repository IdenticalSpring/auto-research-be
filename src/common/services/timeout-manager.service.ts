import { Injectable, Logger, Optional } from '@nestjs/common';
import { JobStatus } from '@prisma/client';
import { PrismaService } from '../../database/prisma.service';
import { getAiErrorMessage } from '../constants/ai-error-codes';

/**
 * Timeout chờ phản hồi AI theo từng event (ms).
 *
 * RÀNG BUỘC: giá trị phải **≥ `TIME_OUT_DICT` của AI server** (giây), nếu không
 * BE sẽ tự cắt job trước khi AI kịp trả kết quả ⇒ lỗi 604 giả. Đối chiếu AI
 * `autoresearching/error_type.py::TIME_OUT_DICT` (quyết định 2026-11-01) và
 * `scripts/lib/e2e-timeouts.ts::AI_TIMEOUT_DICT_SECONDS` (parity test sẽ fail
 * nếu hai bảng lệch nhau).
 */
export const EVENT_TIMEOUT_CONFIG: Record<string, number> = {
  // 1800s (30 phút) - Long generation tasks (AI: write_content/writer/chuyen_de_* = 1800)
  write_content: 1_800_000,
  chuyen_de_1: 1_800_000,
  chuyen_de_2: 1_800_000,
  chuyen_de_3: 1_800_000,
  writer: 1_800_000,

  // 1500s (25 phút) - Outline generation (AI: generate_outline = 1500)
  generate_outline: 1_500_000,

  // 900s (15 phút) - Journal scraper
  get_journals: 900_000,

  // 420s (7 phút) - Heavy analysis & presentation (AI = 420)
  analyzer: 420_000,
  analyzer_tool: 420_000,
  gen_slides: 420_000,
  comment_data: 420_000,
  propose_method: 420_000,
  parsed_value: 420_000,
  edit_report: 420_000,

  // 3600s (60 phút) - Nhóm tài liệu: AI khai `TIME_OUT_DICT = 0` là MARKER,
  // listener thực tế chạy 3600s (document_setup_listener.py:55-58) ⇒ BE phải chờ ≥ 3600s.
  admin_documents: 3_600_000,
  user_guide_documents: 3_600_000,
  user_documents: 3_600_000,
  embed_user_documents: 3_600_000,

  // 300s (5 phút) - Kiểm tra logic dữ liệu
  check_logic_data: 300_000,
  search_papers: 300_000,

  // 420s - Proposal & reference generation (AI = 420)
  generate_titles: 420_000,
  generate_title_description: 420_000,
  generate_title_description_no_info: 420_000,
  mix_titles: 420_000,

  // 180s (3 phút) - Outline chỉnh sửa (AI không đặt timeout riêng)
  generate_outline_with_refs: 180_000,
  modify_outline: 180_000,

  // 240s (4 phút) - Chatbot & enhancement (AI = 240)
  outer_chatbot: 240_000,
  inner_chatbot: 240_000,
  writer_chatbot: 240_000,
  ai_in_doc: 240_000,
  get_suggestions: 240_000,
  enhance: 240_000,
  enhance_all: 240_000,

  // 120s (2 phút) - Ideation (AI = 120)
  generate_domains: 120_000,
  generate_subdomains: 120_000,
  generate_keywords: 120_000,
  get_research_type: 120_000,
  search_user_documents: 120_000,
  delete_documents: 120_000,
  delete_report: 120_000,
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
