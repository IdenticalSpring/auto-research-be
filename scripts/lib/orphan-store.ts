/**
 * Lưu tạm các response đến SỚM (trước khi harness đăng ký request)
 * hoặc không khớp request nào — tránh mất message và tránh nhiễm chéo.
 */

import { buildResponseKey } from './ai-response';

const DEFAULT_MAX_ENTRIES = 50;
const DEFAULT_TTL_MS = 10 * 60 * 1000;

export interface OrphanEntry {
  data: Record<string, unknown>;
  at: number;
}

export class OrphanResponseStore {
  private readonly entries = new Map<string, OrphanEntry>();

  constructor(
    private readonly maxEntries = DEFAULT_MAX_ENTRIES,
    private readonly ttlMs = DEFAULT_TTL_MS,
  ) {}

  /** Lưu response theo document_id + event_type; trả key đã lưu. */
  save(
    documentId: string,
    eventType: string,
    data: Record<string, unknown>,
    now = Date.now(),
  ): string {
    this.prune(now);
    if (this.entries.size >= this.maxEntries) {
      const oldest = this.entries.keys().next().value;
      if (oldest) {
        this.entries.delete(oldest);
      }
    }
    const key = buildResponseKey(documentId, eventType);
    this.entries.set(key, { data, at: now });
    return key;
  }

  /** Lấy (và xóa) response đã lưu sớm cho request tương ứng. */
  take(
    documentId: string,
    eventType: string,
    now = Date.now(),
  ): Record<string, unknown> | undefined {
    this.prune(now);
    const key = buildResponseKey(documentId, eventType);
    const entry = this.entries.get(key);
    if (!entry) {
      return undefined;
    }
    this.entries.delete(key);
    return entry.data;
  }

  size(): number {
    return this.entries.size;
  }

  private prune(now: number): void {
    for (const [key, entry] of this.entries) {
      if (now - entry.at > this.ttlMs) {
        this.entries.delete(key);
      }
    }
  }
}
