/**
 * Ghi nhận & tổng hợp kết quả E2E.
 *
 * Mỗi case in 1 dòng kèm model/timeout/doc/key/thời gian và lý do fail;
 * cuối cùng in bảng tổng hợp theo tier + verdict DONE / NOT DONE + file JSON.
 */

import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

export type Tier = 'T0' | 'T1' | 'T2' | 'T3' | 'T4';
export type CaseStatus = 'PASS' | 'FAIL' | 'SKIP';
export type FailureOwner = 'be' | 'ai-server' | 'env' | 'harness' | 'unknown';

export interface CaseResult {
  id: string;
  tier: Tier;
  feature: string;
  status: CaseStatus;
  durationMs: number;
  owner?: FailureOwner;
  modelId?: string;
  timeoutMs?: number;
  documentId?: string;
  responseKey?: string;
  detail?: string;
  errorMessage?: string;
  steps?: string[];
}

export interface CaseMeta {
  modelId?: string;
  timeoutMs?: number;
  documentId?: string;
  responseKey?: string;
}

export interface ReportMeta {
  modelId?: string;
  modelRequested?: string;
  allowedModels?: string[];
  tiers: string[];
  startedAt?: number;
}

/** Phân loại lỗi để verdict không quy sai trách nhiệm. */
export function classifyFailure(message: string): FailureOwner {
  const text = (message || '').toLowerCase();
  if (!text) return 'unknown';
  if (text.includes('ai error') || text.includes('suspended:')) return 'ai-server';
  // Harness timeout khi chờ AI: không có response nào ⇒ phía AI server
  if (/timeout|quá thời gian|khong nhan them response/i.test(text)) {
    return 'ai-server';
  }
  if (
    /econnrefused|enotfound|etimedout|econnreset|no such container|getaddrinfo/.test(
      text,
    )
  ) {
    return 'env';
  }
  if (/prisma|p1001|p2021|relation .* does not exist|migrat/.test(text)) {
    return 'env';
  }
  if (text.includes('harness')) return 'harness';
  return 'be';
}

function fmtDuration(ms: number): string {
  return ms >= 1000 ? `${(ms / 1000).toFixed(1)}s` : `${ms}ms`;
}

function excerpt(value: unknown, max = 200): string {
  const text =
    typeof value === 'string' ? value : JSON.stringify(value ?? null);
  return text.length > max ? `${text.slice(0, max)}…` : text;
}

export class E2eReport {
  private readonly cases: CaseResult[] = [];

  constructor(
    private readonly meta: ReportMeta,
    private readonly log: (line: string) => void = console.log,
  ) {}

  /** Chạy 1 case, tự đo thời gian, ghi kết quả và in log. */
  async run(
    id: string,
    tier: Tier,
    feature: string,
    meta: CaseMeta,
    fn: () => Promise<string | void>,
  ): Promise<CaseResult> {
    const startedAt = Date.now();
    try {
      const detail = await fn();
      const result: CaseResult = {
        id,
        tier,
        feature,
        status: 'PASS',
        durationMs: Date.now() - startedAt,
        modelId: meta.modelId ?? this.meta.modelId,
        timeoutMs: meta.timeoutMs,
        documentId: meta.documentId,
        responseKey: meta.responseKey,
        detail: detail || undefined,
      };
      this.push(result);
      return result;
    } catch (error) {
      const message = (error as Error)?.message ?? String(error);
      const result: CaseResult = {
        id,
        tier,
        feature,
        status: 'FAIL',
        durationMs: Date.now() - startedAt,
        owner: classifyFailure(message),
        modelId: meta.modelId ?? this.meta.modelId,
        timeoutMs: meta.timeoutMs,
        documentId: meta.documentId,
        responseKey: meta.responseKey,
        errorMessage: excerpt(message),
      };
      this.push(result);
      return result;
    }
  }

  skip(id: string, tier: Tier, feature: string, reason: string): void {
    this.push({ id, tier, feature, status: 'SKIP', durationMs: 0, detail: reason });
  }

  results(): CaseResult[] {
    return [...this.cases];
  }

  /** Bảng tổng hợp + verdict. Trả về verdict để caller set exit code. */
  summary(): { verdict: 'DONE' | 'NOT DONE'; failed: CaseResult[] } {
    const tiers: Tier[] = ['T0', 'T1', 'T2', 'T3', 'T4'];
    this.log('');
    this.log('================ TỔNG HỢP ================');
    this.log('Tier   Total   Pass   Fail   Skip   Thời gian');
    for (const tier of tiers) {
      const group = this.cases.filter((item) => item.tier === tier);
      if (group.length === 0) continue;
      const pass = group.filter((item) => item.status === 'PASS').length;
      const fail = group.filter((item) => item.status === 'FAIL').length;
      const skip = group.filter((item) => item.status === 'SKIP').length;
      const total = group.reduce((sum, item) => sum + item.durationMs, 0);
      this.log(
        `${tier.padEnd(6)} ${String(group.length).padStart(5)} ${String(pass).padStart(6)} ${String(fail).padStart(6)} ${String(skip).padStart(6)}   ${fmtDuration(total)}`,
      );
    }

    const failed = this.cases.filter((item) => item.status === 'FAIL');
    const beBugs = failed.filter((item) => item.owner === 'be');
    const external = failed.filter(
      (item) => item.owner === 'ai-server' || item.owner === 'env',
    );

    if (failed.length > 0) {
      this.log('');
      this.log(`FAILED (${failed.length}):`);
      for (const item of failed) {
        this.log(
          ` - ${item.id} [owner=${item.owner}] ${item.errorMessage ?? item.detail ?? ''}`,
        );
      }
    }

    const verdict = beBugs.length === 0 ? 'DONE' : 'NOT DONE';
    this.log('');
    if (verdict === 'DONE' && external.length > 0) {
      this.log(
        `KẾT LUẬN: DONE (phạm vi BE) — còn ${external.length} lỗi ngoài BE (AI server/môi trường) cần theo dõi.`,
      );
    } else if (verdict === 'DONE') {
      this.log('KẾT LUẬN: DONE — toàn bộ case trong phạm vi đã PASS.');
    } else {
      this.log(
        `KẾT LUẬN: NOT DONE — ${beBugs.length} lỗi BE cần xử lý: ${beBugs.map((item) => item.id).join(', ')}`,
      );
    }

    return { verdict, failed };
  }

  toJSON(): Record<string, unknown> {
    const startedAt = this.meta.startedAt ?? Date.now();
    return {
      generatedAt: new Date().toISOString(),
      durationMs: Date.now() - startedAt,
      meta: this.meta,
      cases: this.cases,
      totals: {
        total: this.cases.length,
        pass: this.cases.filter((item) => item.status === 'PASS').length,
        fail: this.cases.filter((item) => item.status === 'FAIL').length,
        skip: this.cases.filter((item) => item.status === 'SKIP').length,
      },
    };
  }

  writeJSON(dir = 'reports'): string {
    mkdirSync(dir, { recursive: true });
    const file = join(dir, `e2e-${Date.now()}.json`);
    writeFileSync(file, JSON.stringify(this.toJSON(), null, 2));
    this.log(`📄 Báo cáo JSON: ${file}`);
    return file;
  }

  private push(result: CaseResult): void {
    this.cases.push(result);
    const badge =
      result.status === 'PASS' ? '✅ PASS' : result.status === 'FAIL' ? '❌ FAIL' : '⏭️  SKIP';
    const parts = [`${badge} ${result.tier}.${result.id} ${result.feature}`];
    if (result.modelId) parts.push(`model=${result.modelId}`);
    if (result.timeoutMs) parts.push(`timeout=${Math.round(result.timeoutMs / 1000)}s`);
    parts.push(fmtDuration(result.durationMs));
    if (result.documentId) parts.push(`doc=${result.documentId}`);
    if (result.responseKey) parts.push(`key=${result.responseKey}`);
    this.log(parts.join('  |  '));
    if (result.detail) this.log(`       └ ${result.detail}`);
    if (result.errorMessage) {
      this.log(`       ├ error: ${result.errorMessage}`);
      this.log(`       └ owner: ${result.owner}`);
    }
  }
}
