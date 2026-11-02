/**
 * Orchestrator E2E: T0 preflight → T1 unit → T2 HTTP → T3 Kafka → T4 live.
 *
 * Usage:
 *   npx ts-node scripts/e2e/run-all.ts --tier=all --model=deepseek-flash
 *   npx ts-node scripts/e2e/run-all.ts --tier=preflight,http,kafka
 *   npx ts-node scripts/e2e/run-all.ts --tier=live --live-tier=smoke --only=search_papers
 */

import { execSync } from 'node:child_process';
import { assertStrictModel, logScriptModelConfig, resolveScriptModelConfig } from '../lib/ai-model';
import { E2eReport } from '../lib/e2e-report';
import { LiveTier } from '../lib/live-cases';
import { runPreflight } from './preflight';
import { runTierHttp } from './tier2-http';
import { runTierKafka } from './tier3-kafka';
import { runTierLive } from './tier4-live';
import { installTimeoutNegativeWarningSilencer } from '../../src/common/utils/process-warning-guard';

// kafkajs 2.2.4 tự gọi setTimeout với số âm khi hàng đợi rỗng (lỗi upstream) ⇒
// lọc để log E2E sạch, xem src/common/utils/process-warning-guard.ts.
installTimeoutNegativeWarningSilencer();

declare const process: {
  env: Record<string, string | undefined>;
  argv: string[];
  exit: (code?: number) => never;
};

type TierName = 'preflight' | 'unit' | 'http' | 'kafka' | 'live';

const ALL_TIERS: TierName[] = ['preflight', 'unit', 'http', 'kafka', 'live'];

function argValue(name: string): string | undefined {
  const prefix = `--${name}=`;
  const found = process.argv.find((item) => item.startsWith(prefix));
  return found ? found.slice(prefix.length) : undefined;
}

function hasFlag(name: string): boolean {
  return process.argv.includes(`--${name}`);
}

function resolveTiers(): TierName[] {
  const raw = argValue('tier') || 'preflight';
  if (raw === 'all') return [...ALL_TIERS];
  const requested = raw
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean) as TierName[];
  const invalid = requested.filter((item) => !ALL_TIERS.includes(item));
  if (invalid.length > 0) {
    throw new Error(
      `tier không hợp lệ: ${invalid.join(', ')} (hợp lệ: ${ALL_TIERS.join(', ')}, all)`,
    );
  }
  return requested;
}

function runJest(): string {
  try {
    const output = execSync('npm test --silent 2>&1', {
      encoding: 'utf8',
      timeout: 15 * 60_000,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    const match = output.match(/Tests:\s+(.*)/);
    return match ? match[1].trim() : 'jest OK';
  } catch (error) {
    const output = String((error as any)?.stdout ?? '').slice(-1500);
    const failed = output.match(/Tests:\s+(.*)/);
    throw new Error(`jest fail: ${failed ? failed[1].trim() : output.slice(0, 400)}`);
  }
}

async function main(): Promise<void> {
  const modelConfig = resolveScriptModelConfig();
  logScriptModelConfig(modelConfig);
  // Quyết định của AI server (2026-11-01): KHÔNG fallback im lặng khi
  // AI_DEFAULT_MODEL rỗng/ngoài whitelist. Mặc định fail-fast (exit 2);
  // chỉ cho phép fallback khi truyền tường minh --allow-model-fallback.
  const allowModelFallback = hasFlag('allow-model-fallback');
  if (allowModelFallback && modelConfig.fallbackReason) {
    console.warn(
      `⚠️ [--allow-model-fallback] tiếp tục với model=${modelConfig.selectedModel} ` +
        `(requested=${modelConfig.requestedModel ?? '(empty)'}, reason=${modelConfig.fallbackReason})`,
    );
  }
  assertStrictModel(modelConfig, !allowModelFallback);

  const modelId = argValue('model') || modelConfig.selectedModel;
  if (modelId !== modelConfig.selectedModel && !modelConfig.allowedModels.includes(modelId)) {
    throw new Error(
      `model "${modelId}" không thuộc AI_ALLOWED_MODELS (${modelConfig.allowedModels.join(', ')})`,
    );
  }

  const tiers = resolveTiers();
  const liveTier = (argValue('live-tier') || 'smoke') as LiveTier;
  const only = (argValue('only') || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
  const dryRun = hasFlag('dry-run');

  const report = new E2eReport({
    modelId,
    modelRequested: modelConfig.requestedModel ?? undefined,
    allowedModels: modelConfig.allowedModels,
    tiers,
    startedAt: Date.now(),
  });

  console.log(`🎯 E2E tiers: ${tiers.join(', ')} | model=${modelId}${dryRun ? ' | DRY-RUN' : ''}`);

  try {
    if (tiers.includes('preflight')) {
      await runPreflight(report, { includeAiServer: tiers.includes('live') });
    }
    if (tiers.includes('unit')) {
      await report.run('unit.jest', 'T1', 'Toàn bộ unit/integration spec', { modelId }, async () =>
        runJest(),
      );
    }
    if (tiers.includes('http')) {
      await runTierHttp(report, modelId);
    }
    if (tiers.includes('kafka')) {
      await runTierKafka(report, modelId);
    }
    if (tiers.includes('live')) {
      await runTierLive(report, { tier: liveTier, only, modelId, dryRun });
    }
  } catch (error) {
    await report.run('suite.bootstrap', 'T0', 'Khởi tạo suite', { modelId }, async () => {
      throw error;
    });
  }

  const { verdict } = report.summary();
  report.writeJSON();

  // Luôn exit: nếu để process sống, kafkajs/consumer giữ handle ⇒ script treo
  // vô hạn sau khi đã in tổng hợp (đặc biệt khi chạy trong docker exec/CI).
  process.exit(verdict === 'NOT DONE' ? 1 : 0);
}

main().catch((error: Error) => {
  console.error('Fatal error:', error.message);
  process.exit(1);
});
