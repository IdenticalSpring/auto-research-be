/**
 * T4 — Live: gửi payload thật qua Kafka tới AI server thật.
 * Model lấy từ cấu hình tập trung; timeout theo EVENT_TIMEOUT_CONFIG + biên.
 */

import { Kafka, logLevel } from 'kafkajs';
import { buildResponseKey } from '../lib/ai-response';
import { E2eReport } from '../lib/e2e-report';
import { KafkaResponseHarness } from '../lib/kafka-harness';
import { LiveTier, selectLiveCases } from '../lib/live-cases';
import { resolveEventTimeout } from '../lib/e2e-timeouts';
import {
  closeMongoFixtureClient,
  isMongoFixtureEnabled,
} from '../lib/mongo-fixtures';
import {
  isInfraEnabled,
  loadInfraFixture,
  type InfraFixture,
} from '../lib/infra-fixtures';
import type { LiveCase } from '../lib/live-cases';

declare const process: { env: Record<string, string | undefined> };

const BROKER = process.env.KAFKA || 'kafka:29092';
const RESPONSE_TOPICS = [
  'document_setup_response',
  'write_section_response',
  'chatbot_response',
  'enhancement_response',
];

export interface LiveOptions {
  tier?: LiveTier;
  only?: string[];
  modelId: string;
  dryRun?: boolean;
}

let infraFixtureCache: InfraFixture | undefined;

/**
 * document_id/session_id của case:
 * - case cần hạ tầng (`infra`) + `E2E_WITH_INFRA=1` ⇒ dùng đúng id AI đã seed
 *   (bắt buộc để chuỗi user_documents → embed → delete cùng key).
 * - còn lại ⇒ id sinh theo run để mỗi lần chạy độc lập.
 */
function resolveCaseIds(
  liveCase: LiveCase,
  runId: string,
): { documentId: string; sessionId: string } {
  if (liveCase.infra && isInfraEnabled()) {
    if (!infraFixtureCache) {
      infraFixtureCache = loadInfraFixture();
    }
    return {
      documentId: infraFixtureCache.document_id,
      sessionId: infraFixtureCache.session_id,
    };
  }
  const documentId = `${runId}-${liveCase.id}`;
  return { documentId, sessionId: `sess-${documentId}` };
}

async function createHarness(): Promise<KafkaResponseHarness> {
  const kafka = new Kafka({
    clientId: `e2e-live-${Date.now()}`,
    brokers: BROKER.split(',').map((broker) => broker.trim()),
    logLevel: logLevel.NOTHING,
    connectionTimeout: 10_000,
  });
  const harness = new KafkaResponseHarness(kafka, RESPONSE_TOPICS, {
    defaultTimeoutMs: 400_000,
  });
  await harness.start();
  return harness;
}

export async function runTierLive(
  report: E2eReport,
  options: LiveOptions,
): Promise<void> {
  const tier = options.tier ?? 'smoke';
  const cases = selectLiveCases(tier, options.only);
  const runId = `e2e-live-${Date.now()}`;
  const userId =
    process.env.E2E_USER_ID || '00000000-0000-0000-0000-000000000001';

  console.log(
    `🧪 T4 live: tier=${tier}, ${cases.length} case, model=${options.modelId}${options.dryRun ? ' (DRY-RUN)' : ''}`,
  );

  if (cases.length === 0) {
    report.skip('live.none', 'T4', 'Không có case nào khớp filter', 'kiểm tra --only');
    return;
  }

  if (options.dryRun) {
    for (const liveCase of cases) {
      const { documentId, sessionId } = resolveCaseIds(liveCase, runId);
      const payload = liveCase.buildPayload({
        documentId,
        sessionId,
        userId,
        modelId: options.modelId,
      });
      report.skip(
        `live.${liveCase.id}`,
        'T4',
        `${liveCase.eventType} (${liveCase.tier})`,
        `dry-run topic=${liveCase.topic} key=${buildResponseKey(documentId, liveCase.eventType)} timeout=${Math.round(resolveEventTimeout(liveCase.eventType) / 1000)}s model=${String(payload.model_id)}`,
      );
    }
    return;
  }

  const harness = await createHarness();
  try {
    for (const liveCase of cases) {
      const useInfra = Boolean(liveCase.infra) && isInfraEnabled();
      const { documentId, sessionId } = resolveCaseIds(liveCase, runId);
      if (liveCase.skipReason && !useInfra) {
        report.skip(
          `live.${liveCase.id}`,
          'T4',
          `${liveCase.eventType} (${liveCase.tier})`,
          liveCase.skipReason,
        );
        continue;
      }
      if (liveCase.fixture === 'mongo' && !isMongoFixtureEnabled()) {
        report.skip(
          `live.${liveCase.id}`,
          'T4',
          `${liveCase.eventType} (${liveCase.tier})`,
          'Thiếu MONGO_URI — không seed được fixture Mongo cho case này',
        );
        continue;
      }
      const ctx = {
        documentId,
        sessionId,
        userId,
        modelId: options.modelId,
      };
      const timeoutMs = resolveEventTimeout(liveCase.eventType);
      await report.run(
        `live.${liveCase.id}`,
        'T4',
        `${liveCase.eventType} (${liveCase.tier})`,
        {
          modelId: options.modelId,
          timeoutMs,
          documentId,
          responseKey: buildResponseKey(documentId, liveCase.eventType),
        },
        async () => {
          if (liveCase.setup) {
            await liveCase.setup(ctx);
          }
          try {
            const payload = liveCase.buildPayload(ctx);
            const response = await harness.send({
              topic: liveCase.topic,
              payload,
              timeoutMs,
            });
            return liveCase.assert(response);
          } catch (error: unknown) {
            const message =
              error instanceof Error ? error.message : String(error);
            if (liveCase.allowSuspend && message.startsWith('SUSPENDED:')) {
              return message.slice(0, 120);
            }
            throw error;
          } finally {
            if (liveCase.teardown) {
              await liveCase
                .teardown(ctx)
                .catch((error: unknown) =>
                  console.log(
                    `   ⚠️ teardown ${liveCase.id} lỗi: ${String(error)}`,
                  ),
                );
            }
          }
        },
      );
    }
  } finally {
    await harness.stop().catch(() => undefined);
    await closeMongoFixtureClient().catch(() => undefined);
  }
}
