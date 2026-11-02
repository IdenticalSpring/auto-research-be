/**
 * PROBE TẠM (không commit): tìm shape `headings` mà AI server chấp nhận cho
 * `generate_outline` (hiện trả 617 KeyError 'heading').
 *
 * Usage (trong container):
 *   npx ts-node scripts/probe-generate-outline.ts
 */

import { Kafka, logLevel } from 'kafkajs';
import { KafkaResponseHarness } from './lib/kafka-harness';

declare const process: {
  env: Record<string, string | undefined>;
  exit: (code?: number) => never;
};

const BROKER = process.env.KAFKA || 'kafka:29092';
const MODEL_ID = process.env.AI_DEFAULT_MODEL || 'deepseek-flash';
const USER_ID =
  process.env.E2E_USER_ID || '00000000-0000-0000-0000-000000000001';
const TIMEOUT_MS = 30_000;

const PROPOSAL = {
  title: 'Nghiên cứu tác động của Chuyển đổi số đến hiệu quả SMEs',
  problem_statement: 'SMEs gặp khó khăn khi chuyển đổi số.',
  motivation: 'Làm rõ nhân tố công nghệ ảnh hưởng năng suất.',
  research_gap: 'Thiếu nghiên cứu định lượng tại Việt Nam.',
  level: 'KHOA_LUAN',
  research_type: 'quantitative',
};

const VARIANTS: Array<{
  id: string;
  description: string;
  headings: unknown;
  headingsPercent: number[];
}> = [
  {
    id: 'H1',
    description: '{title, percent} (production)',
    headings: [
      { title: 'Chương 1: Tổng quan', percent: 50 },
      { title: 'Chương 2: Phương pháp', percent: 50 },
    ],
    headingsPercent: [50, 50],
  },
  {
    id: 'H2',
    description: '{heading, percent}',
    headings: [
      { heading: 'Chương 1: Tổng quan', percent: 50 },
      { heading: 'Chương 2: Phương pháp', percent: 50 },
    ],
    headingsPercent: [50, 50],
  },
  {
    id: 'H3',
    description: '{heading, title, percent}',
    headings: [
      { heading: 'Chương 1: Tổng quan', title: 'Chương 1: Tổng quan', percent: 50 },
      { heading: 'Chương 2: Phương pháp', title: 'Chương 2: Phương pháp', percent: 50 },
    ],
    headingsPercent: [50, 50],
  },
  {
    id: 'H4',
    description: 'list[str] (chỉ tên chương)',
    headings: ['Chương 1: Tổng quan', 'Chương 2: Phương pháp'],
    headingsPercent: [50, 50],
  },
];

function buildPayload(
  variant: (typeof VARIANTS)[number],
  documentId: string,
  sessionId: string,
): Record<string, unknown> {
  return {
    event_type: 'generate_outline',
    model_id: MODEL_ID,
    user_id: USER_ID,
    document_id: documentId,
    session_id: sessionId,
    language: 'vi',
    field: 'Kinh tế học',
    domain: 'Quản trị kinh doanh',
    final_proposal: PROPOSAL,
    research_type: { type: 0, reason: 'quantitative' },
    word_count_str: '10000',
    headings: variant.headings,
    headings_percent: variant.headingsPercent,
    outline_code: ['chapter_1', 'chapter_2'],
    references_style: 'APA 7th',
  };
}

async function main(): Promise<void> {
  console.log(`🔬 Probe generate_outline headings | model=${MODEL_ID}`);

  const kafka = new Kafka({
    clientId: `probe-outline-${Date.now()}`,
    brokers: BROKER.split(',').map((item) => item.trim()),
    logLevel: logLevel.NOTHING,
    connectionTimeout: 10_000,
  });
  const harness = new KafkaResponseHarness(kafka, ['document_setup_response'], {
    defaultTimeoutMs: TIMEOUT_MS,
  });
  await harness.start();

  try {
    for (const variant of VARIANTS) {
      const documentId = `probe-outline-${Date.now()}-${variant.id.toLowerCase()}`;
      const sessionId = `sess-${documentId}`;
      console.log(`\n── ${variant.id} (${variant.description})`);
      const startedAt = Date.now();
      try {
        const response = await harness.send({
          topic: 'document_setup_request',
          payload: buildPayload(variant, documentId, sessionId),
          timeoutMs: TIMEOUT_MS,
        });
        const duration = ((Date.now() - startedAt) / 1000).toFixed(1);
        const code = response.error_code ?? response.errorCode;
        if (code !== undefined) {
          console.log(
            `   ❌ ${duration}s | error_code=${String(code)} | ${String(response.error ?? '')}`,
          );
          continue;
        }
        const outline = response.outline ?? response.data;
        console.log(
          `   ✅ ${duration}s | keys=${Object.keys(response).join(',')} | outline=${outline ? 'yes' : 'no'}`,
        );
      } catch (error) {
        const duration = ((Date.now() - startedAt) / 1000).toFixed(1);
        console.log(`   ❌ ${duration}s | ${(error as Error).message}`);
      }
    }
  } finally {
    await harness.stop().catch(() => undefined);
  }
}

main().catch((error: Error) => {
  console.error('Fatal:', error.message);
  process.exit(1);
});
