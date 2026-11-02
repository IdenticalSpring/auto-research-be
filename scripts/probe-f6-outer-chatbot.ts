/**
 * PROBE TẠM (không commit): xác định field nào AI server thật sự BẮT BUỘC với
 * `outer_chatbot`. AI team báo 603 Missing field 'paragraphs'/'final_proposal'/'section'
 * nhưng dispatcher BE hiện chưa gửi 3 field này (F6).
 *
 * Chạy 5 biến thể tuần tự, timeout 120s/biến thể:
 *   P1 đầy đủ · P2 bỏ paragraphs · P3 bỏ final_proposal · P4 bỏ section
 *   P5 gửi key nhưng rỗng ('', {}, {})
 *
 * Usage (trong container):
 *   npx ts-node scripts/probe-f6-outer-chatbot.ts
 *   npx ts-node scripts/probe-f6-outer-chatbot.ts --only=P1,P3
 */

import { Kafka, logLevel } from 'kafkajs';
import { KafkaResponseHarness } from './lib/kafka-harness';

declare const process: {
  env: Record<string, string | undefined>;
  argv: string[];
  exit: (code?: number) => never;
};

const BROKER = process.env.KAFKA || 'kafka:29092';
const MODEL_ID = process.env.AI_DEFAULT_MODEL || 'deepseek-flash';
const USER_ID =
  process.env.E2E_USER_ID || '00000000-0000-0000-0000-000000000001';
const TIMEOUT_MS = 120_000;

const PROPOSAL = {
  title: 'Nghiên cứu tác động của Chuyển đổi số đến hiệu quả SMEs',
  problem_statement: 'SMEs gặp khó khăn khi chuyển đổi số.',
  motivation: 'Làm rõ nhân tố công nghệ ảnh hưởng năng suất.',
  research_gap: 'Thiếu nghiên cứu định lượng tại Việt Nam.',
  level: 'KHOA_LUAN',
  research_type: 'quantitative',
};

const SECTION = { code: '1.1', name: 'Tính cấp thiết của đề tài' };
const PARAGRAPHS = 'Bối cảnh nghiên cứu về chuyển đổi số tại SMEs Việt Nam.';

type Mode = 'full' | 'no_paragraphs' | 'no_proposal' | 'no_section' | 'empty';

interface Variant {
  id: string;
  mode: Mode;
  description: string;
}

const VARIANTS: Variant[] = [
  { id: 'P1', mode: 'full', description: 'đầy đủ 3 field' },
  { id: 'P2', mode: 'no_paragraphs', description: 'bỏ paragraphs' },
  { id: 'P3', mode: 'no_proposal', description: 'bỏ final_proposal' },
  { id: 'P4', mode: 'no_section', description: 'bỏ section' },
  {
    id: 'P5',
    mode: 'empty',
    description: "paragraphs='' · final_proposal={} · section={}",
  },
];

function buildPayload(
  variant: Variant,
  convId: string,
): Record<string, unknown> {
  const payload: Record<string, unknown> = {
    event_type: 'outer_chatbot',
    model_id: MODEL_ID,
    user_id: USER_ID,
    conv_id: convId,
    session_id: convId,
    language: 'vi',
    question: 'Chuyển đổi số là gì?',
    chat_history: [],
    short_answer: true,
    direct_answer: true,
    use_web_search_chatbot: false,
  };

  if (variant.mode === 'full') {
    payload.paragraphs = PARAGRAPHS;
    payload.final_proposal = PROPOSAL;
    payload.section = SECTION;
  } else if (variant.mode === 'no_paragraphs') {
    payload.final_proposal = PROPOSAL;
    payload.section = SECTION;
  } else if (variant.mode === 'no_proposal') {
    payload.paragraphs = PARAGRAPHS;
    payload.section = SECTION;
  } else if (variant.mode === 'no_section') {
    payload.paragraphs = PARAGRAPHS;
    payload.final_proposal = PROPOSAL;
  } else {
    payload.paragraphs = '';
    payload.final_proposal = {};
    payload.section = {};
  }

  return payload;
}

function argValue(name: string): string | undefined {
  const prefix = `--${name}=`;
  const found = process.argv.find((item) => item.startsWith(prefix));
  return found ? found.slice(prefix.length) : undefined;
}

function textOf(response: Record<string, unknown>): string {
  const value =
    response.response ??
    response.answer ??
    response.content ??
    response.text ??
    '';
  return typeof value === 'string' ? value : JSON.stringify(value);
}

async function main(): Promise<void> {
  const only = (argValue('only') || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
  const variants = VARIANTS.filter(
    (variant) => only.length === 0 || only.includes(variant.id),
  );

  console.log(`🔬 Probe F6 outer_chatbot | model=${MODEL_ID} | broker=${BROKER}`);
  console.log(
    `   timeout=${TIMEOUT_MS / 1000}s/biến thể | ${variants.length} biến thể\n`,
  );

  const kafka = new Kafka({
    clientId: `probe-f6-${Date.now()}`,
    brokers: BROKER.split(',').map((item) => item.trim()),
    logLevel: logLevel.NOTHING,
    connectionTimeout: 10_000,
  });
  const harness = new KafkaResponseHarness(kafka, ['chatbot_response'], {
    defaultTimeoutMs: TIMEOUT_MS,
  });
  await harness.start();

  const results: Array<{ id: string; ok: boolean; detail: string }> = [];

  try {
    for (const variant of variants) {
      const convId = `probe-f6-${Date.now()}-${variant.id.toLowerCase()}`;
      const payload = buildPayload(variant, convId);
      console.log(`── ${variant.id} (${variant.description})`);
      console.log(`   keys=${Object.keys(payload).join(',')}`);

      const startedAt = Date.now();
      try {
        const response = await harness.send({
          topic: 'chatbot_request',
          payload,
          timeoutMs: TIMEOUT_MS,
        });
        const duration = ((Date.now() - startedAt) / 1000).toFixed(1);
        const code = response.error_code ?? response.errorCode;
        if (code !== undefined) {
          const message = String(response.error ?? '(no message)');
          console.log(
            `   ❌ error_code=${String(code)} | ${duration}s | ${message}\n`,
          );
          results.push({
            id: variant.id,
            ok: false,
            detail: `${code}: ${message}`,
          });
          continue;
        }
        const text = textOf(response).trim();
        const echo = [
          `session_id=${String(response.session_id ?? '(none)')}`,
          `conv_id=${String(response.conv_id ?? '(none)')}`,
          `has_finished=${String(response.has_finished ?? '(none)')}`,
        ].join(' · ');
        console.log(`   ✅ ${duration}s | ${echo}`);
        console.log(`   text=${text.slice(0, 100)}\n`);
        results.push({
          id: variant.id,
          ok: text.length > 0,
          detail: text.length > 0 ? `${duration}s` : 'response rỗng',
        });
      } catch (error) {
        const duration = ((Date.now() - startedAt) / 1000).toFixed(1);
        const message = (error as Error).message;
        console.log(`   ❌ ${duration}s | ${message}\n`);
        results.push({ id: variant.id, ok: false, detail: message });
      }
    }
  } finally {
    await harness.stop().catch(() => undefined);
  }

  console.log('================ KẾT LUẬN PROBE F6 ================');
  for (const result of results) {
    console.log(
      `${result.ok ? 'PASS' : 'FAIL'}  ${result.id}  ${result.detail}`,
    );
  }
  const failed = results.filter((item) => !item.ok);
  const p1 = results.find((item) => item.id === 'P1');
  if (failed.length === 0) {
    console.log('\n⇒ Cả 5 biến thể PASS: 3 field là OPTIONAL với outer_chatbot.');
  } else if (p1 && !p1.ok) {
    console.log(
      '\n⇒ P1 (đầy đủ) cũng FAIL: payload còn thiếu field khác — đọc error ở trên.',
    );
  } else {
    console.log(
      `\n⇒ ${failed.map((item) => item.id).join(', ')} FAIL ⇒ field tương ứng là REQUIRED.`,
    );
  }
}

main().catch((error: Error) => {
  console.error('Fatal:', error.message);
  process.exit(1);
});
