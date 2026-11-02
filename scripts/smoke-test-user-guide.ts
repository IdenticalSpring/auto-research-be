/**
 * Smoke test THẬT cho event `user_guide_documents` (Kafka document_setup_*).
 * Kiểm chứng AI worker có trả 603 (INVALID_PARAMS - missing `documents_urls`) không.
 *
 * Chạy (Node >= 22.6 chạy trực tiếp .ts, KHÔNG cần ts-node):
 *   node scripts/smoke-test-user-guide.ts --case=all --dry-run
 *   node scripts/smoke-test-user-guide.ts --case=good --urls=autoresearching/user-guide/a.pdf
 * Tham số: --case=good|missing-urls|wrong-field|empty-urls|all · --urls=a,b · --data-id=x
 *          · --timeout=90000 · --match=strict · --dry-run
 *
 * Kết quả đã kiểm chứng (sau fix 2026-10-21):
 *   thiếu field / sai tên field / [] → 603; URL → 603 "expected 'bucket/key'";
 *   bucket/key đúng nhưng object không tồn tại → 619 NoSuchKey (object_name đã sạch dấu "/").
 *   Response lỗi echo đủ user_id/document_id/session_id/data_id → BE correlate được.
 *
 * ── GHI CHÚ  (giữ file này để tự verify, đừng xoá) ──
 * · Cổng nghiệm thu: `npm run build` + `npm test -- --testPathPatterns=admin` + `npm run lint`
 *   KHÔNG dùng `npx tsc --noEmit`: repo có 93 lỗi type ở spec CŨ, 0 lỗi ở `scripts/`.
 * · `admin.guard.spec.ts` đã đủ 5 case — không cần thêm.
 * · File này không thuộc lint (`oxlint src/ test/`) và không bị `tsconfig.build.json` compile.
 * · ĐIỀU KIỆN: AI không tự tải file → upload PDF lên MinIO trước khi publish.
 * · KHÔNG chạy ở production: case `good` ghi dữ liệu thật vào Qdrant `admin_user_guide`.
 */
import { Kafka, logLevel } from 'kafkajs';
import { resolveScriptModelId } from './lib/ai-model';

type CaseName = 'good' | 'missing-urls' | 'wrong-field' | 'empty-urls';
type Verdict = 'PASS' | 'FAIL' | 'TIMEOUT' | 'DRY-RUN';
interface CaseResult {
  caseName: CaseName;
  verdict: Verdict;
  errorCode?: number | string;
  errorMessage?: string;
  note?: string;
}
interface RunOptions {
  brokers: string[];
  requestTopic: string;
  responseTopic: string;
  timeoutMs: number;
  urls: string[];
  dryRun: boolean;
  lenient: boolean;
  dataId?: string;
}

const EVENT = 'user_guide_documents';
const ALL_CASES: CaseName[] = [
  'good',
  'missing-urls',
  'wrong-field',
  'empty-urls',
];
// "bucket/key", KHÔNG phải URL: AI tách bucket = url.split('/')[0] (ingest_docs.py:749)
const DEFAULT_URLS = ['autoresearching/user-guide/guide.pdf'];
const getArg = (name: string): string | undefined => {
  const prefix = `--${name}=`;
  const hit = process.argv.slice(2).find((a) => a.startsWith(prefix));
  return hit ? hit.slice(prefix.length) : undefined;
};
const hasFlag = (name: string) => process.argv.slice(2).includes(`--${name}`);
function resolveCases(): CaseName[] {
  const requested = getArg('case') || 'good';
  if (requested === 'all') return ALL_CASES;
  if (!ALL_CASES.includes(requested as CaseName)) {
    throw new Error(`--case không hợp lệ: ${requested}`);
  }
  return [requested as CaseName];
}
/** Payload giống AdminDocumentsAiService.uploadUserGuide() rồi cố tình làm sai. */
function buildPayload(
  caseName: CaseName,
  c: { userId: string; sessionId: string; dataId: string; urls: string[] },
): Record<string, unknown> {
  const payload: Record<string, unknown> = {
    event_type: EVENT,
    model_id: resolveScriptModelId(),
    user_id: c.userId,
    document_id: 'admin',
    session_id: c.sessionId,
    language: 'vi',
    data_id: c.dataId,
    title: `Smoke test user guide ${c.dataId}`,
    guide_type: 'faq',
    content: 'Nội dung kiểm thử tự động.',
    version: 'v1.0',
    documents_urls: c.urls,
  };
  if (caseName === 'missing-urls') delete payload.documents_urls;
  if (caseName === 'wrong-field') {
    delete payload.documents_urls;
    payload.documents = c.urls;
  }
  if (caseName === 'empty-urls') payload.documents_urls = [];
  return payload;
}

async function runCase(
  caseName: CaseName,
  opts: RunOptions,
): Promise<CaseResult> {
  const stamp = Date.now();
  const userId = '00000000-0000-0000-0000-000000000001';
  const sessionId = `smoke-ug-${caseName}-${stamp}`;
  const dataId = opts.dataId || `guide-smoke-${caseName}-${stamp}`;
  const payload = buildPayload(caseName, {
    userId,
    sessionId,
    dataId,
    urls: opts.urls,
  });

  console.log(`\n──────── CASE: ${caseName} (session=${sessionId}) ────────`);
  console.log('payload    :', JSON.stringify(payload, null, 2));
  if (opts.dryRun) {
    return {
      caseName,
      verdict: 'DRY-RUN',
      note: `sẽ publish vào ${opts.requestTopic}`,
    };
  }

  const kafka = new Kafka({
    clientId: `smoke-ug-${caseName}-${stamp}`,
    brokers: opts.brokers,
    logLevel: logLevel.NOTHING,
  });
  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: `smoke-ug-group-${stamp}` });

  try {
    await producer.connect();
    await consumer.connect();
    // fromBeginning:false + chờ GROUP_JOIN trước khi publish: AI trả lỗi
    // validate rất nhanh, publish ngay sau subscribe sẽ bị mất message.
    await consumer.subscribe({
      topic: opts.responseTopic,
      fromBeginning: false,
    });
    const joined = new Promise<void>((resolve) => {
      consumer.on(consumer.events.GROUP_JOIN, () => resolve());
    });

    let resolveResponse!: (v: Record<string, unknown>) => void;
    let rejectResponse!: (e: Error) => void;
    const responsePromise = new Promise<Record<string, unknown>>((res, rej) => {
      resolveResponse = res;
      rejectResponse = rej;
    });
    const publishEpoch = Date.now();

    await consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) return;
        let data: Record<string, unknown>;
        try {
          data = JSON.parse(message.value.toString()) as Record<string, unknown>;
        } catch {
          return;
        }
        if (String(data.event_type ?? data.eventType ?? '') !== EVENT) return;
        const ts = Number(message.timestamp);
        if (Number.isFinite(ts) && ts < publishEpoch - 5000) return;
        const hasIdentity = Boolean(
          data.session_id ?? data.user_id ?? data.document_id ?? data.data_id,
        );
        if (data.session_id === sessionId || (opts.lenient && !hasIdentity)) {
          resolveResponse(data);
        }
      },
    });

    await Promise.race([
      joined,
      new Promise<void>((r) => setTimeout(r, 3000)),
    ]);
    await producer.send({
      topic: opts.requestTopic,
      messages: [
        {
          key: `${userId}:admin:${sessionId}:${EVENT}`,
          value: JSON.stringify(payload),
        },
      ],
    });
    console.log('→ đã publish, đang chờ response...');

    const timer = setTimeout(
      () => rejectResponse(new Error(`TIMEOUT sau ${opts.timeoutMs}ms`)),
      opts.timeoutMs,
    );
    const response = await responsePromise;
    clearTimeout(timer);
    console.log('← response:', JSON.stringify(response));

    if (
      !(
        response.session_id ||
        response.user_id ||
        response.document_id ||
        response.data_id
      )
    ) {
      console.warn(
        `⚠️  Response không có session_id/user_id → BE tạo key ":none:default:${EVENT}" ` +
          'và KHÔNG resolve được request đang chờ.',
      );
    }

    const raw = response.error_code ?? response.errorCode;
    const errorCode =
      raw === undefined || raw === null ? undefined : (raw as number | string);
    const errorMessage =
      (response.error as string) || (response.message as string) || undefined;
    const hasError =
      errorCode !== undefined ||
      Boolean(errorMessage) ||
      response.success === false;

    if (caseName === 'good') {
      const msg = errorMessage || '';
      const note = msg.includes('NoSuchKey')
        ? 'bucket/key hợp lệ nhưng object chưa có trên MinIO → upload PDF trước'
        : msg.includes("expected 'bucket/key'") || msg.includes('must be')
          ? 'AI đã chặn URL/sai format bằng 603 (đúng contract mới) → dùng bucket/key'
          : 'Payload đúng nhưng AI vẫn báo lỗi';
      return hasError
        ? { caseName, verdict: 'FAIL', errorCode, errorMessage, note }
        : { caseName, verdict: 'PASS', note: 'AI xử lý thành công' };
    }

    const reproduced = String(errorCode) === '603';
    return {
      caseName,
      verdict: reproduced ? 'PASS' : 'FAIL',
      errorCode,
      errorMessage,
      note: reproduced
        ? 'Đã tái hiện đúng lỗi 603'
        : 'Không nhận được 603 như kỳ vọng',
    };
  } catch (error) {
    const message = (error as Error).message;
    return {
      caseName,
      verdict: message.startsWith('TIMEOUT') ? 'TIMEOUT' : 'FAIL',
      errorMessage: message,
    };
  } finally {
    await consumer.disconnect().catch(() => undefined);
    await producer.disconnect().catch(() => undefined);
  }
}

async function main(): Promise<void> {
  const brokerString =
    process.env.KAFKA || process.env.KAFKA_BROKERS || '167.254.70.125:9092';
  const suffix = process.env.TOPIC_SUFFIX ?? '';
  const urlsArg = getArg('urls');
  const opts: RunOptions = {
    brokers: brokerString.split(',').map((b) => b.trim()),
    requestTopic:
      process.env.REQUEST_TOPIC || `document_setup_request${suffix}`,
    responseTopic:
      process.env.RESPONSE_TOPIC || `document_setup_response${suffix}`,
    timeoutMs: Math.max(
      1000,
      Number(getArg('timeout') || process.env.TIMEOUT_MS) || 90000,
    ),
    urls: urlsArg ? urlsArg.split(',').map((u) => u.trim()) : DEFAULT_URLS,
    dryRun: hasFlag('dry-run'),
    lenient: getArg('match') !== 'strict',
    dataId: getArg('data-id'),
  };

  console.log('🧪 SMOKE TEST user_guide_documents (lỗi 603?)');
  console.log(
    `brokers=${opts.brokers.join(',')} | request=${opts.requestTopic} | response=${opts.responseTopic} | timeout=${opts.timeoutMs}ms | match=${opts.lenient ? 'lenient' : 'strict'} | dryRun=${opts.dryRun}`,
  );

  const results: CaseResult[] = [];
  for (const caseName of resolveCases()) {
    results.push(await runCase(caseName, opts));
  }

  console.log('\n================ TỔNG KẾT ================');
  for (const r of results) {
    const code = r.errorCode === undefined ? '-' : String(r.errorCode);
    console.log(
      `${r.caseName.padEnd(13)} | ${r.verdict.padEnd(7)} | error_code=${code} | ${r.note || r.errorMessage || ''}`,
    );
  }
  console.log('=========================================');
  process.exit(results.some((r) => r.verdict === 'FAIL') ? 1 : 0);
}

void main();
