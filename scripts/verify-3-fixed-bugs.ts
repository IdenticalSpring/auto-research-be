/**
 * Live test 3 bug AI server (AI-BUG-10/11/12).
 *
 * Đặc điểm:
 * - Model lấy từ `scripts/lib/ai-model.ts`, luôn in cảnh báo nếu bị fallback.
 * - Ghép response theo `document_id` + `event_type` (không chỉ session_id).
 * - Timeout riêng từng event; streaming chunk không bị coi là PASS.
 * - `--dry-run`: in payload/model/timeout/responseKey rồi thoát, không gửi Kafka.
 * - `--strict-model`: thoát code 2 nếu model bị fallback (dùng cho CI).
 *
 * Chạy: npm run live:verify-bugs (wrapper truyền đủ AI_DEFAULT_MODEL + AI_ALLOWED_MODELS)
 */

import { Kafka, logLevel } from 'kafkajs';
import {
  assertStrictModel,
  logScriptModelConfig,
  resolveScriptModelConfig,
} from './lib/ai-model';
import { buildResponseKey } from './lib/ai-response';
import { KafkaResponseHarness } from './lib/kafka-harness';
import {
  HARNESS_TIMEOUTS,
  RESPONSE_TOPICS,
  TEST_CASES,
  docIdFor,
} from './lib/verify-bug-cases';

const BROKER = process.env.KAFKA || process.env.KAFKA_BROKERS || 'kafka:29092';
const DRY_RUN = process.argv.includes('--dry-run');
const RUN_ID = `verify-${Date.now()}`;

function printDryRun(modelId: string): void {
  console.log('🧪 DRY-RUN — không gửi message nào tới Kafka\n');
  for (const testCase of TEST_CASES) {
    const documentId = docIdFor(testCase.docTag, modelId, RUN_ID);
    const payload = testCase.buildPayload(documentId, modelId);
    console.log(`• ${testCase.name}`);
    console.log(`  topic        : ${testCase.topic}`);
    console.log(`  event_type   : ${testCase.eventType}`);
    console.log(`  document_id  : ${documentId}`);
    console.log(`  model_id     : ${String(payload.model_id)}`);
    console.log(
      `  responseKey  : ${buildResponseKey(documentId, testCase.eventType)}`,
    );
    console.log(`  timeout      : ${Math.round(testCase.timeoutMs / 1000)}s\n`);
  }
}

async function main(): Promise<void> {
  const modelConfig = resolveScriptModelConfig();
  logScriptModelConfig(modelConfig);
  // --strict-model: thoát code 2 nếu model bị fallback (tránh test nhầm model)
  assertStrictModel(modelConfig);

  console.log('========================================================================');
  console.log('🎯 KIỂM THỬ TRỰC TIẾP 3 BUG ĐÃ FIX TRÊN AI SERVER (LIVE TEST)');
  console.log('========================================================================');
  console.log(`📡 Kafka Broker: ${BROKER}`);
  console.log(
    `🤖 Model gửi đi: ${modelConfig.selectedModel} (requested: ${modelConfig.requestedModel ?? '(empty)'})`,
  );
  console.log(`✅ Allowed:      ${modelConfig.allowedModels.join(', ')}\n`);

  if (DRY_RUN) {
    printDryRun(modelConfig.selectedModel);
    return;
  }

  const kafka = new Kafka({
    clientId: `verify-bugs-${RUN_ID}`,
    brokers: BROKER.split(',').map((broker) => broker.trim()),
    logLevel: logLevel.NOTHING,
    connectionTimeout: 10_000,
  });

  const harness = new KafkaResponseHarness(kafka, RESPONSE_TOPICS, {
    defaultTimeoutMs: HARNESS_TIMEOUTS.default,
  });
  await harness.start();
  console.log('✅ Đã kết nối Kafka producer + consumer\n');

  const results: Array<{ name: string; passed: boolean }> = [];

  for (const testCase of TEST_CASES) {
    const documentId = docIdFor(
      testCase.docTag,
      modelConfig.selectedModel,
      RUN_ID,
    );
    console.log(
      '------------------------------------------------------------------------',
    );
    console.log(`🔍 ${testCase.name}`);
    console.log(
      `⏱️  Timeout: ${Math.round(testCase.timeoutMs / 1000)}s | doc: ${documentId} | key: ${buildResponseKey(documentId, testCase.eventType)}\n`,
    );

    const startedAt = Date.now();
    try {
      const response = await harness.send({
        topic: testCase.topic,
        payload: testCase.buildPayload(
          documentId,
          modelConfig.selectedModel,
        ),
        timeoutMs: testCase.timeoutMs,
      });
      const seconds = ((Date.now() - startedAt) / 1000).toFixed(1);
      console.log(`✅ PASS (${seconds}s): ${testCase.assert(response)}\n`);
      results.push({ name: testCase.name, passed: true });
    } catch (error) {
      const seconds = ((Date.now() - startedAt) / 1000).toFixed(1);
      console.log(`❌ FAIL (${seconds}s): ${(error as Error).message}\n`);
      results.push({ name: testCase.name, passed: false });
    }
    await new Promise((resolve) => setTimeout(resolve, 3000));
  }

  console.log(
    '========================================================================',
  );
  console.log('🏁 TỔNG HỢP');
  console.log(
    '========================================================================',
  );
  for (const result of results) {
    console.log(`${result.passed ? '✅' : '❌'} ${result.name}`);
  }
  const failed = results.filter((result) => !result.passed).length;
  console.log(
    `\nKết quả: ${results.length - failed}/${results.length} PASS | model_id gửi đi: ${modelConfig.selectedModel}`,
  );

  await harness.stop();
  process.exit(failed === 0 ? 0 : 1);
}

main().catch((error: Error) => {
  console.error('Fatal error:', error.message);
  process.exit(1);
});
