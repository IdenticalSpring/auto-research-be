import {
  BROKER, MODEL_ID, REAL_PROPOSAL, TestResult, createKafkaClient, createKafkaSender,
} from './live-test-helpers';
import { TEST_CASES } from './live-test-cases';

const results: TestResult[] = [];

async function main() {
  console.log('========================================================================');
  console.log('🎯 KIỂM THỬ TRỌNG TÂM CÁC TÍNH NĂNG CHƯA TEST ĐƯỢC (LIVE AI SERVER)');
  console.log('========================================================================');
  console.log(`📡 Broker: ${BROKER} | Model: ${MODEL_ID}`);
  console.log(`🎯 Đề tài: ${REAL_PROPOSAL.title}\n`);

  const uid = `rem-${Date.now()}`;
  const { producer, consumer } = createKafkaClient(uid);
  await Promise.all([producer.connect(), consumer.connect()]);
  console.log('✅ Đã kết nối thành công Kafka Broker VPS!\n');

  await Promise.all([
    consumer.subscribe({ topic: 'document_setup_response', fromBeginning: false }),
    consumer.subscribe({ topic: 'write_section_response', fromBeginning: false }),
    consumer.subscribe({ topic: 'chatbot_response', fromBeginning: false }),
    consumer.subscribe({ topic: 'enhancement_response', fromBeginning: false }),
  ]);

  const responseMap = new Map<string, (res: Record<string, unknown>) => void>();
  const receivedResponses = new Map<string, Record<string, unknown>>();

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      try {
        const data = JSON.parse(message.value.toString()) as Record<string, unknown>;
        const keys = [
          String(data.session_id || data.sessionId || (data.data as any)?.session_id || ''),
          String(data.conv_id || ''),
          data.event_type && data.document_id ? `${data.event_type}_${data.document_id}` : '',
        ].filter(Boolean);

        let matched = false;
        for (const k of keys) {
          const resolver = responseMap.get(k);
          if (resolver) {
            responseMap.delete(k);
            resolver(data);
            matched = true;
            break;
          }
        }
        if (!matched && keys.length > 0) {
          for (const k of keys) receivedResponses.set(k, data);
        }
      } catch {}
    },
  });

  await new Promise((r) => setTimeout(r, 2000));
  const sendKafka = createKafkaSender(producer, responseMap, receivedResponses);
  const context = { genOutline: null as any };

  for (const tc of TEST_CASES) {
    const start = Date.now();
    process.stdout.write(`[TEST ${String(tc.step).padStart(2, '0')}/${TEST_CASES.length}] ${tc.name} ... `);
    try {
      const payload = tc.payload(context);
      const res = await sendKafka(tc.topic, payload, tc.timeoutMs);
      const summary = tc.validate(res, context);
      const sec = Number(((Date.now() - start) / 1000).toFixed(1));
      console.log(`✅ PASS (${sec}s) | ${summary}`);
      results.push({ step: tc.step, name: tc.name, status: 'PASS', durationSec: sec, summary });
    } catch (err: any) {
      const sec = Number(((Date.now() - start) / 1000).toFixed(1));
      const msg = err.message || String(err);
      const isTimeout = msg.includes('Timeout');
      const isAiIssue = msg.includes('603') || msg.includes('617') || msg.includes('640') || msg.includes('AI Error');
      const st = isTimeout ? 'TIMEOUT' : isAiIssue ? 'AI_ISSUE' : 'FAIL';
      console.log(`⚠️ ${st} (${sec}s) | ${msg}`);
      results.push({ step: tc.step, name: tc.name, status: st, durationSec: sec, summary: msg });
    }
    // Nghỉ 1.5s giữa các request để worker giải phóng hoàn toàn hàng đợi
    await new Promise((r) => setTimeout(r, 1500));
  }

  console.log('\n========================================================================');
  console.log('📊 TỔNG KẾT KIỂM THỬ TRỌNG TÂM CÁC TÍNH NĂNG CÒN LẠI');
  console.log('========================================================================');
  const pass = results.filter((r) => r.status === 'PASS').length;
  console.log(`✅ Passed:    ${pass} / ${results.length}`);
  console.log(`⏳ Timeouts:  ${results.filter((r) => r.status === 'TIMEOUT').length} / ${results.length}`);
  console.log(`🤖 AI Issues: ${results.filter((r) => r.status === 'AI_ISSUE').length} / ${results.length}`);
  console.log(`❌ Failures:  ${results.filter((r) => r.status === 'FAIL').length} / ${results.length}`);

  await Promise.all([producer.disconnect(), consumer.disconnect()]);
  process.exit(0);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
