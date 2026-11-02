import { Kafka, logLevel } from 'kafkajs';
import { randomUUID } from 'node:crypto';
import { resolveScriptModelId } from './lib/ai-model';

interface TestResult {
  task: string;
  event: string;
  status: 'PASS' | 'FAIL' | 'TIMEOUT';
  durationMs: number;
  details: string;
  fullResponse?: Record<string, unknown>;
}

const BROKER = process.env.KAFKA || '167.254.70.125:9092';
const REQUEST_TOPIC = 'write_section_request';
const RESPONSE_TOPIC = 'write_section_response';
const TIMEOUT_MS = Number(process.env.TIMEOUT_MS) || 60000;
const MODEL_ID = resolveScriptModelId();

const USER_ID = '00000000-0000-0000-0000-000000000001';
// 24-character hex ID compatible with PyMongo ObjectId
const DOCUMENT_ID_MONGO = '000000000000000000000002';

async function runTask08And09SmokeTest(): Promise<void> {
  console.log('===============================================================');
  console.log('🧪 E2E VERIFICATION: TASK-08 & TASK-09 WITH AI SERVER');
  console.log('===============================================================');
  console.log(`📡 Broker:        ${BROKER}`);
  console.log(`🤖 Model ID:      ${MODEL_ID}`);
  console.log(`📤 Request Topic: ${REQUEST_TOPIC}`);
  console.log(`📥 Response Topic:${RESPONSE_TOPIC}`);
  console.log(`⏱️  Timeout:       ${TIMEOUT_MS / 1000}s per event`);

  const uniqueId = `test-${Date.now()}`;
  const kafka = new Kafka({
    clientId: `smoke-runner-${uniqueId}`,
    brokers: BROKER.split(',').map((b) => b.trim()),
    logLevel: logLevel.NOTHING,
  });

  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: `smoke-group-${uniqueId}` });

  const pendingResolvers = new Map<
    string,
    { resolve: (data: Record<string, unknown>) => void; reject: (err: Error) => void }
  >();

  await Promise.all([producer.connect(), consumer.connect()]);
  console.log('✅ Connected Kafka Producer and Consumer successfully');

  await consumer.subscribe({ topic: RESPONSE_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      try {
        const payload = JSON.parse(message.value.toString()) as Record<string, unknown>;
        const sessionId = String(payload.session_id || payload.sessionId || '');
        const eventType = String(payload.event_type || payload.eventType || '');

        const resolver = pendingResolvers.get(sessionId) || pendingResolvers.get(eventType);
        if (resolver) {
          resolver.resolve(payload);
        }
      } catch {
        // Ignore unparseable frames
      }
    },
  });

  const results: TestResult[] = [];

  async function sendAndAwait(
    task: string,
    eventType: string,
    payload: Record<string, unknown>,
    customTimeout = TIMEOUT_MS,
  ): Promise<Record<string, unknown>> {
    const sessionId = `sess-${eventType}-${randomUUID()}`;
    const fullPayload = {
      model_id: MODEL_ID,
      user_id: USER_ID,
      document_id: DOCUMENT_ID_MONGO,
      session_id: sessionId,
      language: 'Vietnamese',
      ...payload,
      event_type: eventType,
    };

    const startTime = Date.now();
    return new Promise<Record<string, unknown>>((resolve, reject) => {
      const timer = setTimeout(() => {
        pendingResolvers.delete(sessionId);
        const dur = Date.now() - startTime;
        results.push({
          task,
          event: eventType,
          status: 'TIMEOUT',
          durationMs: dur,
          details: `No response within ${customTimeout / 1000}s`,
        });
        reject(new Error(`Timeout awaiting response for ${eventType}`));
      }, customTimeout);

      pendingResolvers.set(sessionId, {
        resolve: (data) => {
          clearTimeout(timer);
          pendingResolvers.delete(sessionId);
          const dur = Date.now() - startTime;
          const isErr = data.error_code && Number(data.error_code) >= 600;
          results.push({
            task,
            event: eventType,
            status: isErr ? 'FAIL' : 'PASS',
            durationMs: dur,
            details: isErr
              ? `AI Error ${data.error_code}: ${data.error || data.message || 'Error'}`
              : `Keys: ${Object.keys(data).slice(0, 8).join(', ')}`,
            fullResponse: data,
          });
          resolve(data);
        },
        reject: (err) => {
          clearTimeout(timer);
          pendingResolvers.delete(sessionId);
          results.push({
            task,
            event: eventType,
            status: 'FAIL',
            durationMs: Date.now() - startTime,
            details: err.message,
          });
          reject(err);
        },
      });

      producer
        .send({
          topic: REQUEST_TOPIC,
          messages: [
            {
              key: `${USER_ID}:${DOCUMENT_ID_MONGO}:${sessionId}:${eventType}`,
              value: JSON.stringify(fullPayload),
            },
          ],
        })
        .catch(reject);
    });
  }

  const sampleVariables = [
    { name: 'Tuổi', code: 'age', variable_type: 'observed', role: 'independent', scale: 'ratio' },
    { name: 'Thu nhập', code: 'income', variable_type: 'observed', role: 'dependent', scale: 'ratio' },
  ];
  const sampleData = { age: [22, 28, 35, 42, 50], income: [12, 18, 25, 30, 45] };
  const sampleProposal = { title: 'Nghiên cứu tác động thu nhập theo độ tuổi' };

  console.log('\n--- BẮT ĐẦU KIỂM THỬ TASK-08 (Data Analysis) ---');

  // Test 1: check_logic_data
  try {
    console.log('▶ [TASK-08] Gửi event: check_logic_data...');
    const res = await sendAndAwait('TASK-08', 'check_logic_data', { variables: sampleVariables });
    console.log('   ✅ Phản hồi check_logic_data:', JSON.stringify(res));
  } catch (e) {
    console.log('   ❌ Thất bại check_logic_data:', (e as Error).message);
  }

  // Test 2: comment_data
  try {
    console.log('▶ [TASK-08] Gửi event: comment_data...');
    const res = await sendAndAwait('TASK-08', 'comment_data', {
      thread_id: randomUUID(),
      final_proposal: sampleProposal,
      data: sampleData,
      variables: sampleVariables,
    });
    console.log('   ✅ Phản hồi comment_data:', JSON.stringify(res));
  } catch (e) {
    console.log('   ❌ Thất bại comment_data:', (e as Error).message);
  }

  // Test 3: analyzer_tool
  try {
    console.log('▶ [TASK-08] Gửi event: analyzer_tool...');
    const res = await sendAndAwait('TASK-08', 'analyzer_tool', {
      thread_id: randomUUID(),
      conv_id: `conv-${randomUUID()}`,
      final_proposal: sampleProposal,
      data: sampleData,
      variables: sampleVariables,
      auto_plan_mode: true,
      auto_action_mode: true,
      manual_plan_mode: false,
      tool_configs: [],
      resume: false,
      resume_error: false,
      db: 'redis',
    }, 90000);
    console.log('   ✅ Phản hồi analyzer_tool:', JSON.stringify(res));
  } catch (e) {
    console.log('   ❌ Thất bại analyzer_tool:', (e as Error).message);
  }

  console.log('\n--- BẮT ĐẦU KIỂM THỬ TASK-09 (Proposed Method & Translate) ---');

  const sharedThreadId = randomUUID();

  // Test 4: propose_method (Turn 1 - start)
  try {
    console.log('▶ [TASK-09] Gửi event: propose_method (Start - Lượt 1)...');
    const res = await sendAndAwait('TASK-09', 'propose_method', {
      thread_id: sharedThreadId,
      user_input: 'Đề xuất mô hình nghiên cứu các yếu tố ảnh hưởng hành vi tiêu dùng của Gen Z',
      final_proposal: { title: 'Nghiên cứu hành vi tiêu dùng Gen Z' },
      research_type: { type: 1 },
      auto_mode: false,
      inherit_mode: false,
      resume_method: false,
      resume_error: false,
      db: 'redis',
    });
    console.log('   ✅ Phản hồi propose_method (Start):', JSON.stringify(res));
  } catch (e) {
    console.log('   ❌ Thất bại propose_method (Start):', (e as Error).message);
  }

  // Test 5: propose_method (Turn 2 - resume)
  try {
    console.log('▶ [TASK-09] Gửi event: propose_method (Resume - Lượt 2 cùng thread_id)...');
    const res = await sendAndAwait('TASK-09', 'propose_method', {
      thread_id: sharedThreadId,
      user_input: 'Đồng ý với các thang đo Likert 5 mức độ trên',
      final_proposal: { title: 'Nghiên cứu hành vi tiêu dùng Gen Z' },
      research_type: { type: 1 },
      resume_method: true,
      resume_error: false,
      db: 'redis',
    });
    console.log('   ✅ Phản hồi propose_method (Resume):', JSON.stringify(res));
  } catch (e) {
    console.log('   ❌ Thất bại propose_method (Resume):', (e as Error).message);
  }

  // Test 6: translate_report
  try {
    console.log('▶ [TASK-09] Gửi event: translate_report...');
    const res = await sendAndAwait('TASK-09', 'translate_report', {
      target_language: 'English',
      is_outline: true,
    });
    console.log('   ✅ Phản hồi translate_report:', JSON.stringify(res));
  } catch (e) {
    console.log('   ❌ Thất bại translate_report:', (e as Error).message);
  }

  try {
    await consumer.disconnect();
    await producer.disconnect();
  } catch {
    // Ignore cleanup error
  }

  console.log('\n===============================================================');
  console.log('📊 KẾT QUẢ KIỂM CHỨNG E2E TỔNG HỢP (DOCKER <-> AI SERVER VPS)');
  console.log('===============================================================');
  console.table(
    results.map((r) => ({
      task: r.task,
      event: r.event,
      status: r.status,
      durationMs: r.durationMs,
      details: r.details,
    })),
  );
}

void runTask08And09SmokeTest();
