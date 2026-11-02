/**
 * T3 — Kafka round-trip với "AI server giả".
 *
 * Phần 1: 6 case Jobs API (envelope + status/result + error mapping + suspend).
 * Phần 2: 3 edge case của harness (response đến sớm, streaming chunk, error_code)
 *         chạy trên topic probe riêng để không đụng AI server thật.
 */

import { Kafka, logLevel } from 'kafkajs';
import { E2eHttpClient, expectStatus, registerAndLogin } from '../lib/e2e-http';
import { E2eReport } from '../lib/e2e-report';
import { FakeAiServer, createFakeAiServer } from '../lib/fake-ai-server';
import { JOB_CASES, runJobCase } from '../lib/job-case-runner';
import { KafkaResponseHarness } from '../lib/kafka-harness';

declare const process: { env: Record<string, string | undefined> };

const BROKER = process.env.KAFKA || 'kafka:29092';
const PROBE_TIMEOUT_MS = 15_000;
const PROBE_REQUEST_TOPIC = 'e2e_probe_request';
const PROBE_RESPONSE_TOPIC = 'e2e_probe_response';

export async function runTierKafka(
  report: E2eReport,
  modelId: string,
): Promise<void> {
  const runId = `e2e-kafka-${Date.now()}`;
  const anon = new E2eHttpClient();
  const credentials = await registerAndLogin(anon, runId);
  const client = anon.withToken(credentials.token);

  const fake: FakeAiServer = createFakeAiServer(BROKER);
  const harnessKafka = new Kafka({
    clientId: `e2e-harness-${Date.now()}`,
    brokers: BROKER.split(',').map((broker) => broker.trim()),
    logLevel: logLevel.NOTHING,
    connectionTimeout: 10_000,
  });
  const harness = new KafkaResponseHarness(harnessKafka, [PROBE_RESPONSE_TOPIC], {
    defaultTimeoutMs: PROBE_TIMEOUT_MS,
  });

  // Job.documentId là FK ⇒ phải dùng document thật
  const documentResponse = await client.post('/documents', {
    title: `E2E kafka ${runId}`,
    language: 'vi',
  });
  expectStatus(documentResponse, [200, 201], 'POST /documents (T3 setup)');
  const documentId: string =
    documentResponse.body?.id ?? documentResponse.body?.data?.id;
  if (!documentId) {
    throw new Error('Không tạo được document cho T3');
  }

  await report.run(
    'kafka.fake-ai-server',
    'T3',
    'Kết nối fake AI server + tạo probe topic',
    {},
    async () => {
      const admin = harnessKafka.admin();
      await admin.connect();
      try {
        await admin.createTopics({
          topics: [
            { topic: PROBE_REQUEST_TOPIC },
            { topic: PROBE_RESPONSE_TOPIC },
          ],
          waitForLeaders: true,
        });
      } catch {
        // topic đã tồn tại
      } finally {
        await admin.disconnect().catch(() => undefined);
      }
      await fake.start();
      await harness.start();
      return 'producer + consumer + probe topics ready';
    },
  );

  try {
    for (const spec of JOB_CASES) {
      await runJobCase(report, client, fake, spec, modelId, documentId);
    }

    await report.run(
      'harness.early-response',
      'T3',
      'Response đến trước request (orphan store)',
      { modelId, timeoutMs: PROBE_TIMEOUT_MS },
      async () => {
        const probeDoc = `${runId}-orphan`;
        const eventType = 'e2e_orphan_probe';
        await fake.publishResponse(PROBE_RESPONSE_TOPIC, {
          document_id: probeDoc,
          event_type: eventType,
          model_id: modelId,
          session_id: `sess-${probeDoc}`,
          has_finished: true,
          marker: 'orphan-ok',
        });
        await new Promise((resolve) => setTimeout(resolve, 800));
        const startedAt = Date.now();
        const response = await harness.send({
          topic: PROBE_REQUEST_TOPIC,
          payload: {
            document_id: probeDoc,
            event_type: eventType,
            model_id: modelId,
            session_id: `sess-${probeDoc}`,
          },
          timeoutMs: PROBE_TIMEOUT_MS,
        });
        if ((response as any).marker !== 'orphan-ok') {
          throw new Error(
            `không nhận response lưu tạm: ${JSON.stringify(response)}`,
          );
        }
        return `lấy từ orphan trong ${Date.now() - startedAt}ms`;
      },
    );

    await report.run(
      'harness.chunk-streaming',
      'T3',
      'Streaming chunk has_finished=false rồi true',
      { modelId, timeoutMs: PROBE_TIMEOUT_MS },
      async () => {
        const probeDoc = `${runId}-chunk`;
        const eventType = 'e2e_chunk_probe';
        const sending = harness.send({
          topic: PROBE_REQUEST_TOPIC,
          payload: {
            document_id: probeDoc,
            event_type: eventType,
            model_id: modelId,
            session_id: `sess-${probeDoc}`,
          },
          timeoutMs: PROBE_TIMEOUT_MS,
        });
        await new Promise((resolve) => setTimeout(resolve, 300));
        for (const chunk of [1, 2]) {
          await fake.publishResponse(PROBE_RESPONSE_TOPIC, {
            document_id: probeDoc,
            event_type: eventType,
            model_id: modelId,
            session_id: `sess-${probeDoc}`,
            has_finished: false,
            chunk,
          });
        }
        await fake.publishResponse(PROBE_RESPONSE_TOPIC, {
          document_id: probeDoc,
          event_type: eventType,
          model_id: modelId,
          session_id: `sess-${probeDoc}`,
          has_finished: true,
          marker: 'chunk-done',
        });
        const response = await sending;
        if ((response as any).marker !== 'chunk-done') {
          throw new Error('không nhận chunk cuối');
        }
        return 'nhận 2 chunk rồi finished';
      },
    );

    await report.run(
      'harness.api-error',
      'T3',
      'error_code từ response ⇒ fail-fast kèm mã lỗi',
      { modelId, timeoutMs: PROBE_TIMEOUT_MS },
      async () => {
        const probeDoc = `${runId}-err`;
        const eventType = 'e2e_error_probe';
        const sending = harness.send({
          topic: PROBE_REQUEST_TOPIC,
          payload: {
            document_id: probeDoc,
            event_type: eventType,
            model_id: modelId,
            session_id: `sess-${probeDoc}`,
          },
          timeoutMs: PROBE_TIMEOUT_MS,
        });
        await new Promise((resolve) => setTimeout(resolve, 300));
        await fake.publishResponse(PROBE_RESPONSE_TOPIC, {
          document_id: probeDoc,
          event_type: eventType,
          error_code: 633,
          error: 'UnboundLocalError current_report',
        });
        try {
          await sending;
        } catch (error) {
          const message = (error as Error).message;
          if (message.includes('AI Error') && message.includes('633')) {
            return `reject đúng: ${message.slice(0, 80)}`;
          }
          throw new Error(`thông điệp lỗi không đúng: ${message}`);
        }
        throw new Error('harness không reject khi có error_code');
      },
    );
  } finally {
    await harness.stop().catch(() => undefined);
    await fake.stop().catch(() => undefined);
  }
}
