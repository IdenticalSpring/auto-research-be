/**
 * Chạy 1 case qua Jobs API: publish → bắt envelope → giả lập AI trả response
 * → chờ job đạt trạng thái kỳ vọng.
 */

import { buildResponseKey } from './ai-response';
import { E2eHttpClient, expectStatus } from './e2e-http';
import { E2eReport } from './e2e-report';
import { FakeAiServer } from './fake-ai-server';

export const JOB_PROBE_TIMEOUT_MS = 20_000;

export interface JobCaseSpec {
  id: string;
  module: 'document_setup' | 'write_report' | 'chatbot' | 'enhancement';
  action: string;
  eventType: string;
  response: Record<string, unknown>;
  expectStatus: 'finished' | 'error' | 'suspended';
  expectMarker?: string;
}

export const JOB_CASES: JobCaseSpec[] = [
  {
    id: 'jobs.document_setup',
    module: 'document_setup',
    action: 'generate_domains',
    eventType: 'generate_domains',
    response: { status: 'finished', data: { domains: ['Kinh tế', 'Quản trị'] } },
    expectStatus: 'finished',
    expectMarker: 'Kinh tế',
  },
  {
    id: 'jobs.write_report',
    module: 'write_report',
    action: 'chuyen_de_2',
    eventType: 'chuyen_de_2',
    response: { status: 'finished', data: { content: 'Nội dung chuyên đề 2' } },
    expectStatus: 'finished',
    expectMarker: 'Nội dung chuyên đề 2',
  },
  {
    id: 'jobs.chatbot',
    module: 'chatbot',
    action: 'outer_chatbot',
    eventType: 'outer_chatbot',
    response: { status: 'finished', response: 'Xin chào từ AI giả' },
    expectStatus: 'finished',
  },
  {
    id: 'jobs.enhancement',
    module: 'enhancement',
    action: 'ai_in_doc',
    eventType: 'ai_in_doc',
    response: { status: 'finished', response: 'Đoạn văn đã sửa' },
    expectStatus: 'finished',
  },
  {
    id: 'jobs.error-mapping',
    module: 'enhancement',
    action: 'enhance',
    eventType: 'enhance',
    response: { error_code: 633, error: 'UnboundLocalError current_report' },
    expectStatus: 'error',
    expectMarker: '633',
  },
  {
    id: 'jobs.suspend',
    module: 'write_report',
    action: 'chuyen_de_3',
    eventType: 'chuyen_de_3',
    response: {
      status: 'suspended',
      done_chat: false,
      ai_message: 'Chọn thang đo nào?',
    },
    expectStatus: 'suspended',
  },
];

async function waitJob(
  client: E2eHttpClient,
  jobId: string,
  predicate: (job: any) => boolean,
  timeoutMs = 20_000,
): Promise<any> {
  const deadline = Date.now() + timeoutMs;
  let last: any;
  while (Date.now() < deadline) {
    const result = await client.get(`/jobs/${jobId}`);
    expectStatus(result, 200, `GET /jobs/${jobId}`);
    last = result.body;
    if (predicate(last)) return last;
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(
    `job ${jobId} không đạt trạng thái mong đợi sau ${timeoutMs}ms — last=${JSON.stringify(last).slice(0, 200)}`,
  );
}

function assertEnvelope(
  payload: Record<string, any>,
  spec: JobCaseSpec,
  modelId: string,
  documentId: string,
): void {
  const problems: string[] = [];
  if (payload.event_type !== spec.eventType) {
    problems.push(`event_type=${payload.event_type}`);
  }
  if (payload.model_id !== modelId) {
    problems.push(`model_id=${payload.model_id} (kỳ vọng ${modelId})`);
  }
  if (payload.document_id !== documentId) {
    problems.push(`document_id=${payload.document_id}`);
  }
  if (!payload.session_id) problems.push('thiếu session_id');
  if (!payload.language) problems.push('thiếu language');
  if (problems.length > 0) {
    throw new Error(`envelope sai: ${problems.join(', ')}`);
  }
}

export async function runJobCase(
  report: E2eReport,
  client: E2eHttpClient,
  fake: FakeAiServer,
  spec: JobCaseSpec,
  modelId: string,
  documentId: string,
): Promise<void> {
  await report.run(
    spec.id,
    'T3',
    `jobs ${spec.module}/${spec.eventType}`,
    { modelId, documentId, timeoutMs: JOB_PROBE_TIMEOUT_MS },
    async () => {
      const created = await client.post('/jobs', {
        module: spec.module,
        action: spec.action,
        document_id: documentId,
        session_id: `sess-${spec.id}-${Date.now()}`,
        payload: { event_type: spec.eventType, language: 'vi' },
      });
      expectStatus(created, [200, 201], 'POST /jobs');
      const jobId: string = created.body?.job_id ?? created.body?.id;
      if (!jobId) throw new Error('không có job_id');

      const request = await fake.nextJobId(jobId);
      assertEnvelope(request.payload, spec, modelId, documentId);

      await fake.respond(request, { job_id: jobId, ...spec.response });

      const job = await waitJob(
        client,
        jobId,
        (item) => item?.status === spec.expectStatus,
      );
      if (spec.expectMarker) {
        // marker có thể nằm ở result (data) hoặc errorCode/errorMessage tuỳ nhánh
        const text = JSON.stringify(job);
        if (!text.includes(spec.expectMarker)) {
          throw new Error(
            `job result thiếu marker "${spec.expectMarker}": ${text.slice(0, 160)}`,
          );
        }
      }
      return `status=${job.status}, envelope OK, key=${buildResponseKey(documentId, spec.eventType)}`;
    },
  );
}
