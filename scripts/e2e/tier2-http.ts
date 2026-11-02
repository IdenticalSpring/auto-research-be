/**
 * T2 — HTTP/DB contract: guards, CRUD, validation, error format.
 * Chạy với BE đang sống + Postgres thật, KHÔNG cần AI server.
 */

import {
  E2eHttpClient,
  expectStatus,
  registerAndLogin,
} from '../lib/e2e-http';
import { E2eReport } from '../lib/e2e-report';

declare const process: { env: Record<string, string | undefined> };

/** Các GET cần JWT — kiểm tra guard wiring của controller. */
const PROTECTED_GET_PATHS = [
  '/documents',
  '/document-keywords',
  '/proposals',
  '/outlines',
  '/references',
  '/uploaded-documents',
  '/datasets',
  '/dataset-variables',
  '/proposed-methods',
  '/generated-slides',
  '/reports',
  '/chat-sessions',
  '/chat-messages',
  '/enhancement-sessions',
  '/enhancement-suggestions',
  '/users',
];

async function guardSweep(report: E2eReport, client: E2eHttpClient) {
  for (const path of PROTECTED_GET_PATHS) {
    const id = `guard${path.replace(/\//g, '.')}`;
    await report.run(id, 'T2', `401 khi thiếu JWT: GET ${path}`, {}, async () => {
      const result = await client.get(path);
      expectStatus(result, 401, `GET ${path} không token`);
      return 'HTTP 401';
    });
  }
}

async function crudFlow(
  report: E2eReport,
  client: E2eHttpClient,
  runId: string,
  modelId: string,
) {
  let documentId = '';

  await report.run('documents.create', 'T2', 'POST /documents', {}, async () => {
    const result = await client.post('/documents', {
      title: `E2E document ${runId}`,
      language: 'vi',
      field: 'Kinh tế học',
    });
    expectStatus(result, [200, 201], 'POST /documents');
    documentId = result.body?.id ?? result.body?.data?.id ?? '';
    if (!documentId) throw new Error(`không có id trong response: ${JSON.stringify(result.body).slice(0, 200)}`);
    return `id=${documentId}`;
  });

  await report.run(
    'documents.validation',
    'T2',
    'POST /documents numberOfDomains âm ⇒ 400',
    {},
    async () => {
      const result = await client.post('/documents', {
        title: 'E2E invalid',
        numberOfDomains: -1,
      });
      expectStatus(result, 400, 'POST /documents numberOfDomains=-1');
      return `HTTP 400 (${result.body?.message ?? 'no message'})`;
    },
  );

  await report.run('documents.list', 'T2', 'GET /documents có bản ghi mới', {}, async () => {
    const result = await client.get('/documents');
    expectStatus(result, 200, 'GET /documents');
    const list = Array.isArray(result.body) ? result.body : result.body?.data;
    if (!Array.isArray(list)) throw new Error('response không phải mảng');
    const found = list.some((item: any) => item.id === documentId);
    if (!found) throw new Error(`không thấy document ${documentId} trong danh sách`);
    return `${list.length} document`;
  });

  await report.run(
    'chat-session.model-sync',
    'T2',
    'ChatSession lưu đúng model cấu hình (B3)',
    { modelId },
    async () => {
      const created = await client.post('/chat-sessions', {
        title: `E2E session ${runId}`,
        mode: 'outer_chatbot',
        documentId: documentId || undefined,
      });
      expectStatus(created, [200, 201], 'POST /chat-sessions');
      const sessionId = created.body?.id ?? created.body?.data?.id;
      if (!sessionId) throw new Error('không có id session');
      const fetched = await client.get(`/chat-sessions/${sessionId}`);
      expectStatus(fetched, 200, 'GET /chat-sessions/:id');
      const stored = fetched.body?.model ?? fetched.body?.data?.model;
      if (stored !== modelId) {
        throw new Error(`session.model=${stored} khác model cấu hình ${modelId}`);
      }
      await client.delete(`/chat-sessions/${sessionId}`);
      return `model=${stored}`;
    },
  );

  await report.run('datasets.crud', 'T2', 'Dataset CRUD', {}, async () => {
    const created = await client.post('/datasets', {
      documentId: documentId || undefined,
      name: `E2E dataset ${runId}`,
    });
    expectStatus(created, [200, 201], 'POST /datasets');
    const id = created.body?.id ?? created.body?.data?.id;
    if (!id) throw new Error('không có id dataset');
    const patched = await client.patch(`/datasets/${id}`, {
      name: `E2E dataset ${runId} updated`,
    });
    expectStatus(patched, 200, 'PATCH /datasets/:id');
    const removed = await client.delete(`/datasets/${id}`);
    expectStatus(removed, [200, 204], 'DELETE /datasets/:id');
    return `create→update→delete OK (${id})`;
  });

  await report.run('health-check.status', 'T2', 'GET /health-check/:id', {}, async () => {
    // route này được exclude globalPrefix ⇒ gọi absolute
    const result = await client.rootGet(`/health-check/${documentId || runId}`);
    expectStatus(result, 200, 'GET /health-check/:id');
    const status = result.body?.status;
    if (!status || typeof status.processStatus !== 'number') {
      throw new Error(`shape status sai: ${JSON.stringify(result.body).slice(0, 200)}`);
    }
    return `processStatus=${status.processStatus}, eventType=${status.eventType}`;
  });

  await report.run('token-usage.summary', 'T2', 'GET /token-usage/user/:id', {}, async () => {
    const result = await client.get('/token-usage/user/00000000-0000-0000-0000-000000000001');
    expectStatus(result, 200, 'GET /token-usage/user/:id');
    return 'shape OK';
  });

  await report.run('error.404-handling', 'T2', 'Route không tồn tại ⇒ 404 JSON', {}, async () => {
    const result = await client.get('/khong-ton-tai-route');
    expectStatus(result, 404, 'GET /khong-ton-tai-route');
    const isJson = typeof result.body === 'object' && result.body !== null;
    if (!isJson) {
      throw new Error(
        `404 không phải JSON: ${JSON.stringify(result.body).slice(0, 160)}`,
      );
    }
    if (result.body.statusCode !== 404) {
      throw new Error(`JSON body thiếu statusCode: ${JSON.stringify(result.body).slice(0, 160)}`);
    }
    return '404 JSON chuẩn';
  });

  if (documentId) {
    await report.run('documents.cleanup', 'T2', 'DELETE /documents/:id', {}, async () => {
      const result = await client.delete(`/documents/${documentId}`);
      expectStatus(result, [200, 204], 'DELETE /documents/:id');
      return 'đã xoá dữ liệu test';
    });
  }
}

export async function runTierHttp(
  report: E2eReport,
  modelId: string,
): Promise<void> {
  const runId = `e2e-${Date.now()}`;
  const anon = new E2eHttpClient();
  const credentials = await registerAndLogin(anon, runId);
  const client = anon.withToken(credentials.token);

  await report.run('auth.register-login', 'T2', 'Register/Login lấy JWT', { modelId }, async () => {
    if (!credentials.token) throw new Error('không có token');
    return `email=${credentials.email}`;
  });

  await guardSweep(report, anon);
  await crudFlow(report, client, runId, modelId);

  await report.run('auth.logout', 'T2', 'POST /auth/logout', {}, async () => {
    const result = await client.post('/auth/logout', {});
    expectStatus(result, [200, 201, 204], 'POST /auth/logout');
    return 'logout OK';
  });
}
