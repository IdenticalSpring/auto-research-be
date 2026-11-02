/**
 * HTTP client tối giản cho E2E (Node 24 có sẵn `fetch`, không cần dependency mới).
 * Tự resolve base URL từ env, hỗ trợ login/register và assert status.
 */

declare const process: {
  env: Record<string, string | undefined>;
  argv: string[];
};

export interface HttpResult {
  status: number;
  body: any;
  durationMs: number;
  headers: Record<string, string>;
}

export interface RequestOptions {
  token?: string;
  body?: unknown;
  timeoutMs?: number;
  headers?: Record<string, string>;
  /** true ⇒ path tuyệt đối từ origin (bỏ qua API_PREFIX), dùng cho route exclude như /health-check. */
  rawUrl?: boolean;
}

export function resolveBaseUrl(
  env: Record<string, string | undefined> = process.env,
): string {
  if (env.E2E_BASE_URL) {
    return env.E2E_BASE_URL.replace(/\/$/, '');
  }
  const port = env.PORT || '3001';
  const prefix = (env.API_PREFIX || 'api/v1').replace(/^\/|\/$/g, '');
  return `http://localhost:${port}/${prefix}`;
}

export function pickToken(body: any): string | undefined {
  if (!body || typeof body !== 'object') return undefined;
  return (
    body.access_token ||
    body.accessToken ||
    body.token ||
    body.data?.access_token ||
    body.data?.accessToken ||
    undefined
  );
}

export function pickUserId(body: any): string | undefined {
  if (!body || typeof body !== 'object') return undefined;
  return (
    body.user?.id ||
    body.user?.userId ||
    body.data?.user?.id ||
    body.userId ||
    body.sub ||
    undefined
  );
}

export class E2eHttpClient {
  readonly baseUrl: string;

  constructor(
    baseUrl = resolveBaseUrl(),
    private readonly defaultToken?: string,
  ) {
    this.baseUrl = baseUrl;
  }

  withToken(token: string): E2eHttpClient {
    return new E2eHttpClient(this.baseUrl, token);
  }

  async request(
    method: string,
    path: string,
    options: RequestOptions = {},
  ): Promise<HttpResult> {
    const startedAt = Date.now();
    const headers: Record<string, string> = {
      accept: 'application/json',
      ...(options.headers ?? {}),
    };
    const token = options.token ?? this.defaultToken;
    if (token) headers.authorization = `Bearer ${token}`;
    if (options.body !== undefined) headers['content-type'] = 'application/json';

    const controller = new AbortController();
    const timer = setTimeout(
      () => controller.abort(),
      options.timeoutMs ?? 30_000,
    );
    const url = options.rawUrl
      ? `${new URL(this.baseUrl).origin}${path}`
      : `${this.baseUrl}${path}`;
    try {
      const response = await fetch(url, {
        method,
        headers,
        body:
          options.body === undefined ? undefined : JSON.stringify(options.body),
        signal: controller.signal,
      });
      const text = await response.text();
      let body: any = text;
      try {
        body = text ? JSON.parse(text) : null;
      } catch {
        body = text;
      }
      return {
        status: response.status,
        body,
        durationMs: Date.now() - startedAt,
        headers: Object.fromEntries(response.headers.entries()),
      };
    } finally {
      clearTimeout(timer);
    }
  }

  get(path: string, options?: RequestOptions) {
    return this.request('GET', path, options);
  }
  /** GET route nằm ngoài API_PREFIX (ví dụ `health-check` được exclude globalPrefix). */
  rootGet(path: string, options?: RequestOptions) {
    return this.request('GET', path, { ...options, rawUrl: true });
  }
  /** POST route nằm ngoài API_PREFIX. */
  rootPost(path: string, body?: unknown, options?: RequestOptions) {
    return this.request('POST', path, { ...options, body, rawUrl: true });
  }
  post(path: string, body?: unknown, options?: RequestOptions) {
    return this.request('POST', path, { ...options, body });
  }
  put(path: string, body?: unknown, options?: RequestOptions) {
    return this.request('PUT', path, { ...options, body });
  }
  patch(path: string, body?: unknown, options?: RequestOptions) {
    return this.request('PATCH', path, { ...options, body });
  }
  delete(path: string, options?: RequestOptions) {
    return this.request('DELETE', path, options);
  }
}

/** Assert status; khi sai thì ném lỗi kèm body rút gọn để dễ debug. */
export function expectStatus(
  result: HttpResult,
  expected: number | number[],
  label: string,
): void {
  const allowed = Array.isArray(expected) ? expected : [expected];
  if (allowed.includes(result.status)) {
    return;
  }
  const body =
    typeof result.body === 'string'
      ? result.body
      : JSON.stringify(result.body ?? null);
  throw new Error(
    `${label}: mong đợi HTTP ${allowed.join('/')} nhưng nhận ${result.status} — ${body.slice(0, 300)}`,
  );
}

export interface Credentials {
  token: string;
  userId?: string;
  email: string;
  password: string;
}

/** Đăng ký user mới theo runId (idempotent trong 1 lần chạy), fallback sang login. */
export async function registerAndLogin(
  client: E2eHttpClient,
  runId: string,
): Promise<Credentials> {
  const email = `${runId}@e2e.local`.toLowerCase();
  const password = 'E2ePassword123!';
  // userName là unique trong DB ⇒ lấy đuôi runId (phần timestamp) để không trùng
  const userName = `e2e${runId.replace(/[^a-zA-Z0-9]/g, '').slice(-10)}`;
  const register = await client.post('/auth/register', {
    email,
    userName,
    password,
    fullName: `E2E ${runId}`,
  });

  if (register.status === 201 || register.status === 200) {
    const token = pickToken(register.body);
    if (!token) {
      throw new Error(
        `register: HTTP ${register.status} nhưng không có token — ${JSON.stringify(register.body).slice(0, 300)}`,
      );
    }
    return { token, userId: pickUserId(register.body), email, password };
  }
  if (register.status !== 409) {
    // 409 = email đã tồn tại (chạy lại) → thử login; còn lại là lỗi thật
    throw new Error(
      `register: HTTP ${register.status} — ${JSON.stringify(register.body).slice(0, 300)}`,
    );
  }

  // LoginDto dùng `identifier` (email hoặc username), không phải `email`
  const login = await client.post('/auth/login', {
    identifier: email,
    email,
    password,
  });
  if (login.status !== 200 && login.status !== 201) {
    throw new Error(
      `login: HTTP ${login.status} — ${JSON.stringify(login.body).slice(0, 300)} (email=${email})`,
    );
  }
  const token = pickToken(login.body);
  if (!token) {
    throw new Error(
      `login: không tìm thấy access token trong response — ${JSON.stringify(login.body).slice(0, 200)}`,
    );
  }
  return { token, userId: pickUserId(login.body), email, password };
}
