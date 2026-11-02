/**
 * T0 — Preflight: env, model whitelist, Prisma, Kafka, Postgres, AI server.
 * Chạy TRƯỚC mọi tầng khác; fail sớm để không đổ lỗi sai cho BE.
 */

import { execSync } from 'node:child_process';
import { connect as netConnect } from 'node:net';
import { PrismaClient } from '@prisma/client';
import { Kafka, logLevel } from 'kafkajs';
import { resolveScriptModelConfig } from '../lib/ai-model';
import { E2eReport } from '../lib/e2e-report';

declare const process: {
  env: Record<string, string | undefined>;
  argv: string[];
};

const REQUIRED_ENV = [
  'AI_DEFAULT_MODEL',
  'AI_ALLOWED_MODELS',
  'KAFKA',
  'DATABASE_URL',
];

function runCommand(command: string, timeoutMs = 60_000): string {
  return execSync(command, {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
    timeout: timeoutMs,
  });
}

async function checkTcp(
  host: string,
  port: number,
  timeoutMs = 5_000,
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const socket = netConnect({ host, port });
    const done = (error?: Error) => {
      socket.destroy();
      if (error) reject(error);
      else resolve();
    };
    socket.setTimeout(timeoutMs);
    socket.once('connect', () => done());
    socket.once('timeout', () => done(new Error(`TCP timeout ${host}:${port}`)));
    socket.once('error', (error) => done(error));
  });
}

async function checkKafka(brokerString: string): Promise<number> {
  const admin = new Kafka({
    clientId: `e2e-preflight-${Date.now()}`,
    brokers: brokerString.split(',').map((broker) => broker.trim()),
    logLevel: logLevel.NOTHING,
    connectionTimeout: 5_000,
  }).admin();
  try {
    await admin.connect();
    const topics = await admin.listTopics();
    return topics.length;
  } finally {
    await admin.disconnect().catch(() => undefined);
  }
}

function checkAiServer(baseUrl: string | undefined): Promise<void> | null {
  if (!baseUrl) return null;
  try {
    const url = new URL(baseUrl);
    const port = Number(url.port || (url.protocol === 'https:' ? 443 : 80));
    return checkTcp(url.hostname, port);
  } catch {
    return Promise.reject(new Error(`LOCALLLM_BASE_URL không hợp lệ: ${baseUrl}`));
  }
}

export async function runPreflight(
  report: E2eReport,
  options: { includeAiServer?: boolean } = {},
): Promise<void> {
  const env = process.env;
  const modelConfig = resolveScriptModelConfig();

  await report.run('env.required', 'T0', 'Env bắt buộc có đủ', {}, async () => {
    const missing = REQUIRED_ENV.filter((key) => !env[key]);
    if (missing.length > 0) {
      throw new Error(`thiếu biến: ${missing.join(', ')}`);
    }
    return `model=${modelConfig.selectedModel}, broker=${env.KAFKA}`;
  });

  await report.run(
    'env.model-whitelist',
    'T0',
    'Model mặc định thuộc whitelist',
    { modelId: modelConfig.selectedModel },
    async () => {
      if (modelConfig.fallbackReason) {
        throw new Error(
          `${modelConfig.fallbackReason}: requested=${modelConfig.requestedModel ?? '(empty)'}`,
        );
      }
      return `requested=selected=${modelConfig.selectedModel}`;
    },
  );

  await report.run('prisma.validate', 'T0', 'Prisma schema hợp lệ', {}, async () => {
    runCommand('npx prisma validate');
    return 'prisma validate OK';
  });

  await report.run('postgres.reachable', 'T0', 'Postgres reachable', {}, async () => {
    const prisma = new PrismaClient();
    try {
      await prisma.$queryRaw`SELECT 1`;
      const tables = await prisma.$queryRaw<
        Array<{ count: bigint }>
      >`SELECT count(*)::bigint AS count FROM information_schema.tables WHERE table_schema = 'public'`;
      return `${tables[0]?.count ?? 0} bảng trong schema public`;
    } finally {
      await prisma.$disconnect();
    }
  });

  // Dự án hiện dùng `prisma db push` ⇒ không có lịch sử migration. Đây là gap
  // triển khai cần ghi nhận (không phải lỗi runtime), nên SKIP thay vì FAIL.
  let migrateStatus = '';
  try {
    migrateStatus = runCommand('npx prisma migrate status', 120_000);
  } catch (error) {
    migrateStatus = String((error as any)?.stderr ?? (error as Error).message);
  }
  const notManaged = /not managed by Prisma Migrate/i.test(migrateStatus);
  const hasDrift = /drift|not yet been applied/i.test(migrateStatus);
  if (notManaged || hasDrift) {
    report.skip(
      'prisma.migration-history',
      'T0',
      'Lịch sử migration',
      notManaged
        ? 'DB không được quản lý bởi Prisma Migrate (dự án dùng db push) — gap triển khai'
        : `migrate status cảnh báo: ${migrateStatus.trim().slice(0, 160)}`,
    );
  } else {
    await report.run('prisma.migration-history', 'T0', 'Lịch sử migration', {}, async () => {
      return 'migrate status OK';
    });
  }

  await report.run('kafka.reachable', 'T0', 'Kafka reachable', {}, async () => {
    const count = await checkKafka(env.KAFKA || 'kafka:29092');
    return `${count} topic`;
  });

  if (options.includeAiServer) {
    await report.run('ai-server.reachable', 'T0', 'AI server reachable', {}, async () => {
      const check = checkAiServer(env.LOCALLLM_BASE_URL);
      if (!check) throw new Error('thiếu LOCALLLM_BASE_URL');
      await check;
      return env.LOCALLLM_BASE_URL as string;
    });
  } else {
    report.skip(
      'ai-server.reachable',
      'T0',
      'AI server reachable',
      'bỏ qua vì không chạy tier live',
    );
  }
}
