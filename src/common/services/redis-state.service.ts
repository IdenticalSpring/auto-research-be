import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import Redis from 'ioredis';
import { HealthCheckWebhookDto } from '../../modules/health-check/dto/health-check-webhook.dto';

export interface AiStatusRecord {
  eventType: string;
  processStatus: number;
  aiError: {
    errorCode: number | string | null;
    error: string | null;
  };
  updatedAt?: number;
}

@Injectable()
export class RedisStateService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RedisStateService.name);
  private rawClient: Redis | null = null;
  private readonly inMemoryCache = new Map<string, string>();
  private isEnabled = false;

  constructor(private readonly configService: ConfigService) {}

  async onModuleInit(): Promise<void> {
    this.isEnabled = this.configService.get<boolean>('redis.enabled', false);
    const redisUrl =
      this.configService.get<string>('redis.url') || 'redis://localhost:6379';

    if (this.isEnabled) {
      try {
        this.rawClient = new Redis(redisUrl, {
          lazyConnect: true,
          keyPrefix: '', // Khong dung prefix de dong bo 100% voi AI Service
          connectTimeout: this.configService.get<number>(
            'redis.connectTimeout',
            10000,
          ),
          maxRetriesPerRequest: 1,
        });
        await this.rawClient.connect();
        this.logger.log('RedisStateService raw client connected successfully');
      } catch (err: unknown) {
        this.logger.warn(
          `RedisStateService connection failed: ${(err as Error).message}. Fallback to in-memory mode.`,
        );
        this.rawClient = null;
      }
    } else {
      this.logger.log('Redis is disabled. Operating in in-memory mode.');
    }
  }

  async onModuleDestroy(): Promise<void> {
    if (this.rawClient) {
      try {
        await this.rawClient.quit();
      } catch {
        this.rawClient.disconnect();
      }
      this.rawClient = null;
    }
  }

  async setAiStatus(
    id: string,
    dto: HealthCheckWebhookDto,
    ttlSeconds = 3600,
  ): Promise<void> {
    const key = `AI_STATUS:${id}`;
    const payload: AiStatusRecord = {
      eventType: dto.eventType || 'unknown',
      processStatus: dto.processStatus,
      aiError: {
        errorCode: dto.aiError?.errorCode ?? null,
        error: dto.aiError?.error ?? null,
      },
      updatedAt: Date.now(),
    };
    const valueStr = JSON.stringify(payload);

    if (this.rawClient) {
      try {
        await this.rawClient.set(key, valueStr, 'EX', ttlSeconds);
        return;
      } catch (err) {
        this.logger.warn(
          `Failed to set Redis key ${key}: ${(err as Error).message}`,
        );
      }
    }
    this.inMemoryCache.set(key, valueStr);
  }

  async getAiStatus(id: string): Promise<AiStatusRecord | null> {
    const key = `AI_STATUS:${id}`;
    if (this.rawClient) {
      try {
        const val = await this.rawClient.get(key);
        if (val) {
          return JSON.parse(val) as AiStatusRecord;
        }
      } catch (err) {
        this.logger.warn(
          `Failed to get Redis key ${key}: ${(err as Error).message}`,
        );
      }
    }

    const memoryVal = this.inMemoryCache.get(key);
    return memoryVal ? (JSON.parse(memoryVal) as AiStatusRecord) : null;
  }

  async setAiKey(modelId: string, apiKey: string): Promise<void> {
    const key = `AI_KEY:${modelId}`;
    if (this.rawClient) {
      try {
        await this.rawClient.set(key, apiKey);
        return;
      } catch (err) {
        this.logger.warn(
          `Failed to set AI_KEY ${key}: ${(err as Error).message}`,
        );
      }
    }
    this.inMemoryCache.set(key, apiKey);
  }

  async setDbKey(apiKey: string): Promise<void> {
    await this.setAiKey('db_key', apiKey);
  }

  async setSearchWebKey(apiKey: string): Promise<void> {
    await this.setAiKey('search_web_key', apiKey);
  }

  async getAiKey(keyName: string): Promise<string | null> {
    const key = `AI_KEY:${keyName}`;
    if (this.rawClient) {
      try {
        return await this.rawClient.get(key);
      } catch (err) {
        this.logger.warn(
          `Failed to get AI_KEY ${key}: ${(err as Error).message}`,
        );
      }
    }
    return this.inMemoryCache.get(key) || null;
  }
}
