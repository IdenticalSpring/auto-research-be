import { jest } from '@jest/globals';
import { Test, TestingModule } from '@nestjs/testing';
import { JobStatus } from '@prisma/client';
import { HealthCheckService } from './health-check.service';
import { PrismaService } from '../../database/prisma.service';
import { RedisStateService } from '../../common/services/redis-state.service';
import { HealthCheckWebhookDto } from './dto/health-check-webhook.dto';

describe('HealthCheckService', () => {
  let service: HealthCheckService;
  let prisma: PrismaService;
  let redisState: RedisStateService;

  const mockPrisma = {
    job: {
      findFirst: jest.fn(),
      update: jest.fn(),
    },
  };

  const mockRedisState = {
    setAiStatus: jest.fn().mockResolvedValue(undefined),
    getAiStatus: jest.fn().mockResolvedValue({
      eventType: 'search_papers',
      processStatus: 2,
      aiError: { errorCode: null, error: null },
    }),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        HealthCheckService,
        { provide: PrismaService, useValue: mockPrisma },
        { provide: RedisStateService, useValue: mockRedisState },
      ],
    }).compile();

    service = module.get<HealthCheckService>(HealthCheckService);
    prisma = module.get<PrismaService>(PrismaService);
    redisState = module.get<RedisStateService>(RedisStateService);
    jest.clearAllMocks();
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
    expect(prisma).toBeDefined();
    expect(redisState).toBeDefined();
  });

  it('should handle heartbeat and update active job to error when processStatus = 4', async () => {
    (mockPrisma.job.findFirst as jest.Mock).mockResolvedValue({
      id: 'job-123',
      status: JobStatus.processing,
    });

    const dto: HealthCheckWebhookDto = {
      eventType: 'write_content',
      processStatus: 4,
      aiError: {
        errorCode: 604,
        error: 'Timeout calling LLM provider',
      },
    };

    const res = await service.handleHeartbeat('doc-abc', dto);

    expect(res.success).toBe(true);
    expect(mockRedisState.setAiStatus).toHaveBeenCalledWith('doc-abc', dto, 3600);
    expect(mockPrisma.job.update).toHaveBeenCalledWith({
      where: { id: 'job-123' },
      data: {
        status: JobStatus.error,
        errorCode: '604',
        errorMessage: 'Timeout calling LLM provider',
      },
    });
  });

  it('should handle heartbeat and update active job to suspended when processStatus = 5', async () => {
    (mockPrisma.job.findFirst as jest.Mock).mockResolvedValue({
      id: 'job-123',
      status: JobStatus.processing,
    });

    const dto: HealthCheckWebhookDto = {
      eventType: 'write_content',
      processStatus: 5,
      aiError: null,
    };

    const res = await service.handleHeartbeat('doc-abc', dto);

    expect(res.success).toBe(true);
    expect(mockPrisma.job.update).toHaveBeenCalledWith({
      where: { id: 'job-123' },
      data: {
        status: JobStatus.suspended,
      },
    });
  });

  it('should handle heartbeat and update active job to finished when processStatus = 3', async () => {
    (mockPrisma.job.findFirst as jest.Mock).mockResolvedValue({
      id: 'job-123',
      status: JobStatus.processing,
    });

    const dto: HealthCheckWebhookDto = {
      eventType: 'search_papers',
      processStatus: 3,
      aiError: null,
    };

    const res = await service.handleHeartbeat('doc-abc', dto);

    expect(res.success).toBe(true);
    expect(mockPrisma.job.update).toHaveBeenCalledWith({
      where: { id: 'job-123' },
      data: {
        status: JobStatus.finished,
      },
    });
  });

  it('should update job from pending to processing when processStatus = 2', async () => {
    (mockPrisma.job.findFirst as jest.Mock).mockResolvedValue({
      id: 'job-456',
      status: JobStatus.pending,
    });

    const dto: HealthCheckWebhookDto = {
      eventType: 'generate_outline',
      processStatus: 2,
      aiError: null,
    };

    const res = await service.handleHeartbeat('doc-456', dto);

    expect(res.success).toBe(true);
    expect(mockPrisma.job.update).toHaveBeenCalledWith({
      where: { id: 'job-456' },
      data: {
        status: JobStatus.processing,
      },
    });
  });

  it('should gracefully handle database exceptions during job update', async () => {
    (mockPrisma.job.findFirst as jest.Mock).mockRejectedValue(
      new Error('Database query failure'),
    );

    const dto: HealthCheckWebhookDto = {
      eventType: 'search_papers',
      processStatus: 2,
      aiError: null,
    };

    const res = await service.handleHeartbeat('doc-err', dto);
    expect(res.success).toBe(true);
  });

  it('should get status from redisState', async () => {
    const status = await service.getStatus('doc-abc');
    expect(status?.eventType).toBe('search_papers');
    expect(mockRedisState.getAiStatus).toHaveBeenCalledWith('doc-abc');
  });

  it('should fallback eventType to activeJob action when eventType is omitted', async () => {
    (mockPrisma.job.findFirst as jest.Mock).mockResolvedValue({
      id: 'job-789',
      action: 'generate_titles',
      status: JobStatus.processing,
    });

    const dto: HealthCheckWebhookDto = {
      processStatus: 2,
    };

    const res = await service.handleHeartbeat('doc-789', dto);
    expect(res.success).toBe(true);
    expect(res.data.eventType).toBe('generate_titles');
    expect(mockRedisState.setAiStatus).toHaveBeenCalledWith(
      'doc-789',
      expect.objectContaining({ eventType: 'generate_titles' }),
      3600,
    );
  });
});
