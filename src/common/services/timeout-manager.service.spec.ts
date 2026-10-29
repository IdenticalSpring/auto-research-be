import { jest } from '@jest/globals';
import { Test, TestingModule } from '@nestjs/testing';
import { JobStatus } from '@prisma/client';
import { PrismaService } from '../../database/prisma.service';
import {
  TimeoutManagerService,
  DEFAULT_TIMEOUT_MS,
} from './timeout-manager.service';

describe('TimeoutManagerService', () => {
  let service: TimeoutManagerService;
  let prismaMock: {
    job: {
      update: jest.MockedFunction<any>;
    };
  };

  beforeEach(async () => {
    prismaMock = {
      job: {
        update: jest.fn(),
      },
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        TimeoutManagerService,
        {
          provide: PrismaService,
          useValue: prismaMock,
        },
      ],
    }).compile();

    service = module.get<TimeoutManagerService>(TimeoutManagerService);
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  describe('getTimeoutForEvent', () => {
    it('should return 1500s (1500000ms) for heavy generation events', () => {
      expect(service.getTimeoutForEvent('write_content')).toBe(1500_000);
      expect(service.getTimeoutForEvent('chuyen_de_1')).toBe(1500_000);
      expect(service.getTimeoutForEvent('chuyen_de_2')).toBe(1500_000);
      expect(service.getTimeoutForEvent('chuyen_de_3')).toBe(1500_000);
      expect(service.getTimeoutForEvent('writer')).toBe(1500_000);
    });

    it('should return 900s (900000ms) for journal scraping', () => {
      expect(service.getTimeoutForEvent('get_journals')).toBe(900_000);
    });

    it('should return 300s (300000ms) for analysis & slide events', () => {
      expect(service.getTimeoutForEvent('analyzer')).toBe(300_000);
      expect(service.getTimeoutForEvent('gen_slides')).toBe(300_000);
      expect(service.getTimeoutForEvent('comment_data')).toBe(300_000);
      expect(service.getTimeoutForEvent('propose_method')).toBe(300_000);
      expect(service.getTimeoutForEvent('admin_documents')).toBe(300_000);
    });

    it('should return 180s (180000ms) for proposal, outline and papers events', () => {
      expect(service.getTimeoutForEvent('generate_titles')).toBe(180_000);
      expect(service.getTimeoutForEvent('generate_title_description')).toBe(180_000);
      expect(service.getTimeoutForEvent('mix_titles')).toBe(180_000);
      expect(service.getTimeoutForEvent('generate_outline')).toBe(180_000);
      expect(service.getTimeoutForEvent('search_papers')).toBe(180_000);
      expect(service.getTimeoutForEvent('embed_user_documents')).toBe(180_000);
    });

    it('should return 120s (120000ms) for chatbot & text enhancement events', () => {
      expect(service.getTimeoutForEvent('outer_chatbot')).toBe(120_000);
      expect(service.getTimeoutForEvent('inner_chatbot')).toBe(120_000);
      expect(service.getTimeoutForEvent('writer_chatbot')).toBe(120_000);
      expect(service.getTimeoutForEvent('enhance')).toBe(120_000);
      expect(service.getTimeoutForEvent('generate_domains')).toBe(120_000);
      expect(service.getTimeoutForEvent('delete_report')).toBe(120_000);
    });

    it('should fallback to default 120s for unknown or undefined events', () => {
      expect(service.getTimeoutForEvent(undefined)).toBe(DEFAULT_TIMEOUT_MS);
      expect(service.getTimeoutForEvent('unknown_event')).toBe(
        DEFAULT_TIMEOUT_MS,
      );
    });
  });

  describe('registerTimeout & clearTimeout lifecycle', () => {
    it('should trigger callback with 604 error when timeout fires', () => {
      jest.useFakeTimers();

      const onTimeoutMock = jest.fn();
      const key = 'user-1:doc-1:sess-1:write_content';

      service.registerTimeout(key, 'write_content', onTimeoutMock, 5000);

      expect(service.hasTimeout(key)).toBe(true);
      expect(service.getActiveTimeoutCount()).toBe(1);

      // Advance by 4999ms -> not fired yet
      jest.advanceTimersByTime(4999);
      expect(onTimeoutMock).not.toHaveBeenCalled();

      // Advance past 5000ms -> fired
      jest.advanceTimersByTime(2);
      expect(onTimeoutMock).toHaveBeenCalledWith(
        604,
        expect.stringContaining('quá thời gian chờ'),
      );
      expect(service.hasTimeout(key)).toBe(false);
      expect(service.getActiveTimeoutCount()).toBe(0);
    });

    it('should cancel timer cleanly when clearTimeout is invoked', () => {
      jest.useFakeTimers();

      const onTimeoutMock = jest.fn();
      const key = 'user-2:doc-2:sess-2:search_papers';

      service.registerTimeout(key, 'search_papers', onTimeoutMock, 10000);
      expect(service.hasTimeout(key)).toBe(true);

      service.clearTimeout(key);
      expect(service.hasTimeout(key)).toBe(false);

      jest.advanceTimersByTime(12000);
      expect(onTimeoutMock).not.toHaveBeenCalled();
    });

    it('should update Job record with error 604 when jobId is provided', async () => {
      jest.useFakeTimers();
      prismaMock.job.update.mockResolvedValue({ id: 'job-999' });

      const onTimeoutMock = jest.fn();
      const key = 'user-3:doc-3:sess-3:gen_slides';

      service.registerTimeout(
        key,
        'gen_slides',
        onTimeoutMock,
        1000,
        'job-999',
      );

      jest.advanceTimersByTime(1500);

      // Allow pending microtasks/promises to resolve
      await Promise.resolve();

      expect(prismaMock.job.update).toHaveBeenCalledWith({
        where: { id: 'job-999' },
        data: {
          status: JobStatus.error,
          errorCode: '604',
          errorMessage: expect.stringContaining('quá thời gian chờ'),
        },
      });
    });

    it('should handle timeout gracefully when prisma is not provided', async () => {
      jest.useFakeTimers();
      const standalone = new TimeoutManagerService(undefined);
      const onTimeoutMock = jest.fn();

      standalone.registerTimeout(
        'key-standalone',
        'outer_chatbot',
        onTimeoutMock,
        500,
        'job-no-prisma',
      );

      jest.advanceTimersByTime(600);
      await Promise.resolve();

      expect(onTimeoutMock).toHaveBeenCalledWith(604, expect.any(String));
    });
  });
});
