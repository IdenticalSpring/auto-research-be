import { jest } from '@jest/globals';
import { NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { SmokeTestController } from './smoke-test.controller';
import { KafkaProducerService } from './kafka-producer.service';
import { RequestCorrelationService } from './request-correlation.service';

describe('SmokeTestController', () => {
  let controller: SmokeTestController;
  let mockProducer: {
    publish: jest.MockedFunction<any>;
  };
  let mockCorrelation: {
    buildCorrelationKey: jest.MockedFunction<any>;
    registerPending: jest.MockedFunction<any>;
  };
  let mockConfig: {
    get: jest.MockedFunction<any>;
  };

  beforeEach(async () => {
    mockProducer = {
      publish: jest.fn().mockImplementation(async () => undefined),
    };
    mockCorrelation = {
      buildCorrelationKey: jest
        .fn<any>()
        .mockReturnValue('u-1:doc-2:test-sess:search_papers'),
      registerPending: jest.fn<any>(),
    };
    mockConfig = {
      get: jest
        .fn<any>()
        .mockImplementation((key: string, defaultVal?: any) => {
          if (key === 'kafka.isLocal') return false;
          if (key === 'kafka.topics.document_setup.request') {
            return 'document_setup_request';
          }
          return defaultVal;
        }),
    };

    const module: TestingModule = await Test.createTestingModule({
      controllers: [SmokeTestController],
      providers: [
        { provide: KafkaProducerService, useValue: mockProducer },
        { provide: RequestCorrelationService, useValue: mockCorrelation },
        { provide: ConfigService, useValue: mockConfig },
      ],
    }).compile();

    controller = module.get<SmokeTestController>(SmokeTestController);
  });

  it('should throw NotFoundException if not in local/development environment', async () => {
    mockConfig.get.mockImplementation((key: string) => {
      if (key === 'kafka.isLocal') return false;
      return undefined;
    });
    const originalEnv = process.env.NODE_ENV;
    process.env.NODE_ENV = 'production';

    try {
      await expect(controller.runKafkaSmokeTest()).rejects.toThrow(
        NotFoundException,
      );
    } finally {
      process.env.NODE_ENV = originalEnv;
    }
  });

  it('should trigger smoke test and return response on success', async () => {
    const mockResponse = {
      event_type: 'search_papers',
      papers: [{ title: 'Paper 1', authors: ['Author A'] }],
      input_tokens: 100,
      output_tokens: 50,
    };
    mockCorrelation.registerPending.mockResolvedValue(mockResponse);

    const result = await controller.runKafkaSmokeTest();

    expect(mockCorrelation.buildCorrelationKey).toHaveBeenCalled();
    expect(mockCorrelation.registerPending).toHaveBeenCalledWith(
      'u-1:doc-2:test-sess:search_papers',
      30000,
      'search_papers',
    );
    expect(mockProducer.publish).toHaveBeenCalledWith(
      'document_setup_request',
      expect.objectContaining({
        event_type: 'search_papers',
        query: 'Artificial Intelligence in Education',
      }),
    );
    expect(result).toEqual({
      success: true,
      correlationKey: 'u-1:doc-2:test-sess:search_papers',
      request: expect.objectContaining({
        event_type: 'search_papers',
      }),
      response: mockResponse,
    });
  });

  it('should accept custom timeout from query param', async () => {
    mockCorrelation.registerPending.mockResolvedValue({ status: 'ok' });

    await controller.runKafkaSmokeTest('15000');

    expect(mockCorrelation.registerPending).toHaveBeenCalledWith(
      'u-1:doc-2:test-sess:search_papers',
      15000,
      'search_papers',
    );
  });

  it('should return success false if response promise rejects', async () => {
    mockCorrelation.registerPending.mockRejectedValue({
      error_code: 604,
      error: 'Timeout',
    });

    const result = await controller.runKafkaSmokeTest();

    expect(result).toEqual({
      success: false,
      correlationKey: 'u-1:doc-2:test-sess:search_papers',
      request: expect.objectContaining({
        event_type: 'search_papers',
      }),
      response: {
        error_code: 604,
        error: 'Timeout',
      },
    });
  });
});
