import { jest } from '@jest/globals';
import { BadRequestException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { of, throwError } from 'rxjs';
import { KAFKA_CLIENT } from '../common/messaging.tokens';
import { KafkaProducerService } from './kafka-producer.service';
import { KAFKA_ERROR_CODES, AI_DEFAULT_MODEL } from '../common/kafka/kafka.constants';

describe('KafkaProducerService', () => {
  let service: KafkaProducerService;
  let mockKafkaClient: Record<string, any>;
  let mockConfigService: Record<string, any>;

  const sampleValidPayload = {
    event_type: 'search_papers',
    model_id: AI_DEFAULT_MODEL,
    user_id: 'user-uuid-1234',
    document_id: 'doc-uuid-5678',
    session_id: 'sess-uuid-9999',
    language: 'vi',
  };

  beforeEach(async () => {
    mockKafkaClient = {
      connect: jest.fn().mockImplementation(async () => {}),
      close: jest.fn().mockImplementation(async () => {}),
      emit: jest.fn().mockReturnValue(of({ offset: '0' })),
    };

    mockConfigService = {
      get: jest.fn<any>((key: string) => {
        if (key === 'kafka.brokers') return ['167.254.70.125:9092'];
        if (key === 'kafka.isLocal') return false;
        if (key === 'kafka.topics') {
          return {
            document_setup: {
              request: 'document_setup_request',
              response: 'document_setup_response',
            },
            chatbot: {
              request: 'chatbot_request',
              response: 'chatbot_response',
            },
          };
        }
        return undefined;
      }),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        KafkaProducerService,
        { provide: KAFKA_CLIENT, useValue: mockKafkaClient },
        { provide: ConfigService, useValue: mockConfigService },
      ],
    }).compile();

    service = module.get<KafkaProducerService>(KafkaProducerService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('Lifecycle', () => {
    it('should connect to Kafka and log topics on init', async () => {
      await expect(service.onModuleInit()).resolves.toBeUndefined();
      expect(mockKafkaClient.connect).toHaveBeenCalledTimes(1);
    });

    it('should catch connection failure on init without throwing', async () => {
      mockKafkaClient.connect.mockRejectedValueOnce(
        new Error('Broker unreachable'),
      );
      await expect(service.onModuleInit()).resolves.toBeUndefined();
    });

    it('should close Kafka client on destroy', async () => {
      await expect(service.onModuleDestroy()).resolves.toBeUndefined();
      expect(mockKafkaClient.close).toHaveBeenCalledTimes(1);
    });

    it('should catch close error without throwing', async () => {
      mockKafkaClient.close.mockRejectedValueOnce(new Error('Close failure'));
      await expect(service.onModuleDestroy()).resolves.toBeUndefined();
    });

    it('should handle logConfiguredTopics when topics config is undefined', () => {
      mockConfigService.get.mockReturnValue(undefined);
      expect(() => service.logConfiguredTopics()).not.toThrow();
    });
  });

  describe('Validation & Publish', () => {
    it('should publish message successfully when payload is valid with document_id', async () => {
      await expect(
        service.publish('document_setup_request', sampleValidPayload),
      ).resolves.toBeUndefined();

      expect(mockKafkaClient.emit).toHaveBeenCalledWith(
        'document_setup_request',
        sampleValidPayload,
      );
    });

    it('should publish message successfully when payload has conv_id (chatbot)', async () => {
      const chatbotPayload = {
        event_type: 'outer_chatbot',
        model_id: AI_DEFAULT_MODEL,
        user_id: 'user-uuid-1234',
        conv_id: 'conv-uuid-5678',
        session_id: 'sess-uuid-9999',
        language: 'vi',
      };

      await expect(
        service.publish('chatbot_request', chatbotPayload),
      ).resolves.toBeUndefined();

      expect(mockKafkaClient.emit).toHaveBeenCalledWith(
        'chatbot_request',
        chatbotPayload,
      );
    });

    it('should publish message successfully when both document_id and conv_id exist', async () => {
      const innerPayload = {
        ...sampleValidPayload,
        conv_id: 'conv-1234',
      };

      await expect(
        service.publish('chatbot_request', innerPayload),
      ).resolves.toBeUndefined();
      expect(mockKafkaClient.emit).toHaveBeenCalledTimes(1);
    });

    it('should throw 603 INVALID_PARAMS if payload is null or not object', async () => {
      await expect(
        service.publish('test_topic', null as unknown as object),
      ).rejects.toThrow(BadRequestException);

      try {
        await service.publish('test_topic', null as unknown as object);
      } catch (err: unknown) {
        const errorResponse = (
          err as BadRequestException
        ).getResponse() as Record<string, unknown>;
        expect(errorResponse.error_code).toBe(KAFKA_ERROR_CODES.INVALID_PARAMS);
      }
    });

    it('should throw 603 INVALID_PARAMS when missing required field (user_id)', async () => {
      const invalidPayload = { ...sampleValidPayload, user_id: '' };

      await expect(
        service.publish('document_setup_request', invalidPayload),
      ).rejects.toThrow(BadRequestException);

      try {
        await service.publish('document_setup_request', invalidPayload);
      } catch (err: unknown) {
        const errorResponse = (
          err as BadRequestException
        ).getResponse() as Record<string, unknown>;
        expect(errorResponse.error_code).toBe(KAFKA_ERROR_CODES.INVALID_PARAMS);
        expect(errorResponse.event_type).toBe('search_papers');
      }

      expect(mockKafkaClient.emit).not.toHaveBeenCalled();
    });

    it('should throw 603 INVALID_PARAMS when missing both document_id and conv_id', async () => {
      const invalidPayload = {
        event_type: 'search_papers',
        model_id: 'localhost',
        user_id: 'user-123',
        session_id: 'sess-123',
        language: 'vi',
      };

      await expect(
        service.publish('document_setup_request', invalidPayload),
      ).rejects.toThrow(BadRequestException);
      expect(mockKafkaClient.emit).not.toHaveBeenCalled();
    });

    it('should throw 603 INVALID_PARAMS when model_id is not supported', async () => {
      const invalidPayload = {
        ...sampleValidPayload,
        model_id: 'unsupported-model-xyz',
      };

      await expect(
        service.publish('document_setup_request', invalidPayload),
      ).rejects.toThrow(BadRequestException);
    });

    it('should rethrow error when emit fails', async () => {
      jest.spyOn(service['logger'], 'error').mockImplementation(() => {});
      mockKafkaClient.emit.mockReturnValueOnce(
        throwError(() => new Error('Kafka broker connection error')),
      );

      await expect(
        service.publish('document_setup_request', sampleValidPayload),
      ).rejects.toThrow('Kafka broker connection error');
    });
  });
});
