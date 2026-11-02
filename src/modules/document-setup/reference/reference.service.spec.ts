import { jest } from '@jest/globals';
import { NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { ReferenceService } from './reference.service';
import { CreateReferenceDto } from './dto/create-reference.dto';
import { UpdateReferenceDto } from './dto/update-reference.dto';

describe('ReferenceService', () => {
  let service: ReferenceService;
  let mockPrisma: Record<string, any>;
  let mockKafkaProducer: Record<string, any>;
  let mockCorrelationService: Record<string, any>;
  let mockConfigService: Record<string, any>;

  const userId = '00000000-0000-0000-0000-000000000001';
  const documentId = '00000000-0000-0000-0000-000000000002';

  beforeEach(async () => {
    mockPrisma = {
      reference: {
        create: jest.fn<any>(),
        findMany: jest.fn<any>(),
        findUnique: jest.fn<any>(),
        update: jest.fn<any>(),
        delete: jest.fn<any>(),
      },
      document: {
        findFirst: jest.fn<any>(),
      },
      job: {
        create: jest.fn<any>(),
        update: jest.fn<any>(),
      },
    };

    mockKafkaProducer = {
      publish: jest.fn<any>().mockResolvedValue(undefined),
    };

    mockCorrelationService = {
      buildCorrelationKey: jest
        .fn<any>()
        .mockReturnValue(`${userId}:${documentId}:sess-123:search_papers`),
      registerPending: jest.fn<any>(),
      clearPending: jest.fn<any>(),
    };

    mockConfigService = {
      get: jest.fn<any>((key: string) => {
        if (key === 'kafka.topics.document_setup.request') {
          return 'document_setup_request';
        }
        return undefined;
      }),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ReferenceService,
        { provide: PrismaService, useValue: mockPrisma },
        { provide: KafkaProducerService, useValue: mockKafkaProducer },
        {
          provide: RequestCorrelationService,
          useValue: mockCorrelationService,
        },
        { provide: ConfigService, useValue: mockConfigService },
      ],
    }).compile();

    service = module.get<ReferenceService>(ReferenceService);
  });

  describe('CRUD Operations', () => {
    it('should create a reference successfully', async () => {
      const dto: CreateReferenceDto = { title: 'Test Paper', year: 2023 };
      const created = { id: 'ref-1', ...dto };
      mockPrisma.reference.create.mockResolvedValue(created);

      const result = await service.create(dto);
      expect(result).toEqual(created);
      expect(mockPrisma.reference.create).toHaveBeenCalledWith({ data: dto });
    });

    it('should find all references', async () => {
      const list = [{ id: 'ref-1', title: 'Paper 1' }];
      mockPrisma.reference.findMany.mockResolvedValue(list);

      const result = await service.findAll();
      expect(result).toEqual(list);
    });

    it('should find one reference by id', async () => {
      const ref = { id: 'ref-1', title: 'Paper 1' };
      mockPrisma.reference.findUnique.mockResolvedValue(ref);

      const result = await service.findOne('ref-1');
      expect(result).toEqual(ref);
    });

    it('should throw NotFoundException if reference not found', async () => {
      mockPrisma.reference.findUnique.mockResolvedValue(null);
      await expect(service.findOne('invalid-id')).rejects.toThrow(
        NotFoundException,
      );
    });

    it('should update reference successfully', async () => {
      const ref = { id: 'ref-1', title: 'Old Title' };
      const dto: UpdateReferenceDto = { title: 'New Title' };
      mockPrisma.reference.findUnique.mockResolvedValue(ref);
      mockPrisma.reference.update.mockResolvedValue({ id: 'ref-1', ...dto });

      const result = await service.update('ref-1', dto);
      expect(result.title).toBe('New Title');
    });

    it('should remove reference successfully', async () => {
      const ref = { id: 'ref-1', title: 'Paper 1' };
      mockPrisma.reference.findUnique.mockResolvedValue(ref);
      mockPrisma.reference.delete.mockResolvedValue(ref);

      const result = await service.remove('ref-1');
      expect(result).toEqual({ message: 'Reference deleted successfully' });
    });
  });
});
