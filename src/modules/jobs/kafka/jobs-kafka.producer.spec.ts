import { jest } from '@jest/globals';
import { Test, TestingModule } from '@nestjs/testing';
import { ConfigService } from '@nestjs/config';
import { Job } from '@prisma/client';
import { JobsKafkaProducer } from './jobs-kafka.producer';
import { KAFKA_CLIENT } from '../../../common/messaging.tokens';

describe('JobsKafkaProducer', () => {
  let producer: JobsKafkaProducer;
  let clientMock: { connect: jest.Mock<any>; emit: jest.Mock<any> };
  let configMock: { get: jest.Mock<any> };

  const baseJob = (overrides: Partial<Job> = {}) =>
    ({
      id: 'job-1',
      module: 'document_setup',
      action: 'generate_domains',
      documentId: 'doc-1',
      sessionId: 'sess-1',
      resumeStep: null,
      payload: {},
      ...overrides,
    }) as unknown as Job;

  beforeEach(async () => {
    clientMock = {
      connect: jest.fn(() => Promise.resolve()),
      emit: jest.fn(() => Promise.resolve()),
    };
    configMock = {
      get: jest.fn((key: string) => {
        if (key === 'kafka.topics') {
          return {
            document_setup: { request: 'document_setup_request' },
            write_report: { request: 'write_section_request' },
          };
        }
        if (key === 'app.isLocal') return true;
        if (key === 'app.localLlmBaseUrl') return 'http://localhost:8000/v1';
        return undefined;
      }),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        JobsKafkaProducer,
        { provide: KAFKA_CLIENT, useValue: clientMock },
        { provide: ConfigService, useValue: configMock },
      ],
    }).compile();

    producer = module.get<JobsKafkaProducer>(JobsKafkaProducer);
  });

  afterEach(() => jest.clearAllMocks());

  it('onModuleInit: connect thành công', async () => {
    await expect(producer.onModuleInit()).resolves.toBeUndefined();
    expect(clientMock.connect).toHaveBeenCalledTimes(1);
  });

  it('onModuleInit: không throw khi Kafka không sẵn sàng', async () => {
    clientMock.connect.mockRejectedValueOnce(new Error('kafka down'));

    await expect(producer.onModuleInit()).resolves.toBeUndefined();
  });

  it('publishJob: bỏ qua khi module không có request topic', async () => {
    await producer.publishJob(baseJob({ module: 'unknown_module' }));

    expect(clientMock.emit).not.toHaveBeenCalled();
  });

  it('publishJob: emit envelope có default + localllm_base_url khi isLocal', async () => {
    await producer.publishJob(baseJob());

    expect(clientMock.emit).toHaveBeenCalledTimes(1);
    const [topic, raw] = clientMock.emit.mock.calls[0] as [string, string];
    expect(topic).toBe('document_setup_request');
    const payload = JSON.parse(raw);
    expect(payload).toMatchObject({
      job_id: 'job-1',
      module: 'document_setup',
      action: 'generate_domains',
      event_type: 'generate_domains',
      document_id: 'doc-1',
      session_id: 'sess-1',
      domains_num: 3,
      subdomains_num: 3,
      titles_num: 3,
      keywords_num: 5,
      is_tool: false,
      localllm_base_url: 'http://localhost:8000/v1',
    });
    expect(payload.payload).toEqual({});
  });

  it('publishJob: payload của job được ưu tiên (event_type/document_id/language)', async () => {
    await producer.publishJob(
      baseJob({
        payload: {
          event_type: 'generate_titles',
          document_id: 'doc-from-payload',
          language: 'Vietnamese',
          is_tool: true,
          domainsNum: 7,
        } as any,
      }),
    );

    const payload = JSON.parse(clientMock.emit.mock.calls[0][1] as string);
    expect(payload.event_type).toBe('generate_titles');
    expect(payload.document_id).toBe('doc-from-payload');
    expect(payload.is_tool).toBe(true);
    expect(payload.domains_num).toBe(7);
  });

  it('publishJob: không gửi localllm_base_url khi không phải local', async () => {
    configMock.get.mockImplementation((key: string) => {
      if (key === 'kafka.topics') {
        return { document_setup: { request: 'document_setup_request' } };
      }
      if (key === 'app.isLocal') return false;
      return undefined;
    });

    await producer.publishJob(baseJob());

    const payload = JSON.parse(clientMock.emit.mock.calls[0][1] as string);
    expect(payload.localllm_base_url).toBeUndefined();
  });

  it('publishJob: emit lỗi được nuốt (không throw)', async () => {
    clientMock.emit.mockImplementationOnce(() => {
      throw new Error('emit failed');
    });

    await expect(producer.publishJob(baseJob())).resolves.toBeUndefined();
  });
});
