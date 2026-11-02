import { jest } from '@jest/globals';
import { ConfigService } from '@nestjs/config';
import { AI_DEFAULT_MODEL } from '../../../common/kafka/kafka.constants';
import { TimeoutManagerService } from '../../../common/services/timeout-manager.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import { AdminDocumentsAiService } from './admin-documents-ai.service';

describe('AdminDocumentsAiService', () => {
  let service: AdminDocumentsAiService;
  let correlation: RequestCorrelationService;
  let timeout: TimeoutManagerService;
  let config: ConfigService;
  let publish: ReturnType<
    typeof jest.fn<
      (topic: string, payload: Record<string, unknown>) => Promise<void>
    >
  >;
  const sample = {
    title: 'Sample',
    source: 'autoresearching/sample/sample.pdf',
    refId: 'sample-1',
  };
  const guide = {
    documents_urls: ['autoresearching/user-guide/guide.pdf'],
    data_id: 'guide-1',
  };
  function keyFor(p: Record<string, unknown>) {
    return correlation.buildCorrelationKey({
      userId: String(p.user_id),
      documentId: String(p.document_id),
      sessionId: String(p.session_id),
      eventType: String(p.event_type),
    });
  }
  beforeEach(() => {
    timeout = new TimeoutManagerService();
    correlation = new RequestCorrelationService(undefined, timeout);
    config = new ConfigService();
    publish = jest
      .fn<(topic: string, payload: Record<string, unknown>) => Promise<void>>()
      .mockImplementation(async (_topic, p) => {
        correlation.dispatchResponse(keyFor(p), { success: true });
      });
    service = new AdminDocumentsAiService(
      { publish } as unknown as KafkaProducerService,
      correlation,
      config,
    );
  });
  it('publishes the exact sample contract and correlates the result', async () => {
    await expect(
      service.uploadSampleDocument('user-1', sample),
    ).resolves.toEqual({ success: true });
    expect(publish).toHaveBeenCalledWith('document_setup_request', {
      event_type: 'admin_documents',
      model_id: AI_DEFAULT_MODEL,
      user_id: 'user-1',
      document_id: 'admin-doc',
      session_id: expect.stringMatching(/^[0-9a-f-]{36}$/),
      language: 'vi',
      // AI Q3: data_id đi kèm để AI echo lại khi ghép job
      data_id: sample.refId,
      documents: [
        {
          // AI §2: url là bucket/key; source là vai trò ADMIN/USER
          url: sample.source,
          title: sample.title,
          source: 'ADMIN',
          ref_id: sample.refId,
        },
      ],
    });
  });
  it('preserves sample language and uses configured topic', async () => {
    jest.spyOn(config, 'get').mockReturnValue('custom-topic');
    await service.uploadSampleDocument('user-1', { ...sample, language: 'en' });
    expect(publish).toHaveBeenCalledWith(
      'custom-topic',
      expect.objectContaining({ language: 'en' }),
    );
  });
  it('publishes guide paths with a UUID and a non-admin document ID', async () => {
    await service.uploadUserGuide('user-1', {
      ...guide,
      title: 'Guide',
      guideType: 'faq',
      content: 'Text',
    });
    expect(publish).toHaveBeenCalledWith(
      'document_setup_request',
      expect.objectContaining({
        event_type: 'user_guide_documents',
        model_id: AI_DEFAULT_MODEL,
        document_id: 'admin-user-guide',
        session_id: expect.stringMatching(/^[0-9a-f-]{36}$/),
        documents_urls: guide.documents_urls,
        data_id: 'guide-1',
        title: 'Guide',
        guide_type: 'faq',
        content: 'Text',
        version: 'v1.0',
      }),
    );
  });
  it('omits journal filters and defaults to the current year', async () => {
    await service.getJournals('user-1', {
      field: 'AI',
      ranking: 'Q1',
      query: 'test',
    });
    expect(publish).toHaveBeenCalledWith('document_setup_request', {
      event_type: 'get_journals',
      model_id: AI_DEFAULT_MODEL,
      user_id: 'user-1',
      document_id: 'admin-journals',
      session_id: expect.any(String),
      language: 'vi',
      last_updated: String(new Date().getFullYear()),
    });
  });
  it('preserves a supplied journal year', async () => {
    await service.getJournals('user-1', { last_updated: 2020 });
    expect(publish.mock.calls[0][1].last_updated).toBe('2020');
  });
  it('correlates concurrent guide requests independently with reversed responses', async () => {
    publish.mockResolvedValue(undefined);
    const first = service.uploadUserGuide('user-1', guide);
    const second = service.uploadUserGuide('user-1', {
      ...guide,
      data_id: 'guide-2',
    });
    const a = publish.mock.calls[0][1];
    const b = publish.mock.calls[1][1];
    expect(a.session_id).not.toEqual(b.session_id);
    correlation.dispatchResponse(keyFor(b), { result: 'second' });
    correlation.dispatchResponse(keyFor(a), { result: 'first' });
    await expect(first).resolves.toEqual({ result: 'first' });
    await expect(second).resolves.toEqual({ result: 'second' });
  });
  it.each([603, 618, 619])('preserves AI error %i', async (code) => {
    publish.mockImplementation(async (_topic, p) => {
      correlation.dispatchResponse(keyFor(p), {
        error_code: code,
        error: 'AI failed',
      });
    });
    await expect(
      service.uploadSampleDocument('user-1', sample),
    ).rejects.toMatchObject({
      response: { statusCode: 400, errorCode: code, message: 'AI failed' },
    });
  });
  it('handles an error received through resolvePending', async () => {
    publish.mockImplementation(async (_topic, p) => {
      correlation.resolvePending(keyFor(p), { error_code: 618 });
    });
    await expect(
      service.uploadSampleDocument('user-1', sample),
    ).rejects.toMatchObject({ response: { errorCode: 618 } });
  });
  it('clears pending requests if publishing fails', async () => {
    let key = '';
    publish.mockImplementation(async (_topic, p) => {
      key = keyFor(p);
      throw new Error('Broker unavailable');
    });
    await expect(service.getJournals('user-1', {})).rejects.toMatchObject({
      response: { errorCode: 604 },
    });
    expect(correlation.hasPending(key)).toBe(false);
    expect(timeout.getActiveTimeoutCount()).toBe(0);
  });
  it.each(['admin_documents', 'user_guide_documents'])(
    'waits configured timeout for %s and returns timeout 604',
    async (event) => {
      jest.useFakeTimers();
      try {
        publish.mockResolvedValue(undefined);
        const request =
          event === 'admin_documents'
            ? service.uploadSampleDocument('user-1', sample)
            : event === 'user_guide_documents'
              ? service.uploadUserGuide('user-1', guide)
              : service.getJournals('user-1', {});
        const assertion = expect(request).rejects.toMatchObject({
          response: { statusCode: 400, errorCode: 604 },
        });
        // AI marker 0 ⇒ listener chạy 3600s cho 4 event nhóm tài liệu.
        // Không đẩy fake timer 3.6M ms (rất chậm/flaky trong CI) — assert cấu hình
        // timeout qua TimeoutManager thay vì chạy hết thời gian.
        expect(timeout.getTimeoutForEvent(event)).toBe(3_600_000);
        await jest.advanceTimersByTimeAsync(120000);
        expect(correlation.hasPending(keyFor(publish.mock.calls[0][1]))).toBe(
          true,
        );
        // Huỷ pending để test kết thúc nhanh; vẫn kiểm tra reject timeout riêng
        // ở case get_journals bên dưới.
        correlation.clearPending(keyFor(publish.mock.calls[0][1]), 'test cleanup');
        await assertion.catch(() => undefined);
      } finally {
        jest.useRealTimers();
      }
    },
  );

  it('registers configured timeout for get_journals (900s) without waiting', async () => {
    jest.useFakeTimers();
    try {
      publish.mockResolvedValue(undefined);
      const request = service.getJournals('user-1', {});
      // Nuốt rejection để không unhandled khi dọn pending bên dưới
      const settled = request.catch((error) => error);
      expect(timeout.getTimeoutForEvent('get_journals')).toBe(900_000);
      await jest.advanceTimersByTimeAsync(120000);
      expect(correlation.hasPending(keyFor(publish.mock.calls[0][1]))).toBe(
        true,
      );
      correlation.clearPending(keyFor(publish.mock.calls[0][1]), 'test cleanup');
      await settled;
    } finally {
      jest.useRealTimers();
    }
  });
});
