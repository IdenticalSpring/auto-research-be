/**
 * Regression cho contract AI server (13/11): mọi response echo
 * event_type + session_id/document_id/conv_id.
 * Consumer phải build đúng correlation key như khi đăng ký pending.
 */

import { jest } from '@jest/globals';
import { KafkaConsumerController } from './kafka-consumer.controller';

function buildController() {
  const correlationService = {
    // mirror RequestCorrelationService.buildCorrelationKey
    buildCorrelationKey: jest.fn(
      (params: {
        userId: string;
        documentId?: string;
        convId?: string;
        sessionId?: string;
        eventType: string;
      }) =>
        `${params.userId}:${params.convId || params.documentId || 'none'}:${
          params.sessionId || 'default'
        }:${params.eventType}`,
    ),
    dispatchResponse: jest.fn(),
  };
  const controller = new KafkaConsumerController(correlationService as any);
  return { controller, correlationService };
}

describe('KafkaConsumerController — echo contract', () => {
  it('chatbot: echo conv_id + session_id ⇒ key dùng conv_id', async () => {
    const { controller, correlationService } = buildController();

    await controller.processMessage('chatbot', {
      event_type: 'outer_chatbot',
      user_id: 'u-1',
      conv_id: 'conv-1',
      session_id: 'conv-1',
      model_id: 'deepseek-flash',
      has_finished: true,
      response: 'Xin chào',
    });

    expect(correlationService.buildCorrelationKey).toHaveBeenCalledWith({
      userId: 'u-1',
      documentId: undefined,
      convId: 'conv-1',
      sessionId: 'conv-1',
      eventType: 'outer_chatbot',
    });
    expect(correlationService.dispatchResponse).toHaveBeenCalledWith(
      'u-1:conv-1:conv-1:outer_chatbot',
      expect.objectContaining({ event_type: 'outer_chatbot' }),
    );
  });

  it('ai_in_doc: echo document_id + session_id ⇒ key dùng document_id', async () => {
    const { controller, correlationService } = buildController();

    await controller.processMessage('enhancement', {
      event_type: 'ai_in_doc',
      user_id: 'u-2',
      document_id: 'doc-1',
      session_id: 'sess-1',
      has_finished: true,
      response: 'Đoạn văn đã sửa',
    });

    expect(correlationService.dispatchResponse).toHaveBeenCalledWith(
      'u-2:doc-1:sess-1:ai_in_doc',
      expect.objectContaining({ event_type: 'ai_in_doc' }),
    );
  });

  it('echo đủ 3 id (document_id + conv_id + session_id) vẫn resolve', async () => {
    const { controller, correlationService } = buildController();

    await controller.processMessage('write_report', {
      event_type: 'chuyen_de_2',
      user_id: 'u-3',
      document_id: 'doc-9',
      conv_id: 'conv-9',
      session_id: 'sess-9',
      has_finished: true,
    });

    expect(correlationService.dispatchResponse).toHaveBeenCalledWith(
      'u-3:conv-9:sess-9:chuyen_de_2',
      expect.objectContaining({ document_id: 'doc-9' }),
    );
  });

  it('payload là string JSON vẫn parse được (kafkajs có thể trả string)', async () => {
    const { controller, correlationService } = buildController();

    await controller.processMessage(
      'document_setup',
      JSON.stringify({
        event_type: 'search_papers',
        user_id: 'u-4',
        document_id: 'doc-4',
        session_id: 'sess-4',
        papers: [],
        has_finished: true,
      }),
    );

    expect(correlationService.dispatchResponse).toHaveBeenCalledWith(
      'u-4:doc-4:sess-4:search_papers',
      expect.objectContaining({ event_type: 'search_papers' }),
    );
  });

  it('payload không phải object ⇒ không dispatch, không throw', async () => {
    const { controller, correlationService } = buildController();

    await expect(
      controller.processMessage('chatbot', 'not-json'),
    ).resolves.toBeUndefined();
    expect(correlationService.dispatchResponse).not.toHaveBeenCalled();
  });

  it('echo data_id + field lạ ⇒ vẫn dispatch bình thường (không whitelist)', async () => {
    const { controller, correlationService } = buildController();

    await controller.processMessage(
      'document_setup',
      {
        event_type: 'user_documents',
        user_id: 'u-5',
        document_id: 'doc-5',
        session_id: 'sess-5',
        data_id: 'data-5',
        // AI được phép thêm identifier mới trong tương lai (§1.10)
        trace_id: 'trace-x',
        request_id: 'req-x',
        processed_docs: [],
      },
    );

    expect(correlationService.dispatchResponse).toHaveBeenCalledWith(
      'u-5:doc-5:sess-5:user_documents',
      expect.objectContaining({
        data_id: 'data-5',
        trace_id: 'trace-x',
        request_id: 'req-x',
      }),
    );
  });

  it('response không có section_id ⇒ vẫn correlate (AI không echo field này)', async () => {
    const { controller, correlationService } = buildController();

    await controller.processMessage('enhancement', {
      event_type: 'get_suggestions',
      user_id: 'u-6',
      document_id: 'doc-6',
      session_id: 'sess-6',
      suggested_paragraphs: ['a'],
    });

    expect(correlationService.dispatchResponse).toHaveBeenCalledWith(
      'u-6:doc-6:sess-6:get_suggestions',
      expect.not.objectContaining({ section_id: expect.anything() }),
    );
  });

  it('nhánh lỗi có data_id + event_type ⇒ vẫn dispatch đúng key', async () => {
    const { controller, correlationService } = buildController();

    await controller.processMessage('document_setup', {
      event_type: 'user_documents',
      user_id: 'u-7',
      document_id: 'doc-7',
      session_id: 'sess-7',
      data_id: 'data-7',
      error_code: 603,
      error: "Missing field 'documents'",
    });

    expect(correlationService.dispatchResponse).toHaveBeenCalledWith(
      'u-7:doc-7:sess-7:user_documents',
      expect.objectContaining({ error_code: 603, data_id: 'data-7' }),
    );
  });
});
