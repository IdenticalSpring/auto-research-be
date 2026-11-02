import { buildResponseKey, normalizeAiNotification } from './ai-response';

describe('buildResponseKey', () => {
  it('D-t11: ghép document_id + event_type', () => {
    expect(buildResponseKey('d1', 'search_papers')).toBe('d1:search_papers');
  });
});

describe('normalizeAiNotification', () => {
  it('D-t1: has_finished=false ⇒ processing (không phải PASS)', () => {
    const result = normalizeAiNotification({
      document_id: 'd1',
      event_type: 'search_papers',
      has_finished: false,
    });

    expect(result.kind).toBe('processing');
    expect(result.documentId).toBe('d1');
    expect(result.eventType).toBe('search_papers');
  });

  it('D-t1b: interrupt (has_finished=false + ai_message) ⇒ suspended', () => {
    // Đúng shape AI trả cho propose_method khi ngắt để chờ user review
    // (không có process_status): trước đây bị coi là chunk ⇒ idle timeout oan.
    const result = normalizeAiNotification({
      document_id: 'd1',
      event_type: 'propose_method',
      conv_id: 'sess-1',
      has_finished: false,
      ai_message: 'Vui lòng xem lại danh sách tài liệu tham khảo',
      final_proposal: { title: 'x' },
    });

    expect(result.kind).toBe('suspended');
    expect(result.errorMessage).toContain('danh sách tài liệu');
  });

  it('D-t1c: ai_message nhưng has_finished=true ⇒ vẫn finished', () => {
    const result = normalizeAiNotification({
      document_id: 'd1',
      event_type: 'propose_method',
      has_finished: true,
      ai_message: 'xong',
    });

    expect(result.kind).toBe('finished');
  });

  it('D-t2: has_finished=true ⇒ finished', () => {
    const result = normalizeAiNotification({
      document_id: 'd1',
      event_type: 'search_papers',
      has_finished: true,
    });

    expect(result.kind).toBe('finished');
  });

  it('D-t3: error_code + error ⇒ error kèm mã lỗi', () => {
    const result = normalizeAiNotification({
      error_code: 633,
      error: "cannot access local variable 'current_report'",
    });

    expect(result.kind).toBe('error');
    expect(result.errorCode).toBe(633);
    expect(result.errorMessage).toContain('current_report');
  });

  it('D-t4: ai_error lồng nhau ⇒ error kèm mã lỗi', () => {
    const result = normalizeAiNotification({
      ai_error: { errorCode: 604, error: 'timeout' },
    });

    expect(result.kind).toBe('error');
    expect(result.errorCode).toBe(604);
    expect(result.errorMessage).toBe('timeout');
  });

  it('D-t5: process_status=5 ⇒ suspended', () => {
    expect(normalizeAiNotification({ process_status: 5 }).kind).toBe(
      'suspended',
    );
  });

  it('D-t6: process_status=4 + ai_error ⇒ error, ưu tiên mã trong ai_error', () => {
    const result = normalizeAiNotification({
      process_status: 4,
      ai_error: { errorCode: 601, error: 'INVALID_KEY' },
    });

    expect(result.kind).toBe('error');
    expect(result.processStatus).toBe(4);
    expect(result.errorCode).toBe(601);
    expect(result.errorMessage).toBe('INVALID_KEY');
  });

  it('D-t7: process_status=3 ⇒ finished', () => {
    expect(normalizeAiNotification({ process_status: 3 }).kind).toBe('finished');
  });

  it('D-t8: done_chat=false ⇒ suspended kèm ai_message', () => {
    const result = normalizeAiNotification({
      done_chat: false,
      ai_message: 'Chọn thang đo nào?',
    });

    expect(result.kind).toBe('suspended');
    expect(result.errorMessage).toBe('Chọn thang đo nào?');
  });

  it('D-t9: trích document_id/event_type từ tầng data', () => {
    const result = normalizeAiNotification({
      data: {
        document_id: 'd9',
        event_type: 'chuyen_de_2',
        has_finished: true,
      },
    });

    expect(result.documentId).toBe('d9');
    expect(result.eventType).toBe('chuyen_de_2');
    expect(result.kind).toBe('finished');
  });

  it('D-t13: chatbot chỉ echo conv_id ⇒ vẫn lấy được sessionId', () => {
    const result = normalizeAiNotification({
      event_type: 'outer_chatbot',
      conv_id: 'conv-13',
      has_finished: true,
      response: 'Xin chào',
    });

    expect(result.sessionId).toBe('conv-13');
    expect(result.documentId).toBeUndefined();
    expect(result.eventType).toBe('outer_chatbot');
    expect(result.kind).toBe('finished');
  });

  it('D-t14: ưu tiên session_id hơn conv_id khi có cả hai', () => {
    const result = normalizeAiNotification({
      event_type: 'outer_chatbot',
      conv_id: 'conv-14',
      session_id: 'sess-14',
      has_finished: true,
    });

    expect(result.sessionId).toBe('sess-14');
  });

  it('D-t10: error rỗng không bị coi là lỗi', () => {
    const result = normalizeAiNotification({
      document_id: 'd10',
      event_type: 'search_papers',
      error: '   ',
      has_finished: true,
    });

    expect(result.kind).toBe('finished');
  });

  it('D-t12: payload rỗng ⇒ unknown', () => {
    expect(normalizeAiNotification({}).kind).toBe('unknown');
    expect(normalizeAiNotification(null).kind).toBe('unknown');
    expect(normalizeAiNotification('abc').kind).toBe('unknown');
  });

  it('process_status=2 ⇒ processing dù không có has_finished', () => {
    expect(normalizeAiNotification({ process_status: 2 }).kind).toBe(
      'processing',
    );
  });

  it('action_required=user_input ⇒ suspended', () => {
    expect(
      normalizeAiNotification({ action_required: 'user_input' }).kind,
    ).toBe('suspended');
  });

  it('error string không có mã ⇒ error 500', () => {
    const result = normalizeAiNotification({ error: 'boom' });

    expect(result.kind).toBe('error');
    expect(result.errorCode).toBe(500);
    expect(result.errorMessage).toBe('boom');
  });
});
