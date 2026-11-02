import { ReferenceSyncHelper } from './reference-sync.helper';

const asDto = (dto: Record<string, any>) => dto as any;

describe('ReferenceSyncHelper.buildSearchPayload', () => {
  const originalModel = process.env.AI_DEFAULT_MODEL;
  const originalAllowed = process.env.AI_ALLOWED_MODELS;

  beforeAll(() => {
    process.env.AI_DEFAULT_MODEL = 'deepseek-flash';
    process.env.AI_ALLOWED_MODELS = 'deepseek-flash';
  });

  afterAll(() => {
    if (originalModel === undefined) delete process.env.AI_DEFAULT_MODEL;
    else process.env.AI_DEFAULT_MODEL = originalModel;
    if (originalAllowed === undefined) delete process.env.AI_ALLOWED_MODELS;
    else process.env.AI_ALLOWED_MODELS = originalAllowed;
  });

  it('should dùng field/domain/subdomains từ dto khi có', () => {
    const payload = ReferenceSyncHelper.buildSearchPayload(
      'u-1',
      's-1',
      asDto({
        documentId: 'doc-1',
        field: 'Kinh tế',
        domain: 'QTKD',
        subdomains: ['A'],
        query: 'q',
      }),
      { field: 'Other', domains: ['X'], subdomains: [] },
      { title: 'P' },
      'job-1',
    );

    expect(payload).toEqual(
      expect.objectContaining({
        event_type: 'search_papers',
        user_id: 'u-1',
        session_id: 's-1',
        document_id: 'doc-1',
        field: 'Kinh tế',
        domain: 'QTKD',
        subdomains: ['A'],
        proposal: { title: 'P' },
        job_id: 'job-1',
      }),
    );
  });

  it('should fallback doc.domains dạng array và subdomains của doc', () => {
    const payload = ReferenceSyncHelper.buildSearchPayload(
      'u-1',
      's-1',
      asDto({ documentId: 'doc-1' }),
      { domains: ['Domain 1', 'Domain 2'], subdomains: ['S1'] },
      undefined,
      'job-1',
    );

    expect(payload.field).toBe('General');
    expect(payload.domain).toBe('Domain 1');
    expect(payload.subdomains).toEqual(['S1']);
    expect(payload).not.toHaveProperty('proposal');
  });

  it('should fallback doc.domains dạng string', () => {
    const payload = ReferenceSyncHelper.buildSearchPayload(
      'u-1',
      's-1',
      asDto({ documentId: 'doc-1' }),
      { domains: 'Domain string' },
      undefined,
      'job-1',
    );

    expect(payload.domain).toBe('Domain string');
  });

  it('should fallback General khi doc không có domain và default filter', () => {
    const payload = ReferenceSyncHelper.buildSearchPayload(
      'u-1',
      's-1',
      asDto({ documentId: 'doc-1' }),
      {},
      undefined,
      'job-1',
    );

    expect(payload.domain).toBe('General');
    expect(payload.country_filter).toBe('both');
    // AI Q2: mặc định `""` (không nối gì vào query)
    expect(payload.search_range).toBe('');
    expect(payload.oa_filter).toBe(false);
    expect(payload.is_oa).toBe(false);
    expect(payload.is_tool).toBe(false);
    expect(payload.downloaded_papers).toEqual([]);
    expect(payload.cut_off_year_low).toBe(2018);
  });

  it('should dùng giá trị dto khi được set', () => {
    const payload = ReferenceSyncHelper.buildSearchPayload(
      'u-1',
      's-1',
      asDto({
        documentId: 'doc-1',
        countryFilter: 'vietnam',
        oaFilter: true,
        isOa: true,
        isTool: true,
        downloadedPapers: ['p1'],
        cutOffYearLow: 2000,
        cutOffYearHigh: 2030,
        searchRange: 'all',
        useWebSearch: true,
        language: 'en',
      }),
      {},
      undefined,
      'job-1',
    );

    expect(payload.country_filter).toBe('vietnam');
    expect(payload.oa_filter).toBe(true);
    expect(payload.is_oa).toBe(true);
    expect(payload.is_tool).toBe(true);
    expect(payload.downloaded_papers).toEqual(['p1']);
    expect(payload.cut_off_year_low).toBe(2000);
    expect(payload.cut_off_year_high).toBe(2030);
    expect(payload.search_range).toBe('all');
    expect(payload.use_web_search).toBe(true);
    expect(payload.language).toBe('en');
  });
});

describe('ReferenceSyncHelper.parseSearchError', () => {
  it('should mặc định 610 khi err không phải object', () => {
    expect(ReferenceSyncHelper.parseSearchError('boom')).toEqual(
      expect.objectContaining({ errorCode: 610 }),
    );
    expect(ReferenceSyncHelper.parseSearchError(null).errorCode).toBe(610);
  });

  it('should đọc error_code + error string', () => {
    const res = ReferenceSyncHelper.parseSearchError({
      error_code: 617,
      error: 'KeyError heading',
    });

    expect(res).toEqual({ errorCode: 617, errorMessage: 'KeyError heading' });
  });

  it('should fallback message mặc định khi error không phải string', () => {
    const res = ReferenceSyncHelper.parseSearchError({
      error_code: '609',
      error: 123,
    });

    expect(res.errorCode).toBe(609);
    expect(typeof res.errorMessage).toBe('string');
    expect(res.errorMessage.length).toBeGreaterThan(0);
  });

  it('should dùng err.message khi không có error_code', () => {
    const res = ReferenceSyncHelper.parseSearchError({
      message: 'publish failed',
    });

    expect(res).toEqual({ errorCode: 610, errorMessage: 'publish failed' });
  });
});
