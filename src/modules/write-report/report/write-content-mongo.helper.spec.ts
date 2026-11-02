import { WriteContentMongoHelper } from './write-content-mongo.helper';

describe('WriteContentMongoHelper', () => {
  const originalUri = process.env.MONGO_URI;

  afterEach(() => {
    if (originalUri === undefined) delete process.env.MONGO_URI;
    else process.env.MONGO_URI = originalUri;
  });

  it('toMongoDocId: bỏ dấu gạch và pad đủ 24 ký tự', () => {
    expect(
      WriteContentMongoHelper.toMongoDocId('11111111-1111-4111-8111-111111111111'),
    ).toBe('111111111111411181111111');
    expect(WriteContentMongoHelper.toMongoDocId('abc')).toBe(
      'abc000000000000000000000',
    );
  });

  it('toMongoDocId: cắt còn 24 ký tự khi id dài hơn', () => {
    expect(
      WriteContentMongoHelper.toMongoDocId('abcdefghijklmnopqrstuvwxyz0123456789'),
    ).toBe('abcdefghijklmnopqrstuvwx');
  });

  it('readDocumentConfiguration: báo lỗi rõ khi thiếu MONGO_URI', async () => {
    delete process.env.MONGO_URI;

    await expect(
      WriteContentMongoHelper.readDocumentConfiguration('doc-1'),
    ).rejects.toThrow(/Thiếu biến môi trường MONGO_URI/);
  });

  it('readDocumentConfiguration: propagate lỗi kết nối Mongo', async () => {
    process.env.MONGO_URI = 'mongodb://127.0.0.1:1/autoresearch';

    await expect(
      WriteContentMongoHelper.readDocumentConfiguration('doc-1', ['field']),
    ).rejects.toBeInstanceOf(Error);
  }, 20_000);

  it('ensureMongoReady: nuốt lỗi kết nối (không chặn luồng chính)', async () => {
    process.env.MONGO_URI = 'mongodb://127.0.0.1:1/autoresearch';

    await expect(
      WriteContentMongoHelper.ensureMongoReady('doc-1', [], { outline: [] }),
    ).resolves.toBeUndefined();
  }, 20_000);

  it('findRemainingReportRefs: trả rỗng ngay khi input rỗng', async () => {
    await expect(
      WriteContentMongoHelper.findRemainingReportRefs([]),
    ).resolves.toEqual([]);
  });

  it('findRemainingReportRefs: propagate lỗi kết nối Mongo', async () => {
    process.env.MONGO_URI = 'mongodb://127.0.0.1:1/autoresearch';

    await expect(
      WriteContentMongoHelper.findRemainingReportRefs(['doc-1']),
    ).rejects.toBeInstanceOf(Error);
  }, 20_000);
});
