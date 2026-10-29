import { resolveKafkaEnabled, resolveKafkaTopicSuffix } from './kafka.config';

describe('resolveKafkaEnabled', () => {
  it('mặc định BẬT khi biến không được set', () => {
    expect(resolveKafkaEnabled(undefined)).toBe(true);
  });

  it('mặc định BẬT khi biến rỗng (xử lý giống trường hợp không set)', () => {
    expect(resolveKafkaEnabled('')).toBe(true);
    expect(resolveKafkaEnabled('   ')).toBe(true);
  });

  it('BẬT với "true" (không phân biệt hoa/thường)', () => {
    expect(resolveKafkaEnabled('true')).toBe(true);
    expect(resolveKafkaEnabled('TRUE')).toBe(true);
    expect(resolveKafkaEnabled(' True ')).toBe(true);
  });

  it('TẮT với mọi giá trị khác "true"', () => {
    expect(resolveKafkaEnabled('false')).toBe(false);
    expect(resolveKafkaEnabled('FALSE')).toBe(false);
    expect(resolveKafkaEnabled('0')).toBe(false);
    expect(resolveKafkaEnabled('no')).toBe(false);
  });
});

describe('resolveKafkaTopicSuffix', () => {
  it('ưu tiên giá trị explicitSuffix nếu được truyền', () => {
    expect(resolveKafkaTopicSuffix('_custom', false)).toBe('_custom');
    expect(resolveKafkaTopicSuffix('', true)).toBe('');
  });

  it('trả về "_local" khi useLocalTopics là true', () => {
    expect(resolveKafkaTopicSuffix(undefined, true)).toBe('_local');
    expect(resolveKafkaTopicSuffix(undefined, 'true')).toBe('_local');
    expect(resolveKafkaTopicSuffix(undefined, 'TRUE')).toBe('_local');
  });

  it('mặc định trả về rỗng khi không bật local topics', () => {
    expect(resolveKafkaTopicSuffix(undefined, false)).toBe('');
    expect(resolveKafkaTopicSuffix(undefined, 'false')).toBe('');
    expect(resolveKafkaTopicSuffix(undefined, undefined)).toBe('');
  });
});
