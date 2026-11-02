import { Kafka, logLevel, Producer, Consumer } from 'kafkajs';
import { randomUUID } from 'node:crypto';
import { resolveScriptModelId } from './lib/ai-model';
import { MIN_E2E_TIMEOUT_MS } from './lib/e2e-timeouts';

export const BROKER = process.env.KAFKA || 'kafka:29092';
export const MODEL_ID = resolveScriptModelId();
export const USER_ID = '00000000-0000-0000-0000-000000000001';
export const DOC_ID = '00000000-0000-0000-0000-000000000002';

export const REAL_PROPOSAL = {
  title: 'Nghiên cứu tác động của Chuyển đổi số đến Hiệu quả hoạt động của các Doanh nghiệp Vừa và Nhỏ (SMEs) tại Việt Nam',
  problem_statement: 'Nhiều SMEs gặp khó khăn khi chuyển đổi số do thiếu vốn và nhân lực.',
  motivation: 'Làm rõ các nhân tố công nghệ thúc đẩy tăng trưởng năng suất cho SMEs.',
  research_gap: 'Thiếu các nghiên cứu định lượng quy mô tại thị trường Việt Nam.',
  level: 'KHOA_LUAN',
};

export const REAL_DATA = {
  doanh_thu: [120, 145, 110, 160, 200, 210, 180, 195, 230, 250],
  chi_phi_cntt: [15, 20, 12, 25, 35, 40, 30, 32, 45, 50],
  nhan_su_so: [3, 4, 2, 5, 8, 9, 6, 7, 10, 12],
  roa: [8.5, 9.2, 7.8, 10.1, 12.4, 13.0, 11.2, 11.8, 14.1, 15.0],
};

export const REAL_VARIABLES = [
  { name: 'Hiệu quả hoạt động', code: 'roa', scale: 'ratio', role: 'dependent', variable_type: 'observed' },
  { name: 'Chi phí CNTT', code: 'chi_phi_cntt', scale: 'ratio', role: 'independent', variable_type: 'observed' },
  { name: 'Nhân sự số', code: 'nhan_su_so', scale: 'ratio', role: 'independent', variable_type: 'observed' },
];

export const REAL_PAPERS = [
  {
    title: 'Digital transformation and firm performance in Vietnamese SMEs',
    authors: 'Nguyen Van A, Tran Thi B',
    year: 2024,
    abstract: 'This empirical study explores how digital technology adoption enhances productivity in Vietnam SMEs.',
    url: 'https://doi.org/10.1000/smes-vietnam',
  },
];

export const DEFAULT_OUTLINE = {
  title: REAL_PROPOSAL.title,
  outline: [
    {
      id: 'c1',
      heading: '1. Chương 1: Tổng quan nghiên cứu',
      title: 'Chương 1: Tổng quan nghiên cứu',
      overview: 'Tổng quan bối cảnh và mục tiêu nghiên cứu chuyển đổi số.',
      percent: 20,
      word_count: '2000',
      subsections: [
        { id: 's1', heading: '1.1. Bối cảnh nghiên cứu', title: '1.1. Bối cảnh nghiên cứu', overview: 'Bối cảnh SMEs.', word_count: '1000', percent: 100 },
      ],
    },
    {
      id: 'c2',
      heading: '2. Chương 2: Cơ sở lý thuyết và mô hình',
      title: 'Chương 2: Cơ sở lý thuyết và mô hình',
      overview: 'Cơ sở lý thuyết và các giả thuyết nghiên cứu.',
      percent: 30,
      word_count: '3000',
      subsections: [
        { id: 's2', heading: '2.1. Khung lý thuyết', title: '2.1. Khung lý thuyết', overview: 'Lý thuyết số hóa.', word_count: '1500', percent: 100 },
      ],
    },
    {
      id: 'c3',
      heading: '3. Chương 3: Phương pháp nghiên cứu',
      title: 'Chương 3: Phương pháp nghiên cứu',
      overview: 'Phương pháp thu thập dữ liệu và phân tích định lượng.',
      percent: 25,
      word_count: '2500',
      subsections: [
        { id: 's3', heading: '3.1. Thiết kế nghiên cứu', title: '3.1. Thiết kế nghiên cứu', overview: 'Thiết kế mẫu.', word_count: '1200', percent: 100 },
      ],
    },
    {
      id: 'c4',
      heading: '4. Chương 4: Kết quả nghiên cứu và thảo luận',
      title: 'Chương 4: Kết quả nghiên cứu và thảo luận',
      overview: 'Kết quả hồi quy OLS và thảo luận các phát hiện.',
      percent: 25,
      word_count: '2500',
      subsections: [
        { id: 's4', heading: '4.1. Kết quả hồi quy', title: '4.1. Kết quả hồi quy', overview: 'Kết quả OLS.', word_count: '1200', percent: 100 },
      ],
    },
  ],
};

export interface TestResult {
  step: number;
  name: string;
  status: 'PASS' | 'FAIL' | 'TIMEOUT' | 'AI_ISSUE';
  durationSec: number;
  summary: string;
}

export function createKafkaClient(clientId: string) {
  const kafka = new Kafka({
    clientId,
    brokers: BROKER.split(',').map((b) => b.trim()),
    logLevel: logLevel.NOTHING,
  });
  return {
    producer: kafka.producer(),
    consumer: kafka.consumer({ groupId: `group-${clientId}` }),
  };
}

export function createKafkaSender(
  producer: Producer,
  responseMap: Map<string, (res: Record<string, unknown>) => void>,
  receivedResponses: Map<string, Record<string, unknown>>,
) {
  return (topic: string, payload: Record<string, unknown>, timeoutMs = MIN_E2E_TIMEOUT_MS) => {
    const sess = String(payload.session_id || `sess-${randomUUID()}`);
    payload.session_id = sess;
    payload.model_id = payload.model_id || MODEL_ID;
    payload.user_id = payload.user_id || USER_ID;
    payload.document_id = payload.document_id || DOC_ID;
    payload.language = payload.language || 'vi';

    const matchKeys = [sess];
    if (payload.conv_id) matchKeys.push(String(payload.conv_id));
    if (payload.event_type) matchKeys.push(`${payload.event_type}_${payload.document_id}`);

    return new Promise<Record<string, unknown>>((resolve, reject) => {
      for (const k of matchKeys) {
        if (receivedResponses.has(k)) {
          const cached = receivedResponses.get(k)!;
          receivedResponses.delete(k);
          return resolve(cached);
        }
      }

      const timer = setTimeout(() => {
        for (const k of matchKeys) responseMap.delete(k);
        reject(new Error(`Timeout sau ${timeoutMs / 1000}s chờ phản hồi từ AI Server`));
      }, timeoutMs);

      const resolver = (data: Record<string, unknown>) => {
        clearTimeout(timer);
        for (const k of matchKeys) responseMap.delete(k);
        if (
          data.error &&
          !data.response &&
          !data.domains &&
          !data.subdomains &&
          !data.proposals &&
          !data.keywords &&
          !(data as any).main_keywords &&
          !data.content &&
          !data.data &&
          !data.papers &&
          !data.suggested_paragraphs
        ) {
          reject(new Error(`AI Error ${data.error_code || ''}: ${data.error}`));
        } else {
          resolve(data);
        }
      };

      for (const k of matchKeys) {
        responseMap.set(k, resolver);
      }

      producer.send({
        topic,
        messages: [{ key: sess, value: JSON.stringify(payload) }],
      }).catch(reject);
    });
  };
}
