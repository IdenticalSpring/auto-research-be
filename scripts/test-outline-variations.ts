import { Kafka, logLevel } from 'kafkajs';
import { resolveScriptModelId } from './lib/ai-model';

const BROKER = process.env.KAFKA || '100.120.3.64:9092';
const MODEL_ID = resolveScriptModelId();
const USER_ID = '00000000-0000-0000-0000-000000000001';
const DOC_ID = '00000000-0000-0000-0000-000000000002';

async function testVariations(): Promise<void> {
  const uid = `var-${Date.now()}`;
  const kafka = new Kafka({
    clientId: `probe-var-${uid}`,
    brokers: BROKER.split(',').map((b) => b.trim()),
    logLevel: logLevel.NOTHING,
  });

  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: `probe-var-grp-${uid}` });

  await Promise.all([producer.connect(), consumer.connect()]);
  await consumer.subscribe({ topic: 'document_setup_response', fromBeginning: false });

  const responseMap = new Map<string, (res: Record<string, unknown>) => void>();

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      try {
        const raw = JSON.parse(message.value.toString()) as Record<string, unknown>;
        const sess = String(raw.session_id || raw.sessionId || '');
        const r = responseMap.get(sess);
        if (r) {
          responseMap.delete(sess);
          r(raw);
        }
      } catch {}
    },
  });

  const send = (payload: Record<string, unknown>, sess: string, timeoutMs = 15000) => {
    return new Promise<Record<string, unknown>>((resolve, reject) => {
      const timer = setTimeout(() => {
        responseMap.delete(sess);
        reject(new Error(`Timeout ${timeoutMs / 1000}s`));
      }, timeoutMs);

      responseMap.set(sess, (data) => {
        clearTimeout(timer);
        resolve(data);
      });

      producer.send({
        topic: 'document_setup_request',
        messages: [{ key: sess, value: JSON.stringify(payload) }],
      }).catch(reject);
    });
  };

  const baseProposal = {
    title: 'Tác động của Live-commerce đến Gen Z',
    problem_statement: 'Live-commerce phát triển nhanh nhưng rủi ro cao.',
    motivation: 'Đóng góp lý luận cho thương mại điện tử hiện đại.',
    research_gap: 'Chưa có nghiên cứu tại thị trường Việt Nam.',
  };

  const variants = [
    {
      name: 'V1: research_type string "quantitative"',
      research_type: 'quantitative',
      headings: [{ title: 'Chương 1', percent: 20 }, { title: 'Chương 2', percent: 20 }, { title: 'Chương 3', percent: 20 }, { title: 'Chương 4', percent: 20 }, { title: 'Chương 5', percent: 20 }],
    },
    {
      name: 'V2: headings with sub_headings/children',
      research_type: { type: 2, reason: 'mixed' },
      headings: [
        { title: 'Chương 1', percent: 20, sub_headings: ['1.1', '1.2'] },
        { title: 'Chương 2', percent: 20, sub_headings: ['2.1', '2.2'] },
        { title: 'Chương 3', percent: 20, sub_headings: ['3.1', '3.2'] },
        { title: 'Chương 4', percent: 20, sub_headings: ['4.1', '4.2'] },
        { title: 'Chương 5', percent: 20, sub_headings: ['5.1', '5.2'] },
      ],
    },
    {
      name: 'V3: headings with key "heading"',
      research_type: { type: 2, reason: 'mixed' },
      headings: [
        { heading: 'Chương 1: Mở đầu' },
        { heading: 'Chương 2: Cơ sở lý thuyết' },
        { heading: 'Chương 3: Phương pháp' },
        { heading: 'Chương 4: Kết quả' },
        { heading: 'Chương 5: Kết luận' },
      ],
    },
    {
      name: 'V4: research_type int 2',
      research_type: 2,
      headings: [{ title: 'Chương 1' }, { title: 'Chương 2' }, { title: 'Chương 3' }, { title: 'Chương 4' }, { title: 'Chương 5' }],
    },
  ];

  for (const v of variants) {
    const s = `s-v-${Date.now()}`;
    process.stdout.write(`Testing [${v.name}]... `);
    try {
      const res = await send({
        event_type: 'generate_outline',
        model_id: MODEL_ID,
        user_id: USER_ID,
        document_id: DOC_ID,
        session_id: s,
        language: 'vi',
        field: 'Kinh tế số',
        domain: 'Thương mại điện tử',
        subdomains: ['B2C'],
        final_proposal: baseProposal,
        research_type: v.research_type,
        word_count_str: '8000',
        headings: v.headings,
        headings_percent: [20, 20, 20, 20, 20],
        outline_code: ['c1', 'c2', 'c3', 'c4', 'c5'],
      }, s);
      console.log(`Result: err=${res.error_code || 'none'} msg=${res.error || (res.outline ? 'SUCCESS' : 'UNKNOWN')}`);
    } catch (e: unknown) {
      console.log(`Error: ${(e as Error).message}`);
    }
  }

  await producer.disconnect();
  await consumer.disconnect();
}

testVariations().catch(console.error);
