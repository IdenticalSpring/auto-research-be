import {
  BROKER, REAL_PROPOSAL, REAL_DATA, REAL_VARIABLES, REAL_PAPERS,
  DEFAULT_OUTLINE, createKafkaClient, createKafkaSender,
} from './live-test-helpers';
import { MIN_E2E_TIMEOUT_MS } from './lib/e2e-timeouts';

async function run() {
  console.log('🚀 FAST PROBE ON LOCAL AI (BROKER:', BROKER, ')');
  const uid = `probe-${Date.now()}`;
  const { producer, consumer } = createKafkaClient(uid);
  await Promise.all([producer.connect(), consumer.connect()]);

  await Promise.all([
    consumer.subscribe({ topic: 'document_setup_response', fromBeginning: false }),
    consumer.subscribe({ topic: 'write_section_response', fromBeginning: false }),
  ]);

  const responseMap = new Map<string, (res: Record<string, unknown>) => void>();
  const receivedResponses = new Map<string, Record<string, unknown>>();

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      try {
        const data = JSON.parse(message.value.toString()) as Record<string, unknown>;
        const sess = String(data.session_id || data.sessionId || '');
        const resolver = responseMap.get(sess);
        if (resolver) {
          responseMap.delete(sess);
          resolver(data);
        } else if (sess) {
          receivedResponses.set(sess, data);
        }
      } catch {}
    },
  });

  await new Promise((r) => setTimeout(r, 2000));
  const sendKafka = createKafkaSender(producer, responseMap, receivedResponses);

  // Probe 1: search_papers với oa_filter: false
  try {
    process.stdout.write('Testing search_papers with oa_filter=false ... ');
    const res = await sendKafka('document_setup_request', {
      event_type: 'search_papers',
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      query: 'Digital transformation SMEs Vietnam',
      // AI contract (2026-11-01): country_filter là string enum both|in|out
      country_filter: 'in',
      cut_off_year_low: 2020,
      cut_off_year_high: 2025,
      oa_filter: false,
      is_tool: false,
      is_oa: false,
      downloaded_papers: [],
    }, MIN_E2E_TIMEOUT_MS);
    console.log('✅ PASS! Papers found:', (res.papers as any[])?.length ?? 0);
  } catch (e: any) {
    console.log('❌ Result:', e.message);
  }

  // Probe 2: generate_outline với headings dạng mảng
  try {
    process.stdout.write('Testing generate_outline with headings as list ... ');
    const res = await sendKafka('document_setup_request', {
      event_type: 'generate_outline',
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      academic_level: 'KHOA_LUAN',
      research_type: { type: 1, reason: 'Nghiên cứu định lượng' },
      final_proposal: REAL_PROPOSAL,
      headings: [
        'Chương 1: Tổng quan nghiên cứu',
        'Chương 2: Cơ sở lý thuyết và mô hình',
        'Chương 3: Phương pháp nghiên cứu',
        'Chương 4: Kết quả nghiên cứu và thảo luận',
        'Chương 5: Kết luận và khuyến nghị',
      ],
      headings_percent: [15, 25, 25, 25, 10],
      outline_code: ['chapter_1', 'chapter_2', 'chapter_3', 'chapter_4', 'chapter_5'],
      word_count_str: '8000',
    }, MIN_E2E_TIMEOUT_MS);
    console.log('✅ PASS! Outline:', res.outline ? 'OK' : 'Done');
  } catch (e: any) {
    console.log('❌ Result:', e.message);
  }

  // Probe 3: write_content với DEFAULT_OUTLINE có heading
  try {
    process.stdout.write('Testing write_content with heading in outline ... ');
    const res = await sendKafka('write_section_request', {
      event_type: 'write_content',
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      final_proposal: REAL_PROPOSAL,
      outline: DEFAULT_OUTLINE,
      references_style: 'APA 7th',
      papers_search: REAL_PAPERS,
      heading: '1. Chương 1: Tổng quan nghiên cứu',
      outline_code: ['chapter_1'],
    }, MIN_E2E_TIMEOUT_MS);
    console.log('✅ PASS! write_content finish:', res.has_finished ?? true);
  } catch (e: any) {
    console.log('❌ Result:', e.message);
  }

  await Promise.all([producer.disconnect(), consumer.disconnect()]);
  process.exit(0);
}

run().catch(console.error);
