import {
  BROKER, REAL_PROPOSAL, REAL_DATA, REAL_VARIABLES, REAL_PAPERS,
  DEFAULT_OUTLINE, createKafkaClient, createKafkaSender,
} from './live-test-helpers';

async function run() {
  console.log('🚀 TESTING CHUYÊN ĐỀ 2 VÀ 3 TRÊN LOCAL AI (BROKER:', BROKER, ')');
  const uid = `cd-${Date.now()}`;
  const { producer, consumer } = createKafkaClient(uid);
  await Promise.all([producer.connect(), consumer.connect()]);
  await consumer.subscribe({ topic: 'write_section_response', fromBeginning: false });

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

  // Test chuyen_de_2
  try {
    process.stdout.write('Testing chuyen_de_2 ... ');
    const res = await sendKafka('write_section_request', {
      event_type: 'chuyen_de_2',
      document_id: '65a000000000000000000002',
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      final_proposal: REAL_PROPOSAL,
      outline: DEFAULT_OUTLINE,
      data: REAL_DATA,
      references_style: 'APA 7th',
      papers_search: REAL_PAPERS,
      heading: 'chapter_1',
      outline_code: ['chapter_1'],
      word_count_str: '8000',
    }, 60000);
    console.log('✅ PASS! chuyen_de_2 finish:', res.has_finished ?? true);
  } catch (e: any) {
    console.log('❌ Result:', e.message);
  }

  // Test chuyen_de_3
  try {
    process.stdout.write('Testing chuyen_de_3 ... ');
    const res = await sendKafka('write_section_request', {
      event_type: 'chuyen_de_3',
      document_id: '65a000000000000000000002',
      field: 'Kinh tế học',
      domain: 'Quản trị kinh doanh',
      final_proposal: REAL_PROPOSAL,
      outline: DEFAULT_OUTLINE,
      data: REAL_DATA,
      variables: REAL_VARIABLES,
      references_style: 'APA 7th',
      papers_search: REAL_PAPERS,
      heading: 'chapter_1',
      outline_code: ['chapter_1'],
      word_count_str: '8000',
    }, 60000);
    console.log('✅ PASS! chuyen_de_3 finish:', res.has_finished ?? true);
  } catch (e: any) {
    console.log('❌ Result:', e.message);
  }

  await Promise.all([producer.disconnect(), consumer.disconnect()]);
  process.exit(0);
}

run().catch(console.error);
