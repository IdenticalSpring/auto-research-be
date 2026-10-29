import { Kafka, logLevel } from 'kafkajs';

async function runSmokeTest(): Promise<void> {
  const brokerString =
    process.env.KAFKA || process.env.KAFKA_BROKERS || '167.254.70.125:9092';
  const brokers = brokerString.split(',').map((b) => b.trim());

  const requestTopic =
    process.env.REQUEST_TOPIC || 'document_setup_request';
  const responseTopic =
    process.env.RESPONSE_TOPIC || 'document_setup_response';
  const timeoutMs = Number(process.env.TIMEOUT_MS) || 90000;

  console.log('====================================================');
  console.log('🚀 KAFKA E2E SMOKE TEST - AUTO RESEARCH BACKEND');
  console.log('====================================================');
  console.log(`📡 Broker(s): ${brokers.join(', ')}`);
  console.log(`📤 Request Topic:  ${requestTopic}`);
  console.log(`📥 Response Topic: ${responseTopic}`);
  console.log(`⏱️  Timeout:        ${timeoutMs / 1000}s`);

  const uniqueId = `smoke-${Date.now()}`;
  const userId = '00000000-0000-0000-0000-000000000001';
  const documentId = '00000000-0000-0000-0000-000000000002';
  const sessionId = `sess-${uniqueId}`;

  const payload = {
    event_type: 'search_papers',
    model_id:
      process.env.AI_DEFAULT_MODEL ||
      process.env.DEFAULT_AI_MODEL ||
      'gemini-3.5-flash-lite',
    user_id: userId,
    document_id: documentId,
    session_id: sessionId,
    language: 'vi',
    field: 'Computer Science',
    domain: 'Artificial Intelligence',
    subdomains: ['Machine Learning'],
    query: 'Artificial Intelligence in Education',
    search_range: 'all',
    country_filter: 'both',
    oa_filter: false,
    is_oa: false,
    is_tool: false,
    downloaded_papers: [],
    cut_off_year_low: 2018,
    cut_off_year_high: 2026,
    use_web_search: false,
  };

  const kafka = new Kafka({
    clientId: `smoke-test-runner-${uniqueId}`,
    brokers,
    logLevel: logLevel.NOTHING,
  });

  const producer = kafka.producer();
  const consumer = kafka.consumer({
    groupId: `smoke-test-group-${uniqueId}`,
  });

  let isResolved = false;
  let timeoutTimer: NodeJS.Timeout;

  const cleanup = async () => {
    clearTimeout(timeoutTimer);
    try {
      await consumer.stop();
      await consumer.disconnect();
    } catch {
      // Ignore disconnect error
    }
    try {
      await producer.disconnect();
    } catch {
      // Ignore disconnect error
    }
  };

  try {
    console.log('\n[1/4] Connecting Producer & Consumer...');
    await Promise.all([producer.connect(), consumer.connect()]);
    console.log('      Connected successfully!');

    console.log(`\n[2/4] Subscribing to response topic [${responseTopic}]...`);
    await consumer.subscribe({
      topic: responseTopic,
      fromBeginning: false,
    });

    const responsePromise = new Promise<Record<string, unknown>>(
      (resolve, reject) => {
        timeoutTimer = setTimeout(() => {
          if (!isResolved) {
            isResolved = true;
            reject(
              new Error(
                `Timeout: No response received after ${timeoutMs / 1000} seconds from topic [${responseTopic}]`,
              ),
            );
          }
        }, timeoutMs);

        consumer
          .run({
            eachMessage: async ({ message }) => {
              if (!message.value) return;

              try {
                const text = message.value.toString();
                const data = JSON.parse(text) as Record<string, unknown>;

                const matchesSession =
                  data.session_id === sessionId || data.sessionId === sessionId;
                const matchesUser =
                  data.user_id === userId || data.userId === userId;

                if (matchesSession || (matchesUser && data.event_type === 'search_papers')) {
                  if (!isResolved) {
                    isResolved = true;
                    resolve(data);
                  }
                }
              } catch {
                // Ignore non-json or other messages
              }
            },
          })
          .catch((err) => {
            if (!isResolved) {
              isResolved = true;
              reject(err);
            }
          });
      },
    );

    console.log(`\n[3/4] Publishing request for event [search_papers] to [${requestTopic}]...`);
    console.log(`      Session ID: ${sessionId}`);
    await producer.send({
      topic: requestTopic,
      messages: [
        {
          key: `${userId}:${documentId}:${sessionId}:search_papers`,
          value: JSON.stringify(payload),
        },
      ],
    });
    console.log('      Message published! Waiting for AI Engine response...');

    console.log('\n[4/4] Awaiting AI response...');
    const response = await responsePromise;

    console.log('\n🎉 SUCCESS! Received AI response:');
    console.log('----------------------------------------------------');
    console.log(`Event Type:    ${response.event_type || 'unknown'}`);
    console.log(`Session ID:    ${response.session_id || response.sessionId}`);
    if (response.error_code || response.errorCode) {
      console.log(`⚠️  AI Warning/Error: Code ${response.error_code || response.errorCode}`);
      console.log(`   Message:          ${response.error || response.errorMessage || 'N/A'}`);
    } else {
      console.log(
        `Papers count:  ${Array.isArray(response.papers) ? response.papers.length : Array.isArray(response.data) ? response.data.length : 'N/A'}`,
      );
    }
    console.log(
      `Tokens:        Input=${response.input_tokens ?? 0}, Output=${response.output_tokens ?? 0}`,
    );
    console.log('----------------------------------------------------');

    await cleanup();
    process.exit(0);
  } catch (error) {
    console.error('\n❌ SMOKE TEST FAILED:');
    console.error((error as Error).message);
    await cleanup();
    process.exit(1);
  }
}

void runSmokeTest();
