// PHẢI đứng đầu tiên: nạp .env trước khi mọi hằng số đọc process.env được evaluate.
import { logResolvedAiModel } from './config/env.loader';
import { NestFactory } from '@nestjs/core';
import { Logger, ValidationPipe } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Transport, MicroserviceOptions } from '@nestjs/microservices';
import { json, urlencoded } from 'express';
import { AppModule } from './app.module';
import { AllExceptionsFilter } from './common/filters/http-exception.filter';
import { KafkaTunnelHelper } from './common/kafka/kafka-tunnel.helper';
import { installTimeoutNegativeWarningSilencer } from './common/utils/process-warning-guard';

async function bootstrap() {
  // Lọc warning timeout âm phát sinh bên trong kafkajs 2.2.4 (lỗi upstream,
  // xem src/common/utils/process-warning-guard.ts) để log khởi động sạch.
  installTimeoutNegativeWarningSilencer();
  const rawBrokersEnv =
    process.env.KAFKA || process.env.KAFKA_BROKERS || '167.254.70.125:9092';
  KafkaTunnelHelper.startProxyIfRemote(rawBrokersEnv);

  const logger = new Logger('Bootstrap');
  logResolvedAiModel();
  const app = await NestFactory.create(AppModule);

  const configService = app.get(ConfigService);
  const port = configService.get<number>('app.port', 3001);
  const apiPrefix = configService.get<string>('app.apiPrefix', 'api/v1');
  const corsOrigins = configService.get<string[]>('app.corsOrigins', [
    'http://localhost:3000',
  ]);
  const bodyLimit = configService.get<string>('app.bodyLimit', '10mb');
  // Nguồn cấu hình duy nhất: `kafka.enabled` (xem src/config/kafka.config.ts)
  const kafkaEnabled = configService.get<boolean>('kafka.enabled', true);

  app.enableCors({
    origin: corsOrigins,
    credentials: true,
  });
  app.use(json({ limit: bodyLimit }));
  app.use(urlencoded({ extended: true, limit: bodyLimit }));
  app.setGlobalPrefix(apiPrefix, {
    exclude: ['health-check', 'health-check/(.*)'],
  });
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      transform: true,
    }),
  );
  app.useGlobalFilters(new AllExceptionsFilter());
  app.enableShutdownHooks();

  // Start HTTP server immediately — always available
  await app.listen(port);
  logger.log(
    `🚀 Application is running on: http://localhost:${port}/${apiPrefix}`,
  );

  // F3: route không khớp do Express trả HTML và bỏ qua AllExceptionsFilter.
  // Đăng ký sau khi listen ⇒ middleware chạy cuối, trả JSON thống nhất.
  app.getHttpAdapter().getInstance().use((req: any, res: any) => {
    res.status(404).json({
      statusCode: 404,
      error: 'Not Found',
      message: `Cannot ${req.method} ${req.originalUrl}`,
      path: req.originalUrl,
      timestamp: new Date().toISOString(),
    });
  });

  // Chỉ kết nối Kafka khi `kafka.enabled` = true (nguồn: KAFKA_ENABLED trong .env)
  if (kafkaEnabled) {
    const rawBrokers =
      configService.get<string | string[]>('kafka.brokers') ||
      '167.254.70.125:9092';
    const brokers = Array.isArray(rawBrokers)
      ? rawBrokers
      : String(rawBrokers)
          .split(',')
          .map((b) => b.trim());

    KafkaTunnelHelper.startProxyIfRemote(brokers);

    app.connectMicroservice<MicroserviceOptions>({
      transport: Transport.KAFKA,
      options: {
        client: {
          clientId:
            configService.get<string>(
              'kafka.clientId',
              'autoresearch-backend',
            ) + '-consumer',
          brokers,
          ssl: configService.get<boolean>('kafka.ssl', false),
        },
        consumer: {
          groupId: configService.get<string>(
            'kafka.groupId',
            'autoresearch-group',
          ),
          allowAutoTopicCreation: true,
        },
      },
    });

    app
      .startAllMicroservices()
      .then(() => logger.log('✅ Kafka microservice connected'))
      .catch((err: any) =>
        logger.warn(`⚠️  Kafka unavailable (non-fatal): ${err.message}`),
      );
  } else {
    logger.warn(
      '⚠️  Kafka disabled (KAFKA_ENABLED=false). Set KAFKA_ENABLED=true in .env to enable.',
    );
  }
}
bootstrap();
