import {
  BadRequestException,
  HttpStatus,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { JobStatus, ModuleType, Prisma } from '@prisma/client';
import { randomUUID } from 'node:crypto';
import { PrismaService } from '../../../database/prisma.service';
import { KafkaProducerService } from '../../../messaging/kafka-producer.service';
import { RequestCorrelationService } from '../../../messaging/request-correlation.service';
import {
  getAiErrorMessage,
  getAiErrorName,
} from '../../../common/constants/ai-error-codes';
import { CreateReferenceDto } from './dto/create-reference.dto';
import { UpdateReferenceDto } from './dto/update-reference.dto';
import { SearchPapersDto } from './dto/search-papers.dto';
import {
  PaperItemDto,
  SearchPapersResponseDto,
} from './dto/search-papers-response.dto';
import { ReferenceSyncHelper } from './reference-sync.helper';

@Injectable()
export class ReferenceService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly correlationService: RequestCorrelationService,
    private readonly configService: ConfigService,
  ) {}

  create(dto: CreateReferenceDto) {
    return this.prisma.reference.create({ data: dto });
  }

  findAll() {
    return this.prisma.reference.findMany({ orderBy: { createdAt: 'desc' } });
  }

  async findOne(id: string) {
    const reference = await this.prisma.reference.findUnique({ where: { id } });
    if (!reference) throw new NotFoundException('Reference not found');
    return reference;
  }

  async update(id: string, dto: UpdateReferenceDto) {
    await this.findOne(id);
    return this.prisma.reference.update({ where: { id }, data: dto });
  }

  async remove(id: string) {
    await this.findOne(id);
    await this.prisma.reference.delete({ where: { id } });
    return { message: 'Reference deleted successfully' };
  }

  async searchPapers(
    userId: string,
    dto: SearchPapersDto,
  ): Promise<SearchPapersResponseDto> {
    const doc = await this.ensureDocument(userId, dto.documentId);
    const sessionId = dto.sessionId || `sess-search-${randomUUID()}`;
    let proposal = await ReferenceSyncHelper.resolveProposal(this.prisma, dto);
    if (!proposal && !dto.isTool) {
      proposal = {
        title: doc.title || 'Research',
        problem_statement: '',
        motivation: '',
        research_gap: '',
      };
    }

    const job = await this.prisma.job.create({
      data: {
        module: ModuleType.document_setup,
        action: 'search_papers',
        documentId: dto.documentId,
        sessionId,
        status: JobStatus.pending,
        payload: dto as unknown as Prisma.InputJsonValue,
      },
    });

    const payload = ReferenceSyncHelper.buildSearchPayload(
      userId,
      sessionId,
      dto,
      doc,
      proposal,
      job.id,
    );

    const correlationKey = this.correlationService.buildCorrelationKey({
      userId,
      documentId: dto.documentId,
      sessionId,
      eventType: 'search_papers',
    });

    const pendingPromise = this.correlationService.registerPending<
      Record<string, unknown>
    >(correlationKey, undefined, 'search_papers', job.id);

    const topic =
      this.configService.get<string>('kafka.topics.document_setup.request') ||
      'document_setup_request_local';

    try {
      await this.kafkaProducer.publish(topic, payload);
      await this.prisma.job.update({
        where: { id: job.id },
        data: { status: JobStatus.processing },
      });
    } catch (publishError: unknown) {
      const errMsg =
        publishError instanceof Error
          ? publishError.message
          : 'Publish to Kafka failed';
      this.correlationService.clearPending(correlationKey, errMsg);
      await this.prisma.job.update({
        where: { id: job.id },
        data: {
          status: JobStatus.error,
          errorCode: '610',
          errorMessage: getAiErrorMessage(610),
        },
      });
      throw new BadRequestException({
        statusCode: HttpStatus.BAD_REQUEST,
        errorCode: 610,
        error: getAiErrorName(610),
        message: getAiErrorMessage(610),
      });
    }

    try {
      const response = await pendingPromise;
      const rawPapers = response.papers || response.data || [];
      const papers: PaperItemDto[] = Array.isArray(rawPapers)
        ? (rawPapers as PaperItemDto[])
        : [];

      await ReferenceSyncHelper.syncFoundPapers(
        this.prisma,
        dto.documentId,
        papers,
      );

      await this.prisma.job.update({
        where: { id: job.id },
        data: {
          status: JobStatus.finished,
          result: {
            papers,
            total: papers.length,
          } as unknown as Prisma.InputJsonValue,
        },
      });

      return { jobId: job.id, sessionId, papers, total: papers.length };
    } catch (err: unknown) {
      const { errorCode, errorMessage } = ReferenceSyncHelper.parseSearchError(err);
      await this.prisma.job.update({
        where: { id: job.id },
        data: {
          status: JobStatus.error,
          errorCode: String(errorCode),
          errorMessage,
        },
      });

      throw new BadRequestException({
        statusCode: HttpStatus.BAD_REQUEST,
        errorCode,
        error: getAiErrorName(errorCode),
        message: errorMessage,
      });
    }
  }

  async getJournals(lastUpdated?: number) {
    const sessionId = `sess-journal-${randomUUID()}`;
    const topic =
      this.configService.get<string>('kafka.topics.document_setup.request') ||
      'document_setup_request_local';
    const correlationKey = this.correlationService.buildCorrelationKey({
      userId: 'system',
      documentId: 'system',
      sessionId,
      eventType: 'get_journals',
    });
    const pending = this.correlationService.registerPending<Record<string, unknown>>(
      correlationKey,
      undefined,
      'get_journals',
    );
    await this.kafkaProducer.publish(topic, {
      event_type: 'get_journals',
      last_updated: lastUpdated || Date.now(),
      session_id: sessionId,
      user_id: 'system',
      document_id: 'system',
    });
    return pending;
  }

  private async ensureDocument(userId: string, documentId: string) {
    const document = await this.prisma.document.findFirst({
      where: { id: documentId, userId },
      select: { id: true, title: true, field: true, domains: true, subdomains: true },
    });
    if (!document) {
      throw new NotFoundException('Document not found');
    }
    return document;
  }
}
