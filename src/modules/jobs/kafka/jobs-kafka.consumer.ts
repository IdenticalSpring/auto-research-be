import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { JobStatus, Prisma } from '@prisma/client';
import { JobService } from '../job.service';
import { PrismaService } from '../../../database/prisma.service';

@Controller()
export class JobsKafkaConsumer {
  private readonly logger = new Logger(JobsKafkaConsumer.name);

  constructor(
    private readonly jobService: JobService,
    private readonly prisma: PrismaService,
  ) {}

  @EventPattern('document_setup_response')
  async handleDocumentSetupResponse(@Payload() data: any) {
    await this.processWorkerResponse('document_setup', data);
  }

  @EventPattern('write_section_response')
  async handleWriteSectionResponse(@Payload() data: any) {
    await this.processWorkerResponse('write_report', data);
  }

  @EventPattern('chatbot_response')
  async handleChatbotResponse(@Payload() data: any) {
    await this.processWorkerResponse('chatbot', data);
  }

  @EventPattern('enhancement_response')
  async handleEnhancementResponse(@Payload() data: any) {
    await this.processWorkerResponse('enhancement', data);
  }

  private async processWorkerResponse(module: string, rawData: any) {
    try {
      const rawObj =
        typeof rawData === 'string' ? JSON.parse(rawData) : rawData;
      const jobId = rawObj.job_id || rawObj.jobId;

      this.logger.log(
        `Received Kafka response for ${module} job ${jobId}: status=${rawObj.status || (rawObj.error ? 'error' : 'finished')}`,
      );

      if (!jobId) {
        this.logger.warn(`Kafka response missing job_id in module ${module}`);
        return;
      }

      const isError = Boolean(rawObj.error || rawObj.error_code);
      const status = isError
        ? JobStatus.error
        : rawObj.status || JobStatus.finished;
      const resultData =
        rawObj.data ||
        rawObj.domains ||
        rawObj.subdomains ||
        rawObj.titles ||
        rawObj.proposals ||
        rawObj;

      const updatedJob = await this.jobService.update(jobId, {
        status,
        result: resultData as Prisma.InputJsonValue,
        errorMessage:
          typeof rawObj.error === 'string' ? rawObj.error : undefined,
        errorCode: rawObj.error_code ? String(rawObj.error_code) : undefined,
      });

      if (!isError && updatedJob.documentId && module === 'document_setup') {
        await this.syncDocumentSetupData(
          updatedJob.documentId,
          updatedJob.action,
          rawObj,
        );
      }
    } catch (error) {
      this.logger.error(
        `Error processing Kafka response for ${module}: ${error}`,
      );
    }
  }

  private async syncDocumentSetupData(
    documentId: string,
    action: string,
    rawObj: any,
  ) {
    try {
      if (
        action === 'generate_domains' &&
        (rawObj.domains || Array.isArray(rawObj.data))
      ) {
        const domains = rawObj.domains || rawObj.data;
        const normalized = Array.isArray(domains)
          ? domains.map((d: any, idx: number) => {
              if (typeof d === 'string') {
                return {
                  id: `domain-${idx + 1}`,
                  domainName: d,
                  subdomains: [],
                };
              }
              return {
                id: d.id || `domain-${idx + 1}`,
                domainName: d.domainName || d.name || String(d),
                subdomains: Array.isArray(d.subdomains) ? d.subdomains : [],
              };
            })
          : domains;
        await this.prisma.document.update({
          where: { id: documentId },
          data: { domains: normalized as Prisma.InputJsonValue },
        });
      } else if (
        action === 'generate_subdomains' &&
        (rawObj.subdomains || rawObj.data)
      ) {
        await this.prisma.document.update({
          where: { id: documentId },
          data: {
            subdomains: (rawObj.subdomains ||
              rawObj.data) as Prisma.InputJsonValue,
          },
        });
      } else if (
        action === 'generate_titles' &&
        (rawObj.proposals || rawObj.titles || rawObj.data)
      ) {
        const titles = rawObj.proposals || rawObj.titles || rawObj.data;
        if (Array.isArray(titles)) {
          for (const item of titles) {
            const title = typeof item === 'string' ? item : item.title;
            if (title) {
              await this.prisma.proposal.create({
                data: {
                  documentId,
                  title,
                  problemStatement:
                    item.problem_statement || item.problemStatement || null,
                  motivation: item.motivation || null,
                  isSelected: false,
                },
              });
            }
          }
        }
      }
    } catch (err) {
      this.logger.warn(
        `Failed to sync document data for job document ${documentId}: ${err}`,
      );
    }
  }
}
