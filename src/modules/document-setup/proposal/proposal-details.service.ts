import { Injectable, NotFoundException } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { PrismaService } from '../../../database/prisma.service';
import { ProposalAiDispatcher } from './proposal-ai.dispatcher';
import {
  GenerateTitleDescriptionDto,
  GenerateTitleDescriptionNoInfoDto,
  GetResearchTypeDto,
  MixTitlesDto,
} from './dto/proposal-details.dto';
import { normalizeAcademicLevel } from '../../../common/helpers/academic-level.helper';

@Injectable()
export class ProposalDetailsService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly dispatcher: ProposalAiDispatcher,
  ) {}

  generateTitleDescription(userId: string, dto: GenerateTitleDescriptionDto) {
    return this.runTitleDescription(userId, dto, false);
  }

  generateTitleDescriptionNoInfo(
    userId: string,
    dto: GenerateTitleDescriptionNoInfoDto,
  ) {
    return this.runTitleDescription(userId, dto, true);
  }

  private async runTitleDescription(
    userId: string,
    dto: GenerateTitleDescriptionDto | GenerateTitleDescriptionNoInfoDto,
    noInfo: boolean,
  ) {
    const doc = await this.ensureDocument(userId, dto.documentId);
    const eventType = noInfo
      ? 'generate_title_description_no_info'
      : 'generate_title_description';

    const kafkaData = noInfo
      ? {
          title: dto.title,
          level: normalizeAcademicLevel(
            (dto as GenerateTitleDescriptionNoInfoDto).level,
          ),
          use_web_search:
            (dto as GenerateTitleDescriptionNoInfoDto).useWebSearch ?? false,
        }
      : {
          title: dto.title,
          field: (dto as GenerateTitleDescriptionDto).field || doc.field,
          domain: this.resolveDomain(
            (dto as GenerateTitleDescriptionDto).domain,
            doc.domains,
          ),
          subdomains: (dto as GenerateTitleDescriptionDto).subdomains ?? [],
          keywords: (dto as GenerateTitleDescriptionDto).keywords ?? [],
          level: normalizeAcademicLevel(
            (dto as GenerateTitleDescriptionDto).level,
          ),
          knowledge_base:
            Array.isArray((dto as GenerateTitleDescriptionDto).knowledgeBase) &&
            ((dto as GenerateTitleDescriptionDto).knowledgeBase ?? []).length > 0
              ? ((dto as GenerateTitleDescriptionDto).knowledgeBase as any[]).map((k) =>
                  typeof k === 'object' && k ? k : { title: dto.title, content: String(k) },
                )
              : [{ title: dto.title, content: 'Tài liệu tham khảo cơ sở đề tài' }],
        };

    return this.dispatcher.dispatch({
      userId,
      documentId: dto.documentId,
      sessionId: dto.sessionId,
      eventType,
      errorCode: noInfo ? 624 : 615,
      dto,
      kafkaData,
      onSuccess: async (data) => {
        if (noInfo) {
          const docUpdate: Prisma.DocumentUpdateInput = {};
          if (data.field) docUpdate.field = String(data.field);
          if (data.subdomains) {
            docUpdate.subdomains = data.subdomains as Prisma.InputJsonValue;
          }
          if (Object.keys(docUpdate).length > 0) {
            await this.prisma.document.update({
              where: { id: dto.documentId },
              data: docUpdate,
            });
          }
        }
        await this.upsertDescription(
          dto.documentId,
          dto.title,
          data,
          dto.proposalId,
        );
      },
    });
  }

  async mixTitles(userId: string, dto: MixTitlesDto) {
    const doc = await this.ensureDocument(userId, dto.documentId);
    const proposalsPayload =
      dto.proposals || (dto.titles ? dto.titles.map((t) => ({ title: t })) : []);

    return this.dispatcher.dispatch({
      userId,
      documentId: dto.documentId,
      sessionId: dto.sessionId,
      eventType: 'mix_titles',
      errorCode: 616,
      dto,
      kafkaData: {
        field: dto.field || doc.field || 'General',
        domain: this.resolveDomain(dto.domain, doc.domains),
        level: normalizeAcademicLevel(dto.level),
        proposals: proposalsPayload,
        titles: dto.titles ?? [],
      },
      onSuccess: async (data) => {
        const proposal = data.proposal as Record<string, unknown> | undefined;
        if (proposal?.title) {
          await this.prisma.proposal.create({
            data: {
              documentId: dto.documentId,
              title: String(proposal.title),
              problemStatement: String(proposal.problem_statement || proposal.problemStatement || ''),
              motivation: String(proposal.motivation || ''),
              researchGap: String(proposal.research_gap || proposal.researchGap || ''),
            },
          });
          return;
        }

        const list = (data.proposals || data.data || []) as Array<{
          title: string;
          problem_statement?: string;
          problemStatement?: string;
          motivation?: string;
          research_gap?: string;
          researchGap?: string;
        }>;
        for (const p of list) {
          if (p.title) {
            await this.prisma.proposal.create({
              data: {
                documentId: dto.documentId,
                title: p.title,
                problemStatement:
                  p.problem_statement || p.problemStatement || null,
                motivation: p.motivation || null,
                researchGap: p.research_gap || p.researchGap || null,
              },
            });
          }
        }
      },
    });
  }

  async getResearchType(userId: string, dto: GetResearchTypeDto) {
    const doc = await this.ensureDocument(userId, dto.documentId);
    const proposal = await this.resolveResearchTypeProposal(dto);

    return this.dispatcher.dispatch({
      userId,
      documentId: dto.documentId,
      sessionId: dto.sessionId,
      eventType: 'get_research_type',
      errorCode: 623,
      dto,
      kafkaData: {
        field: dto.field || doc.field || 'General',
        domain: this.resolveDomain(dto.domain, doc.domains),
        proposal: proposal || {
          title: dto.title || doc.title || 'Research',
          problem_statement: dto.problemStatement || '',
          motivation: dto.motivation || '',
          research_gap: dto.researchGap || '',
        },
      },
      onSuccess: async (data) => {
        const rType = data.research_type || data.researchType || data.type;
        if (rType !== undefined && rType !== null) {
          const typeStr =
            typeof rType === 'object' ? JSON.stringify(rType) : String(rType);
          await this.prisma.document.update({
            where: { id: dto.documentId },
            data: { researchType: typeStr },
          });
        }
      },
    });
  }

  async ensureDocument(userId: string, id: string) {
    const doc = await this.prisma.document.findFirst({ where: { id, userId } });
    if (!doc) throw new NotFoundException('Document not found');
    return doc;
  }

  private async resolveResearchTypeProposal(dto: GetResearchTypeDto) {
    if (dto.proposal) return dto.proposal;
    const p = dto.proposalId
      ? await this.prisma.proposal.findUnique({ where: { id: dto.proposalId } })
      : await this.prisma.proposal.findFirst({
          where: { documentId: dto.documentId, isSelected: true },
        });
    return p
      ? {
          title: p.title,
          problem_statement: p.problemStatement || '',
          motivation: p.motivation || '',
          research_gap: p.researchGap || '',
        }
      : undefined;
  }

  private async upsertDescription(
    docId: string,
    title: string,
    data: Record<string, unknown>,
    proposalId?: string,
  ) {
    const prop = (data.proposal as Record<string, unknown>) || data;
    const updateData = {
      problemStatement: String(
        prop.problem_statement || prop.problemStatement || '',
      ),
      motivation: String(prop.motivation || ''),
      researchGap: String(prop.research_gap || prop.researchGap || ''),
    };
    if (proposalId) {
      await this.prisma.proposal.update({ where: { id: proposalId }, data: updateData });
      return;
    }
    const exist = await this.prisma.proposal.findFirst({ where: { documentId: docId, title } });
    if (exist) {
      await this.prisma.proposal.update({ where: { id: exist.id }, data: updateData });
    } else {
      await this.prisma.proposal.create({ data: { documentId: docId, title, ...updateData } });
    }
  }

  private resolveDomain(dtoDomain?: string, docDomains?: unknown): string {
    if (dtoDomain) return dtoDomain;
    if (Array.isArray(docDomains) && docDomains.length > 0) return String(docDomains[0]);
    return typeof docDomains === 'string' && docDomains ? docDomains : 'General';
  }
}
