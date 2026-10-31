import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma.service';
import { OutlineAiDispatcher } from './outline-ai.dispatcher';
import { OutlineSyncHelper } from './outline-sync.helper';
import {
  GenerateOutlineAiDto,
  GenerateOutlineWithRefsDto,
  ModifyOutlineAiDto,
  UpdateUserRefsDto,
} from './dto/outline-ai.dto';

@Injectable()
export class OutlineAiService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly dispatcher: OutlineAiDispatcher,
  ) {}

  async generateOutline(userId: string, dto: GenerateOutlineAiDto) {
    const doc = await this.ensureDocument(userId, dto.documentId);
    const prop = await this.resolveSelectedProposal(dto.documentId, dto.proposalId);
    const finalProposal = OutlineSyncHelper.resolveFinalProposal(prop, dto.finalProposal);
    const { headings, headingsPercent, outlineCode } = OutlineSyncHelper.resolveHeadingsAndPercents(
      dto.headings,
      dto.headingsPercent,
      dto.outlineCode,
    );
    const wordCountStr = dto.wordCountStr
      ? String(dto.wordCountStr).replace(/\D/g, '') || '8000'
      : String(dto.totalWordCount || 8000);
    const researchType =
      typeof dto.researchType === 'object' && dto.researchType !== null
        ? dto.researchType
        : { type: 0, reason: String(dto.researchType || doc.researchType || 'quantitative') };

    return this.dispatcher.dispatch({
      userId,
      documentId: dto.documentId,
      sessionId: dto.sessionId,
      eventType: 'generate_outline',
      errorCode: 617,
      dto,
      kafkaData: {
        field: dto.field || doc.field || 'General',
        domain:
          dto.domain ||
          (Array.isArray(doc.domains) && doc.domains.length > 0
            ? String(doc.domains[0])
            : typeof (doc as any).domains === 'string' && (doc as any).domains
              ? (doc as any).domains
              : (doc as any).domain || 'General'),
        research_type: researchType,
        word_count_str: wordCountStr,
        headings,
        headings_percent: headingsPercent,
        final_proposal: finalProposal,
        outline_code: outlineCode,
      },
      onSuccess: async (data) => {
        await OutlineSyncHelper.syncOutlineDb(this.prisma, dto.documentId, data, {
          headings,
          headingsPercent,
          outlineCode,
          wordCountStr,
        });
      },
    });
  }

  async modifyOutline(userId: string, dto: ModifyOutlineAiDto) {
    const doc = await this.ensureDocument(userId, dto.documentId);
    const prop = await this.resolveSelectedProposal(dto.documentId);
    const finalProposal = OutlineSyncHelper.resolveFinalProposal(prop, dto.finalProposal);
    const existing = await this.prisma.outline.findUnique({ where: { documentId: dto.documentId } });
    const currentStructure = dto.outline || (existing?.structure as Record<string, unknown>) || {};

    const headings = dto.headings || (existing?.headings as any[]) || [];
    const headingsPercent = dto.headingsPercent || (existing?.headingsPercent as number[]) || [];
    const newHeadings = dto.newHeadings || headings;
    const newHeadingsPercent = dto.newHeadingsPercent || headingsPercent;

    return this.dispatcher.dispatch({
      userId,
      documentId: dto.documentId,
      sessionId: dto.sessionId,
      eventType: 'modify_outline',
      errorCode: 627,
      dto,
      kafkaData: {
        field: dto.field || doc.field || 'General',
        domain:
          dto.domain ||
          (Array.isArray(doc.domains) && doc.domains.length > 0
            ? String(doc.domains[0])
            : typeof (doc as any).domains === 'string' && (doc as any).domains
              ? (doc as any).domains
              : (doc as any).domain || 'General'),
        research_type: dto.researchType || { type: 0, reason: doc.researchType || 'quantitative' },
        final_proposal: finalProposal,
        headings,
        headings_percent: headingsPercent,
        new_headings: newHeadings,
        new_headings_percent: newHeadingsPercent,
        outline: currentStructure,
        instruction: dto.instruction,
      },
      onSuccess: async (data) => {
        await OutlineSyncHelper.syncOutlineDb(this.prisma, dto.documentId, data, {
          headings: newHeadings,
          headingsPercent: newHeadingsPercent,
        });
      },
    });
  }

  async generateOutlineWithRefs(userId: string, dto: GenerateOutlineWithRefsDto) {
    await this.ensureDocument(userId, dto.documentId);
    const prop = await this.resolveSelectedProposal(dto.documentId);
    const finalProposal = OutlineSyncHelper.resolveFinalProposal(prop, dto.finalProposal);
    const existing = await this.prisma.outline.findUnique({ where: { documentId: dto.documentId } });
    const currentOutline = dto.outline || (existing?.structure as Record<string, unknown>) || {};
    const outlineCode = dto.outlineCode || (existing?.outlineCode as string[]) || [];

    const processedDocs =
      dto.processedDocs && dto.processedDocs.length > 0
        ? dto.processedDocs
        : await OutlineSyncHelper.loadProcessedDocs(this.prisma, dto.documentId, dto.referenceIds);

    return this.dispatcher.dispatch({
      userId,
      documentId: dto.documentId,
      sessionId: dto.sessionId,
      eventType: 'generate_outline_with_refs',
      errorCode: 626,
      dto,
      kafkaData: {
        outline: currentOutline,
        outline_code: outlineCode,
        final_proposal: finalProposal,
        processed_docs: processedDocs,
      },
      onSuccess: async (data) => {
        await OutlineSyncHelper.syncOutlineDb(this.prisma, dto.documentId, data);
      },
    });
  }

  async updateUserRefs(userId: string, dto: UpdateUserRefsDto) {
    await this.ensureDocument(userId, dto.documentId);
    const documents =
      dto.documents && dto.documents.length > 0
        ? dto.documents
        : await OutlineSyncHelper.loadProcessedDocs(this.prisma, dto.documentId, dto.referenceIds);

    return this.dispatcher.dispatch({
      userId,
      documentId: dto.documentId,
      sessionId: dto.sessionId,
      eventType: 'update_user_refs',
      errorCode: 628,
      dto,
      kafkaData: { documents },
      onSuccess: async () => {
        if (dto.referenceIds && dto.referenceIds.length > 0) {
          for (const refId of dto.referenceIds) {
            await this.prisma.documentReference.upsert({
              where: {
                documentId_referenceId: {
                  documentId: dto.documentId,
                  referenceId: refId,
                },
              },
              update: {},
              create: { documentId: dto.documentId, referenceId: refId },
            });
          }
        }
        if (dto.documents && dto.documents.length > 0) {
          await OutlineSyncHelper.applyHeadingRefs(this.prisma, dto.documentId, dto.documents);
        }
      },
    });
  }

  async ensureDocument(userId: string, id: string) {
    const doc = await this.prisma.document.findFirst({ where: { id, userId } });
    if (!doc) throw new NotFoundException('Document not found');
    return doc;
  }

  private async resolveSelectedProposal(documentId: string, proposalId?: string) {
    if (proposalId) {
      return this.prisma.proposal.findFirst({ where: { id: proposalId, documentId } });
    }
    const selected = await this.prisma.proposal.findFirst({
      where: { documentId, isSelected: true },
    });
    return selected || this.prisma.proposal.findFirst({ where: { documentId } });
  }
}
