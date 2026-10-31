import { Prisma } from '@prisma/client';
import { PrismaService } from '../../../database/prisma.service';

export class OutlineSyncHelper {
  static resolveFinalProposal(proposal: any, customProposal?: any) {
    if (customProposal && typeof customProposal === 'object') {
      return customProposal;
    }
    if (!proposal) return {};
    return {
      title: proposal.title || '',
      problem_statement: proposal.problemStatement || '',
      motivation: proposal.motivation || '',
      research_gap: proposal.researchGap || '',
      final_model: proposal.finalModel || null,
      hypothesis: proposal.hypothesis || null,
      variables: proposal.variables || null,
      survey_questions: proposal.surveyQuestions || null,
      questions: proposal.questions || null,
    };
  }

  static resolveHeadingsAndPercents(
    headings?: any[],
    percents?: number[],
    outlineCode?: string[],
  ) {
    const defaultHeadings = [
      { title: 'Chương 1: Mở đầu', percent: 15 },
      { title: 'Chương 2: Cơ sở lý thuyết', percent: 25 },
      { title: 'Chương 3: Phương pháp nghiên cứu', percent: 25 },
      { title: 'Chương 4: Kết quả và thảo luận', percent: 25 },
      { title: 'Chương 5: Kết luận', percent: 10 },
    ];
    const rawHeadings = headings && headings.length > 0 ? headings : defaultHeadings;
    let finalPercents = percents;
    if (!finalPercents || finalPercents.length !== rawHeadings.length) {
      if (rawHeadings.length === 5) {
        finalPercents = [15, 25, 25, 25, 10];
      } else {
        const avg = Math.floor(100 / rawHeadings.length);
        finalPercents = rawHeadings.map((_, i) =>
          i === rawHeadings.length - 1 ? 100 - avg * (rawHeadings.length - 1) : avg,
        );
      }
    }

    const finalHeadings = rawHeadings.map((h, idx) => {
      const p = finalPercents[idx];
      if (typeof h === 'string') {
        return { title: h, percent: p };
      }
      if (h && typeof h === 'object') {
        return {
          title: h.title || h.name || `Chương ${idx + 1}`,
          percent: typeof h.percent === 'number' ? h.percent : p,
          ...h,
        };
      }
      return { title: `Chương ${idx + 1}`, percent: p };
    });

    const finalCode =
      outlineCode && outlineCode.length === finalHeadings.length
        ? outlineCode
        : finalHeadings.map((_, idx) => `chapter_${idx + 1}`);

    return { headings: finalHeadings, headingsPercent: finalPercents, outlineCode: finalCode };
  }

  static async loadProcessedDocs(
    prisma: PrismaService,
    documentId: string,
    referenceIds?: string[],
  ): Promise<any[]> {
    const whereClause: any = { documentId };
    if (referenceIds && referenceIds.length > 0) {
      whereClause.referenceId = { in: referenceIds };
    }
    const docRefs = await prisma.documentReference.findMany({
      where: whereClause,
      include: { reference: true },
    });
    return docRefs.map((dr) => ({
      id: dr.reference.id,
      title: dr.reference.title,
      authors: dr.reference.authors,
      journal: dr.reference.journal,
      year: dr.reference.year,
      url: dr.reference.url,
      open_access: dr.reference.openAccess,
      q: dr.reference.q,
      abstract: dr.reference.abstract,
      user_language_title: dr.reference.userLanguageTitle,
    }));
  }

  static async syncOutlineDb(
    prisma: PrismaService,
    documentId: string,
    data: Record<string, unknown>,
    meta?: {
      headings?: any[];
      headingsPercent?: number[];
      outlineCode?: string[];
      wordCountStr?: string;
    },
  ) {
    const struct = (data.outline || data.structure || data.new_outline || data) as Prisma.InputJsonValue;
    const count = Number(data.total_word_count || data.totalWordCount || 8000);
    const wordCountStr = meta?.wordCountStr || (data.word_count_str as string) || `${count} words`;

    await prisma.outline.upsert({
      where: { documentId },
      update: {
        totalWordCount: count,
        structure: struct,
        wordCountStr,
        headings: meta?.headings ? (meta.headings as Prisma.InputJsonValue) : undefined,
        headingsPercent: meta?.headingsPercent ? (meta.headingsPercent as Prisma.InputJsonValue) : undefined,
        outlineCode: meta?.outlineCode ? (meta.outlineCode as Prisma.InputJsonValue) : undefined,
      },
      create: {
        documentId,
        totalWordCount: count,
        structure: struct,
        wordCountStr,
        headings: meta?.headings ? (meta.headings as Prisma.InputJsonValue) : undefined,
        headingsPercent: meta?.headingsPercent ? (meta.headingsPercent as Prisma.InputJsonValue) : undefined,
        outlineCode: meta?.outlineCode ? (meta.outlineCode as Prisma.InputJsonValue) : undefined,
      },
    });
  }
}
