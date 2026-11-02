import { Prisma } from '@prisma/client';
import { PrismaService } from '../../../database/prisma.service';
import { normalizeAcademicLevel } from '../../../common/helpers/academic-level.helper';

export class OutlineSyncHelper {
  static resolveFinalProposal(proposal: any, customProposal?: any) {
    if (customProposal && typeof customProposal === 'object') {
      return {
        ...customProposal,
        level: normalizeAcademicLevel(customProposal.level),
      };
    }
    if (!proposal) return {};
    return {
      title: proposal.title || '',
      problem_statement: proposal.problemStatement || '',
      motivation: proposal.motivation || '',
      research_gap: proposal.researchGap || '',
      level: normalizeAcademicLevel(proposal.level),
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
      {
        heading: 'Chương 1: Mở đầu',
        title: 'Chương 1: Mở đầu',
        overview: 'Tổng quan nội dung chương.',
        word_count: '1000-1500',
        percent: 15,
      },
      {
        heading: 'Chương 2: Cơ sở lý thuyết',
        title: 'Chương 2: Cơ sở lý thuyết',
        overview: 'Tổng quan nội dung chương.',
        word_count: '1000-1500',
        percent: 25,
      },
      {
        heading: 'Chương 3: Phương pháp nghiên cứu',
        title: 'Chương 3: Phương pháp nghiên cứu',
        overview: 'Tổng quan nội dung chương.',
        word_count: '1000-1500',
        percent: 25,
      },
      {
        heading: 'Chương 4: Kết quả và thảo luận',
        title: 'Chương 4: Kết quả và thảo luận',
        overview: 'Tổng quan nội dung chương.',
        word_count: '1000-1500',
        percent: 25,
      },
      {
        heading: 'Chương 5: Kết luận',
        title: 'Chương 5: Kết luận',
        overview: 'Tổng quan nội dung chương.',
        word_count: '1000-1500',
        percent: 10,
      },
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
        return {
          heading: h,
          title: h,
          overview: 'Tổng quan nội dung chương.',
          word_count: '1000-1500',
          percent: p,
        };
      }
      if (h && typeof h === 'object') {
        // AI server đọc key `heading` (KeyError 617 nếu thiếu); `title` giữ lại
        // để tương thích DB/FE và các bản AI cũ. AI cũng đọc `overview` +
        // `word_count` của từng heading (quyết định 2026-11-01).
        const title = h.title || h.heading || h.name || `Chương ${idx + 1}`;
        return {
          ...h,
          heading: h.heading || title,
          title,
          overview: h.overview || h.description || 'Tổng quan nội dung chương.',
          word_count: h.word_count || '1000-1500',
          percent: typeof h.percent === 'number' ? h.percent : p,
        };
      }
      return {
        heading: `Chương ${idx + 1}`,
        title: `Chương ${idx + 1}`,
        overview: 'Tổng quan nội dung chương.',
        word_count: '1000-1500',
        percent: p,
      };
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
    if (docRefs.length === 0) {
      const doc = prisma.document?.findFirst
        ? await prisma.document.findFirst({ where: { id: documentId } })
        : null;
      return [
        {
          id: 'ref-default',
          title: doc?.title || 'Tài liệu tham khảo cơ sở',
          authors: 'Ban biên tập',
          journal: 'Khoa học tổng hợp',
          year: new Date().getFullYear(),
          url: '',
          open_access: true,
          q: 'Q1',
          abstract: 'Tổng quan tài liệu nghiên cứu đề cương.',
          user_language_title: doc?.title || 'Tài liệu tham khảo cơ sở',
        },
      ];
    }
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

  static async applyHeadingRefs(
    prisma: PrismaService,
    documentId: string,
    mappings: any[],
  ) {
    const outline = await prisma.outline.findUnique({ where: { documentId } });
    if (!outline || !outline.structure) return;

    let struct: any = outline.structure;
    if (typeof struct === 'string') {
      try { struct = JSON.parse(struct); } catch { return; }
    }

    const items = Array.isArray(struct?.outline)
      ? struct.outline
      : Array.isArray(struct)
        ? struct
        : null;
    if (!items) return;

    for (const mapping of mappings) {
      const headingId = mapping.heading_id || mapping.headingId || mapping.id;
      const refs = mapping.references || mapping.documents || [];
      if (!headingId) continue;

      for (const heading of items) {
        if (
          heading.id === headingId ||
          heading.code === headingId ||
          heading.title === headingId
        ) {
          heading.references = refs;
        }
      }
    }

    await prisma.outline.update({
      where: { documentId },
      data: { structure: struct as Prisma.InputJsonValue },
    });
  }
}
