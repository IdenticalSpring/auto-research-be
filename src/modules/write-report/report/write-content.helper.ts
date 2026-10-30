import { BadRequestException } from '@nestjs/common';
import { JobStatus, Prisma } from '@prisma/client';
import { PrismaService } from '../../../database/prisma.service';
import { getAiErrorName } from '../../../common/constants/ai-error-codes';
import { WriteContentRequestDto } from './dto/write-content-request.dto';

export class WriteContentHelper {
  static extractMarkdown(
    sectionsMap: Map<string, string>,
    chunk: Record<string, unknown>,
  ): string | null {
    if (chunk.section && typeof chunk.section === 'object') {
      const sectionObj = chunk.section as Record<string, unknown>;
      const name = String(sectionObj.name || sectionObj.section_code || 'Phần');
      const content = String(sectionObj.content || '');
      if (content.trim()) {
        sectionsMap.set(name, content);
      }
      if (sectionsMap.size === 0) return null;
      return Array.from(sectionsMap.entries())
        .map(([sName, sContent]) => (sContent.startsWith('#') ? sContent : `## ${sName}\n\n${sContent}`))
        .join('\n\n');
    }

    if (typeof chunk.content === 'string') return chunk.content;
    if (typeof chunk.full_content === 'string') return chunk.full_content;
    return null;
  }

  static async resolveWritePayload(
    prisma: PrismaService,
    dto: WriteContentRequestDto,
    doc: any,
  ) {
    let finalProposal = dto.finalProposal;
    if (!finalProposal) {
      const selectedProp = await prisma.proposal.findFirst({
        where: { documentId: dto.documentId, isSelected: true },
      });
      const prop = selectedProp || (await prisma.proposal.findFirst({ where: { documentId: dto.documentId } }));
      if (prop) {
        finalProposal = {
          title: prop.title,
          problem_statement: prop.problemStatement,
          motivation: prop.motivation,
          research_gap: prop.researchGap,
          final_model: prop.finalModel,
        };
      } else {
        finalProposal = { title: doc.title || 'Research Report' };
      }
    }

    let rawOutline =
      dto.outline && typeof dto.outline === 'object' && Object.keys(dto.outline).length > 0
        ? (dto.outline as Record<string, unknown>)
        : null;
    let outlineCode = dto.outlineCode;

    if (!rawOutline || (!rawOutline.outline && !rawOutline.headings)) {
      const outlineRec = await prisma.outline.findUnique({
        where: { documentId: dto.documentId },
      });
      if (outlineRec?.structure && typeof outlineRec.structure === 'object') {
        rawOutline = outlineRec.structure as Record<string, unknown>;
      }
      if (!outlineCode && outlineRec?.outlineCode) {
        outlineCode = outlineRec.outlineCode as string[];
      }
    }

    const title = (rawOutline && (rawOutline.title as string)) || doc.title || 'Research Report';
    let outlineList: any[] = [];
    if (rawOutline) {
      if (Array.isArray(rawOutline.outline) && rawOutline.outline.length > 0) {
        outlineList = rawOutline.outline;
      } else if (Array.isArray(rawOutline.headings) && rawOutline.headings.length > 0) {
        outlineList = rawOutline.headings;
      } else if (Array.isArray((rawOutline as any).structure?.headings)) {
        outlineList = (rawOutline as any).structure.headings;
      }
    }

    if (outlineList.length === 0) {
      outlineList = ['1. Mở đầu', '2. Cơ sở lý thuyết', '3. Phương pháp nghiên cứu', '4. Kết quả và thảo luận', '5. Kết luận']
        .map((title, i) => ({ title, chapter: `chapter_${i + 1}` }));
    }

    const normalizedChapters = outlineList.map((item: any, idx: number) => {
      const code = item.chapter || item.section_code || item.outline_code || `chapter_${idx + 1}`;
      const name = item.name || item.title || `Chương ${idx + 1}`;
      const rawSecs = Array.isArray(item.sections) && item.sections.length > 0
        ? item.sections : Array.isArray(item.subheadings) && item.subheadings.length > 0
          ? item.subheadings : [];
      let sections = rawSecs.map((s: any, sIdx: number) => {
        const secTitle = s.subheading || s.name || s.title || `Tiểu mục ${idx + 1}.${sIdx + 1}`;
        return {
          section_code: s.section_code || s.code || `${idx + 1}.${sIdx + 1}`,
          subheading: secTitle,
          name: secTitle,
          title: secTitle,
          detail_description: s.detail_description || s.description || s.overview || '',
          subheading_word_count: s.subheading_word_count || '500-750',
          description: s.description || s.overview || '',
          refs: Array.isArray(s.refs) ? s.refs : [],
        };
      });
      if (sections.length === 0) {
        sections = [{
          section_code: `${idx + 1}.1`,
          subheading: `${name} - Tổng quan`,
          name: `${name} - Tổng quan`,
          title: `${name} - Tổng quan`,
          detail_description: item.overview || item.description || '',
          subheading_word_count: '500-750',
          description: item.overview || item.description || '',
          refs: Array.isArray(item.referenceIds) ? item.referenceIds : [],
        }];
      }
      return {
        ...item,
        code,
        chapter: code,
        section_code: code,
        outline_code: code,
        heading: name,
        title: name,
        name,
        overview: item.overview || item.description || 'Tổng quan nội dung chương.',
        word_count: item.word_count || '1000-1500',
        sections,
        subheadings: sections,
      };
    });

    const outline: Record<string, unknown> = {
      ...(rawOutline || {}), title, outline: normalizedChapters, headings: normalizedChapters,
    };

    let rawCodes: string[] = [];
    if (Array.isArray(outlineCode)) {
      rawCodes = outlineCode.map(String);
    } else if (typeof outlineCode === 'string' && outlineCode.trim().length > 0) {
      rawCodes = outlineCode.toLowerCase() === 'all'
        ? normalizedChapters.map((c: any) => c.chapter)
        : [outlineCode.trim()];
    } else {
      rawCodes = normalizedChapters.map((c: any) => c.chapter);
    }

    const finalOutlineCode = rawCodes.map((codeStr: string) => {
      const match = normalizedChapters.find(
        (c: any) => c.chapter === codeStr || c.section_code === codeStr || c.id === codeStr,
      );
      if (match) return match.chapter;
      const numMatch = codeStr.match(/^\d+/);
      if (numMatch) {
        const idx = parseInt(numMatch[0], 10) - 1;
        if (idx >= 0 && idx < normalizedChapters.length) return normalizedChapters[idx].chapter;
      }
      return `chapter_${codeStr.replace(/\D/g, '') || '1'}`;
    });

    let papersSearch = dto.papersSearch;
    if (!papersSearch || papersSearch.length === 0) {
      const docRefs = await prisma.documentReference.findMany({
        where: { documentId: dto.documentId },
        include: { reference: true },
        take: 30,
      });
      papersSearch = docRefs.map((dr) => ({
        id: dr.reference.id, title: dr.reference.title,
        authors: dr.reference.authors, journal: dr.reference.journal,
        year: dr.reference.year, url: dr.reference.url,
      }));
    }

    const domain = dto.domain ||
      (Array.isArray(doc.domains) && doc.domains.length > 0
        ? String(doc.domains[0]) : typeof doc.domains === 'string' ? doc.domains : 'General');

    return {
      finalProposal, outline, outlineCode: finalOutlineCode, papersSearch,
      field: dto.field || doc.field || 'General', domain,
    };
  }

  static async handleChunk(
    prisma: PrismaService,
    jobId: string,
    chunk: Record<string, unknown>,
    sectionsMap: Map<string, string>,
    documentId: string,
  ) {
    const threadId = (chunk.thread_id as string) || undefined;
    const markdown = WriteContentHelper.extractMarkdown(sectionsMap, chunk);

    if (markdown) {
      await prisma.report.upsert({
        where: { documentId },
        update: { contentMarkdown: markdown },
        create: { documentId, contentMarkdown: markdown },
      });
    }

    if (chunk.chapter_break === true || chunk.__interrupt__ === true) {
      await prisma.job.update({
        where: { id: jobId },
        data: { status: JobStatus.suspended, resumeStep: 'chapter_break', threadId, result: chunk as unknown as Prisma.InputJsonValue },
      });
      return { isSuspended: true, reason: 'chapter_break' };
    }

    if (chunk.need_data_analyze === true) {
      await prisma.job.update({
        where: { id: jobId },
        data: { status: JobStatus.suspended, resumeStep: 'need_data_analyze', threadId, result: chunk as unknown as Prisma.InputJsonValue },
      });
      return { isSuspended: true, reason: 'need_data_analyze' };
    }

    if (chunk.need_propose_method === true) {
      await prisma.job.update({
        where: { id: jobId },
        data: { status: JobStatus.suspended, resumeStep: 'need_propose_method', threadId, result: chunk as unknown as Prisma.InputJsonValue },
      });
      return { isSuspended: true, reason: 'need_propose_method' };
    }

    if (chunk.has_finished === true) {
      await prisma.job.update({
        where: { id: jobId },
        data: { status: JobStatus.finished, threadId, result: chunk as unknown as Prisma.InputJsonValue },
      });
      return { isFinished: true };
    }

    return { inProgress: true };
  }

  static async failJob(prisma: PrismaService, jobId: string, code: number, msg: string): Promise<never> {
    await prisma.job.update({
      where: { id: jobId },
      data: { status: JobStatus.error, errorCode: String(code), errorMessage: msg },
    });
    throw new BadRequestException({ statusCode: 400, errorCode: code, error: getAiErrorName(code), message: msg });
  }
}
