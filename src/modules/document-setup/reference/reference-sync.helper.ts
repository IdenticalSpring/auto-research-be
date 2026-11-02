import { PrismaService } from '../../../database/prisma.service';
import { SearchPapersDto } from './dto/search-papers.dto';
import { PaperItemDto } from './dto/search-papers-response.dto';
import { getAiErrorMessage } from '../../../common/constants/ai-error-codes';
import { getAiModelConfig } from '../../../common/kafka/kafka.constants';

export class ReferenceSyncHelper {
  static async syncFoundPapers(
    prisma: PrismaService,
    documentId: string,
    papers: PaperItemDto[],
  ) {
    for (const p of papers) {
      if (!p.title) continue;
      const authors = Array.isArray(p.authors)
        ? p.authors.join(', ')
        : p.authors || null;
      const year =
        typeof p.year === 'number'
          ? p.year
          : parseInt(String(p.year), 10) || null;
      const openAccess = Boolean(p.openAccess ?? p.open_access);

      let ref = p.url
        ? await prisma.reference.findFirst({ where: { url: p.url } })
        : null;
      if (!ref) {
        ref = await prisma.reference.findFirst({ where: { title: p.title } });
      }
      if (!ref) {
        ref = await prisma.reference.create({
          data: {
            title: p.title,
            userLanguageTitle:
              p.userLanguageTitle || p.user_language_title || null,
            authors,
            journal: p.journal || null,
            year,
            openAccess,
            q: p.q || null,
            url: p.url || null,
            citation: typeof p.citation === 'number' ? p.citation : 0,
          },
        });
      }
      await prisma.documentReference.upsert({
        where: {
          documentId_referenceId: { documentId, referenceId: ref.id },
        },
        create: { documentId, referenceId: ref.id },
        update: {},
      });
    }
  }

  static async resolveProposal(
    prisma: PrismaService,
    dto: SearchPapersDto,
  ) {
    if (dto.proposal) {
      return {
        title: dto.proposal.title,
        problem_statement:
          dto.proposal.problem_statement ||
          dto.proposal.problemStatement ||
          '',
        motivation: dto.proposal.motivation || '',
        research_gap:
          dto.proposal.research_gap || dto.proposal.researchGap || '',
      };
    }
    const p = dto.proposalId
      ? await prisma.proposal.findUnique({ where: { id: dto.proposalId } })
      : await prisma.proposal.findFirst({
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

  static buildSearchPayload(
    userId: string,
    sessionId: string,
    dto: SearchPapersDto,
    doc: any,
    proposal: any,
    jobId: string,
  ) {
    const field = dto.field || doc.field || 'General';
    const domain =
      dto.domain ||
      (Array.isArray(doc.domains) && doc.domains.length > 0
        ? String(doc.domains[0])
        : typeof doc.domains === 'string'
          ? doc.domains
          : 'General');
    const subdomains =
      dto.subdomains ??
      (Array.isArray(doc.subdomains) ? (doc.subdomains as string[]) : []);

    return {
      event_type: 'search_papers',
      model_id: getAiModelConfig().defaultModel,
      user_id: userId,
      document_id: dto.documentId,
      session_id: sessionId,
      language: dto.language || 'vi',
      field,
      domain,
      subdomains,
      query: dto.query,
      country_filter: dto.countryFilter || 'both',
      oa_filter: dto.oaFilter ?? false,
      is_oa: dto.isOa ?? false,
      is_tool: dto.isTool ?? false,
      downloaded_papers: dto.downloadedPapers ?? [],
      cut_off_year_low: dto.cutOffYearLow ?? 2018,
      cut_off_year_high: dto.cutOffYearHigh ?? new Date().getFullYear(),
      search_range: dto.searchRange ?? '',
      use_web_search: dto.useWebSearch ?? false,
      job_id: jobId,
      ...(proposal ? { proposal } : {}),
    };
  }

  static parseSearchError(err: unknown) {
    let errorCode = 610;
    let errorMessage = getAiErrorMessage(610);

    if (err && typeof err === 'object') {
      const errObj = err as Record<string, unknown>;
      if (errObj.error_code) {
        errorCode = Number(errObj.error_code);
        errorMessage =
          typeof errObj.error === 'string'
            ? errObj.error
            : getAiErrorMessage(errorCode);
      } else if (typeof errObj.message === 'string') {
        errorMessage = errObj.message;
      }
    }
    return { errorCode, errorMessage };
  }
}
