import { Logger } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { PrismaService } from '../../../database/prisma.service';

const PROPOSE_ERROR_CODE = 637;

export class ProposedMethodHelper {
  /**
   * Lưu hoặc cập nhật `final_proposal` từ AI response vào bảng `ProposedMethod`.
   */
  static async saveProposedMethod(
    prisma: PrismaService,
    documentId: string,
    proposal: Record<string, unknown>,
    logger: Logger,
  ): Promise<void> {
    const data = {
      researchModel: (proposal.final_model ?? null) as Prisma.InputJsonValue,
      hypotheses: (proposal.hypothesis ?? null) as Prisma.InputJsonValue,
      variables: (proposal.variables ?? null) as Prisma.InputJsonValue,
      surveyQuestions: (proposal.survey_questions ??
        null) as Prisma.InputJsonValue,
      questions: (proposal.questions ?? null) as Prisma.InputJsonValue,
    };

    try {
      const existing = await prisma.proposedMethod.findFirst({
        where: { documentId },
        select: { id: true },
      });

      if (existing) {
        await prisma.proposedMethod.update({
          where: { id: existing.id },
          data,
        });
        return;
      }

      await prisma.proposedMethod.create({ data: { documentId, ...data } });
    } catch (error: unknown) {
      logger.error(
        `Lưu ProposedMethod thất bại (${PROPOSE_ERROR_CODE}) cho ${documentId}: ${
          (error as Error).message
        }`,
      );
    }
  }

  /**
   * Khôi phục `final_proposal` từ DB nếu client không truyền trong lượt resume.
   */
  static async resolveProposal(
    prisma: PrismaService,
    documentId: string,
    providedProposal?: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    if (providedProposal && Object.keys(providedProposal).length > 0) {
      return providedProposal;
    }

    const existing = await prisma.proposedMethod.findFirst({
      where: { documentId },
    });

    if (!existing) {
      return {};
    }

    const resolved: Record<string, unknown> = {};
    if (existing.researchModel) resolved.final_model = existing.researchModel;
    if (existing.hypotheses) resolved.hypothesis = existing.hypotheses;
    if (existing.variables) resolved.variables = existing.variables;
    if (existing.surveyQuestions) {
      resolved.survey_questions = existing.surveyQuestions;
    }
    if (existing.questions) resolved.questions = existing.questions;

    return resolved;
  }
}
