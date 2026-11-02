import { PrismaService } from '../../../database/prisma.service';

/**
 * Helper tự động đọc kết quả phân tích OLS từ bảng Dataset cho chuyen_de_3 (BE-032).
 */
export class WriteContentOlsHelper {
  static async resolveOlsData(prisma: PrismaService, documentId: string) {
    const dataset = await prisma.dataset.findFirst({
      where: { documentId },
      include: { variables: true },
      orderBy: { createdAt: 'desc' },
    });
    if (!dataset) return null;

    const data = {
      dataset_url: dataset.filePath || '',
      data_raw: dataset.rawData || null,
      analysis_results: dataset.analysisResults || null,
    };

    const variables = dataset.variables.map((v) => ({
      name: v.variableName,
      code: v.variableCode || v.variableName,
      variable_name: v.variableName,
      variable_code: v.variableCode || v.variableName,
      role: v.variableRole || 'independent',
      variable_type: v.variableType || 'observed',
      type: v.variableType || 'observed',
      scale: v.scale,
      unit: v.unit,
    }));

    return {
      data,
      variables,
      analyzeLog:
        dataset.analyzeLog ||
        (dataset.analysisResults
          ? JSON.stringify(dataset.analysisResults)
          : undefined),
      detailedLogs: dataset.detailedLogs as Array<Record<string, unknown>> | undefined,
      generatedFiles: dataset.generatedFiles as Record<string, string> | undefined,
    };
  }
}
