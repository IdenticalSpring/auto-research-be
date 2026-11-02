import { jest } from '@jest/globals';
import { PrismaService } from '../../../database/prisma.service';
import { WriteContentOlsHelper } from './write-content-ols.helper';

type AnyMock = Record<string, any>;
const asPrisma = (p: AnyMock) => p as unknown as PrismaService;

function buildPrisma(dataset: AnyMock | null) {
  return {
    dataset: { findFirst: jest.fn(async () => dataset) as any },
  };
}

describe('WriteContentOlsHelper.resolveOlsData', () => {
  it('should trả null khi document chưa có dataset', async () => {
    const prisma = buildPrisma(null);

    await expect(
      WriteContentOlsHelper.resolveOlsData(asPrisma(prisma), 'doc-1'),
    ).resolves.toBeNull();
  });

  it('should query dataset mới nhất kèm variables', async () => {
    const prisma = buildPrisma(null);

    await WriteContentOlsHelper.resolveOlsData(asPrisma(prisma), 'doc-1');

    expect(prisma.dataset.findFirst).toHaveBeenCalledWith({
      where: { documentId: 'doc-1' },
      include: { variables: true },
      orderBy: { createdAt: 'desc' },
    });
  });

  it('should map variables với fallback code/role/type', async () => {
    const prisma = buildPrisma({
      filePath: 's3://data.csv',
      rawData: { rows: 1 },
      analysisResults: { r2: 0.8 },
      variables: [
        {
          variableName: 'Doanh thu',
          variableCode: null,
          variableRole: null,
          variableType: null,
          scale: 'ratio',
          unit: 'VND',
        },
      ],
    });

    const res = await WriteContentOlsHelper.resolveOlsData(
      asPrisma(prisma),
      'doc-1',
    );

    expect(res).toEqual({
      data: {
        dataset_url: 's3://data.csv',
        data_raw: { rows: 1 },
        analysis_results: { r2: 0.8 },
      },
      variables: [
        {
          name: 'Doanh thu',
          code: 'Doanh thu',
          variable_name: 'Doanh thu',
          variable_code: 'Doanh thu',
          role: 'independent',
          variable_type: 'observed',
          type: 'observed',
          scale: 'ratio',
          unit: 'VND',
        },
      ],
      analyzeLog: JSON.stringify({ r2: 0.8 }),
      detailedLogs: undefined,
      generatedFiles: undefined,
    });
  });

  it('should default dataset_url rỗng và data null khi thiếu field', async () => {
    const prisma = buildPrisma({ variables: [] });

    const res = await WriteContentOlsHelper.resolveOlsData(
      asPrisma(prisma),
      'doc-1',
    );

    expect(res?.data).toEqual({
      dataset_url: '',
      data_raw: null,
      analysis_results: null,
    });
    expect(res?.analyzeLog).toBeUndefined();
    expect(res?.variables).toEqual([]);
  });

  it('should ưu tiên analyzeLog có sẵn và giữ detailedLogs/generatedFiles', async () => {
    const prisma = buildPrisma({
      variables: [],
      analyzeLog: 'log đã lưu',
      analysisResults: { ignored: true },
      detailedLogs: [{ step: 1 }],
      generatedFiles: { chart: 'chart.png' },
    });

    const res = await WriteContentOlsHelper.resolveOlsData(
      asPrisma(prisma),
      'doc-1',
    );

    expect(res?.analyzeLog).toBe('log đã lưu');
    expect(res?.detailedLogs).toEqual([{ step: 1 }]);
    expect(res?.generatedFiles).toEqual({ chart: 'chart.png' });
  });

  it('should giữ variableCode/Role/Type khi đã có', async () => {
    const prisma = buildPrisma({
      variables: [
        {
          variableName: 'ROA',
          variableCode: 'roa',
          variableRole: 'dependent',
          variableType: 'latent',
          scale: 'ratio',
          unit: null,
        },
      ],
    });

    const res = await WriteContentOlsHelper.resolveOlsData(
      asPrisma(prisma),
      'doc-1',
    );

    expect(res?.variables[0]).toEqual(
      expect.objectContaining({
        code: 'roa',
        variable_code: 'roa',
        role: 'dependent',
        variable_type: 'latent',
        type: 'latent',
        unit: null,
      }),
    );
  });
});
