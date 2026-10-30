import { jest } from '@jest/globals';
import { INestApplication } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { of } from 'rxjs';
import { TimeoutManagerService } from '../src/common/services/timeout-manager.service';
import { StreamingManagerService } from '../src/common/streaming/streaming-manager.service';
import { PrismaService } from '../src/database/prisma.service';
import { KafkaProducerService } from '../src/messaging/kafka-producer.service';
import { RequestCorrelationService } from '../src/messaging/request-correlation.service';
import { OutlineAiDispatcher } from '../src/modules/document-setup/outline/outline-ai.dispatcher';
import { OutlineAiService } from '../src/modules/document-setup/outline/outline-ai.service';
import { ProposalAiDispatcher } from '../src/modules/document-setup/proposal/proposal-ai.dispatcher';
import { ProposalDetailsService } from '../src/modules/document-setup/proposal/proposal-details.service';
import { AnalyzeAiService } from '../src/modules/write-report/data-analysis/analyze-ai.service';
import { GeneratedSlideAiService } from '../src/modules/write-report/generated-slide/generated-slide-ai.service';
import { WriteContentAiService } from '../src/modules/write-report/report/write-content-ai.service';

describe('E2E Pipeline 5 Steps (BE-049)', () => {
  let app: INestApplication;
  let proposalService: ProposalDetailsService;
  let outlineService: OutlineAiService;
  let analyzeService: AnalyzeAiService;
  let writeContentService: WriteContentAiService;
  let slideService: GeneratedSlideAiService;

  const prismaMock: Record<string, any> = {
    document: {
      findFirst: jest.fn(() =>
        Promise.resolve({
          id: 'doc-1',
          userId: 'user-1',
          field: 'Kinh tế',
          domains: ['Kinh tế số'],
        }),
      ),
      findUnique: jest.fn(() =>
        Promise.resolve({
          id: 'doc-1',
          userId: 'user-1',
          field: 'Kinh tế',
          domains: ['Kinh tế số'],
        }),
      ),
      update: jest.fn(() => Promise.resolve({ id: 'doc-1' })),
    },
    job: {
      create: jest.fn(() => Promise.resolve({ id: 'job-1' })),
      update: jest.fn(() => Promise.resolve({})),
    },
    proposal: {
      findFirst: jest.fn(() =>
        Promise.resolve({
          id: 'prop-1',
          documentId: 'doc-1',
          title: 'Tác động của thương mại điện tử',
          isSelected: true,
        }),
      ),
      create: jest.fn(() => Promise.resolve({ id: 'prop-1' })),
      update: jest.fn(() => Promise.resolve({})),
    },
    outline: {
      findUnique: jest.fn(() =>
        Promise.resolve({ id: 'out-1', documentId: 'doc-1', structure: {} }),
      ),
      upsert: jest.fn(() => Promise.resolve({ id: 'out-1' })),
    },
    documentReference: {
      findMany: jest.fn(() => Promise.resolve([])),
    },
    dataset: {
      findFirst: jest.fn(() =>
        Promise.resolve({ id: 'ds-1', documentId: 'doc-1' }),
      ),
      findUnique: jest.fn(() =>
        Promise.resolve({ id: 'ds-1', documentId: 'doc-1' }),
      ),
      update: jest.fn(() => Promise.resolve({})),
    },
    datasetVariable: {
      findMany: jest.fn(() => Promise.resolve([])),
      createMany: jest.fn(() => Promise.resolve({ count: 1 })),
      deleteMany: jest.fn(() => Promise.resolve({ count: 0 })),
    },
    report: {
      findUnique: jest.fn(() =>
        Promise.resolve({
          documentId: 'doc-1',
          contentMarkdown: '# Báo cáo nghiên cứu',
        }),
      ),
      upsert: jest.fn(() => Promise.resolve({})),
    },
    generatedSlide: {
      create: jest.fn(() => Promise.resolve({ id: 'slide-1' })),
      update: jest.fn(() => Promise.resolve({})),
    },
  };

  const producerMock = {
    publish: jest.fn(() => Promise.resolve()),
  };

  const correlationMock = {
    buildCorrelationKey: jest.fn(
      (k: any) =>
        `${k.userId}:${k.documentId || k.convId}:${k.sessionId}:${k.eventType}`,
    ),
    registerPending: jest.fn(),
    clearPending: jest.fn(),
    getStreamObservable: jest.fn(() =>
      of({
        has_finished: true,
        pptx_url: 'https://minio.local/slide.pptx',
        progress: 100,
      }),
    ),
  };

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [ConfigModule.forRoot({ isGlobal: true })],
      providers: [
        ProposalDetailsService,
        ProposalAiDispatcher,
        OutlineAiService,
        OutlineAiDispatcher,
        AnalyzeAiService,
        WriteContentAiService,
        GeneratedSlideAiService,
        StreamingManagerService,
        TimeoutManagerService,
        { provide: PrismaService, useValue: prismaMock },
        { provide: KafkaProducerService, useValue: producerMock },
        { provide: RequestCorrelationService, useValue: correlationMock },
      ],
    }).compile();

    app = moduleFixture.createNestApplication();
    await app.init();

    proposalService = moduleFixture.get(ProposalDetailsService);
    outlineService = moduleFixture.get(OutlineAiService);
    analyzeService = moduleFixture.get(AnalyzeAiService);
    writeContentService = moduleFixture.get(WriteContentAiService);
    slideService = moduleFixture.get(GeneratedSlideAiService);
  });

  afterAll(async () => {
    await app.close();
  });

  it('Bước 1: Tạo đề tài (generate_title_description)', async () => {
    (correlationMock.registerPending as any).mockResolvedValueOnce({
      final_proposal: {
        title: 'Tác động của thương mại điện tử',
        description: 'Mô tả chi tiết đề tài',
      },
    });

    const res = await proposalService.generateTitleDescription('user-1', {
      documentId: 'doc-1',
      title: 'Tác động của thương mại điện tử',
      domain: 'Kinh tế số',
      field: 'Kinh tế',
    } as any);

    expect(res).toBeDefined();
    expect(res.final_proposal).toBeDefined();
    expect(res.final_proposal.title).toBe('Tác động của thương mại điện tử');
  });

  it('Bước 2: Đề cương chi tiết (generate_outline)', async () => {
    (correlationMock.registerPending as any).mockResolvedValueOnce({
      outline: {
        sections: [
          { section_name: 'Chương 1: Tổng quan', content: '' },
          { section_name: 'Chương 2: Thực trạng', content: '' },
        ],
      },
    });

    const res = await outlineService.generateOutline('user-1', {
      documentId: 'doc-1',
      title: 'Tác động của thương mại điện tử',
      domain: 'Kinh tế số',
      field: 'Kinh tế',
      outlineType: 'full',
    } as any);

    expect(res).toBeDefined();
    expect(res.outline).toBeDefined();
    expect(res.outline.sections).toHaveLength(2);
  });

  it('Bước 3: Phân tích hồi quy OLS (analyzer)', async () => {
    (correlationMock.registerPending as any).mockResolvedValueOnce({
      has_finished: true,
      report_html: '<table>OLS Regression Results</table>',
      analysis_results: { r_squared: 0.85, f_statistic: 45.2 },
      charts: [{ chart_type: 'scatter', title: 'Hồi quy' }],
    });

    const res = await analyzeService.analyze('user-1', 'ds-1', {
      finalProposal: { title: 'Tác động thương mại điện tử' },
      data: {
        tuoi: [22, 25, 28, 30],
        thu_nhap: [10.0, 15.0, 18.0, 22.0],
        chi_tieu: [4.0, 6.0, 7.5, 9.0],
      },
      variables: [
        {
          name: 'tuoi',
          code: 'tuoi',
          role: 'independent',
          scale: 'ratio',
          variable_type: 'observed',
        },
        {
          name: 'thu_nhap',
          code: 'thu_nhap',
          role: 'independent',
          scale: 'ratio',
          variable_type: 'observed',
        },
        {
          name: 'chi_tieu',
          code: 'chi_tieu',
          role: 'dependent',
          scale: 'ratio',
          variable_type: 'observed',
        },
      ],
    } as any);

    expect(res).toBeDefined();
    expect(res.status).toBe('finished');
    expect(res.analysis_results).toBeDefined();
    expect((res.analysis_results as any).r_squared).toBe(0.85);
  });

  it('Bước 4: Soạn thảo chuyên đề (chuyen_de_1 / write_content)', async () => {
    (correlationMock.registerPending as any).mockResolvedValueOnce({
      has_finished: true,
      section_name: 'Chương 1',
      content: 'Nội dung cơ sở lý luận chi tiết...',
    });

    const res = await writeContentService.startWriteContent('user-1', {
      documentId: 'doc-1',
      eventType: 'chuyen_de_1',
      finalProposal: { title: 'Tác động thương mại điện tử' },
      outline: { sections: [{ section_name: 'Chương 1' }] },
      domain: 'Kinh tế số',
      field: 'Kinh tế',
    } as any);

    expect(res).toBeDefined();
    expect(res.status).toBe('started');
    expect(res.jobId).toBeDefined();
  });

  it('Bước 5: Tải Slide PPTX (gen_slides)', async () => {
    (correlationMock.registerPending as any).mockResolvedValueOnce({
      has_finished: true,
      pptx_url: 'https://minio.local/slide.pptx',
      progress: 100,
    });

    const res = await slideService.generateSlides('user-1', {
      documentId: 'doc-1',
      rawReport: '# Báo cáo nghiên cứu',
      userPreference: { numSlides: 10 },
    } as any);

    expect(res).toBeDefined();
    expect(res.status).toBe('started');
    expect(res.slideId).toBe('slide-1');
  });
});
