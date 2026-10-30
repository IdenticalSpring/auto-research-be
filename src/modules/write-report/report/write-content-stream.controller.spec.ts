import { jest } from '@jest/globals';
import { Test, TestingModule } from '@nestjs/testing';
import { of } from 'rxjs';
import { JwtAuthGuard } from '../../../common/guards/jwt-auth.guard';
import { WriteContentStreamController } from './write-content-stream.controller';
import { WriteContentAiService } from './write-content-ai.service';
import { WriteContentRequestDto } from './dto/write-content-request.dto';

describe('WriteContentStreamController', () => {
  let controller: WriteContentStreamController;
  let mockService: any;

  const userId = '00000000-0000-0000-0000-000000000001';
  const documentId = '00000000-0000-0000-0000-000000000002';

  const validDto: WriteContentRequestDto = {
    documentId,
    field: 'Khoa học máy tính',
    domain: 'Trí tuệ nhân tạo',
    referencesStyle: 'APA',
    finalProposal: { title: 'AI in Healthcare' },
    papersSearch: [],
    outline: { sections: ['Mở đầu'] },
    outlineCode: 'chapter_1',
    language: 'vi',
  };

  beforeEach(async () => {
    mockService = {
      startWriteContent: jest.fn<any>().mockResolvedValue({
        jobId: 'job-wc-1',
        streamKey: 'stream-key',
        status: 'started',
      }),
      resumeChapter: jest.fn<any>().mockResolvedValue({
        jobId: 'job-wc-1',
        status: 'resumed',
      }),
      resumeAfterDataAnalysis: jest.fn<any>().mockResolvedValue({
        job_id: 'job-wc-1',
        status: 'writing_after_analysis',
      }),
      getWriteStream: jest.fn<any>().mockResolvedValue(of({ data: 'chunk' })),
    };

    const module: TestingModule = await Test.createTestingModule({
      controllers: [WriteContentStreamController],
      providers: [{ provide: WriteContentAiService, useValue: mockService }],
    })
      .overrideGuard(JwtAuthGuard)
      .useValue({ canActivate: () => true })
      .compile();

    controller = module.get<WriteContentStreamController>(
      WriteContentStreamController,
    );
  });

  describe('startWriteContent', () => {
    it('should delegate to startWriteContent and assign paramDocId if provided', async () => {
      const res = await controller.startWriteContent(userId, documentId, {
        ...validDto,
        documentId: 'other-id',
      });

      expect(res).toEqual({
        jobId: 'job-wc-1',
        streamKey: 'stream-key',
        status: 'started',
      });
      expect(mockService.startWriteContent).toHaveBeenCalledWith(
        userId,
        expect.objectContaining({ documentId }),
      );
    });

    it('should use dto.documentId if paramDocId is undefined', async () => {
      await controller.startWriteContent(userId, undefined, validDto);

      expect(mockService.startWriteContent).toHaveBeenCalledWith(
        userId,
        expect.objectContaining({ documentId }),
      );
    });
  });

  describe('resumeChapter', () => {
    it('should delegate to resumeChapter with userId and documentId', async () => {
      const res = await controller.resumeChapter(userId, documentId, {
        resumeCharacter: true,
      });

      expect(res).toEqual({ jobId: 'job-wc-1', status: 'resumed' });
      expect(mockService.resumeChapter).toHaveBeenCalledWith(
        userId,
        documentId,
        { resumeCharacter: true },
      );
    });
  });

  describe('resumeAfterDataAnalysis', () => {
    it('should delegate to resumeAfterDataAnalysis', async () => {
      const res = await controller.resumeAfterDataAnalysis(userId, documentId, {
        analyzeLog: 'OLS done',
      });

      expect(res).toEqual({ job_id: 'job-wc-1', status: 'writing_after_analysis' });
      expect(mockService.resumeAfterDataAnalysis).toHaveBeenCalledWith(
        userId,
        documentId,
        { analyzeLog: 'OLS done' },
      );
    });
  });

  describe('streamWriteContent', () => {
    it('should delegate to getWriteStream', async () => {
      const res = await controller.streamWriteContent(userId, documentId);
      expect(res).toBeDefined();
      expect(mockService.getWriteStream).toHaveBeenCalledWith(
        userId,
        documentId,
      );
    });
  });
});
