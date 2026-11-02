import {
  BadRequestException,
  Body,
  Controller,
  MessageEvent,
  Param,
  Post,
  Sse,
  UseGuards,
} from '@nestjs/common';
import { Observable } from 'rxjs';
import { CurrentUser } from '../../../common/decorators/current-user.decorator';
import { JwtAuthGuard } from '../../../common/guards/jwt-auth.guard';
import { ResumeChapterDto } from './dto/resume-chapter.dto';
import { ResumeDataAnalysisDto } from './dto/resume-data-analysis.dto';
import { WriteContentRequestDto } from './dto/write-content-request.dto';
import { WriteContentAiService } from './write-content-ai.service';

@Controller('reports')
@UseGuards(JwtAuthGuard)
export class WriteContentStreamController {
  constructor(private readonly writeContentAiService: WriteContentAiService) {}

  @Post([':documentId/write-content', 'write-content'])
  startWriteContent(
    @CurrentUser('sub') userId: string,
    @Param('documentId') paramDocId: string | undefined,
    @Body() dto: WriteContentRequestDto,
  ) {
    if (paramDocId) {
      dto.documentId = paramDocId;
    }
    if (!dto.documentId) {
      throw new BadRequestException('documentId is required');
    }
    return this.writeContentAiService.startWriteContent(userId, dto);
  }

  /**
   * BE-030: Multi-turn resume sau khi ngắt chapter_break
   */
  @Post([':documentId/resume', ':documentId/resume-chapter'])
  resumeChapter(
    @CurrentUser('sub') userId: string,
    @Param('documentId') documentId: string,
    @Body() dto: ResumeChapterDto,
  ) {
    return this.writeContentAiService.resumeChapter(userId, documentId, dto);
  }

  /**
   * T-11: job đang treo ở `need_data_analyze` → gửi kết quả phân tích để viết tiếp.
   */
  @Post(':documentId/resume-data-analysis')
  resumeAfterDataAnalysis(
    @CurrentUser('sub') userId: string,
    @Param('documentId') documentId: string,
    @Body() dto: ResumeDataAnalysisDto,
  ) {
    return this.writeContentAiService.resumeAfterDataAnalysis(
      userId,
      documentId,
      dto,
    );
  }

  @Sse(':documentId/write-stream')
  streamWriteContent(
    @CurrentUser('sub') userId: string,
    @Param('documentId') documentId: string,
  ): Promise<Observable<MessageEvent>> {
    return this.writeContentAiService.getWriteStream(userId, documentId);
  }
}
