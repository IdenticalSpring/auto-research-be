import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  ParseUUIDPipe,
  Post,
  UseGuards,
} from '@nestjs/common';
import { CurrentUser } from '../../../common/decorators/current-user.decorator';
import { JwtAuthGuard } from '../../../common/guards/jwt-auth.guard';
import { DeleteReportDto } from './dto/delete-report.dto';
import { EditReportDto } from './dto/edit-report.dto';
import { TranslateReportDto } from './dto/translate-report.dto';
import { DeleteReportAiService } from './delete-report-ai.service';
import { EditReportAiService } from './edit-report-ai.service';
import { ReportService } from './report.service';
import { TranslateAiService } from './translate-ai.service';

@Controller('documents')
@UseGuards(JwtAuthGuard)
export class DocumentReportController {
  constructor(
    private readonly reportService: ReportService,
    private readonly translateAiService: TranslateAiService,
    private readonly deleteReportAiService: DeleteReportAiService,
    private readonly editReportAiService: EditReportAiService,
  ) {}

  @Get(':documentId/report')
  findByDocument(
    @CurrentUser('sub') userId: string,
    @Param('documentId') documentId: string,
  ) {
    return this.reportService.findByDocument(userId, documentId);
  }

  /**
   * Event `translate_report` — AI dịch và ghi vào Mongo, BE đọc lại
   * `translatedContent` / `translatedOutline` rồi trả FE.
   */
  @Post(':documentId/translate-report')
  @HttpCode(HttpStatus.OK)
  translate(
    @CurrentUser('sub') userId: string,
    @Param('documentId', ParseUUIDPipe) documentId: string,
    @Body() dto: TranslateReportDto,
  ) {
    return this.translateAiService.translate(userId, documentId, dto);
  }

  /**
   * Event `edit_report` — chỉnh sửa đoạn văn theo instruction (mã lỗi 630).
   */
  @Post(':documentId/edit-report')
  @HttpCode(HttpStatus.OK)
  editReport(
    @CurrentUser('sub') userId: string,
    @Param('documentId', ParseUUIDPipe) documentId: string,
    @Body() dto: EditReportDto,
  ) {
    return this.editReportAiService.edit(userId, documentId, dto);
  }

  /**
   * Event `delete_report` — xoá best-effort nhiều bài viết (Mongo `reports_refs`
   * + Qdrant collection + checkpoint Redis). BE tự xác minh lại sau khi AI báo xong.
   */
  @Post('delete-report')
  @HttpCode(HttpStatus.OK)
  deleteReports(
    @CurrentUser('sub') userId: string,
    @Body() dto: DeleteReportDto,
  ) {
    return this.deleteReportAiService.delete(userId, dto);
  }
}
