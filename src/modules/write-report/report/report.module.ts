import { Module } from '@nestjs/common';
import { AuthModule } from '../../auth/auth/auth.module';
import { ReportController } from './report.controller';
import { DocumentReportController } from './document-report.controller';
import { WriteContentStreamController } from './write-content-stream.controller';
import { ReportService } from './report.service';
import { TranslateAiService } from './translate-ai.service';
import { DeleteReportAiService } from './delete-report-ai.service';
import { EditReportAiService } from './edit-report-ai.service';
import { WriteContentAiService } from './write-content-ai.service';

@Module({
  imports: [AuthModule],
  controllers: [
    ReportController,
    DocumentReportController,
    WriteContentStreamController,
  ],
  providers: [
    ReportService,
    WriteContentAiService,
    TranslateAiService,
    DeleteReportAiService,
    EditReportAiService,
  ],
  exports: [
    ReportService,
    WriteContentAiService,
    TranslateAiService,
    DeleteReportAiService,
    EditReportAiService,
  ],
})
export class ReportModule {}
