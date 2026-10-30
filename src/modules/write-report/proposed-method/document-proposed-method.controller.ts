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
import { ParsedValueDto } from './dto/parsed-value-ai.dto';
import {
  ProposeMethodResumeDto,
  ProposeMethodStartDto,
} from './dto/propose-method-ai.dto';
import { ParsedValueAiService } from './parsed-value-ai.service';
import { ProposedMethodAiService } from './proposed-method-ai.service';
import { ProposedMethodService } from './proposed-method.service';

@Controller('documents')
@UseGuards(JwtAuthGuard)
export class DocumentProposedMethodController {
  constructor(
    private readonly service: ProposedMethodService,
    private readonly aiService: ProposedMethodAiService,
    private readonly parsedValueService: ParsedValueAiService,
  ) {}

  @Get(':documentId/proposed-methods')
  findAll(
    @CurrentUser('sub') userId: string,
    @Param('documentId') documentId: string,
  ) {
    return this.service.findAll(userId, documentId);
  }

  /** Event `propose_method` — lượt đầu (mã lỗi 637 khi thất bại). */
  @Post(':documentId/propose-method/start')
  @HttpCode(HttpStatus.OK)
  start(
    @CurrentUser('sub') userId: string,
    @Param('documentId', ParseUUIDPipe) documentId: string,
    @Body() dto: ProposeMethodStartDto,
  ) {
    return this.aiService.start(userId, documentId, dto);
  }

  /** Lượt tiếp: gửi câu trả lời của người dùng, giữ nguyên `thread_id`. */
  @Post(':documentId/propose-method/resume')
  @HttpCode(HttpStatus.OK)
  resume(
    @CurrentUser('sub') userId: string,
    @Param('documentId', ParseUUIDPipe) documentId: string,
    @Body() dto: ProposeMethodResumeDto,
  ) {
    return this.aiService.resume(userId, documentId, dto);
  }

  /** Event `parsed_value` — chuẩn hóa câu hỏi khảo sát Likert (mã lỗi 639). */
  @Post(':documentId/parsed-value')
  @HttpCode(HttpStatus.OK)
  parseValue(
    @CurrentUser('sub') userId: string,
    @Param('documentId', ParseUUIDPipe) documentId: string,
    @Body() dto: ParsedValueDto,
  ) {
    return this.parsedValueService.parseValue(userId, documentId, dto);
  }
}
