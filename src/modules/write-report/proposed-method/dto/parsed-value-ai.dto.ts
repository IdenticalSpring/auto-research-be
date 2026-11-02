import { IsArray, IsNotEmpty, IsOptional, IsString } from 'class-validator';

export class ParsedValueDto {
  @IsOptional()
  @IsString()
  sessionId?: string;

  /** Danh sách câu hỏi khảo sát Likert cần chuẩn hóa */
  @IsArray()
  @IsNotEmpty()
  survey_questions!: Array<Record<string, unknown>>;

  /** Danh sách biến (tùy chọn; tự fallback lấy từ DB nếu không truyền) */
  @IsOptional()
  @IsArray()
  variables?: Array<Record<string, unknown>>;
}
