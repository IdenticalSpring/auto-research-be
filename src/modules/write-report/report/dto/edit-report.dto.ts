import { IsNotEmpty, IsOptional, IsString } from 'class-validator';

export class EditReportDto {
  @IsOptional()
  @IsString()
  sessionId?: string;

  /** Tên tiêu đề / mục đoạn văn cần chỉnh sửa */
  @IsString()
  @IsNotEmpty()
  section_name!: string;

  /** Nội dung đoạn văn hiện tại */
  @IsString()
  @IsNotEmpty()
  content!: string;

  /** Yêu cầu chỉnh sửa của người dùng (ví dụ: 'viết súc tích hơn') */
  @IsString()
  @IsNotEmpty()
  instruction!: string;

  /** Bối cảnh chung hoặc đoạn xung quanh để AI giữ văn phong */
  @IsString()
  @IsOptional()
  context?: string;
}
