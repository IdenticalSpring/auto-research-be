import { IsBoolean, IsOptional, IsString } from 'class-validator';

/**
 * DTO cho lượt Resume của chuyen_de_1 (BE-030).
 * Sau khi Turn 1 ngắt với { __interrupt__: true, chapter_break: true },
 * FE gọi API resume để AI viết tiếp cho đến khi hoàn thành.
 */
export class ResumeChapterDto {
  /** Cờ yêu cầu AI tiếp tục viết chương kế tiếp */
  @IsOptional()
  @IsBoolean()
  resumeCharacter?: boolean;

  /** Ghi đè thread_id nếu có, mặc định dùng thread_id hoặc document_id của Job */
  @IsOptional()
  @IsString()
  threadId?: string;
}
