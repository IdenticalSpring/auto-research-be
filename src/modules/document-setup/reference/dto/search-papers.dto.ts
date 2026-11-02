import {
  IsArray,
  IsBoolean,
  IsIn,
  IsInt,
  IsNotEmpty,
  IsOptional,
  IsString,
  Min,
  Max,
} from 'class-validator';
import { Type } from 'class-transformer';

export class SearchPapersDto {
  @IsString()
  @IsNotEmpty()
  documentId!: string;

  @IsString()
  @IsNotEmpty()
  field!: string;

  @IsOptional()
  @IsString()
  domain?: string;

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  subdomains?: string[];

  @IsString()
  @IsNotEmpty()
  query!: string;

  /**
   * AI server quyết định (2026-11-01): `country_filter` là **string enum**
   * `both | in | out` (AI + OpenAlex filter dùng dạng này). KHÔNG gửi list.
   */
  @IsOptional()
  @IsIn(['both', 'in', 'out'], {
    message: 'country_filter must be one of both|in|out',
  })
  countryFilter?: 'both' | 'in' | 'out' = 'both';

  @IsOptional()
  @IsBoolean()
  oaFilter?: boolean;

  @IsOptional()
  @IsBoolean()
  isOa?: boolean;

  @IsOptional()
  @IsBoolean()
  isTool?: boolean;

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  downloadedPapers?: string[];

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1900)
  cutOffYearLow?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Max(2100)
  cutOffYearHigh?: number;

  /**
   * AI Q2 (2026-11-01): chỉ nhận `""` hoặc `"all"` — cả hai đều = không nối gì
   * vào query (`search_papers.py:164-166`). Giá trị khác bị nối nguyên văn
   * ⇒ BE chỉ cho phép 2 giá trị này. Khuyến nghị gửi `""`.
   */
  @IsOptional()
  @IsIn(['', 'all'], { message: 'search_range must be "" or "all"' })
  searchRange?: '' | 'all' = '';

  @IsOptional()
  @IsBoolean()
  useWebSearch?: boolean;


  @IsOptional()
  @IsString()
  language?: string = 'vi';

  @IsOptional()
  @IsString()
  sessionId?: string;

  @IsOptional()
  proposal?: {
    title?: string;
    problem_statement?: string;
    problemStatement?: string;
    motivation?: string;
    research_gap?: string;
    researchGap?: string;
  };

  @IsOptional()
  @IsString()
  proposalId?: string;
}
