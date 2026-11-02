import { Type } from 'class-transformer';
import {
  ArrayNotEmpty,
  IsArray,
  IsBoolean,
  IsIn,
  IsNotEmpty,
  IsNotEmptyObject,
  IsObject,
  IsOptional,
  IsString,
  ValidateNested,
} from 'class-validator';

/**
 * Enum do AI Engine quy định (payload Kafka phải dùng giá trị tiếng Anh —
 * KHÔNG gửi nhãn tiếng Việt, xem docs/REVIEW_AI_ANSWERS.md §6 B-1/B-2).
 */
export const AI_SCALES = [
  'nominal',
  'ordinal',
  'interval',
  'ratio',
  'guttman',
  'semantic_differential',
  'hybrid',
  'performance_level',
] as const;

export const AI_ROLES = [
  'dependent',
  'independent',
  'intermediate',
  'moderator',
  'control',
  'endogenous',
  'null',
] as const;

export const AI_VARIABLE_TYPES = [
  'observed',
  'latent',
  'time_index',
  'entity_index',
  'component',
] as const;

export type AiScale = (typeof AI_SCALES)[number];
export type AiRole = (typeof AI_ROLES)[number];
export type AiVariableType = (typeof AI_VARIABLE_TYPES)[number];

/** Một biến theo schema `Variable` của AI (các field optional giữ nguyên tên AI). */
export class AnalyzeVariableDto {
  @IsString()
  @IsNotEmpty()
  name!: string;

  @IsString()
  @IsNotEmpty()
  code!: string;

  @IsIn(AI_VARIABLE_TYPES, {
    message: `variable_type must be one of: ${AI_VARIABLE_TYPES.join(', ')}`,
  })
  variable_type!: AiVariableType;

  @IsIn(AI_ROLES, {
    message: `role must be one of: ${AI_ROLES.join(', ')}`,
  })
  role!: AiRole;

  /** Bắt buộc theo model `Variable` của AI (thiếu ⇒ 631 ở analyzer). */
  @IsIn(AI_SCALES, {
    message: `scale must be one of: ${AI_SCALES.join(', ')}`,
  })
  scale!: AiScale;

  @IsOptional()
  @IsString()
  unit?: string;

  @IsOptional()
  @IsString()
  collection_method?: string;

  @IsOptional()
  @IsString()
  description?: string;

  @IsOptional()
  @IsString()
  type?: string;
}

class BaseDataAnalysisDto {
  @IsOptional()
  @IsString()
  sessionId?: string;

  @IsOptional()
  @IsString()
  threadId?: string;


  @IsArray()
  @ArrayNotEmpty()
  @ValidateNested({ each: true })
  @Type(() => AnalyzeVariableDto)
  variables!: AnalyzeVariableDto[];
}

/** `comment_data` — nhận xét tính hợp lệ của dữ liệu theo từng biến. */
export class CommentDataDto extends BaseDataAnalysisDto {
  @IsObject()
  @IsNotEmptyObject()
  finalProposal!: Record<string, unknown>;

  @IsObject()
  @IsNotEmptyObject()
  data!: Record<string, unknown[]>;
}

/** `check_logic_data` — kiểm tra logic biến và thang đo. */
export class CheckLogicDataDto extends BaseDataAnalysisDto {}

/**
 * `analyzer` / `analyzer_tool` (hợp đồng 7.2 + Phụ lục C.2).
 * Khác biệt duy nhất giữa 2 event: `event_type` + `is_tool` (AI xử lý), shape request giống nhau.
 */
export class AnalyzeRequestDto extends BaseDataAnalysisDto {
  @IsOptional()
  @IsString()
  convId?: string;

  @IsObject()
  @IsNotEmptyObject()
  finalProposal!: Record<string, unknown>;

  /** `{ tên_cột: [giá trị...] }` — mọi cột phải cùng độ dài (AI dựng DataFrame từ đây). */
  @IsObject()
  @IsNotEmptyObject()
  data!: Record<string, unknown[]>;

  @IsOptional()
  @IsBoolean()
  autoPlanMode?: boolean;

  @IsOptional()
  @IsBoolean()
  autoActionMode?: boolean;

  @IsOptional()
  @IsBoolean()
  manualPlanMode?: boolean;

  @IsOptional()
  @IsArray()
  toolConfigs?: Array<Record<string, unknown>>;

  /** `true` khi người dùng trả lời câu hỏi của AI (kèm `feedback` + `threadId`). */
  @IsOptional()
  @IsBoolean()
  resume?: boolean;

  @IsOptional()
  @IsBoolean()
  resumeError?: boolean;

  @IsOptional()
  @IsString()
  feedback?: string;

  /** `redis` (mặc định) hoặc `mongo` — nơi AI lưu checkpoint phân tích. */
  @IsOptional()
  @IsIn(['redis', 'mongo'])
  db?: 'redis' | 'mongo';
}
