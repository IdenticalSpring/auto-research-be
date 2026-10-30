import {
  IsArray,
  IsBoolean,
  IsIn,
  IsNotEmpty,
  IsObject,
  IsOptional,
  IsString,
  IsUUID,
} from 'class-validator';

export class WriteContentRequestDto {
  @IsOptional()
  @IsUUID()
  documentId!: string;

  @IsOptional()
  @IsString()
  sessionId?: string;

  @IsOptional()
  @IsIn(['write_content', 'chuyen_de_1', 'chuyen_de_2', 'chuyen_de_3'])
  eventType?:
    | 'write_content'
    | 'chuyen_de_1'
    | 'chuyen_de_2'
    | 'chuyen_de_3' = 'write_content';

  @IsOptional()
  @IsString()
  field?: string;

  @IsOptional()
  @IsString()
  domain?: string;

  @IsOptional()
  @IsString()
  referencesStyle?: string = 'APA';

  @IsOptional()
  @IsObject()
  finalProposal?: Record<string, unknown>;

  @IsOptional()
  @IsArray()
  papersSearch?: Array<Record<string, unknown>>;

  @IsOptional()
  @IsObject()
  outline?: Record<string, unknown>;

  @IsOptional()
  outlineCode?: string | string[];

  @IsOptional()
  @IsString()
  threadId?: string;

  @IsOptional()
  @IsBoolean()
  resume?: boolean;

  @IsOptional()
  @IsBoolean()
  resumeError?: boolean;

  @IsOptional()
  @IsBoolean()
  resumeMethod?: boolean;

  @IsOptional()
  @IsBoolean()
  resumeCharacter?: boolean;

  @IsOptional()
  @IsString()
  analyzeLog?: string;

  @IsOptional()
  @IsArray()
  detailedLogs?: Array<Record<string, unknown>>;

  @IsOptional()
  @IsObject()
  generatedFiles?: Record<string, string>;

  @IsOptional()
  @IsBoolean()
  useWebSearch?: boolean = false;


  @IsOptional()
  @IsString()
  language?: string = 'vi';

  /** Số liệu thực tiễn cho chuyên đề 2 (BE-031) */
  @IsOptional()
  practicalData?: Record<string, unknown> | Array<Record<string, unknown>> | string;

  /** Dataset cho chuyên đề 3 nếu override (BE-032) */
  @IsOptional()
  @IsObject()
  data?: Record<string, unknown>;

  /** Danh sách biến định lượng cho chuyên đề 3 nếu override (BE-032) */
  @IsOptional()
  @IsArray()
  variables?: Array<Record<string, unknown>>;
}
