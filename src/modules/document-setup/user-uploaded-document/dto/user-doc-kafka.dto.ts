import {
  IsNotEmpty,
  IsNumber,
  IsOptional,
  IsString,
  Min,
} from 'class-validator';

export class ProcessUploadedDocDto {
  @IsOptional()
  @IsString()
  fileUrl?: string;

  @IsOptional()
  @IsString()
  sessionId?: string;


  @IsOptional()
  @IsString()
  language?: string;
}

export class TriggerParseDocDto {
  @IsOptional()
  @IsString()
  fileUrl?: string;

  @IsOptional()
  @IsString()
  sessionId?: string;


  @IsOptional()
  @IsString()
  language?: string;
}

export class TriggerEmbedDocDto {
  @IsOptional()
  @IsString()
  sessionId?: string;


  @IsOptional()
  @IsString()
  language?: string;

  @IsOptional()
  processedDocs?: any[];
}

export class SearchUploadedDocsDto {
  @IsString()
  @IsNotEmpty()
  documentId!: string;

  /** AI Q3: data_id optional — AI echo lại để BE ghép job. */
  @IsOptional()
  @IsString()
  dataId?: string;

  @IsString()
  @IsNotEmpty()
  query!: string;

  @IsOptional()
  @IsNumber()
  @Min(1)
  topK?: number;

  @IsOptional()
  @IsString()
  sessionId?: string;


  @IsOptional()
  @IsString()
  language?: string;
}

export class DeleteUploadedDocDto {
  @IsString()
  @IsNotEmpty()
  fileId!: string;

  @IsString()
  @IsNotEmpty()
  documentId!: string;

  @IsOptional()
  @IsString()
  sessionId?: string;
}
