import {
  IsInt,
  IsNotEmpty,
  IsObject,
  IsOptional,
  IsString,
  Max,
  Min,
  ValidateNested,
} from 'class-validator';
import { Type } from 'class-transformer';

export class AiErrorDto {
  @IsOptional()
  errorCode?: number | string | null;

  @IsOptional()
  @IsString()
  error?: string | null;
}

export class HealthCheckWebhookDto {
  @IsOptional()
  @IsString()
  eventType?: string;

  @IsInt()
  @Min(1)
  @Max(5)
  processStatus!: number;

  @IsOptional()
  @IsObject()
  @ValidateNested()
  @Type(() => AiErrorDto)
  aiError?: AiErrorDto | null;
}
