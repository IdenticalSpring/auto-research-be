import { IsNotEmpty, IsString, Validate, ValidateIf } from 'class-validator';
import { SupportedKafkaModel } from '../kafka.constants';
import { IsAllowedAiModelConstraint } from '../validators/is-allowed-ai-model.validator';

export class BaseKafkaRequestDto {
  @IsString({ message: 'event_type must be a string' })
  @IsNotEmpty({ message: 'event_type cannot be empty' })
  event_type!: string;

  @Validate(IsAllowedAiModelConstraint)
  @IsNotEmpty({ message: 'model_id cannot be empty' })
  model_id!: SupportedKafkaModel | string;

  @IsString({ message: 'user_id must be a string' })
  @IsNotEmpty({ message: 'user_id cannot be empty' })
  user_id!: string;

  @ValidateIf(
    (o: BaseKafkaRequestDto) => !o.conv_id || o.document_id !== undefined,
  )
  @IsString({ message: 'document_id must be a string' })
  @IsNotEmpty({ message: 'document_id cannot be empty when conv_id is absent' })
  document_id?: string;

  @ValidateIf(
    (o: BaseKafkaRequestDto) => !o.document_id || o.conv_id !== undefined,
  )
  @IsString({ message: 'conv_id must be a string' })
  @IsNotEmpty({ message: 'conv_id cannot be empty when document_id is absent' })
  conv_id?: string;

  @IsString({ message: 'session_id must be a string' })
  @IsNotEmpty({ message: 'session_id cannot be empty' })
  session_id!: string;

  @IsString({ message: 'language must be a string' })
  @IsNotEmpty({ message: 'language cannot be empty' })
  language!: string;
}
