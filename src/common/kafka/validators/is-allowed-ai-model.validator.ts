import {
  ValidationArguments,
  ValidatorConstraint,
  ValidatorConstraintInterface,
} from 'class-validator';
import { getAiModelConfig } from '../kafka.constants';

/**
 * Validate `model_id` theo whitelist được resolve TẠI THỜI ĐIỂM VALIDATE.
 *
 * Khác với `@IsIn(SUPPORTED_KAFKA_MODELS)` (mảng snapshot lúc import module),
 * constraint này luôn phản ánh `AI_ALLOWED_MODELS` hiện hành — tránh trường hợp
 * model hợp lệ bị chặn (lỗi 603) hoặc model lạ lọt qua.
 */
@ValidatorConstraint({ name: 'isAllowedAiModel', async: false })
export class IsAllowedAiModelConstraint
  implements ValidatorConstraintInterface
{
  validate(value: unknown): boolean {
    if (typeof value !== 'string' || value.length === 0) {
      return false;
    }
    return getAiModelConfig().allowedModels.includes(value);
  }

  defaultMessage(args: ValidationArguments): string {
    return `model_id must be one of: ${getAiModelConfig().allowedModels.join(', ')} (received: ${String(args.value)})`;
  }
}
