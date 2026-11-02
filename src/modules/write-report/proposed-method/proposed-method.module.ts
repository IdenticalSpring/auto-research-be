import { Module } from '@nestjs/common';
import { AuthModule } from '../../auth/auth/auth.module';
import { DocumentProposedMethodController } from './document-proposed-method.controller';
import { ParsedValueAiService } from './parsed-value-ai.service';
import { ProposedMethodAiService } from './proposed-method-ai.service';
import { ProposedMethodController } from './proposed-method.controller';
import { ProposedMethodService } from './proposed-method.service';

@Module({
  imports: [AuthModule],
  controllers: [ProposedMethodController, DocumentProposedMethodController],
  providers: [
    ProposedMethodService,
    ProposedMethodAiService,
    ParsedValueAiService,
  ],
  exports: [
    ProposedMethodService,
    ProposedMethodAiService,
    ParsedValueAiService,
  ],
})
export class ProposedMethodModule {}
