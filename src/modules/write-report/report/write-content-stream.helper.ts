import { Logger } from '@nestjs/common';
import { JobStatus } from '@prisma/client';
import { Observable } from 'rxjs';
import { PrismaService } from '../../../database/prisma.service';
import { StreamingManagerService } from '../../../common/streaming/streaming-manager.service';
import { getAiErrorMessage } from '../../../common/constants/ai-error-codes';
import { WriteContentHelper } from './write-content.helper';

export interface AttachStreamHandlersOptions {
  streamObservable: Observable<Record<string, unknown>>;
  pendingPromise: Promise<Record<string, unknown>>;
  streamKey: string;
  jobId: string;
  documentId: string;
  errorCode: number;
  prisma: PrismaService;
  streamingManager: StreamingManagerService;
  logger: Logger;
}

export class WriteContentStreamHelper {
  static attachStreamHandlers(options: AttachStreamHandlersOptions) {
    const {
      streamObservable,
      pendingPromise,
      streamKey,
      jobId,
      documentId,
      errorCode,
      prisma,
      streamingManager,
      logger,
    } = options;

    const sectionsMap = new Map<string, string>();
    const subscription = streamObservable.subscribe({
      next: async (chunk: Record<string, unknown>) => {
        try {
          streamingManager.emitChunk(streamKey, chunk);
          const res = await WriteContentHelper.handleChunk(
            prisma,
            jobId,
            chunk,
            sectionsMap,
            documentId,
          );
          if (res.isFinished || res.isSuspended) {
            streamingManager.completeStream(streamKey);
            subscription.unsubscribe();
          }
        } catch (err) {
          logger.error(`Error handling streaming chunk: ${(err as Error).message}`);
        }
      },
      error: async (err: unknown) => {
        const errorMsg = (err as Error)?.message || getAiErrorMessage(errorCode);
        await prisma.job.update({
          where: { id: jobId },
          data: { status: JobStatus.error, errorCode: String(errorCode), errorMessage: errorMsg },
        });
        streamingManager.destroyStream(streamKey);
        subscription.unsubscribe();
      },
    });

    if (pendingPromise?.catch) {
      void pendingPromise.catch(async (err: unknown) => {
        const code = (err as Record<string, unknown>)?.error_code || errorCode;
        const msg =
          (err as Record<string, unknown>)?.error ||
          (err as Error)?.message ||
          getAiErrorMessage(errorCode);

        await prisma.job.update({
          where: { id: jobId },
          data: { status: JobStatus.error, errorCode: String(code), errorMessage: String(msg) },
        });
        streamingManager.destroyStream(streamKey);
        subscription.unsubscribe();
      });
    }

    return subscription;
  }
}
