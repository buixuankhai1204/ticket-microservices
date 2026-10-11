import type { TxContext } from './transactor.js';

export abstract class ProcessedEventRepository {
  abstract markProcessed(
    tx: TxContext,
    eventId: string,
    now: Date,
  ): Promise<boolean>;
}
