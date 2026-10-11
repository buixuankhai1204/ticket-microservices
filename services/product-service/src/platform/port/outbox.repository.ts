import type { OutboxEvent } from '../../domain/outbox-event.js';
import type { TxContext } from './transactor.js';

export abstract class OutboxRepository {
  abstract write(tx: TxContext, events: OutboxEvent[]): Promise<void>;
}
