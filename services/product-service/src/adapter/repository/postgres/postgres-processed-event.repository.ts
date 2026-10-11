import { Injectable } from '@nestjs/common';
import { ProcessedEventRepository } from '../../../platform/port/processed-event.repository.js';
import type { TxContext } from '../../../platform/port/transactor.js';
import { queryRows } from './rows.js';

@Injectable()
export class PostgresProcessedEventRepository extends ProcessedEventRepository {
  async markProcessed(
    tx: TxContext,
    eventId: string,
    now: Date,
  ): Promise<boolean> {
    const rows = await queryRows(
      tx,
      `INSERT INTO processed_events (event_id, processed_at) VALUES ($1, $2)
       ON CONFLICT (event_id) DO NOTHING
       RETURNING event_id`,
      [eventId, now],
    );
    return rows.length === 1;
  }
}
