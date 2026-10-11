import { Injectable } from '@nestjs/common';
import type { OutboxEvent } from '../../../domain/outbox-event.js';
import { OutboxRepository } from '../../../platform/port/outbox.repository.js';
import type { TxContext } from '../../../platform/port/transactor.js';
import { queryRows } from './rows.js';

@Injectable()
export class PostgresOutboxRepository extends OutboxRepository {
  async write(tx: TxContext, events: OutboxEvent[]): Promise<void> {
    if (events.length === 0) {
      return;
    }
    const records = events.map((event) => ({
      id: event.id,
      aggregate_id: event.aggregateId,
      aggregate_type: event.aggregateType,
      event_type: event.eventType,
      payload: event.payload,
      created_at: event.createdAt.toISOString(),
    }));
    await queryRows(
      tx,
      `INSERT INTO outbox_events (id, aggregate_id, aggregate_type, event_type, payload, created_at)
       SELECT id, aggregate_id, aggregate_type, event_type, payload, created_at
       FROM jsonb_to_recordset($1::jsonb) AS e(
         id uuid, aggregate_id uuid, aggregate_type text, event_type text,
         payload jsonb, created_at timestamptz
       )`,
      [JSON.stringify(records)],
    );
    await queryRows(
      tx,
      `DELETE FROM outbox_events WHERE id = ANY($1::uuid[])`,
      [events.map((event) => event.id)],
    );
  }
}
