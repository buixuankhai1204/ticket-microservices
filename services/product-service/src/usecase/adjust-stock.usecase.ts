import { Injectable } from '@nestjs/common';
import {
  StockAdjustment,
  type EventInventory,
} from '../domain/event-inventory.js';
import { EventInventoryRepository } from '../platform/port/event-inventory.repository.js';
import { Transactor } from '../platform/port/transactor.js';

export interface AdjustStockInput {
  ticketedEventId: string;
  productId: string;
  delta: number;
  reason: string;
}

@Injectable()
export class AdjustStockUseCase {
  constructor(
    private readonly transactor: Transactor,
    private readonly inventories: EventInventoryRepository,
  ) {}

  async execute(input: AdjustStockInput): Promise<EventInventory> {
    const adjustment = StockAdjustment.of(input.delta, input.reason);
    const now = new Date();
    return this.transactor.run((tx) =>
      this.inventories.adjust(
        tx,
        input.ticketedEventId,
        input.productId,
        adjustment,
        now,
      ),
    );
  }
}
