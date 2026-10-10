import { Injectable } from '@nestjs/common';
import {
  EventInventory,
  type NewEventInventory,
} from '../domain/event-inventory.js';
import { EventInventoryRepository } from '../platform/port/event-inventory.repository.js';
import { Transactor } from '../platform/port/transactor.js';

@Injectable()
export class UpsertEventInventoryUseCase {
  constructor(
    private readonly transactor: Transactor,
    private readonly inventories: EventInventoryRepository,
  ) {}

  async execute(input: NewEventInventory): Promise<EventInventory> {
    const desired = EventInventory.create(input, new Date());
    return this.transactor.run((tx) => this.inventories.upsert(tx, desired));
  }
}
