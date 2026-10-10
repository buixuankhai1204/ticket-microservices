import { Injectable } from '@nestjs/common';
import type { MenuItem } from '../domain/menu-item.js';
import { Pagination, type Page } from '../domain/pagination.js';
import { EventInventoryRepository } from '../platform/port/event-inventory.repository.js';
import { Transactor } from '../platform/port/transactor.js';

export interface GetEventMenuInput {
  ticketedEventId: string;
  limit?: number;
  offset?: number;
}

export interface GetEventMenuResult {
  page: Page<MenuItem>;
  pagination: Pagination;
}

@Injectable()
export class GetEventMenuUseCase {
  constructor(
    private readonly transactor: Transactor,
    private readonly inventories: EventInventoryRepository,
  ) {}

  async execute(input: GetEventMenuInput): Promise<GetEventMenuResult> {
    const pagination = Pagination.of(input.limit, input.offset);
    const page = await this.transactor.run(
      (tx) => this.inventories.listMenu(tx, input.ticketedEventId, pagination),
      { readOnly: true },
    );
    return { page, pagination };
  }
}
