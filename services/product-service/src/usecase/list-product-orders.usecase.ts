import { Injectable } from '@nestjs/common';
import { Pagination, type Page } from '../domain/pagination.js';
import type { ProductOrder } from '../domain/product-order.js';
import { ProductOrderRepository } from '../platform/port/product-order.repository.js';
import { Transactor } from '../platform/port/transactor.js';

export interface ListProductOrdersInput {
  userId: string;
  limit?: number;
  offset?: number;
}

export interface ListProductOrdersResult {
  page: Page<ProductOrder>;
  pagination: Pagination;
}

@Injectable()
export class ListProductOrdersUseCase {
  constructor(
    private readonly transactor: Transactor,
    private readonly orders: ProductOrderRepository,
  ) {}

  async execute(
    input: ListProductOrdersInput,
  ): Promise<ListProductOrdersResult> {
    const pagination = Pagination.of(input.limit, input.offset);
    const page = await this.transactor.run(
      (tx) => this.orders.listByUser(tx, input.userId, pagination),
      { readOnly: true },
    );
    return { page, pagination };
  }
}
