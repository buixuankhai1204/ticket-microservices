import { Injectable } from '@nestjs/common';
import { NotFoundError } from '../domain/errors.js';
import type { ProductOrder } from '../domain/product-order.js';
import { ProductOrderRepository } from '../platform/port/product-order.repository.js';
import { Transactor } from '../platform/port/transactor.js';

export interface GetProductOrderInput {
  orderId: string;
  userId: string;
}

@Injectable()
export class GetProductOrderUseCase {
  constructor(
    private readonly transactor: Transactor,
    private readonly orders: ProductOrderRepository,
  ) {}

  async execute(input: GetProductOrderInput): Promise<ProductOrder> {
    const order = await this.transactor.run(
      (tx) => this.orders.findForOwner(tx, input.orderId, input.userId),
      { readOnly: true },
    );
    if (!order) {
      throw new NotFoundError('order not found');
    }
    return order;
  }
}
