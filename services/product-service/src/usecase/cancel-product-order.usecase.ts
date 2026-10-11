import { Injectable } from '@nestjs/common';
import { NotFoundError } from '../domain/errors.js';
import type { ProductOrder } from '../domain/product-order.js';
import { OutboxRepository } from '../platform/port/outbox.repository.js';
import { ProductOrderRepository } from '../platform/port/product-order.repository.js';
import { Transactor } from '../platform/port/transactor.js';
import { OrderCanceller } from './order-canceller.js';

export interface CancelProductOrderInput {
  orderId: string;
  userId: string;
}

@Injectable()
export class CancelProductOrderUseCase {
  constructor(
    private readonly transactor: Transactor,
    private readonly orders: ProductOrderRepository,
    private readonly outbox: OutboxRepository,
    private readonly canceller: OrderCanceller,
  ) {}

  async execute(input: CancelProductOrderInput): Promise<ProductOrder> {
    const now = new Date();
    return this.transactor.run(async (tx) => {
      const order = await this.orders.findForOwner(
        tx,
        input.orderId,
        input.userId,
        { forUpdate: true },
      );
      if (!order) {
        throw new NotFoundError('order not found');
      }
      if (order.isCancelled) {
        return order;
      }
      const { cancelled, events } = await this.canceller.cancel(
        tx,
        [order],
        'customer_cancelled',
        now,
      );
      await this.outbox.write(tx, events);
      return cancelled[0];
    });
  }
}
