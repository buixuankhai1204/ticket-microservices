import type { Page, Pagination } from '../../domain/pagination.js';
import type { CancelReason, ProductOrder } from '../../domain/product-order.js';
import type { TxContext } from './transactor.js';

export interface FindOrderOptions {
  forUpdate?: boolean;
}

export abstract class ProductOrderRepository {
  abstract insert(tx: TxContext, order: ProductOrder): Promise<void>;
  abstract findForOwner(
    tx: TxContext,
    orderId: string,
    userId: string,
    options?: FindOrderOptions,
  ): Promise<ProductOrder | null>;
  abstract findPlacedByBooking(
    tx: TxContext,
    bookingId: string,
  ): Promise<ProductOrder[]>;
  abstract markCancelled(
    tx: TxContext,
    orderIds: string[],
    reason: CancelReason,
    now: Date,
  ): Promise<void>;
  abstract listByUser(
    tx: TxContext,
    userId: string,
    pagination: Pagination,
  ): Promise<Page<ProductOrder>>;
}
