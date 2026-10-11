import { randomUUID } from 'node:crypto';
import { ProductNotAvailableError, ValidationError } from './errors.js';
import type { OrderLimits } from './order-limits.js';

export const ORDER_STATUSES = ['placed', 'cancelled'] as const;
export type OrderStatus = (typeof ORDER_STATUSES)[number];

export const CANCEL_REASONS = [
  'customer_cancelled',
  'booking_cancelled',
] as const;
export type CancelReason = (typeof CANCEL_REASONS)[number];

export interface RequestedLine {
  productId: string;
  quantity: number;
}

export interface CatalogLine {
  productId: string;
  name: string;
  unitPriceMinor: number;
  currency: string;
}

export interface OrderItem {
  productId: string;
  name: string;
  quantity: number;
  unitPriceMinor: number;
  lineTotalMinor: number;
}

export interface PricedOrder {
  items: OrderItem[];
  totalMinor: number;
  currency: string;
  itemCount: number;
}

export function validateRequestedLines(
  requested: RequestedLine[],
  limits: OrderLimits,
): void {
  if (requested.length === 0 || requested.length > limits.maxLines) {
    throw new ValidationError(
      `an order needs between 1 and ${limits.maxLines} items`,
    );
  }
  const seen = new Set<string>();
  for (const line of requested) {
    if (seen.has(line.productId)) {
      throw new ValidationError('each product may appear only once per order');
    }
    seen.add(line.productId);
    if (
      !Number.isInteger(line.quantity) ||
      line.quantity < 1 ||
      line.quantity > limits.maxQuantityPerLine
    ) {
      throw new ValidationError(
        `quantity must be an integer between 1 and ${limits.maxQuantityPerLine}`,
      );
    }
  }
}

export function priceOrder(
  requested: RequestedLine[],
  catalog: CatalogLine[],
  limits: OrderLimits,
): PricedOrder {
  validateRequestedLines(requested, limits);
  const byProduct = new Map(catalog.map((line) => [line.productId, line]));
  const items: OrderItem[] = [];
  let totalMinor = 0;
  let itemCount = 0;
  let currency: string | undefined;
  const ordered = [...requested].sort((a, b) =>
    a.productId < b.productId ? -1 : a.productId > b.productId ? 1 : 0,
  );
  for (const line of ordered) {
    const priced = byProduct.get(line.productId);
    if (!priced) {
      throw new ProductNotAvailableError(
        `product not available: ${line.productId}`,
      );
    }
    if (currency !== undefined && currency !== priced.currency) {
      throw new ValidationError(
        'all items in an order must share one currency',
      );
    }
    currency = priced.currency;
    const lineTotalMinor = priced.unitPriceMinor * line.quantity;
    if (!Number.isSafeInteger(lineTotalMinor)) {
      throw new ValidationError('order total is too large');
    }
    totalMinor += lineTotalMinor;
    if (!Number.isSafeInteger(totalMinor)) {
      throw new ValidationError('order total is too large');
    }
    itemCount += line.quantity;
    items.push({
      productId: line.productId,
      name: priced.name,
      quantity: line.quantity,
      unitPriceMinor: priced.unitPriceMinor,
      lineTotalMinor,
    });
  }
  return { items, totalMinor, currency: currency as string, itemCount };
}

export interface ProductOrderProps {
  id: string;
  bookingId: string;
  userId: string;
  ticketedEventId: string;
  currency: string;
  totalMinor: number;
  status: OrderStatus;
  cancelReason: CancelReason | null;
  items: OrderItem[];
  createdAt: Date;
  updatedAt: Date;
}

export interface PlaceOrderInput {
  bookingId: string;
  userId: string;
  ticketedEventId: string;
  priced: PricedOrder;
}

export class ProductOrder {
  readonly id: string;
  readonly bookingId: string;
  readonly userId: string;
  readonly ticketedEventId: string;
  readonly currency: string;
  readonly totalMinor: number;
  readonly status: OrderStatus;
  readonly cancelReason: CancelReason | null;
  readonly items: OrderItem[];
  readonly createdAt: Date;
  readonly updatedAt: Date;

  private constructor(props: ProductOrderProps) {
    this.id = props.id;
    this.bookingId = props.bookingId;
    this.userId = props.userId;
    this.ticketedEventId = props.ticketedEventId;
    this.currency = props.currency;
    this.totalMinor = props.totalMinor;
    this.status = props.status;
    this.cancelReason = props.cancelReason;
    this.items = props.items;
    this.createdAt = props.createdAt;
    this.updatedAt = props.updatedAt;
  }

  static place(input: PlaceOrderInput, now: Date): ProductOrder {
    return new ProductOrder({
      id: randomUUID(),
      bookingId: input.bookingId,
      userId: input.userId,
      ticketedEventId: input.ticketedEventId,
      currency: input.priced.currency,
      totalMinor: input.priced.totalMinor,
      status: 'placed',
      cancelReason: null,
      items: input.priced.items,
      createdAt: now,
      updatedAt: now,
    });
  }

  static restore(props: ProductOrderProps): ProductOrder {
    return new ProductOrder(props);
  }

  get itemCount(): number {
    return this.items.reduce((sum, item) => sum + item.quantity, 0);
  }

  get isCancelled(): boolean {
    return this.status === 'cancelled';
  }

  cancel(reason: CancelReason, now: Date): ProductOrder {
    if (this.isCancelled) {
      return this;
    }
    return new ProductOrder({
      id: this.id,
      bookingId: this.bookingId,
      userId: this.userId,
      ticketedEventId: this.ticketedEventId,
      currency: this.currency,
      totalMinor: this.totalMinor,
      status: 'cancelled',
      cancelReason: reason,
      items: this.items,
      createdAt: this.createdAt,
      updatedAt: now,
    });
  }
}
