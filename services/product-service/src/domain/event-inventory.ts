import { randomUUID } from 'node:crypto';
import { ValidationError } from './errors.js';
import { assertMinorUnits, type CatalogStatus } from './product.js';

export const MAX_STOCK = 1_000_000;
const MAX_REASON_LENGTH = 200;

export interface EventInventoryProps {
  id: string;
  ticketedEventId: string;
  productId: string;
  priceMinor: number | null;
  stockTotal: number;
  stockAvailable: number;
  status: CatalogStatus;
  createdAt: Date;
  updatedAt: Date;
}

export interface NewEventInventory {
  ticketedEventId: string;
  productId: string;
  priceMinor: number | null;
  stockTotal: number;
  status: CatalogStatus;
}

export class EventInventory {
  readonly id: string;
  readonly ticketedEventId: string;
  readonly productId: string;
  readonly priceMinor: number | null;
  readonly stockTotal: number;
  readonly stockAvailable: number;
  readonly status: CatalogStatus;
  readonly createdAt: Date;
  readonly updatedAt: Date;

  private constructor(props: EventInventoryProps) {
    this.id = props.id;
    this.ticketedEventId = props.ticketedEventId;
    this.productId = props.productId;
    this.priceMinor = props.priceMinor;
    this.stockTotal = props.stockTotal;
    this.stockAvailable = props.stockAvailable;
    this.status = props.status;
    this.createdAt = props.createdAt;
    this.updatedAt = props.updatedAt;
  }

  static create(input: NewEventInventory, now: Date): EventInventory {
    if (
      !Number.isInteger(input.stockTotal) ||
      input.stockTotal < 0 ||
      input.stockTotal > MAX_STOCK
    ) {
      throw new ValidationError(
        `stock_total must be an integer between 0 and ${MAX_STOCK}`,
      );
    }
    if (input.priceMinor !== null) {
      assertMinorUnits(input.priceMinor, 'price_minor');
    }
    return new EventInventory({
      id: randomUUID(),
      ticketedEventId: input.ticketedEventId,
      productId: input.productId,
      priceMinor: input.priceMinor,
      stockTotal: input.stockTotal,
      stockAvailable: input.stockTotal,
      status: input.status,
      createdAt: now,
      updatedAt: now,
    });
  }

  static restore(props: EventInventoryProps): EventInventory {
    return new EventInventory(props);
  }
}

export class StockAdjustment {
  private constructor(
    readonly delta: number,
    readonly reason: string,
  ) {}

  static of(delta: number, reason: string): StockAdjustment {
    if (
      !Number.isInteger(delta) ||
      delta === 0 ||
      Math.abs(delta) > MAX_STOCK
    ) {
      throw new ValidationError(
        `delta must be a non-zero integer with absolute value at most ${MAX_STOCK}`,
      );
    }
    const trimmed = reason.trim();
    if (trimmed.length === 0 || trimmed.length > MAX_REASON_LENGTH) {
      throw new ValidationError(
        `reason must be 1-${MAX_REASON_LENGTH} characters`,
      );
    }
    return new StockAdjustment(delta, trimmed);
  }
}
