import type {
  EventInventory,
  StockAdjustment,
} from '../../domain/event-inventory.js';
import type { MenuItem } from '../../domain/menu-item.js';
import type { Page, Pagination } from '../../domain/pagination.js';
import type { CatalogLine, RequestedLine } from '../../domain/product-order.js';
import type { TxContext } from './transactor.js';

export abstract class EventInventoryRepository {
  abstract upsert(
    tx: TxContext,
    inventory: EventInventory,
  ): Promise<EventInventory>;
  abstract adjust(
    tx: TxContext,
    ticketedEventId: string,
    productId: string,
    adjustment: StockAdjustment,
    now: Date,
  ): Promise<EventInventory>;
  abstract listMenu(
    tx: TxContext,
    ticketedEventId: string,
    pagination: Pagination,
  ): Promise<Page<MenuItem>>;
  abstract priceLines(
    tx: TxContext,
    ticketedEventId: string,
    productIds: string[],
  ): Promise<CatalogLine[]>;
  abstract reserve(
    tx: TxContext,
    ticketedEventId: string,
    lines: RequestedLine[],
    now: Date,
  ): Promise<void>;
  abstract restock(
    tx: TxContext,
    ticketedEventId: string,
    lines: RequestedLine[],
    now: Date,
  ): Promise<void>;
}
