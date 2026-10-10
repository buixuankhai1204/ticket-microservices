import { Injectable } from '@nestjs/common';
import {
  EventInventory,
  type StockAdjustment,
} from '../../../domain/event-inventory.js';
import {
  InsufficientStockError,
  NotFoundError,
} from '../../../domain/errors.js';
import { MenuItem } from '../../../domain/menu-item.js';
import type { Page, Pagination } from '../../../domain/pagination.js';
import { Product } from '../../../domain/product.js';
import { EventInventoryRepository } from '../../../platform/port/event-inventory.repository.js';
import type { TxContext } from '../../../platform/port/transactor.js';
import { queryRows, type Row } from './rows.js';

const INVENTORY_COLUMNS = `id, ticketed_event_id, product_id, price_minor, stock_total, stock_available, status, created_at, updated_at`;

function toInventory(row: Row, prefix = ''): EventInventory {
  return EventInventory.restore({
    id: row[`${prefix}id`],
    ticketedEventId: row[`${prefix}ticketed_event_id`],
    productId: row[`${prefix}product_id`],
    priceMinor:
      row[`${prefix}price_minor`] === null
        ? null
        : Number(row[`${prefix}price_minor`]),
    stockTotal: Number(row[`${prefix}stock_total`]),
    stockAvailable: Number(row[`${prefix}stock_available`]),
    status: row[`${prefix}status`],
    createdAt: row[`${prefix}created_at`],
    updatedAt: row[`${prefix}updated_at`],
  });
}

function toMenuProduct(row: Row): Product {
  return Product.restore({
    id: row.p_id,
    sku: row.p_sku,
    name: row.p_name,
    description: row.p_description,
    category: row.p_category,
    priceMinor: Number(row.p_price_minor),
    currency: row.p_currency,
    status: row.p_status,
    createdAt: row.p_created_at,
    updatedAt: row.p_updated_at,
  });
}

@Injectable()
export class PostgresEventInventoryRepository extends EventInventoryRepository {
  async upsert(
    tx: TxContext,
    inventory: EventInventory,
  ): Promise<EventInventory> {
    const rows = await queryRows(
      tx,
      `INSERT INTO event_inventory (${INVENTORY_COLUMNS})
       VALUES ($1, $2, $3, $4, $5, $5, $6, $7, $7)
       ON CONFLICT (ticketed_event_id, product_id) DO UPDATE SET
         price_minor = EXCLUDED.price_minor,
         stock_total = EXCLUDED.stock_total,
         stock_available = EXCLUDED.stock_total - (event_inventory.stock_total - event_inventory.stock_available),
         status = EXCLUDED.status,
         updated_at = EXCLUDED.updated_at
       WHERE EXCLUDED.stock_total >= event_inventory.stock_total - event_inventory.stock_available
       RETURNING ${INVENTORY_COLUMNS}`,
      [
        inventory.id,
        inventory.ticketedEventId,
        inventory.productId,
        inventory.priceMinor,
        inventory.stockTotal,
        inventory.status,
        inventory.updatedAt,
      ],
    );
    if (rows.length === 0) {
      throw new InsufficientStockError(
        'stock_total is below the units already sold',
      );
    }
    return toInventory(rows[0]);
  }

  async adjust(
    tx: TxContext,
    ticketedEventId: string,
    productId: string,
    adjustment: StockAdjustment,
    now: Date,
  ): Promise<EventInventory> {
    const rows = await queryRows(
      tx,
      `UPDATE event_inventory
       SET stock_total = stock_total + $3,
           stock_available = stock_available + $3,
           updated_at = $4
       WHERE ticketed_event_id = $1 AND product_id = $2 AND stock_available + $3 >= 0
       RETURNING ${INVENTORY_COLUMNS}`,
      [ticketedEventId, productId, adjustment.delta, now],
    );
    if (rows.length > 0) {
      return toInventory(rows[0]);
    }
    const existing = await queryRows(
      tx,
      `SELECT 1 AS present FROM event_inventory WHERE ticketed_event_id = $1 AND product_id = $2`,
      [ticketedEventId, productId],
    );
    if (existing.length === 0) {
      throw new NotFoundError('inventory not found');
    }
    throw new InsufficientStockError();
  }

  async listMenu(
    tx: TxContext,
    ticketedEventId: string,
    pagination: Pagination,
  ): Promise<Page<MenuItem>> {
    const countRows = await queryRows(
      tx,
      `SELECT COUNT(*)::int AS total
       FROM event_inventory i JOIN products p ON p.id = i.product_id
       WHERE i.ticketed_event_id = $1 AND i.status = 'active' AND p.status = 'active'`,
      [ticketedEventId],
    );
    const rows = await queryRows(
      tx,
      `SELECT i.id AS i_id, i.ticketed_event_id AS i_ticketed_event_id, i.product_id AS i_product_id,
              i.price_minor AS i_price_minor, i.stock_total AS i_stock_total, i.stock_available AS i_stock_available,
              i.status AS i_status, i.created_at AS i_created_at, i.updated_at AS i_updated_at,
              p.id AS p_id, p.sku AS p_sku, p.name AS p_name, p.description AS p_description,
              p.category AS p_category, p.price_minor AS p_price_minor, p.currency AS p_currency,
              p.status AS p_status, p.created_at AS p_created_at, p.updated_at AS p_updated_at
       FROM event_inventory i JOIN products p ON p.id = i.product_id
       WHERE i.ticketed_event_id = $1 AND i.status = 'active' AND p.status = 'active'
       ORDER BY p.category, p.name, p.id
       LIMIT $2 OFFSET $3`,
      [ticketedEventId, pagination.limit, pagination.offset],
    );
    return {
      items: rows.map(
        (row) => new MenuItem(toMenuProduct(row), toInventory(row, 'i_')),
      ),
      total: Number(countRows[0].total),
    };
  }
}
