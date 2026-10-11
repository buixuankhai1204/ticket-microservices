import { Injectable } from '@nestjs/common';
import type { Page, Pagination } from '../../../domain/pagination.js';
import {
  ProductOrder,
  type CancelReason,
  type OrderItem,
} from '../../../domain/product-order.js';
import {
  ProductOrderRepository,
  type FindOrderOptions,
} from '../../../platform/port/product-order.repository.js';
import type { TxContext } from '../../../platform/port/transactor.js';
import { queryRows, type Row } from './rows.js';

const ORDER_COLUMNS = `id, booking_id, user_id, ticketed_event_id, currency, total_minor, status, cancel_reason, created_at, updated_at`;

function toItem(row: Row): OrderItem {
  return {
    productId: row.product_id,
    name: row.name,
    quantity: Number(row.quantity),
    unitPriceMinor: Number(row.unit_price_minor),
    lineTotalMinor: Number(row.line_total_minor),
  };
}

function toOrder(row: Row, items: OrderItem[]): ProductOrder {
  return ProductOrder.restore({
    id: row.id,
    bookingId: row.booking_id,
    userId: row.user_id,
    ticketedEventId: row.ticketed_event_id,
    currency: row.currency,
    totalMinor: Number(row.total_minor),
    status: row.status,
    cancelReason: row.cancel_reason,
    items,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  });
}

@Injectable()
export class PostgresProductOrderRepository extends ProductOrderRepository {
  async insert(tx: TxContext, order: ProductOrder): Promise<void> {
    await queryRows(
      tx,
      `INSERT INTO product_orders (${ORDER_COLUMNS})
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
      [
        order.id,
        order.bookingId,
        order.userId,
        order.ticketedEventId,
        order.currency,
        order.totalMinor,
        order.status,
        order.cancelReason,
        order.createdAt,
        order.updatedAt,
      ],
    );
    await queryRows(
      tx,
      `INSERT INTO product_order_items (order_id, product_id, name, quantity, unit_price_minor, line_total_minor)
       SELECT $1::uuid, product_id, name, quantity, unit_price_minor, line_total_minor
       FROM unnest($2::uuid[], $3::text[], $4::int[], $5::bigint[], $6::bigint[])
         AS i(product_id, name, quantity, unit_price_minor, line_total_minor)`,
      [
        order.id,
        order.items.map((item) => item.productId),
        order.items.map((item) => item.name),
        order.items.map((item) => item.quantity),
        order.items.map((item) => item.unitPriceMinor),
        order.items.map((item) => item.lineTotalMinor),
      ],
    );
  }

  async findForOwner(
    tx: TxContext,
    orderId: string,
    userId: string,
    options: FindOrderOptions = {},
  ): Promise<ProductOrder | null> {
    const rows = await queryRows(
      tx,
      `SELECT ${ORDER_COLUMNS} FROM product_orders WHERE id = $1 AND user_id = $2${
        options.forUpdate ? ' FOR UPDATE' : ''
      }`,
      [orderId, userId],
    );
    if (rows.length === 0) {
      return null;
    }
    const itemsByOrder = await this.loadItems(tx, [orderId]);
    return toOrder(rows[0], itemsByOrder.get(orderId) ?? []);
  }

  async findPlacedByBooking(
    tx: TxContext,
    bookingId: string,
  ): Promise<ProductOrder[]> {
    const rows = await queryRows(
      tx,
      `SELECT ${ORDER_COLUMNS} FROM product_orders
       WHERE booking_id = $1 AND status = 'placed' FOR UPDATE`,
      [bookingId],
    );
    return this.hydrate(tx, rows);
  }

  async markCancelled(
    tx: TxContext,
    orderIds: string[],
    reason: CancelReason,
    now: Date,
  ): Promise<void> {
    await queryRows(
      tx,
      `UPDATE product_orders
       SET status = 'cancelled', cancel_reason = $2, updated_at = $3
       WHERE id = ANY($1::uuid[]) AND status = 'placed'`,
      [orderIds, reason, now],
    );
  }

  async listByUser(
    tx: TxContext,
    userId: string,
    pagination: Pagination,
  ): Promise<Page<ProductOrder>> {
    const countRows = await queryRows(
      tx,
      `SELECT COUNT(*)::int AS total FROM product_orders WHERE user_id = $1`,
      [userId],
    );
    const rows = await queryRows(
      tx,
      `SELECT ${ORDER_COLUMNS} FROM product_orders WHERE user_id = $1
       ORDER BY created_at DESC, id
       LIMIT $2 OFFSET $3`,
      [userId, pagination.limit, pagination.offset],
    );
    return {
      items: await this.hydrate(tx, rows),
      total: Number(countRows[0].total),
    };
  }

  private async hydrate(tx: TxContext, rows: Row[]): Promise<ProductOrder[]> {
    if (rows.length === 0) {
      return [];
    }
    const itemsByOrder = await this.loadItems(
      tx,
      rows.map((row) => row.id),
    );
    return rows.map((row) => toOrder(row, itemsByOrder.get(row.id) ?? []));
  }

  private async loadItems(
    tx: TxContext,
    orderIds: string[],
  ): Promise<Map<string, OrderItem[]>> {
    const rows = await queryRows(
      tx,
      `SELECT order_id, product_id, name, quantity, unit_price_minor, line_total_minor
       FROM product_order_items WHERE order_id = ANY($1::uuid[])
       ORDER BY order_id, product_id`,
      [orderIds],
    );
    const grouped = new Map<string, OrderItem[]>();
    for (const row of rows) {
      const items = grouped.get(row.order_id) ?? [];
      items.push(toItem(row));
      grouped.set(row.order_id, items);
    }
    return grouped;
  }
}
