import type { ProductOrder } from '../../../domain/product-order.js';
import type { ProductOrderResponse } from '../dto/product-order.dto.js';

export function toProductOrderResponse(
  order: ProductOrder,
): ProductOrderResponse {
  return {
    id: order.id,
    booking_id: order.bookingId,
    ticketed_event_id: order.ticketedEventId,
    status: order.status,
    cancel_reason: order.cancelReason,
    currency: order.currency,
    total_minor: order.totalMinor,
    item_count: order.itemCount,
    items: order.items.map((item) => ({
      product_id: item.productId,
      name: item.name,
      quantity: item.quantity,
      unit_price_minor: item.unitPriceMinor,
      line_total_minor: item.lineTotalMinor,
    })),
    created_at: order.createdAt.toISOString(),
    updated_at: order.updatedAt.toISOString(),
  };
}
