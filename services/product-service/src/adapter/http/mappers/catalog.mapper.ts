import type { EventInventory } from '../../../domain/event-inventory.js';
import type { MenuItem } from '../../../domain/menu-item.js';
import type { Product } from '../../../domain/product.js';
import type {
  InventoryResponse,
  MenuItemResponse,
} from '../dto/inventory.dto.js';
import type { ProductResponse } from '../dto/product.dto.js';

export function toProductResponse(product: Product): ProductResponse {
  return {
    id: product.id,
    sku: product.sku,
    name: product.name,
    description: product.description,
    category: product.category,
    price_minor: product.priceMinor,
    currency: product.currency,
    status: product.status,
    created_at: product.createdAt.toISOString(),
    updated_at: product.updatedAt.toISOString(),
  };
}

export function toInventoryResponse(
  inventory: EventInventory,
): InventoryResponse {
  return {
    id: inventory.id,
    ticketed_event_id: inventory.ticketedEventId,
    product_id: inventory.productId,
    price_minor: inventory.priceMinor,
    stock_total: inventory.stockTotal,
    stock_available: inventory.stockAvailable,
    status: inventory.status,
    created_at: inventory.createdAt.toISOString(),
    updated_at: inventory.updatedAt.toISOString(),
  };
}

export function toMenuItemResponse(item: MenuItem): MenuItemResponse {
  return {
    product: toProductResponse(item.product),
    inventory_id: item.inventory.id,
    unit_price_minor: item.unitPriceMinor,
    currency: item.product.currency,
    stock_available: item.inventory.stockAvailable,
    in_stock: item.inStock,
  };
}
