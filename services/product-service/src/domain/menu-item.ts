import type { EventInventory } from './event-inventory.js';
import type { Product } from './product.js';

export class MenuItem {
  constructor(
    readonly product: Product,
    readonly inventory: EventInventory,
  ) {}

  get unitPriceMinor(): number {
    return this.inventory.priceMinor ?? this.product.priceMinor;
  }

  get inStock(): boolean {
    return this.inventory.stockAvailable > 0;
  }
}
