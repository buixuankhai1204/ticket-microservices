import { describe, expect, it } from 'vitest';
import { ProductNotAvailableError, ValidationError } from './errors.js';
import type { OrderLimits } from './order-limits.js';
import {
  priceOrder,
  type CatalogLine,
  type RequestedLine,
} from './product-order.js';

const limits: OrderLimits = { maxLines: 3, maxQuantityPerLine: 10 };

function catalogLine(
  productId: string,
  unitPriceMinor: number,
  currency = 'USD',
): CatalogLine {
  return { productId, name: `name-${productId}`, unitPriceMinor, currency };
}

describe('priceOrder', () => {
  it('prices each line, sums units and totals, and orders lines by product id', () => {
    const requested: RequestedLine[] = [
      { productId: 'b', quantity: 3 },
      { productId: 'a', quantity: 2 },
    ];
    const priced = priceOrder(
      requested,
      [catalogLine('a', 450), catalogLine('b', 300)],
      limits,
    );
    expect(priced.items.map((item) => item.productId)).toEqual(['a', 'b']);
    expect(priced.items.map((item) => item.lineTotalMinor)).toEqual([900, 900]);
    expect(priced.totalMinor).toBe(1800);
    expect(priced.itemCount).toBe(5);
    expect(priced.currency).toBe('USD');
  });

  it('rejects totals that leave the safe integer range', () => {
    const huge = Math.floor(Number.MAX_SAFE_INTEGER / 10) + 1;
    expect(() =>
      priceOrder(
        [{ productId: 'a', quantity: 10 }],
        [catalogLine('a', huge)],
        limits,
      ),
    ).toThrow(ValidationError);
    const nearLimit = Math.floor(Number.MAX_SAFE_INTEGER / 2);
    expect(() =>
      priceOrder(
        [
          { productId: 'a', quantity: 1 },
          { productId: 'b', quantity: 1 },
          { productId: 'c', quantity: 1 },
        ],
        [
          catalogLine('a', nearLimit),
          catalogLine('b', nearLimit),
          catalogLine('c', nearLimit),
        ],
        limits,
      ),
    ).toThrow(ValidationError);
  });

  it('enforces line count, quantity bounds, uniqueness, currency and availability', () => {
    const one = (productId: string, quantity = 1): RequestedLine => ({
      productId,
      quantity,
    });
    const catalog = [catalogLine('a', 100), catalogLine('b', 100, 'EUR')];
    expect(() => priceOrder([], catalog, limits)).toThrow(ValidationError);
    expect(() =>
      priceOrder([one('a'), one('b'), one('c'), one('d')], catalog, limits),
    ).toThrow(ValidationError);
    for (const quantity of [0, -1, 1.5, 11]) {
      expect(() => priceOrder([one('a', quantity)], catalog, limits)).toThrow(
        ValidationError,
      );
    }
    expect(() => priceOrder([one('a'), one('a')], catalog, limits)).toThrow(
      ValidationError,
    );
    expect(() => priceOrder([one('a'), one('b')], catalog, limits)).toThrow(
      ValidationError,
    );
    expect(() =>
      priceOrder([one('a'), one('missing')], catalog, limits),
    ).toThrow(ProductNotAvailableError);
    expect(priceOrder([one('a', 10)], catalog, limits).totalMinor).toBe(1000);
  });
});
