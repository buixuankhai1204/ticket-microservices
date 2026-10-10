import { describe, expect, it } from 'vitest';
import { ValidationError } from './errors.js';
import { Product } from './product.js';

const now = new Date('2026-01-01T00:00:00Z');
const later = new Date('2026-01-02T00:00:00Z');

function newProduct(
  overrides: Partial<Parameters<typeof Product.create>[0]> = {},
) {
  return Product.create(
    {
      sku: 'COLA-500',
      name: 'Cola',
      description: '',
      category: 'drink',
      priceMinor: 350,
      currency: 'USD',
      ...overrides,
    },
    now,
  );
}

describe('Product', () => {
  it('accepts boundary skus and prices, rejects out-of-range ones', () => {
    expect(newProduct({ sku: 'AB', priceMinor: 0 }).priceMinor).toBe(0);
    expect(newProduct({ sku: 'A'.repeat(40) }).sku).toHaveLength(40);
    for (const sku of ['A', 'A'.repeat(41), 'lower', '-LEADING', 'HAS SPACE']) {
      expect(() => newProduct({ sku })).toThrow(ValidationError);
    }
    for (const priceMinor of [-1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => newProduct({ priceMinor })).toThrow(ValidationError);
    }
  });

  it('applies partial changes without touching identity, sku or currency, and revalidates', () => {
    const original = newProduct();
    const changed = original.withChanges({ priceMinor: 400 }, later);
    expect(changed.priceMinor).toBe(400);
    expect(changed.name).toBe(original.name);
    expect([changed.id, changed.sku, changed.currency]).toEqual([
      original.id,
      original.sku,
      original.currency,
    ]);
    expect(changed.updatedAt).toEqual(later);
    expect(original.priceMinor).toBe(350);
    expect(() => original.withChanges({ priceMinor: -5 }, later)).toThrow(
      ValidationError,
    );
  });
});
