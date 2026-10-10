import { describe, expect, it } from 'vitest';
import { ValidationError } from './errors.js';
import {
  EventInventory,
  MAX_STOCK,
  StockAdjustment,
} from './event-inventory.js';

describe('StockAdjustment', () => {
  it('accepts non-zero integers up to the limit in both directions', () => {
    expect(StockAdjustment.of(MAX_STOCK, 'restock').delta).toBe(MAX_STOCK);
    expect(StockAdjustment.of(-MAX_STOCK, 'write-off').delta).toBe(-MAX_STOCK);
    expect(StockAdjustment.of(-1, '  damaged  ').reason).toBe('damaged');
  });

  it('rejects zero, fractions, out-of-range deltas and blank reasons', () => {
    for (const delta of [0, 0.5, MAX_STOCK + 1, -(MAX_STOCK + 1)]) {
      expect(() => StockAdjustment.of(delta, 'reason')).toThrow(
        ValidationError,
      );
    }
    expect(() => StockAdjustment.of(1, '   ')).toThrow(ValidationError);
  });
});

describe('EventInventory.create', () => {
  const base = {
    ticketedEventId: 'e',
    productId: 'p',
    priceMinor: null,
    status: 'active' as const,
  };
  const now = new Date('2026-01-01T00:00:00Z');

  it('starts with all stock available and bounds the total', () => {
    expect(
      EventInventory.create({ ...base, stockTotal: MAX_STOCK }, now)
        .stockAvailable,
    ).toBe(MAX_STOCK);
    for (const stockTotal of [-1, 1.5, MAX_STOCK + 1]) {
      expect(() => EventInventory.create({ ...base, stockTotal }, now)).toThrow(
        ValidationError,
      );
    }
  });
});
