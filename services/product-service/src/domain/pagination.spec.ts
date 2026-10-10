import { describe, expect, it } from 'vitest';
import { ValidationError } from './errors.js';
import { MAX_LIMIT, Pagination } from './pagination.js';

describe('Pagination', () => {
  it('defaults to 20/0 and clamps limit to the maximum', () => {
    const defaults = Pagination.of();
    expect([defaults.limit, defaults.offset]).toEqual([20, 0]);
    expect(Pagination.of(MAX_LIMIT + 1).limit).toBe(MAX_LIMIT);
    expect(Pagination.of(MAX_LIMIT).limit).toBe(MAX_LIMIT);
  });

  it('rejects non-positive or fractional limits and negative offsets', () => {
    for (const limit of [0, -1, 1.5, Number.NaN]) {
      expect(() => Pagination.of(limit)).toThrow(ValidationError);
    }
    expect(() => Pagination.of(10, -1)).toThrow(ValidationError);
    expect(() => Pagination.of(10, 0.5)).toThrow(ValidationError);
  });

  it('reports has_more only while rows remain beyond the returned page', () => {
    expect(Pagination.of(20, 0).hasMore(20, 20)).toBe(false);
    expect(Pagination.of(20, 0).hasMore(20, 21)).toBe(true);
    expect(Pagination.of(20, 40).hasMore(5, 45)).toBe(false);
    expect(Pagination.of(20, 40).hasMore(20, 61)).toBe(true);
    expect(Pagination.of(20, 100).hasMore(0, 45)).toBe(false);
  });
});
