import { ValidationError } from './errors.js';

export const DEFAULT_LIMIT = 20;
export const MAX_LIMIT = 100;

export interface Page<T> {
  items: T[];
  total: number;
}

export class Pagination {
  private constructor(
    readonly limit: number,
    readonly offset: number,
  ) {}

  static of(limit?: number, offset?: number): Pagination {
    const resolvedLimit = limit ?? DEFAULT_LIMIT;
    const resolvedOffset = offset ?? 0;
    if (!Number.isInteger(resolvedLimit) || resolvedLimit < 1) {
      throw new ValidationError('limit must be an integer >= 1');
    }
    if (!Number.isInteger(resolvedOffset) || resolvedOffset < 0) {
      throw new ValidationError('offset must be an integer >= 0');
    }
    return new Pagination(Math.min(resolvedLimit, MAX_LIMIT), resolvedOffset);
  }

  hasMore(returned: number, total: number): boolean {
    return this.offset + returned < total;
  }
}
