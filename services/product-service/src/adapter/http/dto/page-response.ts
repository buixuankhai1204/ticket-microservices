import type { Page, Pagination } from '../../../domain/pagination.js';

export interface PaginationResponse {
  limit: number;
  offset: number;
  total: number;
  has_more: boolean;
}

export interface PageResponse<T> {
  data: T[];
  pagination: PaginationResponse;
}

export function toPageResponse<T, R>(
  page: Page<T>,
  pagination: Pagination,
  map: (item: T) => R,
): PageResponse<R> {
  return {
    data: page.items.map(map),
    pagination: {
      limit: pagination.limit,
      offset: pagination.offset,
      total: page.total,
      has_more: pagination.hasMore(page.items.length, page.total),
    },
  };
}
