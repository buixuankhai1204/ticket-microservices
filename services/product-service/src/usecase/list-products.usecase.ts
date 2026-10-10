import { Injectable } from '@nestjs/common';
import { Pagination, type Page } from '../domain/pagination.js';
import type { Product } from '../domain/product.js';
import {
  ProductRepository,
  type ProductFilter,
} from '../platform/port/product.repository.js';
import { Transactor } from '../platform/port/transactor.js';

export interface ListProductsInput extends ProductFilter {
  limit?: number;
  offset?: number;
}

export interface ListProductsResult {
  page: Page<Product>;
  pagination: Pagination;
}

@Injectable()
export class ListProductsUseCase {
  constructor(
    private readonly transactor: Transactor,
    private readonly products: ProductRepository,
  ) {}

  async execute(input: ListProductsInput): Promise<ListProductsResult> {
    const pagination = Pagination.of(input.limit, input.offset);
    const filter: ProductFilter = {
      category: input.category,
      status: input.status,
    };
    const page = await this.transactor.run(
      (tx) => this.products.list(tx, filter, pagination),
      { readOnly: true },
    );
    return { page, pagination };
  }
}
