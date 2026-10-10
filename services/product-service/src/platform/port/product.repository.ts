import type { Page, Pagination } from '../../domain/pagination.js';
import type {
  CatalogStatus,
  Product,
  ProductCategory,
} from '../../domain/product.js';
import type { TxContext } from './transactor.js';

export interface ProductFilter {
  category?: ProductCategory;
  status?: CatalogStatus;
}

export interface FindProductOptions {
  forUpdate?: boolean;
}

export abstract class ProductRepository {
  abstract insert(tx: TxContext, product: Product): Promise<void>;
  abstract update(tx: TxContext, product: Product): Promise<void>;
  abstract findById(
    tx: TxContext,
    id: string,
    options?: FindProductOptions,
  ): Promise<Product | null>;
  abstract list(
    tx: TxContext,
    filter: ProductFilter,
    pagination: Pagination,
  ): Promise<Page<Product>>;
}
