import { Injectable } from '@nestjs/common';
import { NotFoundError } from '../../../domain/errors.js';
import type { Page, Pagination } from '../../../domain/pagination.js';
import type { Product } from '../../../domain/product.js';
import {
  ProductRepository,
  type FindProductOptions,
  type ProductFilter,
} from '../../../platform/port/product.repository.js';
import type { TxContext } from '../../../platform/port/transactor.js';
import { ProductEntity } from './entities/product.entity.js';
import { toProduct, toProductValues } from './product.mapper.js';

@Injectable()
export class PostgresProductRepository extends ProductRepository {
  async insert(tx: TxContext, product: Product): Promise<void> {
    await tx.getRepository(ProductEntity).insert(toProductValues(product));
  }

  async update(tx: TxContext, product: Product): Promise<void> {
    const result = await tx.getRepository(ProductEntity).update(
      { id: product.id },
      {
        name: product.name,
        description: product.description,
        category: product.category,
        priceMinor: product.priceMinor,
        status: product.status,
        updatedAt: product.updatedAt,
      },
    );
    if (!result.affected) {
      throw new NotFoundError('product not found');
    }
  }

  async findById(
    tx: TxContext,
    id: string,
    options: FindProductOptions = {},
  ): Promise<Product | null> {
    const query = tx
      .getRepository(ProductEntity)
      .createQueryBuilder('p')
      .where('p.id = :id', { id });
    if (options.forUpdate) {
      query.setLock('pessimistic_write');
    }
    const entity = await query.getOne();
    return entity ? toProduct(entity) : null;
  }

  async list(
    tx: TxContext,
    filter: ProductFilter,
    pagination: Pagination,
  ): Promise<Page<Product>> {
    const query = tx.getRepository(ProductEntity).createQueryBuilder('p');
    if (filter.category) {
      query.andWhere('p.category = :category', { category: filter.category });
    }
    if (filter.status) {
      query.andWhere('p.status = :status', { status: filter.status });
    }
    query
      .orderBy('p.createdAt', 'DESC')
      .addOrderBy('p.id', 'ASC')
      .offset(pagination.offset)
      .limit(pagination.limit);
    const [entities, total] = await query.getManyAndCount();
    return { items: entities.map(toProduct), total };
  }
}
