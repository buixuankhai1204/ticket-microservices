import { Injectable } from '@nestjs/common';
import { NotFoundError } from '../domain/errors.js';
import type { Product, ProductChanges } from '../domain/product.js';
import { ProductRepository } from '../platform/port/product.repository.js';
import { Transactor } from '../platform/port/transactor.js';

@Injectable()
export class UpdateProductUseCase {
  constructor(
    private readonly transactor: Transactor,
    private readonly products: ProductRepository,
  ) {}

  async execute(id: string, changes: ProductChanges): Promise<Product> {
    const now = new Date();
    return this.transactor.run(async (tx) => {
      const current = await this.products.findById(tx, id, {
        forUpdate: true,
      });
      if (!current) {
        throw new NotFoundError('product not found');
      }
      const updated = current.withChanges(changes, now);
      await this.products.update(tx, updated);
      return updated;
    });
  }
}
