import { Injectable } from '@nestjs/common';
import { NotFoundError } from '../domain/errors.js';
import type { Product } from '../domain/product.js';
import { ProductRepository } from '../platform/port/product.repository.js';
import { Transactor } from '../platform/port/transactor.js';

@Injectable()
export class GetProductUseCase {
  constructor(
    private readonly transactor: Transactor,
    private readonly products: ProductRepository,
  ) {}

  async execute(id: string): Promise<Product> {
    const product = await this.transactor.run(
      (tx) => this.products.findById(tx, id),
      { readOnly: true },
    );
    if (!product) {
      throw new NotFoundError('product not found');
    }
    return product;
  }
}
