import { Injectable } from '@nestjs/common';
import { Product, type NewProduct } from '../domain/product.js';
import { ProductRepository } from '../platform/port/product.repository.js';
import { Transactor } from '../platform/port/transactor.js';

@Injectable()
export class CreateProductUseCase {
  constructor(
    private readonly transactor: Transactor,
    private readonly products: ProductRepository,
  ) {}

  async execute(input: NewProduct): Promise<Product> {
    const product = Product.create(input, new Date());
    await this.transactor.run((tx) => this.products.insert(tx, product));
    return product;
  }
}
