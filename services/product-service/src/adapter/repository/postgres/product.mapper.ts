import { Product } from '../../../domain/product.js';
import { ProductEntity } from './entities/product.entity.js';

export function toProduct(entity: ProductEntity): Product {
  return Product.restore({
    id: entity.id,
    sku: entity.sku,
    name: entity.name,
    description: entity.description,
    category: entity.category,
    priceMinor: entity.priceMinor,
    currency: entity.currency,
    status: entity.status,
    createdAt: entity.createdAt,
    updatedAt: entity.updatedAt,
  });
}

export function toProductValues(product: Product) {
  return {
    id: product.id,
    sku: product.sku,
    name: product.name,
    description: product.description,
    category: product.category,
    priceMinor: product.priceMinor,
    currency: product.currency,
    status: product.status,
    createdAt: product.createdAt,
    updatedAt: product.updatedAt,
  };
}
