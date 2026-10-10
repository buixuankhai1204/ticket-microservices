import { Column, Entity, PrimaryColumn } from 'typeorm';
import type {
  CatalogStatus,
  ProductCategory,
} from '../../../../domain/product.js';
import { bigintTransformer } from '../bigint.transformer.js';

@Entity({ name: 'products' })
export class ProductEntity {
  @PrimaryColumn({ type: 'uuid' })
  id!: string;

  @Column({ type: 'text' })
  sku!: string;

  @Column({ type: 'text' })
  name!: string;

  @Column({ type: 'text' })
  description!: string;

  @Column({ type: 'text' })
  category!: ProductCategory;

  @Column({
    name: 'price_minor',
    type: 'bigint',
    transformer: bigintTransformer,
  })
  priceMinor!: number;

  @Column({ type: 'text' })
  currency!: string;

  @Column({ type: 'text' })
  status!: CatalogStatus;

  @Column({ name: 'created_at', type: 'timestamptz' })
  createdAt!: Date;

  @Column({ name: 'updated_at', type: 'timestamptz' })
  updatedAt!: Date;
}
