import {
  IsIn,
  IsInt,
  IsOptional,
  IsString,
  Length,
  Matches,
  Max,
  MaxLength,
  Min,
} from 'class-validator';
import {
  CATALOG_STATUSES,
  PRODUCT_CATEGORIES,
  type CatalogStatus,
  type ProductCategory,
} from '../../../domain/product.js';
import { PaginationQueryDto } from './pagination-query.dto.js';

export class CreateProductDto {
  @IsString()
  @Matches(/^[A-Z0-9][A-Z0-9_-]{1,39}$/)
  sku!: string;

  @IsString()
  @Length(1, 120)
  name!: string;

  @IsOptional()
  @IsString()
  @MaxLength(1000)
  description?: string;

  @IsIn(PRODUCT_CATEGORIES)
  category!: ProductCategory;

  @IsInt()
  @Min(0)
  @Max(Number.MAX_SAFE_INTEGER)
  price_minor!: number;

  @IsOptional()
  @Matches(/^[A-Z]{3}$/)
  currency?: string;
}

export class UpdateProductDto {
  @IsOptional()
  @IsString()
  @Length(1, 120)
  name?: string;

  @IsOptional()
  @IsString()
  @MaxLength(1000)
  description?: string;

  @IsOptional()
  @IsIn(PRODUCT_CATEGORIES)
  category?: ProductCategory;

  @IsOptional()
  @IsInt()
  @Min(0)
  @Max(Number.MAX_SAFE_INTEGER)
  price_minor?: number;

  @IsOptional()
  @IsIn(CATALOG_STATUSES)
  status?: CatalogStatus;
}

export class ListProductsQueryDto extends PaginationQueryDto {
  @IsOptional()
  @IsIn(PRODUCT_CATEGORIES)
  category?: ProductCategory;

  @IsOptional()
  @IsIn(CATALOG_STATUSES)
  status?: CatalogStatus;
}

export class ProductResponse {
  id!: string;
  sku!: string;
  name!: string;
  description!: string;
  category!: ProductCategory;
  price_minor!: number;
  currency!: string;
  status!: CatalogStatus;
  created_at!: string;
  updated_at!: string;
}
