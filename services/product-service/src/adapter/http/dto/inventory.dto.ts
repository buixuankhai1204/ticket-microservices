import {
  IsIn,
  IsInt,
  IsOptional,
  IsString,
  Length,
  Max,
  Min,
} from 'class-validator';
import { MAX_STOCK } from '../../../domain/event-inventory.js';
import {
  CATALOG_STATUSES,
  type CatalogStatus,
} from '../../../domain/product.js';
import { ProductResponse } from './product.dto.js';

export class UpsertInventoryDto {
  @IsInt()
  @Min(0)
  @Max(MAX_STOCK)
  stock_total!: number;

  @IsOptional()
  @IsInt()
  @Min(0)
  @Max(Number.MAX_SAFE_INTEGER)
  price_minor?: number | null;

  @IsOptional()
  @IsIn(CATALOG_STATUSES)
  status?: CatalogStatus;
}

export class StockAdjustmentDto {
  @IsInt()
  @Min(-MAX_STOCK)
  @Max(MAX_STOCK)
  delta!: number;

  @IsString()
  @Length(1, 200)
  reason!: string;
}

export class InventoryResponse {
  id!: string;
  ticketed_event_id!: string;
  product_id!: string;
  price_minor!: number | null;
  stock_total!: number;
  stock_available!: number;
  status!: CatalogStatus;
  created_at!: string;
  updated_at!: string;
}

export class MenuItemResponse {
  product!: ProductResponse;
  inventory_id!: string;
  unit_price_minor!: number;
  currency!: string;
  stock_available!: number;
  in_stock!: boolean;
}
