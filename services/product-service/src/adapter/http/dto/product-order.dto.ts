import { Type } from 'class-transformer';
import {
  ArrayMaxSize,
  ArrayMinSize,
  IsArray,
  IsInt,
  IsUUID,
  ValidateNested,
} from 'class-validator';
import type {
  CancelReason,
  OrderStatus,
} from '../../../domain/product-order.js';

export class OrderLineDto {
  @IsUUID()
  product_id!: string;

  @IsInt()
  quantity!: number;
}

export class CreateProductOrderDto {
  @IsUUID()
  booking_id!: string;

  @IsArray()
  @ArrayMinSize(1)
  @ArrayMaxSize(100)
  @ValidateNested({ each: true })
  @Type(() => OrderLineDto)
  items!: OrderLineDto[];
}

export class OrderItemResponse {
  product_id!: string;
  name!: string;
  quantity!: number;
  unit_price_minor!: number;
  line_total_minor!: number;
}

export class ProductOrderResponse {
  id!: string;
  booking_id!: string;
  ticketed_event_id!: string;
  status!: OrderStatus;
  cancel_reason!: CancelReason | null;
  currency!: string;
  total_minor!: number;
  item_count!: number;
  items!: OrderItemResponse[];
  created_at!: string;
  updated_at!: string;
}
