import {
  Body,
  Controller,
  Delete,
  Get,
  HttpCode,
  Param,
  ParseUUIDPipe,
  Post,
  Query,
  UseGuards,
} from '@nestjs/common';
import { ApiBearerAuth, ApiTags } from '@nestjs/swagger';
import { CancelProductOrderUseCase } from '../../../usecase/cancel-product-order.usecase.js';
import { GetProductOrderUseCase } from '../../../usecase/get-product-order.usecase.js';
import { ListProductOrdersUseCase } from '../../../usecase/list-product-orders.usecase.js';
import { PlaceProductOrderUseCase } from '../../../usecase/place-product-order.usecase.js';
import { CurrentUser } from '../decorators/current-user.decorator.js';
import { toPageResponse, type PageResponse } from '../dto/page-response.js';
import { PaginationQueryDto } from '../dto/pagination-query.dto.js';
import {
  CreateProductOrderDto,
  ProductOrderResponse,
} from '../dto/product-order.dto.js';
import type { AuthenticatedUser } from '../guards/authenticated-user.js';
import { JwtAuthGuard } from '../guards/jwt-auth.guard.js';
import { toProductOrderResponse } from '../mappers/order.mapper.js';

@ApiTags('product-orders')
@ApiBearerAuth()
@UseGuards(JwtAuthGuard)
@Controller({ path: 'product-orders', version: '1' })
export class ProductOrdersController {
  constructor(
    private readonly placeOrder: PlaceProductOrderUseCase,
    private readonly cancelOrder: CancelProductOrderUseCase,
    private readonly getOrder: GetProductOrderUseCase,
    private readonly listOrders: ListProductOrdersUseCase,
  ) {}

  @Post()
  async create(
    @CurrentUser() user: AuthenticatedUser,
    @Body() body: CreateProductOrderDto,
  ): Promise<ProductOrderResponse> {
    const order = await this.placeOrder.execute({
      userId: user.id,
      bookingId: body.booking_id,
      lines: body.items.map((item) => ({
        productId: item.product_id,
        quantity: item.quantity,
      })),
    });
    return toProductOrderResponse(order);
  }

  @Get()
  async list(
    @CurrentUser() user: AuthenticatedUser,
    @Query() query: PaginationQueryDto,
  ): Promise<PageResponse<ProductOrderResponse>> {
    const { page, pagination } = await this.listOrders.execute({
      userId: user.id,
      limit: query.limit,
      offset: query.offset,
    });
    return toPageResponse(page, pagination, toProductOrderResponse);
  }

  @Get(':orderId')
  async get(
    @CurrentUser() user: AuthenticatedUser,
    @Param('orderId', ParseUUIDPipe) orderId: string,
  ): Promise<ProductOrderResponse> {
    return toProductOrderResponse(
      await this.getOrder.execute({ orderId, userId: user.id }),
    );
  }

  @Delete(':orderId')
  @HttpCode(200)
  async cancel(
    @CurrentUser() user: AuthenticatedUser,
    @Param('orderId', ParseUUIDPipe) orderId: string,
  ): Promise<ProductOrderResponse> {
    return toProductOrderResponse(
      await this.cancelOrder.execute({ orderId, userId: user.id }),
    );
  }
}
