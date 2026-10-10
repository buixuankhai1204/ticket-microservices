import {
  Body,
  Controller,
  Get,
  Header,
  HttpCode,
  Inject,
  Param,
  ParseUUIDPipe,
  Post,
  Put,
  Query,
  UseGuards,
} from '@nestjs/common';
import type { ConfigType } from '@nestjs/config';
import { ApiBearerAuth, ApiOkResponse, ApiTags } from '@nestjs/swagger';
import { appConfig } from '../../../platform/config/app.config.js';
import { AdjustStockUseCase } from '../../../usecase/adjust-stock.usecase.js';
import { CreateProductUseCase } from '../../../usecase/create-product.usecase.js';
import { GetEventMenuUseCase } from '../../../usecase/get-event-menu.usecase.js';
import { GetProductUseCase } from '../../../usecase/get-product.usecase.js';
import { ListProductsUseCase } from '../../../usecase/list-products.usecase.js';
import { UpdateProductUseCase } from '../../../usecase/update-product.usecase.js';
import { UpsertEventInventoryUseCase } from '../../../usecase/upsert-event-inventory.usecase.js';
import {
  InventoryResponse,
  MenuItemResponse,
  StockAdjustmentDto,
  UpsertInventoryDto,
} from '../dto/inventory.dto.js';
import { PaginationQueryDto } from '../dto/pagination-query.dto.js';
import { toPageResponse, type PageResponse } from '../dto/page-response.js';
import {
  CreateProductDto,
  ListProductsQueryDto,
  ProductResponse,
  UpdateProductDto,
} from '../dto/product.dto.js';
import { AdminGuard } from '../guards/admin.guard.js';
import { JwtAuthGuard } from '../guards/jwt-auth.guard.js';
import {
  toInventoryResponse,
  toMenuItemResponse,
  toProductResponse,
} from '../mappers/catalog.mapper.js';

const CATALOG_CACHE_CONTROL = 'public, max-age=30';

@ApiTags('products')
@Controller({ path: 'products', version: '1' })
export class ProductsController {
  constructor(
    @Inject(appConfig.KEY)
    private readonly config: ConfigType<typeof appConfig>,
    private readonly createProduct: CreateProductUseCase,
    private readonly updateProduct: UpdateProductUseCase,
    private readonly getProduct: GetProductUseCase,
    private readonly listProducts: ListProductsUseCase,
    private readonly getEventMenu: GetEventMenuUseCase,
    private readonly upsertInventory: UpsertEventInventoryUseCase,
    private readonly adjustStock: AdjustStockUseCase,
  ) {}

  @Get()
  @Header('Cache-Control', CATALOG_CACHE_CONTROL)
  async list(
    @Query() query: ListProductsQueryDto,
  ): Promise<PageResponse<ProductResponse>> {
    const { page, pagination } = await this.listProducts.execute(query);
    return toPageResponse(page, pagination, toProductResponse);
  }

  @Get('events/:ticketedEventId/menu')
  @Header('Cache-Control', CATALOG_CACHE_CONTROL)
  async menu(
    @Param('ticketedEventId', ParseUUIDPipe) ticketedEventId: string,
    @Query() query: PaginationQueryDto,
  ): Promise<PageResponse<MenuItemResponse>> {
    const { page, pagination } = await this.getEventMenu.execute({
      ticketedEventId,
      limit: query.limit,
      offset: query.offset,
    });
    return toPageResponse(page, pagination, toMenuItemResponse);
  }

  @Get(':productId')
  @Header('Cache-Control', CATALOG_CACHE_CONTROL)
  @ApiOkResponse({ type: ProductResponse })
  async get(
    @Param('productId', ParseUUIDPipe) productId: string,
  ): Promise<ProductResponse> {
    return toProductResponse(await this.getProduct.execute(productId));
  }

  @Post()
  @UseGuards(JwtAuthGuard, AdminGuard)
  @ApiBearerAuth()
  async create(@Body() body: CreateProductDto): Promise<ProductResponse> {
    const product = await this.createProduct.execute({
      sku: body.sku,
      name: body.name,
      description: body.description ?? '',
      category: body.category,
      priceMinor: body.price_minor,
      currency: body.currency ?? this.config.defaultCurrency,
    });
    return toProductResponse(product);
  }

  @Put(':productId')
  @HttpCode(200)
  @UseGuards(JwtAuthGuard, AdminGuard)
  @ApiBearerAuth()
  async update(
    @Param('productId', ParseUUIDPipe) productId: string,
    @Body() body: UpdateProductDto,
  ): Promise<ProductResponse> {
    const product = await this.updateProduct.execute(productId, {
      name: body.name,
      description: body.description,
      category: body.category,
      priceMinor: body.price_minor,
      status: body.status,
    });
    return toProductResponse(product);
  }

  @Put('events/:ticketedEventId/inventory/:productId')
  @HttpCode(200)
  @UseGuards(JwtAuthGuard, AdminGuard)
  @ApiBearerAuth()
  async putInventory(
    @Param('ticketedEventId', ParseUUIDPipe) ticketedEventId: string,
    @Param('productId', ParseUUIDPipe) productId: string,
    @Body() body: UpsertInventoryDto,
  ): Promise<InventoryResponse> {
    const inventory = await this.upsertInventory.execute({
      ticketedEventId,
      productId,
      priceMinor: body.price_minor ?? null,
      stockTotal: body.stock_total,
      status: body.status ?? 'active',
    });
    return toInventoryResponse(inventory);
  }

  @Post('events/:ticketedEventId/inventory/:productId/stock-adjustments')
  @HttpCode(200)
  @UseGuards(JwtAuthGuard, AdminGuard)
  @ApiBearerAuth()
  async adjust(
    @Param('ticketedEventId', ParseUUIDPipe) ticketedEventId: string,
    @Param('productId', ParseUUIDPipe) productId: string,
    @Body() body: StockAdjustmentDto,
  ): Promise<InventoryResponse> {
    const inventory = await this.adjustStock.execute({
      ticketedEventId,
      productId,
      delta: body.delta,
      reason: body.reason,
    });
    return toInventoryResponse(inventory);
  }
}
