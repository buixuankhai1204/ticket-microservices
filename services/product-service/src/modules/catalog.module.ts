import { Module } from '@nestjs/common';
import { ProductsController } from '../adapter/http/controllers/products.controller.js';
import { JwtAuthGuard } from '../adapter/http/guards/jwt-auth.guard.js';
import { AdminGuard } from '../adapter/http/guards/admin.guard.js';
import { PostgresEventInventoryRepository } from '../adapter/repository/postgres/postgres-event-inventory.repository.js';
import { PostgresProductRepository } from '../adapter/repository/postgres/postgres-product.repository.js';
import { TypeOrmTransactor } from '../adapter/repository/postgres/typeorm.transactor.js';
import { EventInventoryRepository } from '../platform/port/event-inventory.repository.js';
import { ProductRepository } from '../platform/port/product.repository.js';
import { Transactor } from '../platform/port/transactor.js';
import { AdjustStockUseCase } from '../usecase/adjust-stock.usecase.js';
import { CreateProductUseCase } from '../usecase/create-product.usecase.js';
import { GetEventMenuUseCase } from '../usecase/get-event-menu.usecase.js';
import { GetProductUseCase } from '../usecase/get-product.usecase.js';
import { ListProductsUseCase } from '../usecase/list-products.usecase.js';
import { UpdateProductUseCase } from '../usecase/update-product.usecase.js';
import { UpsertEventInventoryUseCase } from '../usecase/upsert-event-inventory.usecase.js';

@Module({
  controllers: [ProductsController],
  providers: [
    { provide: Transactor, useClass: TypeOrmTransactor },
    { provide: ProductRepository, useClass: PostgresProductRepository },
    {
      provide: EventInventoryRepository,
      useClass: PostgresEventInventoryRepository,
    },
    JwtAuthGuard,
    AdminGuard,
    CreateProductUseCase,
    UpdateProductUseCase,
    GetProductUseCase,
    ListProductsUseCase,
    GetEventMenuUseCase,
    UpsertEventInventoryUseCase,
    AdjustStockUseCase,
  ],
})
export class CatalogModule {}
