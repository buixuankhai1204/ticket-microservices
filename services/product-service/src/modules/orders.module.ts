import { Module } from '@nestjs/common';
import type { ConfigType } from '@nestjs/config';
import { ProductOrdersController } from '../adapter/http/controllers/product-orders.controller.js';
import { JwtAuthGuard } from '../adapter/http/guards/jwt-auth.guard.js';
import { PostgresBookingRefRepository } from '../adapter/repository/postgres/postgres-booking-ref.repository.js';
import { PostgresOutboxRepository } from '../adapter/repository/postgres/postgres-outbox.repository.js';
import { PostgresProcessedEventRepository } from '../adapter/repository/postgres/postgres-processed-event.repository.js';
import { PostgresProductOrderRepository } from '../adapter/repository/postgres/postgres-product-order.repository.js';
import { OrderLimits } from '../domain/order-limits.js';
import { appConfig } from '../platform/config/app.config.js';
import { BookingRefRepository } from '../platform/port/booking-ref.repository.js';
import { OutboxRepository } from '../platform/port/outbox.repository.js';
import { ProcessedEventRepository } from '../platform/port/processed-event.repository.js';
import { ProductOrderRepository } from '../platform/port/product-order.repository.js';
import { CancelProductOrderUseCase } from '../usecase/cancel-product-order.usecase.js';
import { GetProductOrderUseCase } from '../usecase/get-product-order.usecase.js';
import { HandleBookingCancelledUseCase } from '../usecase/handle-booking-cancelled.usecase.js';
import { ListProductOrdersUseCase } from '../usecase/list-product-orders.usecase.js';
import { OrderCanceller } from '../usecase/order-canceller.js';
import { PlaceProductOrderUseCase } from '../usecase/place-product-order.usecase.js';
import { RecordBookingConfirmedUseCase } from '../usecase/record-booking-confirmed.usecase.js';
import { CatalogModule } from './catalog.module.js';

@Module({
  imports: [CatalogModule],
  controllers: [ProductOrdersController],
  providers: [
    { provide: BookingRefRepository, useClass: PostgresBookingRefRepository },
    {
      provide: ProductOrderRepository,
      useClass: PostgresProductOrderRepository,
    },
    { provide: OutboxRepository, useClass: PostgresOutboxRepository },
    {
      provide: ProcessedEventRepository,
      useClass: PostgresProcessedEventRepository,
    },
    {
      provide: OrderLimits,
      inject: [appConfig.KEY],
      useFactory: (config: ConfigType<typeof appConfig>): OrderLimits => ({
        maxLines: config.orderMaxLines,
        maxQuantityPerLine: config.orderMaxQuantityPerLine,
      }),
    },
    JwtAuthGuard,
    OrderCanceller,
    PlaceProductOrderUseCase,
    CancelProductOrderUseCase,
    GetProductOrderUseCase,
    ListProductOrdersUseCase,
    RecordBookingConfirmedUseCase,
    HandleBookingCancelledUseCase,
  ],
  exports: [RecordBookingConfirmedUseCase, HandleBookingCancelledUseCase],
})
export class OrdersModule {}
