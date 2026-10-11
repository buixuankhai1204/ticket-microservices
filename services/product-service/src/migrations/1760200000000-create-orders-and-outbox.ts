import type { MigrationInterface, QueryRunner } from 'typeorm';

export class CreateOrdersAndOutbox1760200000000 implements MigrationInterface {
  name = 'CreateOrdersAndOutbox1760200000000';

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      CREATE TABLE outbox_events (
        id UUID PRIMARY KEY,
        aggregate_id UUID NOT NULL,
        aggregate_type TEXT NOT NULL,
        event_type TEXT NOT NULL,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )
    `);
    await queryRunner.query(`
      CREATE TABLE processed_events (
        event_id UUID PRIMARY KEY,
        processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )
    `);
    await queryRunner.query(`
      CREATE TABLE booking_refs (
        booking_id UUID PRIMARY KEY,
        user_id UUID NOT NULL,
        ticketed_event_id UUID NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('confirmed', 'cancelled')),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )
    `);
    await queryRunner.query(`
      CREATE TABLE product_orders (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        booking_id UUID NOT NULL,
        user_id UUID NOT NULL,
        ticketed_event_id UUID NOT NULL,
        currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
        total_minor BIGINT NOT NULL CHECK (total_minor >= 0),
        status TEXT NOT NULL CHECK (status IN ('placed', 'cancelled')),
        cancel_reason TEXT CHECK (cancel_reason IN ('customer_cancelled', 'booking_cancelled')),
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT product_orders_cancel_reason_matches_status
          CHECK ((status = 'cancelled') = (cancel_reason IS NOT NULL))
      )
    `);
    await queryRunner.query(
      `CREATE UNIQUE INDEX product_orders_one_placed_per_booking ON product_orders (booking_id) WHERE status = 'placed'`,
    );
    await queryRunner.query(
      `CREATE INDEX idx_product_orders_user_list ON product_orders (user_id, created_at DESC, id)`,
    );
    await queryRunner.query(`
      CREATE TABLE product_order_items (
        order_id UUID NOT NULL REFERENCES product_orders (id),
        product_id UUID NOT NULL,
        name TEXT NOT NULL,
        quantity INTEGER NOT NULL CHECK (quantity > 0),
        unit_price_minor BIGINT NOT NULL CHECK (unit_price_minor >= 0),
        line_total_minor BIGINT NOT NULL CHECK (line_total_minor >= 0),
        PRIMARY KEY (order_id, product_id)
      )
    `);
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP TABLE IF EXISTS product_order_items`);
    await queryRunner.query(`DROP TABLE IF EXISTS product_orders`);
    await queryRunner.query(`DROP TABLE IF EXISTS booking_refs`);
    await queryRunner.query(`DROP TABLE IF EXISTS processed_events`);
    await queryRunner.query(`DROP TABLE IF EXISTS outbox_events`);
  }
}
