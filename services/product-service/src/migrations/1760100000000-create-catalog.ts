import type { MigrationInterface, QueryRunner } from 'typeorm';

export class CreateCatalog1760100000000 implements MigrationInterface {
  name = 'CreateCatalog1760100000000';

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      CREATE TABLE products (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        sku TEXT NOT NULL,
        name TEXT NOT NULL,
        description TEXT NOT NULL DEFAULT '',
        category TEXT NOT NULL CHECK (category IN ('food', 'drink', 'snack', 'merchandise')),
        price_minor BIGINT NOT NULL CHECK (price_minor >= 0),
        currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
        status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'inactive')),
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT products_sku_key UNIQUE (sku)
      )
    `);
    await queryRunner.query(
      `CREATE INDEX idx_products_list ON products (status, category, created_at DESC, id)`,
    );
    await queryRunner.query(
      `CREATE INDEX idx_products_category_list ON products (category, created_at DESC, id)`,
    );
    await queryRunner.query(`
      CREATE TABLE event_inventory (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        ticketed_event_id UUID NOT NULL,
        product_id UUID NOT NULL REFERENCES products (id),
        price_minor BIGINT CHECK (price_minor IS NULL OR price_minor >= 0),
        stock_total INTEGER NOT NULL CHECK (stock_total >= 0),
        stock_available INTEGER NOT NULL CHECK (stock_available >= 0),
        status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'inactive')),
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT event_inventory_event_product_key UNIQUE (ticketed_event_id, product_id),
        CONSTRAINT event_inventory_available_lte_total CHECK (stock_available <= stock_total)
      )
    `);
    await queryRunner.query(
      `CREATE INDEX idx_event_inventory_product_id ON event_inventory (product_id)`,
    );
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP TABLE IF EXISTS event_inventory`);
    await queryRunner.query(`DROP TABLE IF EXISTS products`);
  }
}
