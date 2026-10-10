import { Module } from '@nestjs/common';
import type { ConfigType } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ProductEntity } from '../adapter/repository/postgres/entities/product.entity.js';
import { CreateCatalog1760100000000 } from '../migrations/1760100000000-create-catalog.js';
import { appConfig } from '../platform/config/app.config.js';
import { DatabaseMigrator } from '../platform/db/database-migrator.js';

@Module({
  imports: [
    TypeOrmModule.forRootAsync({
      inject: [appConfig.KEY],
      useFactory: (config: ConfigType<typeof appConfig>) => ({
        type: 'postgres' as const,
        url: config.databaseUrl,
        entities: [ProductEntity],
        migrations: [CreateCatalog1760100000000],
        migrationsTableName: 'schema_migrations',
        migrationsRun: false,
        synchronize: false,
        extra: {
          max: config.dbMaxConns,
          options: `-c statement_timeout=${config.dbStatementTimeoutMs} -c lock_timeout=${config.dbLockTimeoutMs}`,
        },
      }),
    }),
  ],
  providers: [DatabaseMigrator],
})
export class DatabaseModule {}
