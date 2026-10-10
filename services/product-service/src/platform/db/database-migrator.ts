import { Injectable, Logger, type OnModuleInit } from '@nestjs/common';
import { DataSource } from 'typeorm';

const MIGRATION_LOCK_KEY = 7431000001;

@Injectable()
export class DatabaseMigrator implements OnModuleInit {
  private readonly logger = new Logger(DatabaseMigrator.name);

  constructor(private readonly dataSource: DataSource) {}

  async onModuleInit(): Promise<void> {
    const lockRunner = this.dataSource.createQueryRunner();
    await lockRunner.connect();
    try {
      await lockRunner.query('SELECT pg_advisory_lock($1)', [
        MIGRATION_LOCK_KEY,
      ]);
      const applied = await this.dataSource.runMigrations({
        transaction: 'each',
      });
      this.logger.log(`migrations applied: ${applied.length}`);
    } finally {
      await lockRunner
        .query('SELECT pg_advisory_unlock($1)', [MIGRATION_LOCK_KEY])
        .catch(() => undefined);
      await lockRunner.release();
    }
  }
}
