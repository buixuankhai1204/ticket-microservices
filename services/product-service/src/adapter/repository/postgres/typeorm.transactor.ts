import { Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';
import {
  Transactor,
  type TransactionOptions,
  type TxContext,
} from '../../../platform/port/transactor.js';
import { translateDbError } from './db-errors.js';

@Injectable()
export class TypeOrmTransactor extends Transactor {
  constructor(private readonly dataSource: DataSource) {
    super();
  }

  async run<T>(
    fn: (tx: TxContext) => Promise<T>,
    options: TransactionOptions = {},
  ): Promise<T> {
    const runner = this.dataSource.createQueryRunner();
    try {
      await runner.connect();
      await runner.startTransaction();
      if (options.readOnly) {
        await runner.query('SET TRANSACTION READ ONLY');
      }
      const result = await fn(runner.manager);
      await runner.commitTransaction();
      return result;
    } catch (error) {
      if (runner.isTransactionActive) {
        await runner.rollbackTransaction().catch(() => undefined);
      }
      throw translateDbError(error);
    } finally {
      await runner.release();
    }
  }
}
