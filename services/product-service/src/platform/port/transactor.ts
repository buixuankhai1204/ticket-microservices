import type { EntityManager } from 'typeorm';

export type TxContext = EntityManager;

export interface TransactionOptions {
  readOnly?: boolean;
}

export abstract class Transactor {
  abstract run<T>(
    fn: (tx: TxContext) => Promise<T>,
    options?: TransactionOptions,
  ): Promise<T>;
}
