import type { TxContext } from '../../../platform/port/transactor.js';

export type Row = Record<string, any>;

export async function queryRows(
  tx: TxContext,
  sql: string,
  params: unknown[] = [],
): Promise<Row[]> {
  const result = await tx.query(sql, params);
  if (Array.isArray(result) && Array.isArray(result[0])) {
    return result[0] as Row[];
  }
  return result as Row[];
}
