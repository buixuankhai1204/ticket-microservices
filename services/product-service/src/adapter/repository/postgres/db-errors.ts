import { QueryFailedError } from 'typeorm';
import {
  AlreadyExistsError,
  DomainError,
  NotFoundError,
  RepositoryError,
  ValidationError,
} from '../../../domain/errors.js';

interface PgDriverError {
  code?: string;
  constraint?: string;
}

const UNIQUE_MESSAGES: Record<string, string> = {
  products_sku_key: 'sku already exists',
};

export function sqlState(error: unknown): string | undefined {
  if (error instanceof QueryFailedError) {
    return (error.driverError as PgDriverError | undefined)?.code;
  }
  return undefined;
}

export function translateDbError(error: unknown): unknown {
  if (error instanceof DomainError) {
    return error;
  }
  if (!(error instanceof QueryFailedError)) {
    return error;
  }
  const driverError = (error.driverError ?? {}) as PgDriverError;
  switch (driverError.code) {
    case '23505':
      return new AlreadyExistsError(
        UNIQUE_MESSAGES[driverError.constraint ?? ''] ??
          'resource already exists',
      );
    case '23503':
      return new NotFoundError('referenced resource not found');
    case '23514':
      return new ValidationError(
        `constraint violated: ${driverError.constraint ?? 'check'}`,
      );
    case '22P02':
    case '22003':
      return new ValidationError('invalid input value');
    default:
      return new RepositoryError(error.message, driverError.code);
  }
}
