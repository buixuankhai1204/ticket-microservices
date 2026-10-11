import { DomainError, RepositoryError } from '../../../domain/errors.js';

export type FailureKind = 'transient' | 'permanent';

const TRANSIENT_SQLSTATES = new Set([
  '40001',
  '40P01',
  '55P03',
  '55006',
  '53300',
  '57P01',
  '57P02',
  '57P03',
]);

export function classifyFailure(error: unknown): FailureKind {
  if (error instanceof RepositoryError) {
    if (!error.code) {
      return 'transient';
    }
    return TRANSIENT_SQLSTATES.has(error.code) || error.code.startsWith('08')
      ? 'transient'
      : 'permanent';
  }
  if (error instanceof DomainError) {
    return 'permanent';
  }
  return 'transient';
}
