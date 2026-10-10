export type DomainErrorKind =
  'not_found' | 'conflict' | 'validation' | 'repository';

export class DomainError extends Error {
  constructor(
    readonly kind: DomainErrorKind,
    message: string,
  ) {
    super(message);
    this.name = new.target.name;
  }
}

export class NotFoundError extends DomainError {
  constructor(message = 'not found') {
    super('not_found', message);
  }
}

export class AlreadyExistsError extends DomainError {
  constructor(message = 'already exists') {
    super('conflict', message);
  }
}

export class InsufficientStockError extends DomainError {
  constructor(message = 'insufficient stock') {
    super('conflict', message);
  }
}

export class ValidationError extends DomainError {
  constructor(message: string) {
    super('validation', message);
  }
}

export class RepositoryError extends DomainError {
  constructor(
    message: string,
    readonly code?: string,
  ) {
    super('repository', message);
  }
}
