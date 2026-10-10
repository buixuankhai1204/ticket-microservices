import { randomUUID } from 'node:crypto';
import { ValidationError } from './errors.js';

export const PRODUCT_CATEGORIES = [
  'food',
  'drink',
  'snack',
  'merchandise',
] as const;
export type ProductCategory = (typeof PRODUCT_CATEGORIES)[number];

export const CATALOG_STATUSES = ['active', 'inactive'] as const;
export type CatalogStatus = (typeof CATALOG_STATUSES)[number];

const SKU_PATTERN = /^[A-Z0-9][A-Z0-9_-]{1,39}$/;
const CURRENCY_PATTERN = /^[A-Z]{3}$/;
const MAX_NAME_LENGTH = 120;
const MAX_DESCRIPTION_LENGTH = 1000;

export function assertMinorUnits(value: number, field: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new ValidationError(`${field} must be a non-negative integer`);
  }
}

function assertName(name: string): void {
  if (name.trim().length === 0 || name.length > MAX_NAME_LENGTH) {
    throw new ValidationError(`name must be 1-${MAX_NAME_LENGTH} characters`);
  }
}

function assertDescription(description: string): void {
  if (description.length > MAX_DESCRIPTION_LENGTH) {
    throw new ValidationError(
      `description must be at most ${MAX_DESCRIPTION_LENGTH} characters`,
    );
  }
}

export interface ProductProps {
  id: string;
  sku: string;
  name: string;
  description: string;
  category: ProductCategory;
  priceMinor: number;
  currency: string;
  status: CatalogStatus;
  createdAt: Date;
  updatedAt: Date;
}

export interface NewProduct {
  sku: string;
  name: string;
  description: string;
  category: ProductCategory;
  priceMinor: number;
  currency: string;
}

export interface ProductChanges {
  name?: string;
  description?: string;
  category?: ProductCategory;
  priceMinor?: number;
  status?: CatalogStatus;
}

export class Product {
  readonly id: string;
  readonly sku: string;
  readonly name: string;
  readonly description: string;
  readonly category: ProductCategory;
  readonly priceMinor: number;
  readonly currency: string;
  readonly status: CatalogStatus;
  readonly createdAt: Date;
  readonly updatedAt: Date;

  private constructor(props: ProductProps) {
    this.id = props.id;
    this.sku = props.sku;
    this.name = props.name;
    this.description = props.description;
    this.category = props.category;
    this.priceMinor = props.priceMinor;
    this.currency = props.currency;
    this.status = props.status;
    this.createdAt = props.createdAt;
    this.updatedAt = props.updatedAt;
  }

  static create(input: NewProduct, now: Date): Product {
    if (!SKU_PATTERN.test(input.sku)) {
      throw new ValidationError(
        'sku must be 2-40 characters of A-Z, 0-9, "_" or "-"',
      );
    }
    if (!CURRENCY_PATTERN.test(input.currency)) {
      throw new ValidationError('currency must be a 3-letter ISO code');
    }
    assertName(input.name);
    assertDescription(input.description);
    assertMinorUnits(input.priceMinor, 'price_minor');
    return new Product({
      id: randomUUID(),
      sku: input.sku,
      name: input.name,
      description: input.description,
      category: input.category,
      priceMinor: input.priceMinor,
      currency: input.currency,
      status: 'active',
      createdAt: now,
      updatedAt: now,
    });
  }

  static restore(props: ProductProps): Product {
    return new Product(props);
  }

  withChanges(changes: ProductChanges, now: Date): Product {
    const name = changes.name ?? this.name;
    const description = changes.description ?? this.description;
    const priceMinor = changes.priceMinor ?? this.priceMinor;
    assertName(name);
    assertDescription(description);
    assertMinorUnits(priceMinor, 'price_minor');
    return new Product({
      id: this.id,
      sku: this.sku,
      name,
      description,
      category: changes.category ?? this.category,
      priceMinor,
      currency: this.currency,
      status: changes.status ?? this.status,
      createdAt: this.createdAt,
      updatedAt: now,
    });
  }
}
