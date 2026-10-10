import { registerAs } from '@nestjs/config';
import { z } from 'zod';

const uuidList = z
  .string()
  .default('')
  .transform((value) =>
    value
      .split(',')
      .map((item) => item.trim())
      .filter((item) => item.length > 0),
  )
  .pipe(z.array(z.uuid()));

const envSchema = z.object({
  NODE_ENV: z
    .enum(['development', 'test', 'production'])
    .default('development'),
  PORT: z.coerce.number().int().min(1).max(65535).default(8086),
  DATABASE_URL: z.string().min(1),
  DB_MAX_CONNS: z.coerce.number().int().min(2).max(200).default(20),
  DB_STATEMENT_TIMEOUT_MS: z.coerce.number().int().min(100).default(10000),
  DB_LOCK_TIMEOUT_MS: z.coerce.number().int().min(100).default(5000),
  JWT_SECRET: z.string().min(16),
  JWT_ISSUER: z.string().min(1).default('user-service'),
  PRODUCT_ADMIN_USER_IDS: uuidList,
  DEFAULT_CURRENCY: z
    .string()
    .regex(/^[A-Z]{3}$/)
    .default('USD'),
  LOG_LEVEL: z
    .enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent'])
    .default('info'),
});

export function loadConfig(env: NodeJS.ProcessEnv) {
  const parsed = envSchema.safeParse(env);
  if (!parsed.success) {
    const issues = parsed.error.issues
      .map((issue) => `${issue.path.join('.') || 'env'}: ${issue.message}`)
      .join('; ');
    throw new Error(`invalid configuration: ${issues}`);
  }
  const values = parsed.data;
  return {
    nodeEnv: values.NODE_ENV,
    port: values.PORT,
    databaseUrl: values.DATABASE_URL,
    dbMaxConns: values.DB_MAX_CONNS,
    dbStatementTimeoutMs: values.DB_STATEMENT_TIMEOUT_MS,
    dbLockTimeoutMs: values.DB_LOCK_TIMEOUT_MS,
    jwtSecret: values.JWT_SECRET,
    jwtIssuer: values.JWT_ISSUER,
    adminUserIds: new Set<string>(values.PRODUCT_ADMIN_USER_IDS),
    defaultCurrency: values.DEFAULT_CURRENCY,
    logLevel: values.LOG_LEVEL,
  };
}

export type AppConfig = ReturnType<typeof loadConfig>;

export const appConfig = registerAs('app', (): AppConfig =>
  loadConfig(process.env),
);
