export interface ConsumedMessage {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
  value: Buffer | null;
  headers: Record<string, string>;
}

export function normalizeHeaders(
  headers: Record<string, unknown> | undefined,
): Record<string, string> {
  const result: Record<string, string> = {};
  for (const [name, value] of Object.entries(headers ?? {})) {
    if (value === undefined || value === null) {
      continue;
    }
    result[name] = Buffer.isBuffer(value) ? value.toString() : String(value);
  }
  return result;
}

export function normalizeKey(key: unknown): string | null {
  if (key === undefined || key === null) {
    return null;
  }
  return Buffer.isBuffer(key) ? key.toString() : String(key);
}
