export abstract class ConsumerPolicy {
  abstract readonly maxAttempts: number;
  abstract readonly baseDelayMs: number;
  abstract readonly maxDelayMs: number;
}
