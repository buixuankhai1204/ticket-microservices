export interface DeadLetterRecord {
  sourceTopic: string;
  sourcePartition: number;
  sourceOffset: string;
  key: string | null;
  value: Buffer | null;
  headers: Record<string, string>;
  reason: string;
}

export abstract class DeadLetterPublisher {
  abstract publish(record: DeadLetterRecord): Promise<void>;
}
