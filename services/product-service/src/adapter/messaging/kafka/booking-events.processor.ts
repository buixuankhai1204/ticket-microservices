import { Injectable, Logger } from '@nestjs/common';
import { HandleBookingCancelledUseCase } from '../../../usecase/handle-booking-cancelled.usecase.js';
import { RecordBookingConfirmedUseCase } from '../../../usecase/record-booking-confirmed.usecase.js';
import {
  parseBookingEvent,
  PoisonMessageError,
  type BookingEvent,
} from './booking-events.js';
import { ConsumerLifecycle } from './consumer-lifecycle.js';
import { ConsumerPolicy } from './consumer-policy.js';
import type { ConsumedMessage } from './consumed-message.js';
import { DeadLetterPublisher } from './dead-letter.publisher.js';
import { classifyFailure } from './failure-classifier.js';

const HEARTBEAT_SLICE_MS = 3000;

export class ConsumerStoppedError extends Error {
  constructor() {
    super('consumer is shutting down');
    this.name = 'ConsumerStoppedError';
  }
}

export interface ProcessorIo {
  heartbeat: () => Promise<void>;
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

@Injectable()
export class BookingEventsProcessor {
  private readonly logger = new Logger(BookingEventsProcessor.name);

  constructor(
    private readonly recordConfirmed: RecordBookingConfirmedUseCase,
    private readonly handleCancelled: HandleBookingCancelledUseCase,
    private readonly deadLetters: DeadLetterPublisher,
    private readonly policy: ConsumerPolicy,
    private readonly lifecycle: ConsumerLifecycle,
  ) {}

  async process(message: ConsumedMessage, io: ProcessorIo): Promise<void> {
    let event: BookingEvent;
    try {
      event = parseBookingEvent(message);
    } catch (error) {
      if (error instanceof PoisonMessageError) {
        this.logger.error(`poison message -> dlq: ${error.message}`);
        await this.deadLetter(message, `poison: ${error.message}`, io);
        return;
      }
      throw error;
    }
    if (event.kind === 'skip') {
      return;
    }
    await this.applyWithRetry(message, event, io);
  }

  private async applyWithRetry(
    message: ConsumedMessage,
    event: Exclude<BookingEvent, { kind: 'skip' }>,
    io: ProcessorIo,
  ): Promise<void> {
    let delay = this.policy.baseDelayMs;
    for (let attempt = 1; ; attempt++) {
      try {
        await this.apply(event);
        return;
      } catch (error) {
        if (classifyFailure(error) === 'permanent') {
          this.logger.error(`permanent error -> dlq: ${describe(error)}`);
          await this.deadLetter(message, `permanent: ${describe(error)}`, io);
          return;
        }
        if (attempt >= this.policy.maxAttempts) {
          this.logger.error(`max retries exhausted -> dlq: ${describe(error)}`);
          await this.deadLetter(
            message,
            `max-retries after ${attempt} attempts: ${describe(error)}`,
            io,
          );
          return;
        }
        this.logger.warn(
          `transient error, retrying (attempt ${attempt}, backoff ${delay}ms): ${describe(error)}`,
        );
        await this.pause(delay, io);
        delay = Math.min(delay * 2, this.policy.maxDelayMs);
      }
    }
  }

  private async apply(
    event: Exclude<BookingEvent, { kind: 'skip' }>,
  ): Promise<void> {
    if (event.kind === 'confirmed') {
      await this.recordConfirmed.execute(event.input);
    } else {
      await this.handleCancelled.execute(event.input);
    }
  }

  private async deadLetter(
    message: ConsumedMessage,
    reason: string,
    io: ProcessorIo,
  ): Promise<void> {
    let delay = this.policy.baseDelayMs;
    for (;;) {
      try {
        await this.deadLetters.publish({
          sourceTopic: message.topic,
          sourcePartition: message.partition,
          sourceOffset: message.offset,
          key: message.key,
          value: message.value,
          headers: message.headers,
          reason,
        });
        return;
      } catch (error) {
        this.logger.error(
          `dead-letter write failed, retrying the same message: ${describe(error)}`,
        );
        await this.pause(delay, io);
        delay = Math.min(delay * 2, this.policy.maxDelayMs);
      }
    }
  }

  private async pause(totalMs: number, io: ProcessorIo): Promise<void> {
    let remaining = totalMs;
    while (remaining > 0) {
      if (this.lifecycle.signal.aborted) {
        throw new ConsumerStoppedError();
      }
      await io.heartbeat();
      const slice = Math.min(remaining, HEARTBEAT_SLICE_MS);
      await new Promise((resolve) => setTimeout(resolve, slice));
      remaining -= slice;
    }
    if (this.lifecycle.signal.aborted) {
      throw new ConsumerStoppedError();
    }
  }
}
