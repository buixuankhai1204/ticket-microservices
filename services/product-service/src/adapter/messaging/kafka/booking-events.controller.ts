import { Controller } from '@nestjs/common';
import {
  Ctx,
  EventPattern,
  KafkaContext,
  Payload,
} from '@nestjs/microservices';
import { BookingEventsProcessor } from './booking-events.processor.js';
import { normalizeHeaders, normalizeKey } from './consumed-message.js';
import { BOOKING_EVENTS_TOPIC } from './topics.js';

@Controller()
export class BookingEventsController {
  constructor(private readonly processor: BookingEventsProcessor) {}

  @EventPattern(BOOKING_EVENTS_TOPIC)
  async handle(
    @Payload() _payload: unknown,
    @Ctx() ctx: unknown,
  ): Promise<void> {
    const context = ctx as KafkaContext;
    const raw = context.getMessage();
    const topic = context.getTopic();
    const partition = context.getPartition();
    await this.processor.process(
      {
        topic,
        partition,
        offset: raw.offset,
        key: normalizeKey(raw.key),
        value: Buffer.isBuffer(raw.value) ? raw.value : null,
        headers: normalizeHeaders(raw.headers),
      },
      { heartbeat: context.getHeartbeat() },
    );
    await context.getConsumer().commitOffsets([
      {
        topic,
        partition,
        offset: (BigInt(raw.offset) + 1n).toString(),
      },
    ]);
  }
}
