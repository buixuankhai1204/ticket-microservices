import { Injectable, type BeforeApplicationShutdown } from '@nestjs/common';

@Injectable()
export class ConsumerLifecycle implements BeforeApplicationShutdown {
  private readonly controller = new AbortController();

  get signal(): AbortSignal {
    return this.controller.signal;
  }

  beforeApplicationShutdown(): void {
    this.controller.abort();
  }
}
