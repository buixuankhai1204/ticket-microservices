import { Injectable } from '@nestjs/common';
import {
  Counter,
  Gauge,
  Histogram,
  Registry,
  collectDefaultMetrics,
} from '@prometheus-io/client';
import { DataSource } from 'typeorm';

interface PgPool {
  totalCount: number;
  idleCount: number;
  options: { max: number };
}

@Injectable()
export class MetricsService {
  readonly registry = new Registry();

  private readonly requests = new Counter({
    name: 'http_requests_total',
    help: 'Total HTTP requests',
    labelNames: ['method', 'path', 'status'],
    registers: [this.registry],
  });

  private readonly durations = new Histogram({
    name: 'http_requests_duration_seconds',
    help: 'HTTP request duration in seconds',
    labelNames: ['method', 'path', 'status'],
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
    registers: [this.registry],
  });

  constructor(private readonly dataSource: DataSource) {
    collectDefaultMetrics({ register: this.registry });
    const pool = () =>
      (this.dataSource.driver as unknown as { master?: PgPool }).master;
    new Gauge({
      name: 'db_pool_connections_in_use',
      help: 'Database connections currently checked out',
      registers: [this.registry],
      collect() {
        const current = pool();
        this.set(current ? current.totalCount - current.idleCount : 0);
      },
    });
    new Gauge({
      name: 'db_pool_connections_max',
      help: 'Configured maximum database connections',
      registers: [this.registry],
      collect() {
        this.set(pool()?.options.max ?? 0);
      },
    });
  }

  observe(method: string, path: string, status: number, seconds: number): void {
    const labels = { method, path, status: String(status) };
    this.requests.inc(labels);
    this.durations.observe(labels, seconds);
  }

  scrape(): Promise<string> {
    return this.registry.metrics();
  }

  get contentType(): string {
    return this.registry.contentType;
  }
}
