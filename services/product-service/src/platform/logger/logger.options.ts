import { randomUUID } from 'node:crypto';
import type { IncomingMessage } from 'node:http';
import type { ConfigType } from '@nestjs/config';
import type { Params } from 'nestjs-pino';
import type { appConfig } from '../config/app.config.js';

const REQUEST_ID_HEADER = 'x-request-id';
const REQUEST_ID_PATTERN = /^[A-Za-z0-9._-]{1,128}$/;
const QUIET_PATHS = new Set(['/healthz', '/readyz', '/metrics']);

function pathOf(url: string | undefined): string {
  return (url ?? '').split('?')[0];
}

function originalUrlOf(request: IncomingMessage): string | undefined {
  return (
    (request as IncomingMessage & { originalUrl?: string }).originalUrl ??
    request.url
  );
}

export function buildLoggerParams(
  config: ConfigType<typeof appConfig>,
): Params {
  return {
    pinoHttp: {
      level: config.logLevel,
      genReqId: (request, response) => {
        const incoming = request.headers[REQUEST_ID_HEADER];
        const id =
          typeof incoming === 'string' && REQUEST_ID_PATTERN.test(incoming)
            ? incoming
            : randomUUID();
        response.setHeader('X-Request-Id', id);
        return id;
      },
      serializers: {
        req: () => undefined,
        res: () => undefined,
      },
      customSuccessObject: (request, response, value) => ({
        request_id: request.id,
        method: request.method,
        path: pathOf(originalUrlOf(request)),
        status: response.statusCode,
        duration_ms: value.responseTime,
      }),
      customErrorObject: (request, response, _error, value) => ({
        request_id: request.id,
        method: request.method,
        path: pathOf(originalUrlOf(request)),
        status: response.statusCode,
        duration_ms: value.responseTime,
      }),
      customLogLevel: (_request, response, error) =>
        error || response.statusCode >= 500 ? 'error' : 'info',
      customSuccessMessage: () => 'request completed',
      customErrorMessage: () => 'request failed',
      autoLogging: {
        ignore: (request) => QUIET_PATHS.has(pathOf(originalUrlOf(request))),
      },
    },
  };
}
