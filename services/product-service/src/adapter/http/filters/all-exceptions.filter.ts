import {
  Catch,
  HttpException,
  HttpStatus,
  Injectable,
  type ArgumentsHost,
  type ExceptionFilter,
} from '@nestjs/common';
import type { Request, Response } from 'express';
import { InjectPinoLogger, PinoLogger } from 'nestjs-pino';
import { DomainError, type DomainErrorKind } from '../../../domain/errors.js';

const KIND_STATUS: Record<DomainErrorKind, number> = {
  not_found: HttpStatus.NOT_FOUND,
  conflict: HttpStatus.CONFLICT,
  validation: HttpStatus.BAD_REQUEST,
  repository: HttpStatus.INTERNAL_SERVER_ERROR,
};

const INTERNAL_MESSAGE = 'internal server error';

interface Resolved {
  status: number;
  message: string;
}

@Catch()
@Injectable()
export class AllExceptionsFilter implements ExceptionFilter {
  constructor(
    @InjectPinoLogger(AllExceptionsFilter.name)
    private readonly logger: PinoLogger,
  ) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    if (host.getType() !== 'http') {
      throw exception;
    }
    const http = host.switchToHttp();
    const response = http.getResponse<Response>();
    const request = http.getRequest<Request & { id?: unknown }>();
    const { status, message } = this.resolve(exception);
    if (status >= 500) {
      this.logger.error(
        { err: exception, request_id: request.id },
        'unhandled error',
      );
    }
    if (response.headersSent) {
      return;
    }
    response.status(status).json({ error: message });
  }

  private resolve(exception: unknown): Resolved {
    if (exception instanceof DomainError) {
      const status = KIND_STATUS[exception.kind];
      return {
        status,
        message: status >= 500 ? INTERNAL_MESSAGE : exception.message,
      };
    }
    if (exception instanceof HttpException) {
      const status = exception.getStatus();
      return {
        status,
        message: status >= 500 ? INTERNAL_MESSAGE : this.httpMessage(exception),
      };
    }
    return {
      status: HttpStatus.INTERNAL_SERVER_ERROR,
      message: INTERNAL_MESSAGE,
    };
  }

  private httpMessage(exception: HttpException): string {
    const body = exception.getResponse();
    if (typeof body === 'string') {
      return body;
    }
    const message = (body as { message?: unknown }).message;
    if (Array.isArray(message)) {
      return message.join('; ');
    }
    return typeof message === 'string' ? message : exception.message;
  }
}
