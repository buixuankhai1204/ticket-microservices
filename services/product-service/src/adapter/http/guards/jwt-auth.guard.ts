import {
  Inject,
  Injectable,
  UnauthorizedException,
  type CanActivate,
  type ExecutionContext,
} from '@nestjs/common';
import type { ConfigType } from '@nestjs/config';
import { jwtVerify } from 'jose';
import { appConfig } from '../../../platform/config/app.config.js';
import type { AuthenticatedRequest } from './authenticated-user.js';

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const BEARER_PATTERN = /^Bearer\s+(\S+)$/i;

@Injectable()
export class JwtAuthGuard implements CanActivate {
  private readonly key: Uint8Array;

  constructor(
    @Inject(appConfig.KEY)
    private readonly config: ConfigType<typeof appConfig>,
  ) {
    this.key = new TextEncoder().encode(config.jwtSecret);
  }

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const request = context.switchToHttp().getRequest<AuthenticatedRequest>();
    const token = BEARER_PATTERN.exec(request.headers.authorization ?? '')?.[1];
    if (!token) {
      throw this.unauthorized();
    }
    try {
      const { payload } = await jwtVerify(token, this.key, {
        issuer: this.config.jwtIssuer,
        algorithms: ['HS256'],
        requiredClaims: ['exp', 'sub'],
      });
      if (typeof payload.sub !== 'string' || !UUID_PATTERN.test(payload.sub)) {
        throw this.unauthorized();
      }
      request.user = {
        id: payload.sub,
        email: typeof payload.email === 'string' ? payload.email : undefined,
      };
      return true;
    } catch {
      throw this.unauthorized();
    }
  }

  private unauthorized(): UnauthorizedException {
    return new UnauthorizedException('invalid or missing bearer token');
  }
}
