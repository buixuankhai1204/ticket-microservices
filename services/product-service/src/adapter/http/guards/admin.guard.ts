import {
  ForbiddenException,
  Inject,
  Injectable,
  type CanActivate,
  type ExecutionContext,
} from '@nestjs/common';
import type { ConfigType } from '@nestjs/config';
import { appConfig } from '../../../platform/config/app.config.js';
import type { AuthenticatedRequest } from './authenticated-user.js';

@Injectable()
export class AdminGuard implements CanActivate {
  constructor(
    @Inject(appConfig.KEY)
    private readonly config: ConfigType<typeof appConfig>,
  ) {}

  canActivate(context: ExecutionContext): boolean {
    const request = context.switchToHttp().getRequest<AuthenticatedRequest>();
    const userId = request.user?.id;
    if (!userId || !this.config.adminUserIds.has(userId)) {
      throw new ForbiddenException('admin access required');
    }
    return true;
  }
}
