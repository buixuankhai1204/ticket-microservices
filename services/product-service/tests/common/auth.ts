import { randomUUID } from 'node:crypto';
import { SignJWT } from 'jose';

export const JWT_SECRET = 'component-test-secret-0123456789';
export const JWT_ISSUER = 'user-service';
export const ADMIN_ID = randomUUID();
export const USER_ID = randomUUID();

export async function signToken(sub: string): Promise<string> {
  return new SignJWT({})
    .setProtectedHeader({ alg: 'HS256' })
    .setIssuer(JWT_ISSUER)
    .setSubject(sub)
    .setExpirationTime('1h')
    .sign(new TextEncoder().encode(JWT_SECRET));
}
