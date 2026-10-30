import type { IncomingMessage } from 'node:http';
import { errors, jwtVerify, SignJWT } from 'jose';

export interface Identity {
  userId: string;
  name: string;
  /** Token expiry in epoch milliseconds. */
  expiresAt: number;
}

export type AuthFailure = 'missing_token' | 'expired' | 'bad_signature' | 'invalid_claims' | 'malformed';

export class AuthError extends Error {
  constructor(readonly reason: AuthFailure) {
    super(`authentication failed: ${reason}`);
  }
}

export interface TokenVerifierOptions {
  issuer?: string;
  audience?: string;
  /** Allowed clock skew when checking exp / nbf. */
  clockToleranceSeconds?: number;
}

const encoder = new TextEncoder();

/**
 * Verifies HS256 JWTs. The algorithm is pinned, so tokens signed with `none` or an asymmetric
 * algorithm (the classic key-confusion attack) are rejected. `sub` and `exp` are mandatory; the
 * display name comes from the `name` claim and falls back to `sub`.
 */
export class TokenVerifier {
  private readonly key: Uint8Array;

  constructor(
    secret: string,
    private readonly options: TokenVerifierOptions = {},
  ) {
    if (secret.length < 32) throw new Error('JWT secret must be at least 32 characters');
    this.key = encoder.encode(secret);
  }

  async verify(token: string | undefined): Promise<Identity> {
    if (!token) throw new AuthError('missing_token');
    try {
      const { payload } = await jwtVerify(token, this.key, {
        algorithms: ['HS256'],
        issuer: this.options.issuer,
        audience: this.options.audience,
        clockTolerance: this.options.clockToleranceSeconds ?? 5,
        requiredClaims: ['sub', 'exp'],
      });
      const userId = payload.sub as string;
      const name = typeof payload.name === 'string' && payload.name.trim() ? payload.name.trim().slice(0, 64) : userId;
      return { userId, name, expiresAt: (payload.exp as number) * 1000 };
    } catch (error) {
      throw new AuthError(classify(error));
    }
  }
}

function classify(error: unknown): AuthFailure {
  if (error instanceof errors.JWTExpired) return 'expired';
  if (error instanceof errors.JWSSignatureVerificationFailed) return 'bad_signature';
  if (error instanceof errors.JWTClaimValidationFailed) return 'invalid_claims';
  return 'malformed';
}

/**
 * Takes the token from `Authorization: Bearer <token>` or, because browsers cannot set headers
 * on a WebSocket handshake, from the `access_token` query parameter.
 */
export function tokenFromRequest(request: IncomingMessage): string | undefined {
  const header = request.headers.authorization;
  if (header?.toLowerCase().startsWith('bearer ')) return header.slice(7).trim() || undefined;
  const url = new URL(request.url ?? '/', 'http://localhost');
  return url.searchParams.get('access_token') ?? undefined;
}

export interface MintOptions {
  userId: string;
  name?: string;
  ttlSeconds?: number;
  issuer?: string;
  audience?: string;
}

/** Issues a token. Used by tests, the token script and the opt-in development endpoint. */
export async function mintToken(secret: string, options: MintOptions): Promise<string> {
  let jwt = new SignJWT(options.name ? { name: options.name } : {})
    .setProtectedHeader({ alg: 'HS256', typ: 'JWT' })
    .setSubject(options.userId)
    .setIssuedAt()
    .setExpirationTime(`${options.ttlSeconds ?? 3600}s`);
  if (options.issuer) jwt = jwt.setIssuer(options.issuer);
  if (options.audience) jwt = jwt.setAudience(options.audience);
  return jwt.sign(encoder.encode(secret));
}
