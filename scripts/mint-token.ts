/**
 * Prints a signed token for local testing.
 *
 *   JWT_SECRET=... npm run token -- <user-id> [display name] [ttl seconds]
 */
import { mintToken } from '../src/auth.js';

const secret = process.env.JWT_SECRET;
const [userId, name, ttl] = process.argv.slice(2);
if (!secret || !userId) {
  console.error('usage: JWT_SECRET=... npm run token -- <user-id> [display name] [ttl seconds]');
  process.exit(2);
}

console.log(
  await mintToken(secret, {
    userId,
    name,
    ttlSeconds: ttl ? Number(ttl) : 3600,
    issuer: process.env.JWT_ISSUER,
    audience: process.env.JWT_AUDIENCE,
  }),
);
