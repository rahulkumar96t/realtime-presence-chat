import { SignJWT } from 'jose';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { metricValue, rejectedStatus, SECRET, startNode, type TestNode, TestClient, token } from './helpers.js';

let node: TestNode;

beforeAll(async () => {
  node = await startNode({ jwtIssuer: 'chat-tests', jwtAudience: 'chat' });
});

afterAll(async () => {
  await node?.close();
});

const valid = (userId: string, name?: string, ttlSeconds?: number) =>
  token(userId, name, { issuer: 'chat-tests', audience: 'chat', ttlSeconds });

describe('authentication on upgrade', () => {
  it('welcomes a client with a valid bearer token', async () => {
    const client = await TestClient.connect(node.wsUrl, await valid('u-1', 'Ada'));
    const welcome = await client.takeType('welcome');
    expect(welcome).toMatchObject({
      v: 1,
      user: { userId: 'u-1', name: 'Ada' },
      protocolVersions: [1],
      rateLimit: { capacity: 10, refillPerSecond: 5 },
    });
    await client.close();
  });

  it('accepts the token as a query parameter for browsers', async () => {
    const client = await TestClient.connect(node.wsUrl, await valid('u-2'), { via: 'query' });
    expect(await client.takeType('welcome')).toMatchObject({ user: { userId: 'u-2', name: 'u-2' } });
    await client.close();
  });

  it('negotiates the chat.v1 subprotocol when offered', async () => {
    const client = await TestClient.connect(node.wsUrl, await valid('u-3'), { protocols: ['chat.v1'] });
    expect(client.socket.protocol).toBe('chat.v1');
    await client.close();
  });

  it('refuses clients that only offer unknown subprotocols', async () => {
    await expect(TestClient.connect(node.wsUrl, await valid('u-4'), { protocols: ['chat.v9'] })).rejects.toThrow(
      /Server sent no subprotocol/,
    );
  });

  it('answers 401 without a token and counts the failure', async () => {
    const before = await metricValue(node.httpUrl, /^chat_auth_failures_total\{reason="missing_token"\}/);
    expect(await rejectedStatus(node.wsUrl)).toBe(401);
    expect(await metricValue(node.httpUrl, /^chat_auth_failures_total\{reason="missing_token"\}/)).toBe(before + 1);
  });

  it('answers 401 for an expired token', async () => {
    const expired = await new SignJWT({})
      .setProtectedHeader({ alg: 'HS256' })
      .setSubject('u-5')
      .setIssuer('chat-tests')
      .setAudience('chat')
      .setExpirationTime(Math.floor(Date.now() / 1000) - 60)
      .sign(new TextEncoder().encode(SECRET));
    expect(await rejectedStatus(node.wsUrl, { authorization: `Bearer ${expired}` })).toBe(401);
    expect(await metricValue(node.httpUrl, /^chat_auth_failures_total\{reason="expired"\}/)).toBeGreaterThan(0);
  });

  it('answers 401 for a token signed with another secret', async () => {
    const forged = await new SignJWT({})
      .setProtectedHeader({ alg: 'HS256' })
      .setSubject('u-6')
      .setIssuer('chat-tests')
      .setAudience('chat')
      .setExpirationTime('1h')
      .sign(new TextEncoder().encode('another-secret-that-is-also-long-enough'));
    expect(await rejectedStatus(node.wsUrl, { authorization: `Bearer ${forged}` })).toBe(401);
    expect(await metricValue(node.httpUrl, /^chat_auth_failures_total\{reason="bad_signature"\}/)).toBeGreaterThan(0);
  });

  it('answers 401 for an unsigned (alg: none) token', async () => {
    const encode = (value: object) => Buffer.from(JSON.stringify(value)).toString('base64url');
    const unsigned = `${encode({ alg: 'none', typ: 'JWT' })}.${encode({
      sub: 'admin',
      iss: 'chat-tests',
      aud: 'chat',
      exp: Math.floor(Date.now() / 1000) + 3600,
    })}.`;
    expect(await rejectedStatus(node.wsUrl, { authorization: `Bearer ${unsigned}` })).toBe(401);
  });

  it('answers 401 for a token without an expiry or with the wrong audience', async () => {
    const key = new TextEncoder().encode(SECRET);
    const noExp = await new SignJWT({})
      .setProtectedHeader({ alg: 'HS256' })
      .setSubject('u-7')
      .setIssuer('chat-tests')
      .setAudience('chat')
      .sign(key);
    const wrongAudience = await new SignJWT({})
      .setProtectedHeader({ alg: 'HS256' })
      .setSubject('u-8')
      .setIssuer('chat-tests')
      .setAudience('billing')
      .setExpirationTime('1h')
      .sign(key);
    expect(await rejectedStatus(node.wsUrl, { authorization: `Bearer ${noExp}` })).toBe(401);
    expect(await rejectedStatus(node.wsUrl, { authorization: `Bearer ${wrongAudience}` })).toBe(401);
    expect(await metricValue(node.httpUrl, /^chat_auth_failures_total\{reason="invalid_claims"\}/)).toBe(2);
  });

  it('answers 404 for upgrades on other paths', async () => {
    expect(await rejectedStatus(`ws://127.0.0.1:${node.port}/elsewhere`)).toBe(404);
  });

  it('closes the connection with 4001 when the token expires', async () => {
    const client = await TestClient.ready(node.wsUrl, await valid('u-9', 'Brief', 1));
    const closed = await client.closed;
    expect(closed).toEqual({ code: 4001, reason: 'token expired' });
  });
});
