import { afterEach, describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config.js';
import { startNode, type TestNode, TestClient, token } from './helpers.js';

let node: TestNode | undefined;

afterEach(async () => {
  await node?.close();
  node = undefined;
});

describe('HTTP endpoints', () => {
  it('serves health, readiness and the demo page', async () => {
    node = await startNode();
    expect(await (await fetch(`${node.httpUrl}/healthz`)).json()).toEqual({ status: 'ok' });
    expect(await (await fetch(`${node.httpUrl}/readyz`)).json()).toEqual({ status: 'ready' });
    const page = await fetch(`${node.httpUrl}/`);
    expect(page.headers.get('content-type')).toContain('text/html');
    expect(await page.text()).toContain('<title>Presence chat demo</title>');
    expect((await fetch(`${node.httpUrl}/nope`)).status).toBe(404);
    expect((await fetch(`${node.httpUrl}/healthz`, { method: 'POST' })).status).toBe(405);
  });

  it('exposes Prometheus metrics that follow the traffic', async () => {
    node = await startNode({ collectProcessMetrics: true });
    const ada = await TestClient.ready(node.wsUrl, await token('ada'));
    await ada.join('lobby');
    ada.send({ type: 'message', room: 'lobby', text: 'counted' });
    await ada.takeType('message');

    const response = await fetch(`${node.httpUrl}/metrics`);
    expect(response.headers.get('content-type')).toContain('text/plain');
    const text = await response.text();
    expect(text).toMatch(/^chat_connections 1$/m);
    expect(text).toMatch(/^chat_connections_opened_total 1$/m);
    expect(text).toMatch(/^chat_local_rooms 1$/m);
    expect(text).toMatch(/^chat_frames_received_total\{type="message"\} 1$/m);
    expect(text).toMatch(/^chat_frames_sent_total\{type="joined"\} 1$/m);
    expect(text).toMatch(/^chat_inbound_handle_seconds_count\{type="join"\} 1$/m);
    expect(text).toMatch(/^process_cpu_user_seconds_total /m);

    await ada.close();
    await new Promise((resolve) => setTimeout(resolve, 50));
    const after = await (await fetch(`${node.httpUrl}/metrics`)).text();
    expect(after).toMatch(/^chat_connections 0$/m);
    expect(after).toMatch(/^chat_disconnects_total\{reason="client"\} 1$/m);
    expect(after).toMatch(/^chat_local_rooms 0$/m);
  });

  it('only mints development tokens when explicitly enabled', async () => {
    node = await startNode();
    expect((await fetch(`${node.httpUrl}/dev/token?name=x`)).status).toBe(404);
    await node.close();

    node = await startNode({ devTokens: true });
    const { token: minted } = (await (await fetch(`${node.httpUrl}/dev/token?name=Grace`)).json()) as { token: string };
    const client = await TestClient.ready(node.wsUrl, minted);
    await client.close();
    expect(minted.split('.')).toHaveLength(3);
  });
});

describe('configuration', () => {
  const secret = { JWT_SECRET: 'x'.repeat(32) };

  it('applies defaults', () => {
    const config = loadConfig(secret);
    expect(config).toMatchObject({
      PORT: 8080,
      HEARTBEAT_INTERVAL_MS: 30_000,
      RATE_LIMIT_CAPACITY: 10,
      DEV_TOKENS: false,
    });
    expect(config.REDIS_URL).toBeUndefined();
  });

  it('parses overrides and rejects unsafe values', () => {
    expect(loadConfig({ ...secret, PORT: '0', DEV_TOKENS: 'true', REDIS_URL: 'redis://cache:6379' })).toMatchObject({
      PORT: 0,
      DEV_TOKENS: true,
      REDIS_URL: 'redis://cache:6379',
    });
    expect(() => loadConfig({})).toThrow(/JWT_SECRET/);
    expect(() => loadConfig({ JWT_SECRET: 'short' })).toThrow(/at least 32/);
    expect(() => loadConfig({ ...secret, BACKPRESSURE_SOFT_BYTES: '10', BACKPRESSURE_HARD_BYTES: '5' })).toThrow(
      /SOFT/,
    );
  });

  it('refuses a weak signing secret at server construction', async () => {
    await expect(startNode({ jwtSecret: 'too-short' })).rejects.toThrow(/at least 32/);
  });

  it('mints tokens the server accepts', async () => {
    node = await startNode();
    const client = await TestClient.ready(node.wsUrl, await token('ok'));
    await client.close();
  });
});
