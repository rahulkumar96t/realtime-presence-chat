import { afterEach, describe, expect, it } from 'vitest';
import { InMemoryBroker } from '../src/cluster/memory.js';
import { type Frame, metricValue, startNode, type TestNode, TestClient, token } from './helpers.js';

let node: TestNode | undefined;
const clients: TestClient[] = [];

async function connect(userId: string, name = userId, target = node as TestNode): Promise<TestClient> {
  const client = await TestClient.ready(target.wsUrl, await token(userId, name));
  clients.push(client);
  return client;
}

afterEach(async () => {
  await Promise.all(clients.splice(0).map((client) => client.close()));
  await node?.close();
  node = undefined;
});

const names = (frame: Frame) => (frame.members as Array<{ name: string }>).map((member) => member.name);

describe('rooms and presence', () => {
  it('sends a snapshot on join and announces newcomers to existing members', async () => {
    node = await startNode();
    const ada = await connect('ada', 'Ada');
    expect(await ada.join('lobby')).toMatchObject({ room: 'lobby', members: [{ userId: 'ada', name: 'Ada' }], history: [] });

    const bob = await connect('bob', 'Bob');
    const snapshot = await bob.join('lobby');
    expect(names(snapshot)).toEqual(['Ada', 'Bob']);
    expect(await ada.takeType('presence')).toMatchObject({
      room: 'lobby',
      event: 'join',
      member: { userId: 'bob', name: 'Bob' },
    });
    // The joiner does not get its own presence event; the snapshot already includes it.
    await bob.expectNone((frame) => frame.type === 'presence');
  });

  it('announces departures on leave and on disconnect', async () => {
    node = await startNode();
    const ada = await connect('ada');
    const bob = await connect('bob');
    const cy = await connect('cy');
    for (const client of [ada, bob, cy]) await client.join('lobby');

    bob.send({ type: 'leave', room: 'lobby' });
    expect(await bob.takeType('left')).toMatchObject({ room: 'lobby' });
    expect(await ada.takeType('presence', (frame) => frame.event === 'leave')).toMatchObject({
      member: { userId: 'bob' },
    });

    await cy.close();
    expect(await ada.takeType('presence', (frame) => frame.event === 'leave' && (frame.member as { userId: string }).userId === 'cy')).toBeTruthy();
  });

  it('treats several connections of one user as one presence', async () => {
    node = await startNode();
    const observer = await connect('obs');
    await observer.join('lobby');
    const tab1 = await connect('dana', 'Dana');
    const tab2 = await connect('dana', 'Dana');

    await tab1.join('lobby');
    await observer.takeType('presence', (frame) => frame.event === 'join');
    expect(names(await tab2.join('lobby'))).toEqual(['Dana', 'obs']);
    await observer.expectNone((frame) => frame.type === 'presence');

    await tab1.close();
    await observer.expectNone((frame) => frame.type === 'presence');
    await tab2.close();
    expect(await observer.takeType('presence')).toMatchObject({ event: 'leave', member: { userId: 'dana' } });
  });

  it('limits the number of rooms per connection', async () => {
    node = await startNode({ maxRoomsPerConnection: 2 });
    const ada = await connect('ada');
    await ada.join('a');
    await ada.join('b');
    ada.send({ type: 'join', room: 'c' });
    expect(await ada.takeType('error')).toMatchObject({ code: 'too_many_rooms', request: 'join' });
    // Re-joining a room it is already in is allowed and returns a fresh snapshot.
    expect(await ada.join('a')).toMatchObject({ room: 'a' });
  });
});

describe('messages', () => {
  it('broadcasts to every member, echoing to the sender with its clientMsgId', async () => {
    node = await startNode();
    const ada = await connect('ada', 'Ada');
    const bob = await connect('bob', 'Bob');
    const outsider = await connect('eve');
    await ada.join('lobby');
    await bob.join('lobby');
    await outsider.join('elsewhere');

    ada.send({ type: 'message', room: 'lobby', text: '  hello there  ', clientMsgId: 'local-1' });
    const echoed = await ada.takeType('message');
    const received = await bob.takeType('message');
    expect(received).toMatchObject({
      room: 'lobby',
      text: 'hello there',
      user: { userId: 'ada', name: 'Ada' },
      clientMsgId: 'local-1',
    });
    expect(received.id).toBe(echoed.id);
    expect(new Date(received.sentAt as string).getTime()).not.toBeNaN();
    await outsider.expectNone((frame) => frame.type === 'message');
  });

  it('keeps per-room history for late joiners, capped at the history size', async () => {
    node = await startNode({ broker: new InMemoryBroker(3) });
    const ada = await connect('ada');
    await ada.join('lobby');
    for (const text of ['one', 'two', 'three', 'four']) {
      ada.send({ type: 'message', room: 'lobby', text });
      await ada.takeType('message', (frame) => frame.text === text);
    }
    const late = await connect('late');
    const snapshot = await late.join('lobby');
    expect((snapshot.history as Array<{ text: string }>).map((message) => message.text)).toEqual([
      'two',
      'three',
      'four',
    ]);
  });

  it('delivers each message exactly once to a client that joins while messages are flowing', async () => {
    node = await startNode();
    const sender = await connect('sender');
    await sender.join('busy');
    const sending = (async () => {
      for (let i = 0; i < 8; i += 1) {
        sender.send({ type: 'message', room: 'busy', text: `m${i}` });
        await sender.takeType('message', (frame) => frame.text === `m${i}`);
      }
    })();
    const joiner = await connect('joiner');
    const snapshot = await joiner.join('busy');
    await sending;
    const live: string[] = [];
    for (;;) {
      const frame = await joiner.takeType('message', () => true, 300).catch(() => undefined);
      if (!frame) break;
      live.push(frame.text as string);
    }
    const seen = [...(snapshot.history as Array<{ text: string }>).map((message) => message.text), ...live];
    expect(seen).toEqual(['m0', 'm1', 'm2', 'm3', 'm4', 'm5', 'm6', 'm7']);
  });

  it('relays typing indicators to others but not back to the typist', async () => {
    node = await startNode();
    const ada = await connect('ada', 'Ada');
    const bob = await connect('bob');
    await ada.join('lobby');
    await bob.join('lobby');
    await ada.takeType('presence');

    ada.send({ type: 'typing', room: 'lobby', isTyping: true });
    expect(await bob.takeType('typing')).toMatchObject({ room: 'lobby', user: { userId: 'ada' }, isTyping: true });
    await ada.expectNone((frame) => frame.type === 'typing');
  });

  it('refuses messages and typing for rooms the client has not joined', async () => {
    node = await startNode();
    const ada = await connect('ada');
    ada.send({ type: 'message', room: 'lobby', text: 'hi' });
    expect(await ada.takeType('error')).toMatchObject({ code: 'not_in_room', request: 'message' });
    ada.send({ type: 'typing', room: 'lobby', isTyping: true });
    expect(await ada.takeType('error')).toMatchObject({ code: 'not_in_room', request: 'typing' });
  });
});

describe('protocol errors', () => {
  it('reports malformed frames without dropping the connection', async () => {
    node = await startNode();
    const ada = await connect('ada');
    ada.socket.send('not json');
    expect(await ada.takeType('error')).toMatchObject({ code: 'bad_json' });
    ada.socket.send(JSON.stringify({ v: 7, type: 'ping' }));
    expect(await ada.takeType('error')).toMatchObject({ code: 'unsupported_version' });
    ada.send({ type: 'join', room: 'Not Valid' });
    expect(await ada.takeType('error')).toMatchObject({ code: 'invalid_message' });
    ada.socket.send(Buffer.from([1, 2, 3]), { binary: true });
    expect(await ada.takeType('error')).toMatchObject({ code: 'invalid_message', message: /Binary/ });

    ada.send({ type: 'ping' });
    expect(await ada.takeType('pong')).toBeTruthy();
    expect(await metricValue(node.httpUrl, /^chat_frames_received_total\{type="invalid"\}/)).toBe(4);
  });

  it('closes the connection with 1009 when a frame exceeds the payload limit', async () => {
    node = await startNode({ maxPayloadBytes: 1_024 });
    const ada = await connect('ada');
    ada.send({ type: 'message', room: 'lobby', text: 'x'.repeat(2_000) });
    expect((await ada.closed).code).toBe(1009);
  });
});

describe('rate limiting', () => {
  it('rejects frames beyond the token bucket and reports when to retry', async () => {
    node = await startNode({ rateLimit: { capacity: 3, refillPerSecond: 1, maxViolations: 50 } });
    const ada = await connect('ada');
    for (let i = 0; i < 5; i += 1) ada.send({ type: 'ping' });
    await ada.takeType('pong');
    await ada.takeType('pong');
    await ada.takeType('pong');
    const errors = [await ada.takeType('error'), await ada.takeType('error')];
    for (const error of errors) {
      expect(error).toMatchObject({ code: 'rate_limited' });
      expect(error.retryAfterMs).toBeGreaterThan(0);
      expect(error.retryAfterMs).toBeLessThanOrEqual(1_000);
    }
    await ada.expectNone((frame) => frame.type === 'pong');
    expect(await metricValue(node.httpUrl, /^chat_frames_dropped_total\{reason="rate_limited"\}/)).toBe(2);
  });

  it('disconnects a client that keeps flooding with 1008', async () => {
    node = await startNode({ rateLimit: { capacity: 2, refillPerSecond: 1, maxViolations: 5 } });
    const flooder = await connect('flood');
    for (let i = 0; i < 20; i += 1) flooder.send({ type: 'ping' });
    expect(await flooder.closed).toEqual({ code: 1008, reason: 'rate limit exceeded' });
    expect(await metricValue(node.httpUrl, /^chat_disconnects_total\{reason="rate_limit"\}/)).toBe(1);
  });
});
