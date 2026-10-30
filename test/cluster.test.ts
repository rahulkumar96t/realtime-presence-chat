import { afterEach, describe, expect, it } from 'vitest';
import { InMemoryBroker } from '../src/cluster/memory.js';
import { rejectedStatus, startNode, type TestNode, TestClient, token } from './helpers.js';

const nodes: TestNode[] = [];
const clients: TestClient[] = [];

async function cluster(size: number): Promise<TestNode[]> {
  const broker = new InMemoryBroker(20);
  for (let i = 0; i < size; i += 1) nodes.push(await startNode({ broker }));
  return nodes.slice(-size);
}

async function connect(node: TestNode, userId: string): Promise<TestClient> {
  const client = await TestClient.ready(node.wsUrl, await token(userId, userId.toUpperCase()));
  clients.push(client);
  return client;
}

afterEach(async () => {
  await Promise.all(clients.splice(0).map((client) => client.close()));
  await Promise.all(nodes.splice(0).map((node) => node.close()));
});

describe('two server instances sharing the in-memory adapter', () => {
  it('fans out messages, typing and presence across nodes', async () => {
    const [one, two] = await cluster(2);
    const ada = await connect(one!, 'ada');
    const bob = await connect(two!, 'bob');

    await ada.join('lobby');
    const snapshot = await bob.join('lobby');
    expect((snapshot.members as Array<{ userId: string }>).map((member) => member.userId)).toEqual(['ada', 'bob']);
    expect(await ada.takeType('presence')).toMatchObject({ event: 'join', member: { userId: 'bob' } });

    ada.send({ type: 'message', room: 'lobby', text: 'from node one' });
    expect(await bob.takeType('message')).toMatchObject({ text: 'from node one', user: { userId: 'ada' } });
    bob.send({ type: 'message', room: 'lobby', text: 'from node two' });
    expect(await ada.takeType('message', (frame) => frame.text === 'from node two')).toBeTruthy();

    bob.send({ type: 'typing', room: 'lobby', isTyping: true });
    expect(await ada.takeType('typing')).toMatchObject({ user: { userId: 'bob' }, isTyping: true });

    bob.send({ type: 'leave', room: 'lobby' });
    expect(await ada.takeType('presence', (frame) => frame.event === 'leave')).toMatchObject({
      member: { userId: 'bob' },
    });
  });

  it('serves shared history to a late joiner on another node', async () => {
    const [one, two] = await cluster(2);
    const ada = await connect(one!, 'ada');
    await ada.join('lobby');
    for (const text of ['first', 'second']) {
      ada.send({ type: 'message', room: 'lobby', text });
      await ada.takeType('message', (frame) => frame.text === text);
    }
    const late = await connect(two!, 'late');
    const snapshot = await late.join('lobby');
    expect((snapshot.history as Array<{ text: string }>).map((message) => message.text)).toEqual(['first', 'second']);
  });

  it('counts one presence for a user connected to both nodes', async () => {
    const [one, two] = await cluster(2);
    const watcher = await connect(one!, 'watcher');
    await watcher.join('lobby');
    const tabOne = await connect(one!, 'dana');
    const tabTwo = await connect(two!, 'dana');
    await tabOne.join('lobby');
    await tabTwo.join('lobby');
    expect(await watcher.takeType('presence')).toMatchObject({ event: 'join', member: { userId: 'dana' } });
    await watcher.expectNone((frame) => frame.type === 'presence');

    await tabOne.close();
    await watcher.expectNone((frame) => frame.type === 'presence');
    await tabTwo.close();
    expect(await watcher.takeType('presence')).toMatchObject({ event: 'leave', member: { userId: 'dana' } });
  });

  it('only delivers to nodes that have members in the room', async () => {
    const [one, two] = await cluster(2);
    const ada = await connect(one!, 'ada');
    const bob = await connect(two!, 'bob');
    await ada.join('alpha');
    await bob.join('beta');
    ada.send({ type: 'message', room: 'alpha', text: 'alpha only' });
    await ada.takeType('message');
    await bob.expectNone((frame) => frame.type === 'message');
  });
});

describe('graceful shutdown', () => {
  it('closes clients with 1001, announces their departure on other nodes and refuses new upgrades', async () => {
    const [one, two] = await cluster(2);
    const ada = await connect(one!, 'ada');
    const bob = await connect(two!, 'bob');
    await ada.join('lobby');
    await bob.join('lobby');
    await ada.takeType('presence');

    await one!.close();
    expect(await ada.closed).toEqual({ code: 1001, reason: 'server shutting down' });
    expect(await bob.takeType('presence', (frame) => frame.event === 'leave')).toMatchObject({
      member: { userId: 'ada' },
    });
    expect(one!.server.connectionCount).toBe(0);
    await expect(TestClient.connect(one!.wsUrl, await token('new'))).rejects.toThrow();

    // The surviving node keeps working.
    bob.send({ type: 'message', room: 'lobby', text: 'still here' });
    expect(await bob.takeType('message')).toMatchObject({ text: 'still here' });
  });

  it('reports not-ready while draining and terminates clients that do not complete the close handshake', async () => {
    const [node] = await cluster(1);
    expect((await fetch(`${node!.httpUrl}/readyz`)).status).toBe(200);
    // A client that never answers the close frame (paused socket) keeps the node draining until
    // the grace period (1 s in tests) runs out.
    const stuck = await connect(node!, 'stuck');
    stuck.socket.pause();
    const closing = node!.close();

    const ready = await fetch(`${node!.httpUrl}/readyz`);
    expect(ready.status).toBe(503);
    expect(await ready.json()).toEqual({ status: 'draining' });
    expect(await rejectedStatus(node!.wsUrl, { authorization: `Bearer ${await token('late')}` })).toBe(503);

    await closing;
    expect(node!.server.connectionCount).toBe(0);
    await expect(fetch(`${node!.httpUrl}/healthz`)).rejects.toThrow();
    stuck.socket.resume();
    expect([1001, 1006]).toContain((await stuck.closed).code);
  });
});
