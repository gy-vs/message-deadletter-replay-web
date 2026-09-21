import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import type {Server} from 'node:http';
import request from 'supertest';
import {createApp, seedStore} from '../src/server/index.js';
import {BatchManager, Broker, SIM_HEADER} from '../src/server/domain.js';

function buildApp(delayMs = 5) {
  const store = seedStore();
  const broker = new Broker({delayMs});
  const manager = new BatchManager(store, broker);
  const app = createApp({store, broker, manager});
  return {app, store, broker, manager};
}

async function pollBatch(app: ReturnType<typeof buildApp>['app'], id: string) {
  for (;;) {
    const response = await request(app).get(`/api/batches/${id}`).expect(200);
    if (['succeeded', 'partial', 'failed', 'cancelled'].includes(response.body.status)) return response.body;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

describe('HTTP API', () => {
  it('filters dead letters and exposes immutable rows', async () => {
    const {app} = buildApp();
    const response = await request(app).get('/api/dead-letters?reason=timeout').expect(200);
    expect(response.body.matched).toBe(2); // dl-0001, dl-0004
    expect(response.body.total).toBeGreaterThanOrEqual(4);
    expect(response.body.rows[0].failures[0]).toHaveProperty('reason');
  });

  it('freezes the preview count so new arrivals do not join the batch', async () => {
    const {app} = buildApp();
    const created = await request(app)
      .post('/api/batches')
      .send({filter: {reason: 'timeout'}, transform: {topic: 'orders.created.v2', headers: {'x-dlq-sim': null}}})
      .expect(201);
    const batchId = created.body.id;
    expect(created.body.status).toBe('preview');
    expect(created.body.items).toHaveLength(2);
    expect(created.body.transformSummary.keepsOriginalTarget).toBe(false);
    expect(created.body.transformSummary.targetTopic).toBe('orders.created.v2');
    expect(created.body.transformSummary.headerChanges).toContainEqual({
      header: SIM_HEADER,
      op: 'remove',
    });

    // A matching dead letter arrives while the user is still reviewing the preview.
    await request(app)
      .post('/api/dead-letters')
      .send({topic: 'orders.created', headers: {[SIM_HEADER]: 'fail-once'}, failures: [{reason: 'timeout'}]})
      .expect(201);
    const live = await request(app).get('/api/dead-letters?reason=timeout').expect(200);
    expect(live.body.matched).toBe(3);
    const preview = await request(app).get(`/api/batches/${batchId}`).expect(200);
    expect(preview.body.items).toHaveLength(2); // frozen
    expect(preview.body.filter.candidateTotal).toBe(4);
  });

  it('duplicate start: the second start loses regardless of revision freshness', async () => {
    const {app} = buildApp(40);
    const created = await request(app).post('/api/batches').send({}).expect(201);
    const started = await request(app)
      .post(`/api/batches/${created.body.id}/start`)
      .send({revision: created.body.revision})
      .expect(200);
    expect(started.body.status).toBe('running');
    // Repeated start with the *new* revision: still an invalid transition.
    const duplicate = await request(app)
      .post(`/api/batches/${created.body.id}/start`)
      .send({revision: started.body.revision})
      .expect(409);
    expect(duplicate.body.error).toBe('invalid_state');
  });

  it('revision conflict blocks two pages continuing the same batch', async () => {
    const {app} = buildApp(40);
    const created = await request(app).post('/api/batches').send({}).expect(201);
    const first = await request(app)
      .post(`/api/batches/${created.body.id}/start`)
      .send({revision: created.body.revision})
      .expect(200);
    // Page B holds the stale preview revision.
    const stale = await request(app)
      .post(`/api/batches/${created.body.id}/cancel`)
      .send({revision: created.body.revision})
      .expect(409);
    expect(stale.body.error).toBe('revision_conflict');
    expect(stale.body.current.revision).toBe(first.body.revision);
  });

  it('retries only failed items after a partial run without duplicating success', async () => {
    const {app, broker} = buildApp(5);
    const created = await request(app).post('/api/batches').send({}).expect(201);
    await request(app)
      .post(`/api/batches/${created.body.id}/start`)
      .send({revision: created.body.revision})
      .expect(200);
    const partial = await pollBatch(app, created.body.id);
    // dl-0001 + dl-0004 are fail-once (2 failed), dl-0002 succeeds, dl-0003 rejected.
    expect(partial.status).toBe('partial');
    expect(partial.counts.failed).toBe(2);
    expect(partial.counts.succeeded).toBe(1);
    expect(partial.counts.rejected).toBe(1);
    const deliveriesBefore = broker.deliveryCount;

    const retry = await request(app)
      .post(`/api/batches/${created.body.id}/retry`)
      .send({revision: partial.revision})
      .expect(200);
    // Serial drain: the first failed item is already claimed as "sending",
    // the other is still pending — crucially no succeeded/rejected item moved.
    const active = retry.body.items.filter(
      (i: {status: string}) => i.status === 'pending' || i.status === 'sending',
    );
    expect(active).toHaveLength(2);
    expect(retry.body.items.filter((i: {status: string}) => i.status === 'failed')).toHaveLength(0);
    const done = await pollBatch(app, created.body.id);
    expect(done.status).toBe('partial'); // rejection stays permanent
    expect(done.counts.succeeded).toBe(3);
    expect(done.counts.rejected).toBe(1);
    expect(broker.deliveryCount).toBe(deliveriesBefore + 2);
  });

  it('rejects an unknown batch and missing revision', async () => {
    const {app} = buildApp();
    await request(app).get('/api/batches/nope').expect(404);
    const created = await request(app).post('/api/batches').send({}).expect(201);
    const bad = await request(app).post(`/api/batches/${created.body.id}/start`).send({}).expect(409);
    expect(bad.body.error).toBe('revision_conflict');
  });

  it('rejects invalid target topics at preview creation', async () => {
    const {app} = buildApp();
    const bad = await request(app)
      .post('/api/batches')
      .send({transform: {topic: 'not a topic!!'}})
      .expect(400);
    expect(bad.body.error).toBe('invalid_transform');
  });

  it('refuses to start with a modified transform and keeps the frozen one', async () => {
    const {app} = buildApp();
    const created = await request(app)
      .post('/api/batches')
      .send({transform: {topic: 'orders.created.v2'}})
      .expect(201);
    const conflict = await request(app)
      .post(`/api/batches/${created.body.id}/start`)
      .send({revision: created.body.revision, transform: {topic: 'orders.created.v3'}})
      .expect(422);
    expect(conflict.body.error).toBe('config_mismatch');
    const stillFrozen = await request(app).get(`/api/batches/${created.body.id}`).expect(200);
    expect(stillFrozen.body.items[0].targetTopic).toBe('orders.created.v2');
  });
});

describe('cancel over HTTP', () => {
  it('stops scheduling after cancel and preserves successes', async () => {
    // Slow broker (80ms/send) so a cancel between serial sends is deterministic.
    const slow = buildApp(80);
    const {app} = slow;
    // Three clean items: at most one in-flight send survives the cancel.
    for (let i = 0; i < 3; i += 1) {
      await request(app)
        .post('/api/dead-letters')
        .send({id: `clean-${i}`, topic: 'replay.clean', headers: {}, body: `clean-${i}`, failures: [{reason: 'timeout'}]})
        .expect(201);
    }
    const batch = await request(app)
      .post('/api/batches')
      .send({filter: {topic: 'replay.clean'}})
      .expect(201);
    expect(batch.body.items).toHaveLength(3);
    const started = await request(app)
      .post(`/api/batches/${batch.body.id}/start`)
      .send({revision: batch.body.revision})
      .expect(200);
    // start() synchronously marked the first item sending; cancel right away:
    // it may finish in flight, the remaining two must never start.
    const cancelled = await request(app)
      .post(`/api/batches/${batch.body.id}/cancel`)
      .send({revision: started.body.revision})
      .expect(200);
    expect(cancelled.body.cancelRequested).toBe(true);
    expect(cancelled.body.items.filter((i: {status: string}) => i.status === 'pending')).toHaveLength(0);

    const done = await pollBatch(app, batch.body.id);
    expect(['cancelled', 'partial']).toContain(done.status);
    // Already-accepted success (if any) is preserved; nothing else sent.
    expect(done.counts.succeeded + done.counts.cancelled).toBe(3);
    expect(done.counts.succeeded).toBeLessThanOrEqual(1);
    expect(done.counts.pending).toBe(0);
    expect(done.counts.sending).toBe(0);
    // Rollback with the stale start revision loses the race.
    await request(app)
      .post(`/api/batches/${batch.body.id}/cancel`)
      .send({revision: started.body.revision})
      .expect(409);
  });
});

/* ----------------------- SSE reconnect coverage ---------------------- */

describe('SSE event stream', () => {
  let server: Server;
  let baseUrl: string;
  const {app} = buildApp(5);

  beforeAll(async () => {
    server = app.listen(0, '127.0.0.1');
    await new Promise<void>((resolve) => server.once('listening', () => resolve()));
    const address = server.address();
    if (address && typeof address === 'object') baseUrl = `http://127.0.0.1:${address.port}`;
  });
  afterAll(
    () =>
      new Promise<void>((resolve) => {
        server.close(() => resolve());
      }),
  );

  async function readEvents(path: string, timeoutMs = 1500): Promise<any[]> {
    const response = await fetch(`${baseUrl}${path}`, {headers: {Accept: 'text/event-stream'}});
    expect(response.ok).toBe(true);
    const reader = response.body!.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    const events: any[] = [];
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const chunk = await Promise.race([
        reader.read(),
        new Promise<{done: true}>((resolve) => setTimeout(() => resolve({done: true as const}), 200)),
      ]);
      if (chunk.done) break;
      buffer += decoder.decode(chunk.value, {stream: true});
      let boundary: number;
      while ((boundary = buffer.indexOf('\n\n')) !== -1) {
        const frame = buffer.slice(0, boundary);
        buffer = buffer.slice(boundary + 2);
        const dataLine = frame.split('\n').find((line) => line.startsWith('data: '));
        if (dataLine) events.push(JSON.parse(dataLine.slice(6)));
      }
      // We only need the snapshot + replay on reconnect.
      if (path.includes('after=0') && events.some((event) => event.kind === 'item')) break;
    }
    await reader.cancel();
    return events;
  }

  it('delivers a snapshot first and merges item events keyed by original message id', async () => {
    const created = await request(app).post('/api/batches').send({}).expect(201);
    await request(app)
      .post(`/api/batches/${created.body.id}/start`)
      .send({revision: created.body.revision})
      .expect(200);
    const terminal = await pollBatch(app, created.body.id);

    // Reconnect after a disconnect with an old cursor.
    const events = await readEvents(`/api/batches/${created.body.id}/events?after=0`);
    const snapshot = events.find((event) => event.kind === 'snapshot');
    expect(snapshot).toBeTruthy();
    expect(snapshot.snapshot.lastSeq).toBe(terminal.lastSeq);
    const itemEvents = events.filter((event) => event.kind === 'item');
    expect(itemEvents.length).toBeGreaterThan(0);
    // Replayed events update the same original-message rows, never duplicate rows.
    const ids = new Set(snapshot.snapshot.items.map((item: {originalMessageId: string}) => item.originalMessageId));
    for (const event of itemEvents) {
      expect(ids.has(event.item.originalMessageId)).toBe(true);
    }
  });
});
