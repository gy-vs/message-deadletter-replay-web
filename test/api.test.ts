import {describe, expect, it} from 'vitest';
import http from 'node:http';
import request from 'supertest';
import type {AddressInfo} from 'node:net';
import {createApp} from '../src/server/index';
import {createBroker, PublishContext, PublishResult} from '../src/server/broker';
import {createService} from '../src/server/service';
import {createStore, deriveIdempotencyKey} from '../src/server/store';
import type {BatchDetail, BatchStatus, FilterSpec, TransformSpec} from '../src/shared/protocol';

const FROZEN_TRANSFORM: TransformSpec = {targetTopic: 'orders.v1.replay', headers: {}};

async function freeze(
  app: ReturnType<typeof createApp>,
  filter: FilterSpec = {},
  transform: TransformSpec = FROZEN_TRANSFORM,
) {
  const response = await request(app).post('/api/batches').send({filter, transform}).expect(201);
  return response.body as BatchDetail;
}

async function waitForStatus(app: ReturnType<typeof createApp>, id: string, ...statuses: BatchStatus[]) {
  const deadline = Date.now() + 3000;
  for (;;) {
    const response = await request(app).get(`/api/batches/${id}`).expect(200);
    if (statuses.includes(response.body.status)) return response.body as BatchDetail;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${statuses}, got ${response.body.status}`);
    await new Promise((r) => setTimeout(r, 5));
  }
}

function appWithBroker(policy?: (ctx: PublishContext) => PublishResult, delay = 6) {
  const broker = createBroker({delay, policy});
  const app = createApp({broker});
  return {app, broker};
}

// ---------------------------------------------------------------------------

describe('dead-letter replay API', () => {
  it('previews the live filter, then freezes a snapshot immune to new arrivals', async () => {
    const app = createApp();
    const filter: FilterSpec = {topic: 'orders.v1'};

    const live = await request(app).post('/api/dead-letters/preview').send(filter).expect(200);
    expect(live.body.count).toBe(2);

    const batch = await freeze(app, filter);
    expect(batch.frozenIds).toHaveLength(2); // 预览数量与冻结集合一致
    expect(batch.counts.total).toBe(2);

    // A new dead letter arrives AFTER the freeze.
    await request(app)
      .post('/api/dev/dead-letters')
      .send({id: 'dl-late-1', topic: 'orders.v1', reason: 'timeout'})
      .expect(201);

    const liveNow = await request(app).post('/api/dead-letters/preview').send(filter).expect(200);
    expect(liveNow.body.count).toBe(3); // live filter sees the new letter…

    const frozen = await request(app).get(`/api/batches/${batch.id}`).expect(200);
    expect(frozen.body.frozenIds).toHaveLength(2); // …but the frozen batch never changes
    expect(frozen.body.counts.total).toBe(2);
    expect(frozen.body.driftAdded).toBe(1);

    const preview = await request(app).get(`/api/batches/${batch.id}/preview`).expect(200);
    expect(preview.body.preview).toHaveLength(2);
    expect(preview.body.preview.map((p: {id: string}) => p.id).sort()).toEqual([...frozen.body.frozenIds].sort());
  });

  it('rejects a freeze when the filter matches nothing', async () => {
    const app = createApp();
    const response = await request(app)
      .post('/api/batches')
      .send({filter: {topic: 'does-not-exist'}, transform: FROZEN_TRANSFORM});
    expect(response.status).toBe(400);
    expect(response.body.error).toBe('empty_freeze');
  });

  it('treats duplicate concurrent starts as one run and rejects stale revisions', async () => {
    const {app, broker} = appWithBroker();
    const batch = await freeze(app);
    const revision = batch.revision;

    // Two pages press start at the same time with the same revision.
    const [a, b] = await Promise.all([
      request(app).post(`/api/batches/${batch.id}/start`).send({revision}),
      request(app).post(`/api/batches/${batch.id}/start`).send({revision}),
    ]);
    expect(a.status).toBe(200);
    expect(b.status).toBe(200); // duplicate start is a no-op, not an error storm

    const done = await waitForStatus(app, batch.id, 'succeeded');
    expect(done.revision).toBeGreaterThan(revision);
    for (const id of batch.frozenIds) {
      expect(broker.publishCount(deriveIdempotencyKey(batch.id, id))).toBe(1); // no double sends
    }

    // A page that still holds the pre-start revision cannot continue/roll back.
    const stale = await request(app).post(`/api/batches/${batch.id}/cancel`).send({revision});
    expect(stale.status).toBe(409);
    expect(stale.body.error).toBe('revision_conflict');
    expect(stale.body.current.revision).toBe(done.revision);
  });

  it('recovers partial failures by retrying only failed items without duplicating success', async () => {
    // "flaky": first attempt per key fails, later attempts succeed.
    const {app, broker} = appWithBroker();
    const transform: TransformSpec = {targetTopic: 'orders.v1.replay', headers: {'x-sim': 'flaky'}};
    const batch = await freeze(app, {}, transform);
    const ids = batch.frozenIds;

    await request(app).post(`/api/batches/${batch.id}/start`).send({revision: batch.revision}).expect(200);
    const partial = await waitForStatus(app, batch.id, 'partial');
    expect(partial.counts.failed).toBe(ids.length);
    for (const id of ids) expect(broker.publishCount(deriveIdempotencyKey(batch.id, id))).toBe(1);

    // Two concurrent retries: second is rejected (batch left partial state).
    const [r1, r2] = await Promise.all([
      request(app).post(`/api/batches/${batch.id}/retry`).send({revision: partial.revision}),
      request(app).post(`/api/batches/${batch.id}/retry`).send({revision: partial.revision}),
    ]);
    expect(r1.status).toBe(200);
    expect(r2.status).toBe(409);

    const done = await waitForStatus(app, batch.id, 'succeeded');
    expect(done.counts.succeeded).toBe(ids.length);
    for (const id of ids) expect(broker.publishCount(deriveIdempotencyKey(batch.id, id))).toBe(2); // exactly one retry each

    // Nothing failed anymore -> retry is an invalid state, success can never be re-sent.
    const again = await request(app).post(`/api/batches/${batch.id}/retry`).send({revision: done.revision});
    expect(again.status).toBe(409);
    expect(again.body.error).toBe('invalid_state');
    for (const id of ids) expect(broker.publishCount(deriveIdempotencyKey(batch.id, id))).toBe(2);
  });

  it('keeps other items succeeding when the target rejects messages', async () => {
    // Mixed policy: one specific original message is rejected, others deliver.
    const {app} = appWithBroker((ctx) =>
      ctx.originalId === 'dl-1002'
        ? {ok: false, code: 'target_rejected', message: 'blocked'}
        : {ok: true, duplicate: false},
    );
    const batch = await freeze(app);

    await request(app).post(`/api/batches/${batch.id}/start`).send({revision: batch.revision}).expect(200);
    const partial = await waitForStatus(app, batch.id, 'partial');
    expect(partial.counts.succeeded).toBe(4);
    expect(partial.counts.failed).toBe(1);
    const failed = partial.items.find((i) => i.originalId === 'dl-1002');
    expect(failed!.error).toContain('target_rejected');

    // Topic-name rejection via the default policy is also rejected on retry.
    const rejected = createApp();
    const b2 = await freeze(rejected, {}, {targetTopic: 'topic.rejected', headers: {}});
    await request(rejected).post(`/api/batches/${b2.id}/start`).send({revision: b2.revision}).expect(200);
    const p2 = await waitForStatus(rejected, b2.id, 'partial');
    expect(p2.items.every((i: {error?: string}) => i.error?.includes('target_rejected'))).toBe(true);
  });

  it('never starts a new send after cancel and keeps already-succeeded items', async () => {
    const app = createApp();

    // Cancel before start: zero sends, every item cancelled.
    const b1 = await freeze(app);
    await request(app).post(`/api/batches/${b1.id}/cancel`).send({revision: b1.revision}).expect(200);
    await request(app).post(`/api/batches/${b1.id}/start`).send({revision: b1.revision + 1}).expect(409);
    const cancelled = await request(app).get(`/api/batches/${b1.id}`).expect(200);
    expect(cancelled.body.status).toBe('cancelled');
    expect(cancelled.body.counts.cancelled).toBe(5);
    expect(cancelled.body.counts.succeeded).toBe(0);
  });

  it('handles cancel racing an in-flight send: inflight settles, queued items stay unsent', async () => {
    let releaseGate: () => void = () => {};
    const gate = new Promise<void>((resolve) => {
      releaseGate = resolve;
    });
    const store = createStore();
    const broker = createBroker({
      delay: 0,
      policy: (ctx) =>
        ctx.originalId === store.listDeadLetters()[0].id
          ? gate.then(() => ({ok: true, duplicate: false}))
          : {ok: true, duplicate: false},
    });
    const service = createService(store, broker);

    const record = service.createBatch({}, FROZEN_TRANSFORM);
    const firstId = store.listDeadLetters()[0].id;
    service.start(record.id, record.revision);

    // Wait until the first publish is in flight, then cancel.
    const firstKey = deriveIdempotencyKey(record.id, firstId);
    while (broker.publishCount(firstKey) < 1) await new Promise((r) => setTimeout(r, 1));
    // Snapshot the primitive: the store mutates in place, so keeping the
    // record reference would silently observe the post-cancel revision.
    const runningRevision = store.getBatch(record.id)!.revision;
    const afterCancel = service.cancel(record.id, runningRevision);
    expect(afterCancel.items.find((i) => i.letter.id === firstId)?.status).toBe('inflight');
    expect(afterCancel.items.filter((i) => i.status === 'cancelled')).toHaveLength(4);

    // Second cancel with the now-stale revision loses the race.
    let second: unknown;
    try {
      service.cancel(record.id, runningRevision);
    } catch (error) {
      second = error;
    }
    expect((second as {code?: string}).code).toBe('revision_conflict');

    releaseGate();
    const settled = await service.settled(record.id);
    const first = settled.items.find((i) => i.letter.id === firstId)!;
    expect(first.status).toBe('succeeded'); // in-flight success is retained
    expect(settled.items.filter((i) => i.status === 'cancelled')).toHaveLength(4);
    expect(settled.finishedAt).toBeTruthy();
    // Queued items were never touched by the broker.
    for (const item of settled.items) {
      const count = broker.publishCount(item.idempotencyKey);
      expect(count).toBe(item.letter.id === firstId ? 1 : 0);
    }
  });

  it('guards transform-config edits with revision and preserves original messages', async () => {
    const app = createApp();
    const batch = await freeze(app, {}, {targetTopic: 'old.target', headers: {'x-one': '1'}});
    const original = await request(app).get('/api/dead-letters/dl-1001').expect(200);

    // Page A edits with the current revision.
    const patched = await request(app)
      .patch(`/api/batches/${batch.id}`)
      .send({revision: batch.revision, transform: {targetTopic: 'new.target', headers: {'x-two': '2'}}})
      .expect(200);
    expect(patched.body.transform.targetTopic).toBe('new.target');
    expect(patched.body.revision).toBe(batch.revision + 1);
    expect(patched.body.items[0].targetTopic).toBe('new.target');

    // Page B edits with the stale revision -> conflict carrying the truth.
    const stale = await request(app)
      .patch(`/api/batches/${batch.id}`)
      .send({revision: batch.revision, transform: {targetTopic: 'stale.target', headers: {}}})
      .expect(409);
    expect(stale.body.current.transform.targetTopic).toBe('new.target');

    // Invalid config is rejected before start.
    const invalid = await request(app)
      .patch(`/api/batches/${batch.id}`)
      .send({revision: patched.body.revision, transform: {targetTopic: '   ', headers: {}}})
      .expect(400);
    expect(invalid.body.error).toBe('invalid_config');

    // Config locks once sending begins.
    await request(app)
      .post(`/api/batches/${batch.id}/start`)
      .send({revision: patched.body.revision})
      .expect(200);
    const locked = await request(app)
      .patch(`/api/batches/${batch.id}`)
      .send({revision: patched.body.revision + 100, transform: {targetTopic: 'late.target', headers: {}}});
    expect(locked.status).toBe(409);

    // Original message + headers + history remain byte-for-byte immutable.
    const stillOriginal = await request(app).get('/api/dead-letters/dl-1001').expect(200);
    expect(stillOriginal.body).toEqual(original.body);
  });

  it('replays only missed SSE events after a disconnect, keyed by seq', async () => {
    const server = appWithBroker(undefined, 25).app.listen(0);
    await new Promise<void>((resolve) => server.once('listening', () => resolve()));
    const port = (server.address() as AddressInfo).port;
    const forceClose = () => server.closeAllConnections?.();

    interface Frame {
      seq: number;
      kind: 'item' | 'batch';
      data: any;
    }
    // Tolerant SSE parser: individual res.write calls may arrive in
    // separate TCP chunks, so accumulate until a blank line.
    const collectFrames = (after: number, until: (frame: Frame) => boolean) =>
      new Promise<Frame[]>((resolve, reject) => {
        const frames: Frame[] = [];
        let buffer = '';
        const req = http.get(`http://127.0.0.1:${port}/api/batches/${created.id}/events?after=${after}`, (res) => {
          if (res.statusCode !== 200) {
            req.destroy();
            reject(new Error(`stream status ${res.statusCode}`));
            return;
          }
          res.on('data', (chunk) => {
            buffer += chunk.toString();
            const parts = buffer.split('\n\n');
            buffer = parts.pop() ?? '';
            for (const frameText of parts) {
              if (!frameText.trim() || frameText.startsWith(': ')) continue;
              const idLine = frameText.split('\n').find((l) => l.startsWith('id:'));
              const kindLine = frameText.split('\n').find((l) => l.startsWith('event:'));
              const dataLine = frameText.split('\n').find((l) => l.startsWith('data:'));
              if (!idLine || !kindLine || !dataLine) continue;
              const frame: Frame = {
                seq: Number(idLine.slice(3).trim()),
                kind: kindLine.includes('batch') ? 'batch' : 'item',
                data: JSON.parse(dataLine.slice(5).trim()),
              };
              frames.push(frame);
              if (until(frame)) {
                req.destroy();
                resolve(frames);
              }
            }
          });
        });
        req.on('error', reject);
      });

    const createdBody = JSON.stringify({filter: {}, transform: FROZEN_TRANSFORM});
    const created = await new Promise<BatchDetail>((resolve, reject) => {
      const req = http.request(
        {
          hostname: '127.0.0.1',
          port,
          path: '/api/batches',
          method: 'POST',
          headers: {'content-type': 'application/json', 'content-length': Buffer.byteLength(createdBody)},
        },
        (res) => {
          let data = '';
          res.on('data', (chunk) => (data += chunk));
          res.on('end', () => resolve(JSON.parse(data)));
        },
      );
      req.on('error', reject);
      req.end(createdBody);
    });

    // Connect first so the running transition is observed live, then start.
    const firstPhase = collectFrames(0, (frame) => frame.kind === 'batch' && frame.data.status === 'running');
    await new Promise((r) => setTimeout(r, 30));
    await new Promise<void>((resolve, reject) => {
      const body = JSON.stringify({revision: created.revision});
      const req = http.request(
        {hostname: '127.0.0.1', port, path: `/api/batches/${created.id}/start`, method: 'POST', headers: {'content-type': 'application/json', 'content-length': Buffer.byteLength(body)}},
        (res) => {
          let data = '';
          res.on('data', (c) => (data += c));
          res.on('end', () => resolve());
        },
      );
      req.on('error', reject);
      req.end(body);
    });
    const firstFrames = await firstPhase;
    const lastId = Math.max(...firstFrames.map((f) => f.seq));
    expect(lastId).toBeGreaterThan(0);

    // Drop the socket while sends are in flight, let the run finish, resume.
    await new Promise((r) => setTimeout(r, 400));
    const resumed = await collectFrames(lastId, (frame) => frame.kind === 'batch' && frame.data.status === 'succeeded');

    expect(Math.min(...resumed.map((f) => f.seq))).toBeGreaterThan(lastId); // no duplicate replay
    expect(resumed.at(-1)!.data.status).toBe('succeeded');
    // Event ordering is gap-free when first phase + resumed are combined.
    const allSeqs = [...firstFrames.map((f) => f.seq), ...resumed.map((f) => f.seq)];
    expect(allSeqs).toEqual([...new Array(allSeqs.length)].map((_, i) => i + 1));
    // Item events always carry the original message id for client-side merge.
    for (const frame of [...firstFrames, ...resumed].filter((f) => f.kind === 'item')) {
      expect(frame.data.originalId).toMatch(/^dl-/);
    }

    server.close();
    forceClose();
  }, 15000);
});
