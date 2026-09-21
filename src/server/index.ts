import express from 'express';
import {fileURLToPath} from 'node:url';
import type {ServerResponse} from 'node:http';
import {
  BatchManager,
  Broker,
  DeadLetterStore,
  DomainError,
  SIM_HEADER,
  summarizeTransform,
  type BatchSnapshot,
  type Transform,
} from './domain.js';

export type AppOptions = {
  store?: DeadLetterStore;
  broker?: Broker;
  manager?: BatchManager;
};

export function seedStore(): DeadLetterStore {
  const store = new DeadLetterStore();
  store.seed([
    {
      id: 'dl-0001',
      topic: 'orders.created',
      headers: {[SIM_HEADER]: 'fail-once', 'x-trace': 'tr-1001'},
      body: '{"orderId":"o-1001","amount":4200}',
      failures: [
        {reason: 'timeout', detail: 'upstream did not ack in 5s', at: '2026-09-19T10:01:00Z'},
        {reason: 'timeout', detail: 'retry exhausted', at: '2026-09-19T10:03:00Z'},
      ],
    },
    {
      id: 'dl-0002',
      topic: 'orders.created',
      headers: {'x-trace': 'tr-1002'},
      body: '{"orderId":"o-1002","amount":990}',
      failures: [{reason: 'connection_reset', at: '2026-09-19T11:20:00Z'}],
    },
    {
      id: 'dl-0003',
      topic: 'payments.captured',
      headers: {[SIM_HEADER]: 'reject', 'x-trace': 'tr-1003'},
      body: '{"paymentId":"p-77","amount":1500}',
      failures: [{reason: 'target_rejected', detail: 'schema v2 required', at: '2026-09-20T08:45:00Z'}],
    },
    {
      id: 'dl-0004',
      topic: 'notifications.email',
      headers: {[SIM_HEADER]: 'fail-once', 'x-trace': 'tr-1004'},
      body: '{"emailId":"e-55"}',
      failures: [{reason: 'timeout', detail: 'mail gateway busy', at: '2026-09-20T09:10:00Z'}],
    },
  ]);
  return store;
}

function errorStatus(code: DomainError['code']): number {
  switch (code) {
    case 'not_found':
      return 404;
    case 'invalid_transform':
      return 400;
    case 'config_mismatch':
      return 422;
    case 'revision_conflict':
    case 'invalid_state':
      return 409;
  }
}

function sendError(res: express.Response, error: unknown): void {
  if (error instanceof DomainError) {
    res.status(errorStatus(error.code)).json({
      error: error.code,
      message: error.message,
      ...(error.current ? {current: error.current} : {}),
    });
    return;
  }
  const message = error instanceof Error ? error.message : 'unknown error';
  res.status(500).json({error: 'internal', message});
}

function writeSse(res: ServerResponse, event: unknown): void {
  res.write(`data: ${JSON.stringify(event)}\n\n`);
}

export function createApp(options: AppOptions = {}) {
  const store = options.store ?? seedStore();
  const broker = options.broker ?? new Broker();
  const manager = options.manager ?? new BatchManager(store, broker);

  const app = express();
  app.use(express.json({limit: '1mb'}));

  app.get('/api/bootstrap', (_req, res) => {
    res.json({family: 'message-delivery', deadLetters: store.total});
  });

  /* dead letters (read-only views of the immutable store) ------------ */

  app.get('/api/dead-letters', (req, res) => {
    const filter = {q: stringParam(req.query.q), topic: stringParam(req.query.topic), reason: stringParam(req.query.reason)};
    const rows = store.list(filter);
    res.json({
      total: store.total,
      matched: rows.length,
      filter,
      rows,
    });
  });

  // Ingestion hook used to simulate dead letters arriving mid-filtering.
  app.post('/api/dead-letters', (req, res) => {
    const body = req.body ?? {};
    if (typeof body.topic !== 'string' || !body.topic) {
      res.status(400).json({error: 'invalid_transform', message: 'topic is required'});
      return;
    }
    const letter = store.add({
      id: typeof body.id === 'string' ? body.id : undefined,
      topic: body.topic,
      headers: body.headers ?? {},
      body: typeof body.body === 'string' ? body.body : '',
      failures: body.failures ?? [{reason: body.reason ?? 'delivery_failed'}],
    });
    res.status(201).json(letter);
  });

  /* batches ----------------------------------------------------------- */

  app.post('/api/batches', (req, res) => {
    try {
      const body = req.body ?? {};
      const filter = {
        q: stringParam(body.filter?.q),
        topic: stringParam(body.filter?.topic),
        reason: stringParam(body.filter?.reason),
      };
      const transform: Transform | undefined = body.transform ?? undefined;
      // Validate before freezing so an illegal target is rejected up front.
      const snapshot = manager.create(filter, transform);
      res.status(201).json(snapshot);
    } catch (error) {
      sendError(res, error);
    }
  });

  app.get('/api/batches/:id', (req, res) => {
    try {
      res.json(manager.get(req.params.id));
    } catch (error) {
      sendError(res, error);
    }
  });

  app.post('/api/batches/:id/start', (req, res) => {
    try {
      const expected = readRevision(req.body);
      const snapshot = manager.start(req.params.id, expected, req.body?.transform);
      res.json(snapshot);
    } catch (error) {
      sendError(res, error);
    }
  });

  app.post('/api/batches/:id/cancel', (req, res) => {
    try {
      const snapshot = manager.cancel(req.params.id, readRevision(req.body));
      res.json(snapshot);
    } catch (error) {
      sendError(res, error);
    }
  });

  app.post('/api/batches/:id/retry', (req, res) => {
    try {
      const snapshot = manager.retry(req.params.id, readRevision(req.body));
      res.json(snapshot);
    } catch (error) {
      sendError(res, error);
    }
  });

  /* event stream — missed events replay first, then live events ------- */

  app.get('/api/batches/:id/events', (req, res) => {
    let snapshot: BatchSnapshot;
    try {
      snapshot = manager.get(req.params.id);
    } catch (error) {
      sendError(res, error);
      return;
    }
    const after = Number.parseInt(String(req.query.after ?? '-1'), 10);
    const lastSeen = Number.isFinite(after) ? after : -1;

    res.set({
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    res.flushHeaders?.();

    // Reconnect recovery: first send a full snapshot so a page that returns
    // with an unknown/old cursor can reconcile, then replay missed deltas.
    writeSse(res, {kind: 'snapshot', snapshot});
    for (const event of manager.eventsAfter(snapshot.id, lastSeen)) writeSse(res, event);

    const unsubscribe = manager.subscribe(snapshot.id, (event) => writeSse(res, event));
    const heartbeat = setInterval(() => res.write(': ping\n\n'), 15_000);

    const close = () => {
      clearInterval(heartbeat);
      unsubscribe();
    };
    req.on('close', close);
    req.on('error', close);
  });

  app.locals.store = store;
  app.locals.broker = broker;
  app.locals.manager = manager;
  app.locals.summarizeTransform = summarizeTransform;
  return app;
}

function stringParam(value: unknown): string | null {
  if (value === undefined || value === null) return null;
  const text = String(value).trim();
  return text.length > 0 ? text : null;
}

function readRevision(body: unknown): number {
  const revision = (body as {revision?: unknown} | null | undefined)?.revision;
  const value = Number(revision);
  if (!Number.isInteger(value) || value < 1) {
    throw new DomainError('revision_conflict', 'a positive integer revision is required');
  }
  return value;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  createApp().listen(4174, '127.0.0.1', () => console.log('server http://127.0.0.1:4174'));
}
