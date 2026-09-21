import express, {Response} from 'express';
import {fileURLToPath} from 'node:url';
import type {
  BatchDetail,
  BatchSummary,
  DeadLetter,
  FilterSpec,
  StoredBatchEvent,
  TransformSpec,
} from '../shared/protocol.js';
import {applyHeaders, Broker, createBroker} from './broker.js';
import {
  createService,
  ReplayService,
  ServiceError,
} from './service.js';
import {
  BatchRecord,
  countItems,
  createStore,
  makeDeadLetter,
  matchesFilter,
  Store,
} from './store.js';

export interface AppOptions {
  store?: Store;
  broker?: Broker;
  service?: ReplayService;
}

// ---------------------------------------------------------------------------
// Serializers — original dead letters/history are emitted read-only.
// ---------------------------------------------------------------------------

function itemViewOf(record: BatchRecord) {
  return record.items.map((item) => ({
    originalId: item.letter.id,
    originalTopic: item.letter.topic,
    targetTopic: record.transform.targetTopic,
    status: item.status,
    sends: item.sends,
    error: item.error,
    idempotencyKey: item.idempotencyKey,
  }));
}

function letterPreview(letter: DeadLetter, transform: TransformSpec) {
  return {
    id: letter.id,
    originalTopic: letter.topic,
    targetTopic: transform.targetTopic,
    reason: letter.reason,
    attempts: letter.attempts,
    failedAt: letter.failedAt,
    headers: applyHeaders(letter.headers, transform.headers),
  };
}

function summarize(service: ReplayService, record: BatchRecord): BatchSummary {
  const frozen = new Set(record.items.map((i) => i.letter.id));
  const driftAdded = service.store
    .listDeadLetters()
    .filter((l) => !frozen.has(l.id) && matchesFilter(l, record.filter)).length;
  return {
    id: record.id,
    filter: record.filter,
    transform: record.transform,
    frozenAt: record.frozenAt,
    status: record.status,
    revision: record.revision,
    frozenIds: record.items.map((i) => i.letter.id),
    counts: countItems(record.items),
    driftAdded,
    startedAt: record.startedAt,
    finishedAt: record.finishedAt,
  };
}

function detail(service: ReplayService, record: BatchRecord): BatchDetail {
  return {...summarize(service, record), items: itemViewOf(record)};
}

function sendError(res: Response, error: unknown) {
  if (error instanceof ServiceError) {
    const status =
      error.code === 'not_found'
        ? 404
        : error.code === 'revision_conflict' || error.code === 'invalid_state'
          ? 409
          : 400;
    return res.status(status).json({error: error.code, message: error.message});
  }
  return res.status(500).json({error: 'internal', message: String(error)});
}

export function createApp(options: AppOptions = {}) {
  const store = options.store ?? createStore();
  const broker = options.broker ?? createBroker();
  const service = options.service ?? createService(store, broker);
  const app = express();
  app.use(express.json({limit: '1mb'}));

  // --- Immutable dead-letter log -------------------------------------------

  app.get('/api/bootstrap', (_req, res) =>
    res.json({family: 'dead-letter-replay', count: store.listDeadLetters().length}),
  );

  app.get('/api/dead-letters', (req, res) => {
    const filter: FilterSpec = {
      topic: String(req.query.topic ?? '').trim() || undefined,
      reason: String(req.query.reason ?? '').trim() || undefined,
    };
    const letters = store.listDeadLetters().filter((l) => matchesFilter(l, filter));
    res.json(letters);
  });

  app.get('/api/dead-letters/:id', (req, res) => {
    const letter = store.getDeadLetter(req.params.id);
    if (!letter) return res.status(404).json({error: 'not_found'});
    res.json(letter);
  });

  /** Live (unfrozen) match count shown while the filter is being edited. */
  app.post('/api/dead-letters/preview', (req, res) => {
    const filter = readFilter(req.body);
    const result = service.preview(filter);
    res.json({
      filter: result.filter,
      total: result.total,
      count: result.matches.length,
      matches: result.matches.map((letter) => ({
        id: letter.id,
        topic: letter.topic,
        reason: letter.reason,
        attempts: letter.attempts,
        failedAt: letter.failedAt,
      })),
    });
  });

  /** Demo/test hook: a new dead letter arrives mid-filtering. Append-only. */
  app.post('/api/dev/dead-letters', (req, res) => {
    const id = String(req.body?.id ?? `dl-${Date.now()}`);
    if (store.getDeadLetter(id)) return res.status(409).json({error: 'duplicate_id'});
    const letter = makeDeadLetter({
      id,
      topic: String(req.body?.topic ?? 'orders.v1'),
      reason: String(req.body?.reason ?? 'rejected'),
      payload: String(req.body?.payload ?? `payload:${id}`),
      failedAt: new Date().toISOString(),
      history: [
        {at: new Date().toISOString(), stage: 'deliver', detail: String(req.body?.reason ?? 'rejected')},
      ],
    });
    store.appendDeadLetter(letter);
    res.status(201).json(letter);
  });

  // --- Replay batches ------------------------------------------------------

  app.post('/api/batches', (req, res) => {
    try {
      const filter = readFilter(req.body?.filter);
      const transform = readTransform(req.body?.transform);
      const record = service.createBatch(filter, transform);
      res.status(201).json(detail(service, record));
    } catch (error) {
      sendError(res, error);
    }
  });

  app.get('/api/batches', (_req, res) => {
    res.json(service.store.listBatches().map((record) => summarize(service, record)).reverse());
  });

  app.get('/api/batches/:id', (req, res) => {
    const record = service.store.getBatch(req.params.id);
    if (!record) return res.status(404).json({error: 'not_found'});
    res.json(detail(service, record));
  });

  /** Confirmation preview: frozen set + frozen transform summary. */
  app.get('/api/batches/:id/preview', (req, res) => {
    const record = service.store.getBatch(req.params.id);
    if (!record) return res.status(404).json({error: 'not_found'});
    res.json({
      ...summarize(service, record),
      preview: record.items.map((item) => letterPreview(item.letter, record.transform)),
      immutable: record.items.map((item) => ({
        id: item.letter.id,
        payload: item.letter.payload,
        headers: item.letter.headers,
        history: item.letter.history,
      })),
    });
  });

  app.patch('/api/batches/:id', (req, res) => {
    try {
      const record = service.updateTransform(
        req.params.id,
        readRevision(req.body?.revision),
        readTransform(req.body?.transform),
      );
      res.json(detail(service, record));
    } catch (error) {
      sendRevisionConflict(res, error, service);
    }
  });

  app.post('/api/batches/:id/start', (req, res) => {
    try {
      const record = service.start(req.params.id, readRevision(req.body?.revision));
      res.json(detail(service, record));
    } catch (error) {
      sendRevisionConflict(res, error, service);
    }
  });

  app.post('/api/batches/:id/retry', (req, res) => {
    try {
      const record = service.retryFailed(req.params.id, readRevision(req.body?.revision));
      res.json(detail(service, record));
    } catch (error) {
      sendRevisionConflict(res, error, service);
    }
  });

  app.post('/api/batches/:id/cancel', (req, res) => {
    try {
      const record = service.cancel(req.params.id, readRevision(req.body?.revision));
      res.json(detail(service, record));
    } catch (error) {
      sendRevisionConflict(res, error, service);
    }
  });

  /**
   * Event stream. Reconnect with ?after=<last seq> to replay nothing twice;
   * events carry originalId so the client merges by original message id.
   */
  app.get('/api/batches/:id/events', (req, res) => {
    const record = service.store.getBatch(req.params.id);
    if (!record) return res.status(404).json({error: 'not_found'});
    const after = Number.parseInt(String(req.query.after ?? '0'), 10) || 0;

    res.set({
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    res.flushHeaders?.();

    const write = (event: StoredBatchEvent) => {
      res.write(`id: ${event.seq}\n`);
      res.write(`event: ${event.kind}\n`);
      res.write(`data: ${JSON.stringify(event.data)}\n\n`);
    };

    let unsubscribe: () => void;
    try {
      unsubscribe = service.subscribe(req.params.id, after, write);
    } catch (error) {
      return sendError(res, error);
    }

    const heartbeat = setInterval(() => res.write(': ping\n\n'), 15_000);
    req.on('close', () => {
      clearInterval(heartbeat);
      unsubscribe();
      res.end();
    });
  });

  return app;
}

function sendRevisionConflict(res: Response, error: unknown, service: ReplayService) {
  if (error instanceof ServiceError && error.code === 'revision_conflict' && error.current) {
    return res.status(409).json({
      error: 'revision_conflict',
      message: error.message,
      current: detail(service, error.current),
    });
  }
  return sendError(res, error);
}

function readFilter(body: any): FilterSpec {
  return {
    topic: String(body?.topic ?? '').trim() || undefined,
    reason: String(body?.reason ?? '').trim() || undefined,
  };
}

function readTransform(body: any): TransformSpec {
  if (!body) return {targetTopic: '', headers: {}};
  const headers: Record<string, string> = {};
  for (const [key, value] of Object.entries(body.headers ?? {})) {
    headers[String(key)] = String(value);
  }
  return {targetTopic: String(body.targetTopic ?? ''), headers};
}

function readRevision(value: unknown): number {
  const revision = Number.parseInt(String(value), 10);
  if (!Number.isInteger(revision)) {
    throw new ServiceError('invalid_config', 'revision is required');
  }
  return revision;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  createApp().listen(4174, '127.0.0.1', () => console.log('server http://127.0.0.1:4174'));
}
