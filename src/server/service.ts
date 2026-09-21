import {
  appendBatchEvent,
  appendItemEvent,
  BatchItem,
  BatchRecord,
  countItems,
  createBatchRecord,
  createStore,
  matchesFilter,
  recomputeStatus,
  Store,
} from './store.js';
import {applyHeaders, Broker, createBroker} from './broker.js';
import {DeadLetter, FilterSpec, StoredBatchEvent, TransformSpec} from '../shared/protocol.js';

export type ServiceErrorCode =
  | 'not_found'
  | 'revision_conflict'
  | 'invalid_state'
  | 'empty_freeze'
  | 'invalid_config';

export class ServiceError extends Error {
  constructor(
    readonly code: ServiceErrorCode,
    message: string,
    readonly current?: BatchRecord,
  ) {
    super(message);
  }
}

// ---------------------------------------------------------------------------
// Filtering + freeze
// ---------------------------------------------------------------------------

export interface PreviewResult {
  filter: FilterSpec;
  matches: DeadLetter[];
  total: number;
}

export function createService(
  store: Store = createStore(),
  broker: Broker = createBroker(),
  now: () => Date = () => new Date(),
) {
  const subscribers = new Map<string, Set<(event: StoredBatchEvent) => void>>();
  // Runtime-only cursor: events stay in record.events forever so a
  // reconnecting client can replay; dispatchedSeq tracks what the
  // live fan-out has already pushed.
  const dispatchedSeq = new Map<string, number>();

  function emit(record: BatchRecord, event: StoredBatchEvent) {
    const set = subscribers.get(record.id);
    if (set) for (const listener of set) listener(event);
  }

  function subscribe(recordId: string, afterSeq: number, listener: (event: StoredBatchEvent) => void) {
    const record = store.getBatch(recordId);
    if (!record) throw new ServiceError('not_found', 'batch not found');
    // Replay any events the subscriber missed (SSE reconnect / 断線恢復).
    for (const event of record.events) if (event.seq > afterSeq) listener(event);
    let set = subscribers.get(recordId);
    if (!set) {
      set = new Set();
      subscribers.set(recordId, set);
    }
    set.add(listener);
    return () => {
      set!.delete(listener);
    };
  }

  function preview(filter: FilterSpec): PreviewResult {
    const all = store.listDeadLetters();
    // The set is computed on demand and frozen only on createBatch:
    // letters that arrive between preview and freeze are handled by the
    // freeze-time snapshot (and exposed as drift afterwards).
    return {filter, matches: all.filter((l) => matchesFilter(l, filter)), total: all.length};
  }

  /**
   * Freeze the filter result together with a transform-config summary.
   * Snapshot semantics: later dead-letter arrivals never enter this batch.
   */
  function createBatch(filter: FilterSpec, transform: TransformSpec): BatchRecord {
    const cleanTransform = validateTransform(transform);
    const letters = store.listDeadLetters().filter((l) => matchesFilter(l, filter));
    if (letters.length === 0) throw new ServiceError('empty_freeze', 'filter matched no dead letters');
    const record = createBatchRecord(filter, cleanTransform, letters, now());
    store.putBatch(record);
    return record;
  }

  /**
   * Change transform config before start. Config is part of the frozen
   * summary, so an edit bumps revision and requires the page to hold the
   * current one — two pages cannot silently overwrite each other.
   */
  function updateTransform(batchId: string, revision: number, transform: TransformSpec): BatchRecord {
    const record = requireBatch(batchId);
    assertRevision(record, revision);
    if (record.status !== 'pending') {
      throw new ServiceError('invalid_state', `cannot edit transform while ${record.status}`, record);
    }
    record.transform = validateTransform(transform);
    record.revision += 1;
    appendBatchEvent(record);
    emitAfterMutation(record);
    return record;
  }

  // -------------------------------------------------------------------------
  // Sending
  // -------------------------------------------------------------------------

  /** Start sending (or continue after partial failure). Idempotent per state. */
  function start(batchId: string, revision: number): BatchRecord {
    const record = requireBatch(batchId);
    // Duplicate start (double-click / two pages) is a safe no-op and must
    // never race the revision CAS into a spurious conflict or a second pump.
    if (record.status === 'running') return record;
    assertRevision(record, revision);
    if (record.status !== 'pending' && record.status !== 'partial') {
      throw new ServiceError('invalid_state', `cannot start batch while ${record.status}`, record);
    }
    if (!record.startedAt) record.startedAt = now().toISOString();
    record.status = 'running';
    record.revision += 1;
    appendBatchEvent(record);
    emitAfterMutation(record);
    void pump(record);
    return record;
  }

  /** Retry only the still-failed items. Succeeded items are never re-sent. */
  function retryFailed(batchId: string, revision: number): BatchRecord {
    const record = requireBatch(batchId);
    assertRevision(record, revision);
    if (record.status !== 'partial') {
      throw new ServiceError('invalid_state', `retry is only available from partial state (was ${record.status})`, record);
    }
    const failed = record.items.filter((i) => i.status === 'failed');
    if (failed.length === 0) throw new ServiceError('invalid_state', 'no failed items', record);
    for (const item of failed) {
      item.status = 'pending';
      item.error = undefined;
      record.revision += 1;
      appendItemEvent(record, item);
    }
    record.status = 'running';
    record.revision += 1;
    appendBatchEvent(record);
    emitAfterMutation(record);
    void pump(record);
    return record;
  }

  /**
   * Cancel: no NEW send is ever started afterwards. Items already in flight
   * finish and keep their outcome; untouched pending items are marked
   * cancelled. Succeeded items are retained.
   */
  function cancel(batchId: string, revision: number): BatchRecord {
    const record = requireBatch(batchId);
    // Revision first: a second page racing a cancel must see the conflict
    // rather than an opaque state error.
    assertRevision(record, revision);
    if (record.status === 'cancelled' || record.status === 'succeeded') {
      throw new ServiceError('invalid_state', `cannot cancel batch while ${record.status}`, record);
    }
    record.status = 'cancelled';
    record.revision += 1;

    const inflight = record.items.some((i) => i.status === 'inflight');
    // Anything not yet started is cancelled immediately. The pump owns
    // inflight items and finalizes the batch when they settle.
    for (const item of record.items) {
      if (item.status === 'pending') {
        item.status = 'cancelled';
        record.revision += 1;
        appendItemEvent(record, item);
      }
    }
    record.finishedAt = inflight ? undefined : now().toISOString();
    appendBatchEvent(record);
    emitAfterMutation(record);
    if (!inflight) finalizeIfDone(record);
    return record;
  }

  // Single-flight serial pump: each batch processes one item at a time, so
  // start/retry races cannot schedule the same item twice and cancel is
  // observed at a clean boundary between sends.
  const pumping = new Set<string>();

  async function pump(record: BatchRecord) {
    if (pumping.has(record.id)) return;
    pumping.add(record.id);
    try {
      for (;;) {
        if (record.status === 'cancelled') break;
        const item = record.items.find((i) => i.status === 'pending');
        if (!item) break;
        await sendOne(record, item);
      }
    } finally {
      pumping.delete(record.id);
    }
    finalizeIfDone(record);
  }

  async function sendOne(record: BatchRecord, item: BatchItem) {
    item.status = 'inflight';
    item.error = undefined;
    record.revision += 1;
    appendItemEvent(record, item);
    appendBatchEvent(record);
    emitAfterMutation(record);

    const headers = applyHeaders(item.letter.headers, record.transform.headers);
    const result = await broker.publish({
      key: item.idempotencyKey,
      originalId: item.letter.id,
      targetTopic: record.transform.targetTopic,
      headers,
      payload: item.letter.payload,
      attempt: item.sends + 1,
    });

    item.sends += 1;
    if (result.ok) {
      item.status = 'succeeded';
      item.error = undefined;
    } else {
      item.status = 'failed';
      item.error = `${result.code}: ${result.message}`;
    }
    record.revision += 1;
    appendItemEvent(record, item);
    appendBatchEvent(record);
    emitAfterMutation(record);
  }

  function finalizeIfDone(record: BatchRecord) {
    if (pumping.has(record.id)) return; // pump still owns the record
    if (record.status === 'cancelled') {
      if (record.items.every((i) => i.status !== 'inflight' && i.status !== 'pending')) {
        if (!record.finishedAt) record.finishedAt = now().toISOString();
        record.revision += 1;
        appendBatchEvent(record);
        emitAfterMutation(record);
      }
      return;
    }
    if (record.status !== 'running') return;
    const busy = record.items.some((i) => i.status === 'pending' || i.status === 'inflight');
    if (busy) return;
    record.status = recomputeStatus(record); // succeeded | partial
    record.finishedAt = now().toISOString();
    record.revision += 1;
    appendBatchEvent(record);
    emitAfterMutation(record);
  }

  // -------------------------------------------------------------------------

  function emitAfterMutation(record: BatchRecord) {
    const from = (dispatchedSeq.get(record.id) ?? 0) + 1;
    for (const event of record.events) {
      if (event.seq >= from) emit(record, event);
    }
    dispatchedSeq.set(record.id, record.events.length);
  }

  function requireBatch(id: string): BatchRecord {
    const record = store.getBatch(id);
    if (!record) throw new ServiceError('not_found', 'batch not found');
    return record;
  }

  function assertRevision(record: BatchRecord, revision: number) {
    if (revision !== record.revision) {
      throw new ServiceError('revision_conflict', `stale revision ${revision}, current ${record.revision}`, record);
    }
  }

  return {
    store,
    broker,
    preview,
    createBatch,
    updateTransform,
    start,
    retryFailed,
    cancel,
    subscribe,
    /** Test hook: wait until the pump settles. */
    async settled(batchId: string, timeout = 2000) {
      const record = requireBatch(batchId);
      const deadline = Date.now() + timeout;
      while (pumping.has(record.id) && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 2));
      }
      return record;
    },
  };
}

function validateTransform(transform: TransformSpec): TransformSpec {
  const targetTopic = String(transform?.targetTopic ?? '').trim();
  if (!targetTopic) throw new ServiceError('invalid_config', 'targetTopic is required');
  const headers: Record<string, string> = {};
  for (const [name, value] of Object.entries(transform?.headers ?? {})) {
    const key = name.trim();
    if (!key) continue;
    headers[key] = String(value);
  }
  return {targetTopic, headers};
}

export type ReplayService = ReturnType<typeof createService>;

// Re-export for route layer convenience.
export {countItems};
