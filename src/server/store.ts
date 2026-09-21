import {randomUUID} from 'node:crypto';
import type {
  BatchCounts,
  BatchEventData,
  BatchItemView,
  BatchStatus,
  DeadLetter,
  FilterSpec,
  ItemEventData,
  ItemStatus,
  StoredBatchEvent,
  TransformSpec,
} from '../shared/protocol.js';

// ---------------------------------------------------------------------------
// In-memory immutable dead-letter log.
// The seed plus test-injected rows are never mutated: replay only *reads* them.
// ---------------------------------------------------------------------------

const ISO = (t: number) => new Date(t).toISOString();

export function makeDeadLetter(partial: Partial<DeadLetter> & {id: string}): DeadLetter {
  return {
    topic: 'orders.v1',
    reason: 'rejected',
    payload: `payload:${partial.id}`,
    headers: {'x-trace': `trace-${partial.id}`},
    attempts: 3,
    failedAt: ISO(Date.parse('2026-09-01T00:00:00Z')),
    history: [{at: ISO(Date.parse('2026-09-01T00:00:00Z')), stage: 'deliver', detail: partial.reason ?? 'rejected'}],
    ...partial,
  };
}

const seed: DeadLetter[] = [
  makeDeadLetter({id: 'dl-1001', topic: 'orders.v1', reason: 'rejected'}),
  makeDeadLetter({id: 'dl-1002', topic: 'orders.v1', reason: 'timeout'}),
  makeDeadLetter({id: 'dl-1003', topic: 'payments.v1', reason: 'rejected'}),
  makeDeadLetter({id: 'dl-1004', topic: 'payments.v1', reason: 'transform_error'}),
  makeDeadLetter({id: 'dl-1005', topic: 'inventory.v1', reason: 'timeout'}),
];

export interface Store {
  listDeadLetters(): DeadLetter[];
  getDeadLetter(id: string): DeadLetter | undefined;
  /** Append-only: used to simulate dead letters arriving while a filter is open. */
  appendDeadLetter(letter: DeadLetter): void;
  listBatches(): BatchRecord[];
  getBatch(id: string): BatchRecord | undefined;
  putBatch(record: BatchRecord): void;
}

export function createStore(initial: DeadLetter[] = seed): Store {
  const letters = [...initial];
  const batches = new Map<string, BatchRecord>();
  return {
    listDeadLetters: () => [...letters],
    getDeadLetter: (id) => letters.find((l) => l.id === id),
    appendDeadLetter: (letter) => {
      letters.push(letter);
    },
    listBatches: () => [...batches.values()],
    getBatch: (id) => batches.get(id),
    putBatch: (record) => {
      batches.set(record.id, record);
    },
  };
}

export function matchesFilter(letter: DeadLetter, filter: FilterSpec): boolean {
  if (filter.topic && letter.topic !== filter.topic) return false;
  if (filter.reason && letter.reason !== filter.reason) return false;
  return true;
}

// ---------------------------------------------------------------------------
// Batch aggregate
// ---------------------------------------------------------------------------

export interface BatchItem {
  letter: DeadLetter;
  status: ItemStatus;
  sends: number;
  error?: string;
  idempotencyKey: string;
}

export interface BatchRecord {
  id: string;
  filter: FilterSpec;
  transform: TransformSpec;
  frozenAt: string;
  status: BatchStatus;
  revision: number;
  items: BatchItem[];
  events: StoredBatchEvent[];
  startedAt?: string;
  finishedAt?: string;
}

/**
 * Deterministic derived idempotency key.
 * Same original message + same frozen batch => same key on every retry,
 * so a broker dedupes redeliveries of already-succeeded messages.
 */
export function deriveIdempotencyKey(batchId: string, originalId: string): string {
  return `replay:${batchId}:${originalId}`;
}

export function createBatchRecord(filter: FilterSpec, transform: TransformSpec, letters: DeadLetter[], now = new Date()): BatchRecord {
  const id = randomUUID();
  const items: BatchItem[] = letters.map((letter) => ({
    letter,
    status: 'pending',
    sends: 0,
    idempotencyKey: deriveIdempotencyKey(id, letter.id),
  }));
  return {
    id,
    filter,
    transform,
    frozenAt: now.toISOString(),
    status: 'pending',
    revision: 1,
    items,
    events: [],
  };
}

export function countItems(items: BatchItem[]): BatchCounts {
  const counts: BatchCounts = {total: items.length, pending: 0, inflight: 0, succeeded: 0, failed: 0, cancelled: 0};
  for (const item of items) counts[item.status] += 1;
  return counts;
}

export function recomputeStatus(record: BatchRecord): BatchStatus {
  const counts = countItems(record.items);
  if (record.status === 'cancelled') return 'cancelled';
  if (counts.succeeded === counts.total) return 'succeeded';
  if (counts.failed > 0 && counts.pending === 0 && counts.inflight === 0) return 'partial';
  if (counts.inflight > 0 || counts.pending > 0) return 'running';
  return record.status;
}

// ---------------------------------------------------------------------------
// Event log helpers (per-batch monotonic seq; survives SSE disconnects).
// ---------------------------------------------------------------------------

export function appendItemEvent(record: BatchRecord, item: BatchItem): ItemEventData {
  const data: ItemEventData = {
    originalId: item.letter.id,
    status: item.status,
    sends: item.sends,
    error: item.error,
    revision: record.revision,
  };
  record.events.push({seq: record.events.length + 1, kind: 'item', data});
  return data;
}

export function appendBatchEvent(record: BatchRecord): BatchEventData {
  const data: BatchEventData = {
    status: record.status,
    revision: record.revision,
    counts: countItems(record.items),
    finishedAt: record.finishedAt,
  };
  record.events.push({seq: record.events.length + 1, kind: 'batch', data});
  return data;
}

// ---------------------------------------------------------------------------
// Views (original letter content is exposed read-only; never a transform input)
// ---------------------------------------------------------------------------

export function itemView(item: BatchItem): BatchItemView {
  return {
    originalId: item.letter.id,
    originalTopic: item.letter.topic,
    targetTopic: '', // filled by caller, which knows the effective transform
    status: item.status,
    sends: item.sends,
    error: item.error,
    idempotencyKey: item.idempotencyKey,
  };
}
