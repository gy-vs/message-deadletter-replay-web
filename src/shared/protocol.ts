// Shared protocol types for the dead-letter replay workbench.
// Original dead letters and their failure history are immutable;
// replay batches are the only mutable aggregate.

export interface HistoryEntry {
  at: string;
  stage: string;
  detail: string;
}

/** A dead letter as it exists in the immutable dead-letter log. */
export interface DeadLetter {
  id: string; // original message id
  topic: string;
  reason: string;
  payload: string;
  headers: Record<string, string>;
  attempts: number;
  failedAt: string;
  history: HistoryEntry[];
}

export interface FilterSpec {
  topic?: string; // exact match, empty = any
  reason?: string; // exact match, empty = any
}

export interface TransformSpec {
  /** Destination topic. Required; per-item target may equal the original. */
  targetTopic: string;
  /**
   * Header patch applied on top of the original headers.
   * Empty string value removes the header. Original headers are never mutated.
   */
  headers: Record<string, string>;
}

export type BatchStatus =
  | 'pending' // frozen, not started; transform may still be edited
  | 'running' // sends in flight
  | 'partial' // finished with failed items; retry of failed items possible
  | 'succeeded' // every frozen item delivered
  | 'cancelled'; // cancel requested; no new sends will ever start

export type ItemStatus =
  | 'pending'
  | 'inflight'
  | 'succeeded'
  | 'failed'
  | 'cancelled';

export interface BatchCounts {
  total: number;
  pending: number;
  inflight: number;
  succeeded: number;
  failed: number;
  cancelled: number;
}

export interface BatchItemView {
  originalId: string;
  originalTopic: string;
  targetTopic: string;
  status: ItemStatus;
  sends: number;
  error?: string;
  /** Derived once at freeze; stable across retries. */
  idempotencyKey: string;
}

export interface BatchSummary {
  id: string;
  filter: FilterSpec;
  transform: TransformSpec;
  frozenAt: string;
  status: BatchStatus;
  /** Optimistic-concurrency token. Bumped on every state/config mutation. */
  revision: number;
  frozenIds: string[];
  counts: BatchCounts;
  /** Dead letters that match the filter but arrived after the freeze. */
  driftAdded: number;
  startedAt?: string;
  finishedAt?: string;
}

export interface BatchDetail extends BatchSummary {
  items: BatchItemView[];
}

export interface ItemEventData {
  originalId: string;
  status: ItemStatus;
  sends: number;
  error?: string;
  revision: number;
}

export interface BatchEventData {
  status: BatchStatus;
  revision: number;
  counts: BatchCounts;
  finishedAt?: string;
}

export type BatchEventKind = 'item' | 'batch';

export interface StoredBatchEvent {
  seq: number; // per-batch monotonic, starts at 1
  kind: BatchEventKind;
  data: ItemEventData | BatchEventData;
}
