import {createHash} from 'node:crypto';

/* ------------------------------------------------------------------ */
/* Dead letters — original message + failure history are immutable.    */
/* ------------------------------------------------------------------ */

export type FailureEntry = {
  reason: string;
  detail?: string;
  at: string;
};

export type DeadLetter = {
  readonly id: string;
  readonly topic: string;
  readonly headers: Record<string, string>;
  readonly body: string;
  readonly failedAt: string;
  readonly failures: readonly FailureEntry[];
};

export type DeadLetterFilter = {
  q?: string | null;
  topic?: string | null;
  reason?: string | null;
};

export class DeadLetterStore {
  private rows: DeadLetter[] = [];
  private seq = 0;

  seed(rows: Array<Parameters<DeadLetterStore['add']>[0]>): void {
    for (const row of rows) this.add(row);
  }

  /** Ingest a new dead letter. Only ingestion may append failure history. */
  add(input: {
    id?: string;
    topic: string;
    headers?: Record<string, string>;
    body?: string;
    failures?: Array<{reason: string; detail?: string; at?: string}>;
  }): DeadLetter {
    const now = new Date().toISOString();
    const letter: DeadLetter = Object.freeze({
      id: input.id ?? `dl-${(++this.seq).toString(16).padStart(4, '0')}`,
      topic: input.topic,
      headers: Object.freeze({...(input.headers ?? {})}),
      body: input.body ?? '',
      failedAt: now,
      failures: Object.freeze(
        (input.failures ?? [{reason: 'delivery_failed'}]).map((f) =>
          Object.freeze({reason: f.reason, detail: f.detail, at: f.at ?? now}),
        ),
      ),
    });
    this.rows.push(letter);
    return letter;
  }

  get(id: string): DeadLetter | undefined {
    return this.rows.find((row) => row.id === id);
  }

  list(filter: DeadLetterFilter = {}): DeadLetter[] {
    const q = filter.q?.trim().toLowerCase();
    const topic = filter.topic?.trim() || undefined;
    const reason = filter.reason?.trim() || undefined;
    return this.rows.filter((row) => {
      if (topic && row.topic !== topic) return false;
      if (reason && !row.failures.some((f) => f.reason === reason)) return false;
      if (q) {
        const haystack = `${row.id}\n${row.topic}\n${row.body}`.toLowerCase();
        if (!haystack.includes(q)) return false;
      }
      return true;
    });
  }

  get total(): number {
    return this.rows.length;
  }
}

/* ------------------------------------------------------------------ */
/* Transforms — target topic / header overrides, frozen per batch.     */
/* ------------------------------------------------------------------ */

export type Transform = {
  topic?: string;
  headers?: Record<string, string | null>;
};

export type HeaderChange = {header: string; op: 'set' | 'remove'; value?: string};
export type TransformSummary = {
  targetTopic: string | null;
  keepsOriginalTarget: boolean;
  headerChanges: HeaderChange[];
};

const TOPIC_RE = /^[A-Za-z0-9._-]{1,128}$/;
const HEADER_RE = /^[A-Za-z0-9-]{1,128}$/;

export function validateTransform(transform: Transform | undefined): Transform {
  const t: Transform = {topic: transform?.topic?.trim() || undefined, headers: {...(transform?.headers ?? {})}};
  if (t.topic !== undefined && !TOPIC_RE.test(t.topic)) {
    throw new DomainError('invalid_transform', `unsupported target topic: ${t.topic}`);
  }
  for (const [name, value] of Object.entries(t.headers ?? {})) {
    if (!HEADER_RE.test(name)) throw new DomainError('invalid_transform', `unsupported header name: ${name}`);
    if (value !== null && typeof value !== 'string') {
      throw new DomainError('invalid_transform', `header ${name} must be a string or null (remove)`);
    }
  }
  return t;
}

function applyHeaders(base: Record<string, string>, transform: Transform): Record<string, string> {
  const out = {...base};
  for (const [name, value] of Object.entries(transform.headers ?? {})) {
    if (value === null) delete out[name];
    else out[name] = value;
  }
  return out;
}

export function summarizeTransform(transform: Transform): TransformSummary {
  const headerChanges: HeaderChange[] = Object.entries(transform.headers ?? {})
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([header, value]) =>
      value === null ? {header, op: 'remove' as const} : {header, op: 'set' as const, value},
    );
  return {
    targetTopic: transform.topic ?? null,
    keepsOriginalTarget: !transform.topic,
    headerChanges,
  };
}

export function transformsEqual(a: Transform, b: Transform): boolean {
  return canonicalTransform(a) === canonicalTransform(b);
}

function canonicalTransform(t: Transform): string {
  const headers = Object.fromEntries(
    Object.entries(t.headers ?? {})
      .map(([k, v]): [string, string | null] => [k.toLowerCase(), v])
      .sort(([a], [b]) => a.localeCompare(b)),
  );
  return JSON.stringify({topic: t.topic ?? null, headers});
}

/* ------------------------------------------------------------------ */
/* Broker simulation — idempotent publisher.                           */
/* ------------------------------------------------------------------ */

/** Per-message simulation header. Values: 'reject' (permanent) | 'fail-once' (transient). */
export const SIM_HEADER = 'x-dlq-sim';

export class BrokerError extends Error {
  constructor(
    public readonly code: string,
    message: string,
    public readonly permanent: boolean,
  ) {
    super(message);
  }
}

export type PublishResult = {duplicate: boolean; at: string};

export class Broker {
  /** idempotencyKey -> first accepted delivery (the dedupe ledger) */
  readonly accepted = new Map<string, {topic: string; at: string}>();
  /** actual outbound publish attempts that reached a target */
  deliveryCount = 0;
  private attempts = new Map<string, number>();

  constructor(
    private readonly options: {rejectTopic?: string; delayMs?: number} = {},
  ) {}

  async publish(
    idempotencyKey: string,
    topic: string,
    headers: Record<string, string>,
  ): Promise<PublishResult> {
    if (this.options.delayMs) await new Promise((resolve) => setTimeout(resolve, this.options.delayMs));

    // Dedupe happens before any validation: a retried replay with the same
    // derived key can never cause a second outbound delivery.
    const done = this.accepted.get(idempotencyKey);
    if (done) return {duplicate: true, at: done.at};

    const n = (this.attempts.get(idempotencyKey) ?? 0) + 1;
    this.attempts.set(idempotencyKey, n);

    if (topic === this.options.rejectTopic || headers[SIM_HEADER] === 'reject') {
      throw new BrokerError('target_rejected', `target ${topic} rejected the message`, true);
    }
    if (headers[SIM_HEADER] === 'fail-once' && n === 1) {
      throw new BrokerError('send_failed', 'transient delivery failure', false);
    }

    this.deliveryCount += 1;
    const at = new Date().toISOString();
    this.accepted.set(idempotencyKey, {topic, at});
    return {duplicate: false, at};
  }
}

/* ------------------------------------------------------------------ */
/* Replay batches.                                                     */
/* ------------------------------------------------------------------ */

export type ItemStatus = 'pending' | 'sending' | 'succeeded' | 'failed' | 'rejected' | 'cancelled';
export type BatchStatus =
  | 'preview'
  | 'running'
  | 'cancelling'
  | 'succeeded'
  | 'partial'
  | 'failed'
  | 'cancelled';

const TERMINAL_STATUSES: ReadonlySet<BatchStatus> = new Set(['succeeded', 'partial', 'failed', 'cancelled']);

export type FrozenFilter = {
  q: string | null;
  topic: string | null;
  reason: string | null;
  matchedAt: string;
  candidateTotal: number;
};

export type BatchItemSnapshot = {
  originalMessageId: string;
  originalTopic: string;
  targetTopic: string;
  headers: Record<string, string>;
  status: ItemStatus;
  attempts: number;
  idempotencyKey: string;
  error?: {code: string; detail: string};
  deliveredAt?: string;
  duplicate?: boolean;
};

export type BatchCounts = Record<ItemStatus, number> & {total: number};

export type BatchSnapshot = {
  id: string;
  status: BatchStatus;
  revision: number;
  lastSeq: number;
  createdAt: string;
  frozenAt: string;
  filter: FrozenFilter;
  transform: Transform;
  transformSummary: TransformSummary;
  cancelRequested: boolean;
  items: BatchItemSnapshot[];
  counts: BatchCounts;
};

type InternalItem = {
  body: string;
  snapshot: BatchItemSnapshot;
};

type InternalBatch = {
  id: string;
  createdAt: string;
  frozenAt: string;
  filter: FrozenFilter;
  transform: Transform;
  items: InternalItem[];
  status: BatchStatus;
  revision: number;
  nextSeq: number;
  events: BatchEventData[];
  cancelRequested: boolean;
  inflight: number;
};

export type BatchEventData = {
  seq: number;
  revision: number;
  at: string;
  kind: 'snapshot' | 'batch' | 'item';
  status?: BatchStatus;
  cancelRequested?: boolean;
  counts?: BatchCounts;
  item?: BatchItemSnapshot;
  snapshot?: BatchSnapshot;
};

export class DomainError extends Error {
  constructor(
    public readonly code:
      | 'not_found'
      | 'revision_conflict'
      | 'invalid_state'
      | 'config_mismatch'
      | 'invalid_transform',
    message: string,
    public readonly current?: BatchSnapshot,
  ) {
    super(message);
  }
}

function deriveIdempotencyKey(
  batchId: string,
  originalMessageId: string,
  targetTopic: string,
  headers: Record<string, string>,
): string {
  const canonicalHeaders = JSON.stringify(
    Object.keys(headers)
      .sort()
      .map((k) => [k, headers[k]]),
  );
  const digest = createHash('sha256')
    .update(`${batchId} ${originalMessageId} ${targetTopic} ${canonicalHeaders}`)
    .digest('hex')
    .slice(0, 32);
  return `rk_${digest}`;
}

export type BatchListener = (event: BatchEventData) => void;

export class BatchManager {
  private batches = new Map<string, InternalBatch>();
  private listeners = new Map<string, Set<BatchListener>>();
  private idSeq = 0;

  constructor(
    private readonly store: DeadLetterStore,
    private readonly broker: Broker,
  ) {}

  /* creation ------------------------------------------------------- */

  create(filter: DeadLetterFilter, transformInput: Transform | undefined): BatchSnapshot {
    const transform = validateTransform(transformInput);
    const matchedAt = new Date().toISOString();
    // Freeze the matched set at creation time: dead letters arriving later
    // (during preview or while running) never join this batch.
    const matched = this.store.list(filter);
    const batchId = `rb-${(++this.idSeq).toString(16).padStart(4, '0')}`;
    const items: InternalItem[] = matched.map((letter) => {
      const headers = applyHeaders(letter.headers, transform);
      const targetTopic = transform.topic ?? letter.topic;
      return {
        body: letter.body,
        snapshot: {
          originalMessageId: letter.id,
          originalTopic: letter.topic,
          targetTopic,
          headers,
          status: 'pending' as const,
          attempts: 0,
          idempotencyKey: deriveIdempotencyKey(batchId, letter.id, targetTopic, headers),
        },
      };
    });
    const frozen: FrozenFilter = {
      q: filter.q?.trim() || null,
      topic: filter.topic?.trim() || null,
      reason: filter.reason?.trim() || null,
      matchedAt,
      candidateTotal: this.store.total,
    };
    const batch: InternalBatch = {
      id: batchId,
      createdAt: matchedAt,
      frozenAt: matchedAt,
      filter: frozen,
      transform,
      items,
      status: 'preview',
      revision: 1,
      nextSeq: 0,
      events: [],
      cancelRequested: false,
      inflight: 0,
    };
    this.batches.set(batchId, batch);
    return this.snapshot(batch);
  }

  /* lifecycle ------------------------------------------------------ */

  start(id: string, expectedRevision: number, transform?: Transform): BatchSnapshot {
    const batch = this.require(id);
    this.checkRevision(batch, expectedRevision);
    if (transform && !transformsEqual(validateTransform(transform), batch.transform)) {
      throw new DomainError('config_mismatch', 'transform differs from the frozen configuration', this.snapshot(batch));
    }
    if (batch.status !== 'preview') {
      // Duplicate start (same revision accepted twice) is an invalid state.
      throw new DomainError('invalid_state', `batch is already ${batch.status}`, this.snapshot(batch));
    }
    batch.status = 'running';
    this.bump(batch, 'batch');
    void this.run(batch);
    return this.snapshot(batch);
  }

  cancel(id: string, expectedRevision: number): BatchSnapshot {
    const batch = this.require(id);
    this.checkRevision(batch, expectedRevision);
    if (TERMINAL_STATUSES.has(batch.status)) {
      throw new DomainError('invalid_state', `cannot cancel a ${batch.status} batch`, this.snapshot(batch));
    }
    // Rollback: no new send may start after this point; in-flight sends finish.
    batch.cancelRequested = true;
    for (const item of batch.items) {
      if (item.snapshot.status === 'pending') {
        item.snapshot.status = 'cancelled';
        this.bump(batch, 'item', item);
      }
    }
    this.recompute(batch, true);
    return this.snapshot(batch);
  }

  retry(id: string, expectedRevision: number): BatchSnapshot {
    const batch = this.require(id);
    this.checkRevision(batch, expectedRevision);
    if (batch.status !== 'partial' && batch.status !== 'failed') {
      throw new DomainError('invalid_state', `only partial/failed batches can be retried (is ${batch.status})`, this.snapshot(batch));
    }
    const retryable = batch.items.filter((item) => item.snapshot.status === 'failed');
    if (retryable.length === 0) {
      // Only transient failures are retried. Succeeded items must not be
      // re-sent; rejected items need a new batch with a new transform.
      throw new DomainError('invalid_state', 'no retryable items', this.snapshot(batch));
    }
    for (const item of retryable) {
      item.snapshot.status = 'pending';
      item.snapshot.error = undefined;
      this.bump(batch, 'item', item);
    }
    batch.cancelRequested = false;
    batch.status = 'running';
    this.bump(batch, 'batch');
    void this.run(batch);
    return this.snapshot(batch);
  }

  /* reads ----------------------------------------------------------- */

  get(id: string): BatchSnapshot {
    return this.snapshot(this.require(id));
  }

  /** Replay stored events with seq > after, followed by live events. */
  eventsAfter(id: string, after: number): BatchEventData[] {
    const batch = this.require(id);
    return batch.events.filter((event) => event.seq > after).map((event) => ({...event}));
  }

  subscribe(id: string, listener: BatchListener): () => void {
    const batch = this.require(id);
    let set = this.listeners.get(id);
    if (!set) {
      set = new Set();
      this.listeners.set(id, set);
    }
    set.add(listener);
    return () => {
      set!.delete(listener);
    };
  }

  subscriberCount(id: string): number {
    return this.listeners.get(id)?.size ?? 0;
  }

  /* internals ------------------------------------------------------- */

  private run(batch: InternalBatch): void {
    void this.drain(batch);
  }

  private async drain(batch: InternalBatch): Promise<void> {
    // Strictly serial: at most one publish is in flight, and no new send is
    // started once cancelRequested is set. Because sends are awaited in order,
    // `sending` here always means the broker call for this item is live, so
    // cancel() never has to guess whether a send can still be prevented.
    for (const item of batch.items) {
      if (batch.cancelRequested) return;
      if (item.snapshot.status !== 'pending') continue;

      item.snapshot.status = 'sending';
      item.snapshot.attempts += 1;
      batch.inflight += 1;
      this.bump(batch, 'item', item);

      try {
        const result = await this.broker.publish(
          item.snapshot.idempotencyKey,
          item.snapshot.targetTopic,
          item.snapshot.headers,
        );
        if (item.snapshot.status === 'sending') {
          item.snapshot.status = 'succeeded';
          item.snapshot.deliveredAt = result.at;
          item.snapshot.duplicate = result.duplicate;
          item.snapshot.error = undefined;
        }
      } catch (err) {
        if (item.snapshot.status === 'sending') {
          const brokerError = err as BrokerError;
          item.snapshot.status = brokerError.permanent ? 'rejected' : 'failed';
          item.snapshot.error = {code: brokerError.code, detail: brokerError.message};
        }
      }

      batch.inflight -= 1;
      this.bump(batch, 'item', item);
      this.recompute(batch, false);
    }
  }

  private recompute(batch: InternalBatch, forceEmit: boolean): void {
    const statuses = batch.items.map((item) => item.snapshot.status);
    const busy = statuses.some((status) => status === 'sending');
    if (busy) {
      this.setStatus(batch, batch.cancelRequested ? 'cancelling' : 'running', forceEmit);
      return;
    }
    const pending = batch.items.filter((item) => item.snapshot.status === 'pending');
    if (pending.length > 0) {
      if (batch.cancelRequested) {
        for (const item of pending) {
          item.snapshot.status = 'cancelled';
          this.bump(batch, 'item', item);
        }
      } else {
        this.setStatus(batch, 'running', forceEmit);
        return;
      }
    }
    const count = (status: ItemStatus) =>
      batch.items.filter((item) => item.snapshot.status === status).length;
    const succeeded = count('succeeded');
    const failedLike = count('failed') + count('rejected');
    let next: BatchStatus;
    if (succeeded === batch.items.length) next = 'succeeded';
    else if (succeeded > 0) next = 'partial';
    else if (failedLike > 0) next = 'failed';
    else next = 'cancelled';
    this.setStatus(batch, next, true);
  }

  private setStatus(batch: InternalBatch, status: BatchStatus, forceEmit: boolean): void {
    if (batch.status !== status) {
      batch.status = status;
      this.bump(batch, 'batch');
    } else if (forceEmit) {
      this.bump(batch, 'batch');
    }
  }

  private bump(batch: InternalBatch, kind: 'batch' | 'item', item?: InternalItem): void {
    batch.revision += 1;
    const event: BatchEventData = {
      seq: batch.nextSeq++,
      revision: batch.revision,
      at: new Date().toISOString(),
      kind,
    };
    if (kind === 'batch') {
      event.status = batch.status;
      event.cancelRequested = batch.cancelRequested;
      event.counts = this.counts(batch);
    } else if (item) {
      event.item = cloneItem(item.snapshot);
    }
    batch.events.push(event);
    this.listeners.get(batch.id)?.forEach((listener) => listener(event));
  }

  private counts(batch: InternalBatch): BatchCounts {
    const counts: BatchCounts = {
      pending: 0,
      sending: 0,
      succeeded: 0,
      failed: 0,
      rejected: 0,
      cancelled: 0,
      total: batch.items.length,
    };
    for (const item of batch.items) counts[item.snapshot.status] += 1;
    return counts;
  }

  private snapshot(batch: InternalBatch): BatchSnapshot {
    return {
      id: batch.id,
      status: batch.status,
      revision: batch.revision,
      lastSeq: batch.nextSeq - 1,
      createdAt: batch.createdAt,
      frozenAt: batch.frozenAt,
      filter: {...batch.filter},
      transform: {
        topic: batch.transform.topic,
        headers: {...(batch.transform.headers ?? {})},
      },
      transformSummary: summarizeTransform(batch.transform),
      cancelRequested: batch.cancelRequested,
      items: batch.items.map((item) => cloneItem(item.snapshot)),
      counts: this.counts(batch),
    };
  }

  private require(id: string): InternalBatch {
    const batch = this.batches.get(id);
    if (!batch) throw new DomainError('not_found', `unknown batch ${id}`);
    return batch;
  }

  private checkRevision(batch: InternalBatch, expectedRevision: number): void {
    if (expectedRevision !== batch.revision) {
      throw new DomainError(
        'revision_conflict',
        `stale revision ${expectedRevision}, current is ${batch.revision}`,
        this.snapshot(batch),
      );
    }
  }
}

function cloneItem(item: BatchItemSnapshot): BatchItemSnapshot {
  return {
    ...item,
    headers: {...item.headers},
    error: item.error ? {...item.error} : undefined,
  };
}
