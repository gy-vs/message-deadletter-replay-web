import {describe, expect, it} from 'vitest';
import {
  BatchManager,
  Broker,
  DeadLetterStore,
  DomainError,
  SIM_HEADER,
  type Transform,
} from '../src/server/domain.js';

function setup(letters: Array<Record<string, unknown>>, delayMs = 5) {
  const store = new DeadLetterStore();
  for (const letter of letters) store.add(letter as never);
  const broker = new Broker({delayMs});
  const manager = new BatchManager(store, broker);
  return {store, broker, manager};
}

const okLetter = (id: string, topic = 't.in') => ({id, topic, headers: {}, body: id});
const flaky = (id: string) => ({id, topic: 't.in', headers: {[SIM_HEADER]: 'fail-once'}, body: id});
const rejected = (id: string, target?: string) => ({
  id,
  topic: target ?? 't.in',
  headers: target ? {} : {[SIM_HEADER]: 'reject'},
  body: id,
});

async function settle(manager: BatchManager, id: string, timeoutMs = 3000) {
  const start = Date.now();
  for (;;) {
    const snapshot = manager.get(id);
    if (['succeeded', 'partial', 'failed', 'cancelled'].includes(snapshot.status)) return snapshot;
    if (Date.now() - start > timeoutMs) throw new Error('batch did not settle');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

describe('dead letter immutability', () => {
  it('freezes original messages and failure history', () => {
    const {store} = setup([okLetter('dl-1')]);
    const letter = store.get('dl-1')!;
    expect(Object.isFrozen(letter)).toBe(true);
    expect(Object.isFrozen(letter.failures)).toBe(true);
    expect(Object.isFrozen(letter.headers)).toBe(true);
    expect(() => {
      (letter as {topic: string}).topic = 'mutated';
    }).toThrow();
  });

  it('never writes replay state back onto the original letter', () => {
    const {manager, store} = setup([okLetter('dl-1'), flaky('dl-2')]);
    const batch = manager.create({}, {topic: 't.out'});
    const started = manager.start(batch.id, batch.revision);
    return settle(manager, started.id).then(() => {
      const letter = store.get('dl-1')!;
      expect(letter.topic).toBe('t.in'); // original target untouched
      expect(letter.headers[SIM_HEADER]).toBeUndefined();
    });
  });
});

describe('frozen filtered set', () => {
  it('excludes dead letters arriving after the freeze', () => {
    const {store, manager} = setup([okLetter('dl-1'), okLetter('dl-2')]);
    const batch = manager.create({topic: 't.in'}, undefined);
    expect(batch.items.map((i) => i.originalMessageId)).toEqual(['dl-1', 'dl-2']);
    // A new dead letter matching the same filter arrives during preview.
    store.add({id: 'dl-3', topic: 't.in'});
    const again = manager.get(batch.id);
    expect(again.items).toHaveLength(2);
    expect(again.filter.candidateTotal).toBe(2); // frozen pool size at freeze time
  });
});

describe('derived idempotency keys', () => {
  it('are deterministic per original message + frozen target/headers', () => {
    const {manager} = setup([okLetter('dl-1')]);
    const transform: Transform = {topic: 't.out', headers: {'x-a': '1'}};
    const b1 = manager.create({}, transform);
    const b2 = manager.create({}, transform);
    const key1 = b1.items[0].idempotencyKey;
    const key2 = b2.items[0].idempotencyKey;
    // Key is scoped by batch, so different batches get different keys.
    expect(key1).not.toBe(key2);
    expect(key1).toMatch(/^rk_[0-9a-f]{32}$/);
    // Within a batch the key stays stable across retries.
    const started = manager.start(b1.id, b1.revision);
    return settle(manager, started.id).then((done) => {
      expect(done.items[0].idempotencyKey).toBe(key1);
    });
  });

  it('dedupe ledger: a redelivered key is acknowledged without a second send', async () => {
    const broker = new Broker();
    const first = await broker.publish('k-1', 't', {});
    const second = await broker.publish('k-1', 't', {});
    expect(first.duplicate).toBe(false);
    expect(second.duplicate).toBe(true);
    expect(broker.deliveryCount).toBe(1);
  });
});

describe('duplicate start and revision guard', () => {
  it('rejects a repeated start even with the fresh revision', () => {
    const {manager} = setup([okLetter('dl-1')], 50);
    const batch = manager.create({}, undefined);
    const started = manager.start(batch.id, batch.revision);
    expect(started.status).toBe('running');
    expect(() => manager.start(batch.id, started.revision)).toThrow(DomainError);
  });

  it('rejects a stale-revision start from a second page', () => {
    const {manager} = setup([okLetter('dl-1')], 50);
    const batch = manager.create({}, undefined);
    manager.start(batch.id, batch.revision); // page A
    try {
      manager.start(batch.id, batch.revision); // page B still holds rev 1
      throw new Error('expected conflict');
    } catch (error) {
      expect((error as DomainError).code).toBe('revision_conflict');
      expect((error as DomainError).current!.revision).toBeGreaterThan(batch.revision);
    }
  });
});

describe('partial failure retry', () => {
  it('retries only transient failures and never re-sends successes', async () => {
    const {manager, broker} = setup([flaky('dl-a'), okLetter('dl-b'), flaky('dl-c')]);
    const batch = manager.create({}, undefined);
    manager.start(batch.id, batch.revision);
    const partial = await settle(manager, batch.id);
    expect(partial.status).toBe('partial');
    expect(partial.counts.succeeded).toBe(1);
    expect(partial.counts.failed).toBe(2);
    const deliveriesAfterFirstRun = broker.deliveryCount;
    expect(deliveriesAfterFirstRun).toBe(1);

    const failedKeys = partial.items.filter((i) => i.status === 'failed').map((i) => i.idempotencyKey);
    const retried = manager.retry(partial.id, partial.revision);
    const done = await settle(manager, retried.id);
    expect(done.status).toBe('succeeded');
    // Two retried items now deliver; the already-succeeded item is not sent again.
    expect(broker.deliveryCount).toBe(deliveriesAfterFirstRun + 2);
    const stillSameKeys = done.items
      .filter((i) => ['dl-a', 'dl-c'].includes(i.originalMessageId))
      .map((i) => i.idempotencyKey);
    expect(stillSameKeys.sort()).toEqual(failedKeys.sort());
    // The first-run success records exactly one delivery attempt outcome.
    expect(done.items.find((i) => i.originalMessageId === 'dl-b')!.attempts).toBe(1);
  });
});

describe('target rejection', () => {
  it('marks permanent rejections separately and refuses to retry them', async () => {
    const {manager} = setup([okLetter('dl-good'), rejected('dl-bad')]);
    const batch = manager.create({}, undefined);
    manager.start(batch.id, batch.revision);
    const done = await settle(manager, batch.id);
    expect(done.status).toBe('partial');
    const bad = done.items.find((i) => i.originalMessageId === 'dl-bad')!;
    expect(bad.status).toBe('rejected');
    expect(bad.error?.code).toBe('target_rejected');
    try {
      manager.retry(done.id, done.revision); // only transient failures are retriable
      throw new Error('expected invalid_state');
    } catch (error) {
      expect((error as DomainError).code).toBe('invalid_state');
    }
  });

  it('validates the target topic before freezing the preview', () => {
    const {manager} = setup([okLetter('dl-1')]);
    expect(() => manager.create({}, {topic: 'bad topic!!'})).toThrow(DomainError);
  });
});

describe('cancel race', () => {
  it('starts no new sends after cancel and keeps successful items', async () => {
    const {manager} = setup([
      okLetter('dl-1'),
      okLetter('dl-2'),
      okLetter('dl-3'),
      okLetter('dl-4'),
    ], 30);
    const batch = manager.create({}, undefined);
    const running = manager.start(batch.id, batch.revision);

    // Wait until at least one send is in flight, then cancel from another page.
    while (manager.get(batch.id).counts.sending === 0 && manager.get(batch.id).counts.succeeded === 0) {
      await new Promise((r) => setTimeout(r, 2));
    }
    const cancelling = manager.cancel(batch.id, running.revision);
    expect(cancelling.cancelRequested).toBe(true);
    expect(cancelling.items.filter((i) => i.status === 'pending')).toHaveLength(0);

    // A second cancel with the stale revision loses the race.
    try {
      manager.cancel(batch.id, running.revision);
      throw new Error('expected conflict');
    } catch (error) {
      expect((error as DomainError).code).toBe('revision_conflict');
    }

    const done = await settle(manager, batch.id);
    expect(['cancelled', 'partial']).toContain(done.status);
    expect(done.counts.sending).toBe(0);
    expect(done.counts.pending).toBe(0);
    // Nothing after cancellation may resume; status is terminal.
    expect(() => manager.start(done.id, done.revision)).toThrow(DomainError);
  });
});

describe('frozen transform configuration', () => {
  it('rejects a start carrying a modified transform', () => {
    const {manager} = setup([okLetter('dl-1')]);
    const batch = manager.create({}, {topic: 't.a'});
    try {
      manager.start(batch.id, batch.revision, {topic: 't.b'});
      throw new Error('expected config_mismatch');
    } catch (error) {
      expect((error as DomainError).code).toBe('config_mismatch');
      expect((error as DomainError).current!.revision).toBe(batch.revision);
    }
    // The frozen config survived; an identical transform starts normally.
    const again = manager.get(batch.id);
    expect(again.items[0].targetTopic).toBe('t.a');
    const started = manager.start(batch.id, batch.revision, {topic: 't.a'});
    expect(started.status).toBe('running');
  });
});

describe('event stream', () => {
  it('replays missed events after a cursor and merges live ones', async () => {
    const {manager} = setup([okLetter('dl-1'), okLetter('dl-2')], 5);
    const batch = manager.create({}, undefined);
    const seen: number[] = [];
    const unsubscribe = manager.subscribe(batch.id, (event) => seen.push(event.seq));
    const running = manager.start(batch.id, batch.revision);
    await settle(manager, batch.id);
    unsubscribe();

    // Reconnect with an old cursor: missed delta events replay in order.
    const missed = manager.eventsAfter(batch.id, 0).map((event) => event.seq);
    expect(missed.length).toBeGreaterThan(0);
    expect(missed).toEqual([...missed].sort((a, b) => a - b));
    expect(missed.every((seq) => seq > 0)).toBe(true);

    // Terminal snapshot agrees with the last replayed revision.
    const current = manager.get(running.id);
    expect(current.revision).toBe(manager.eventsAfter(batch.id, -1).at(-1)!.revision);
  });
});
