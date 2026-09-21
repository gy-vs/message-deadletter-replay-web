import {describe, expect, it} from 'vitest';
import {applyHeaders, createBroker, defaultPolicy} from '../src/server/broker';
import {deriveIdempotencyKey} from '../src/server/store';

const base = {key: 'k', originalId: 'm1', targetTopic: 't', headers: {}, payload: 'p', attempt: 1};

describe('idempotent broker', () => {
  it('accepts a key once and dedupes repeat publishes without re-sending', async () => {
    const broker = createBroker({delay: 0});
    const first = await broker.publish(base);
    const second = await broker.publish({...base, attempt: 2});
    expect(first).toEqual({ok: true, duplicate: false});
    expect(second).toEqual({ok: true, duplicate: true});
    expect(broker.publishCount('k')).toBe(1); // destination touched only once
    expect(broker.acceptedKeys()).toEqual(['k']);
  });

  it('re-publishes a key that previously failed until it is accepted', async () => {
    const broker = createBroker({delay: 0, policy: defaultPolicy});
    const flaky = {...base, headers: {'x-sim': 'flaky'}};
    expect(await broker.publish(flaky)).toMatchObject({ok: false, code: 'transient'});
    expect(await broker.publish({...flaky, attempt: 2})).toMatchObject({ok: true, duplicate: false});
    expect(broker.publishCount('k')).toBe(2);
    // A third call is a duplicate no-op.
    expect(await broker.publish({...flaky, attempt: 3})).toMatchObject({ok: true, duplicate: true});
    expect(broker.publishCount('k')).toBe(2);
  });

  it('refuses rejected targets on every attempt and never accepts the key', async () => {
    const broker = createBroker({delay: 0});
    const r1 = await broker.publish({...base, targetTopic: 'topic.rejected'});
    const r2 = await broker.publish({...base, targetTopic: 'topic.rejected', attempt: 2});
    expect(r1).toMatchObject({ok: false, code: 'target_rejected'});
    expect(r2).toMatchObject({ok: false, code: 'target_rejected'});
    expect(broker.acceptedKeys()).toHaveLength(0);
  });

  it('derives stable per-batch idempotency keys from the original message id', () => {
    const a = deriveIdempotencyKey('batch-1', 'msg-9');
    const b = deriveIdempotencyKey('batch-1', 'msg-9');
    const c = deriveIdempotencyKey('batch-2', 'msg-9');
    expect(a).toBe(b);
    expect(a).toBe('replay:batch-1:msg-9');
    expect(c).not.toBe(a); // different frozen batch -> different replay key
  });

  it('merges header patches without mutating originals; empty value removes', () => {
    const originals = {a: '1', b: '2'};
    const merged = applyHeaders(originals, {b: '', c: '3'});
    expect(merged).toEqual({a: '1', c: '3'});
    expect(originals).toEqual({a: '1', b: '2'});
  });
});
