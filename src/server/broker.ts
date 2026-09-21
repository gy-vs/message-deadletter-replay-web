// Fake message broker. Two guarantees the replay workbench relies on:
//  1. Idempotent producers: a publish carrying a key already accepted is a
//     no-op duplicate -> the original message is never delivered twice,
//     even if a client retries an already-succeeded item.
//  2. Failures are deterministic/injectable so partial-failure and
//     target-reject scenarios are reproducible.

export interface PublishContext {
  key: string;
  originalId: string;
  targetTopic: string;
  headers: Record<string, string>;
  payload: string;
  /** 1-based send attempt for this key within the batch. */
  attempt: number;
}

export type PublishResult =
  | {ok: true; duplicate: boolean}
  | {ok: false; code: string; message: string};

export type PublishPolicy = (ctx: PublishContext) => PublishResult | Promise<PublishResult>;

export interface Broker {
  publish(ctx: PublishContext): Promise<PublishResult>;
  /** Keys the broker has already accepted (dedupe memory). */
  acceptedKeys(): string[];
  /** Publish attempts actually performed, per key (duplicates are not re-sent). */
  publishCount(key: string): number;
}

/**
 * Default simulation rules, driven by the patched headers/topic so tests and
 * the UI can force outcomes without broker internals:
 *  - target topic equal to `topic.rejected` (or ending `.rejected`): broker
 *    rejects the target itself, on every attempt.
 *  - header `x-sim: fail-always`: hard failure on every attempt.
 *  - header `x-sim: flaky`: first attempt fails, later attempts succeed.
 */
export const defaultPolicy: PublishPolicy = (ctx) => {
  if (ctx.targetTopic === 'topic.rejected' || ctx.targetTopic.endsWith('.rejected')) {
    return {ok: false, code: 'target_rejected', message: `broker rejected target topic ${ctx.targetTopic}`};
  }
  const mode = ctx.headers['x-sim'];
  if (mode === 'fail-always') {
    return {ok: false, code: 'broker_failure', message: 'simulated permanent failure'};
  }
  if (mode === 'flaky' && ctx.attempt === 1) {
    return {ok: false, code: 'transient', message: 'simulated transient failure'};
  }
  return {ok: true, duplicate: false};
};

export function createBroker(options: {policy?: PublishPolicy; delay?: number} = {}): Broker {
  const policy = options.policy ?? defaultPolicy;
  const delay = options.delay ?? 8;
  const accepted = new Set<string>();
  const counts = new Map<string, number>();

  return {
    async publish(ctx) {
      // Idempotent producer: an accepted key short-circuits BEFORE the
      // destination is touched, so retries can never double-send.
      if (accepted.has(ctx.key)) {
        return {ok: true, duplicate: true};
      }
      counts.set(ctx.key, (counts.get(ctx.key) ?? 0) + 1);
      const attempt = counts.get(ctx.key)!;
      if (delay > 0) await new Promise((resolve) => setTimeout(resolve, delay));
      const result = await policy({...ctx, attempt});
      if (result.ok && !result.duplicate) accepted.add(ctx.key);
      return result;
    },
    acceptedKeys: () => [...accepted],
    publishCount: (key) => counts.get(key) ?? 0,
  };
}

/** Merge the batch header patch onto immutable original headers. */
export function applyHeaders(
  originals: Record<string, string>,
  patch: Record<string, string>,
): Record<string, string> {
  const merged: Record<string, string> = {...originals};
  for (const [name, value] of Object.entries(patch)) {
    if (value === '') delete merged[name];
    else merged[name] = value;
  }
  return merged;
}
