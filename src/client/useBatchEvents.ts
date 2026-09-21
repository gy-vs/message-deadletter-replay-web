import {useCallback, useEffect, useRef, useState} from 'react';
import type {BatchEventData, ItemEventData} from '../shared/protocol.js';

export interface ItemState {
  status: ItemEventData['status'];
  sends: number;
  error?: string;
  /** Last event seq that touched this original message. */
  seq: number;
}

export interface EventStreamState {
  connected: boolean;
  /** Item events merged BY ORIGINAL MESSAGE ID — late events replace stale ones. */
  items: Record<string, ItemState>;
  batch?: BatchEventData;
  lastSeq: number;
  resync: () => void;
}

/**
 * Subscribe to a batch event stream.
 *  - reconnect resumes with ?after=<lastSeq>, so a dropped socket replays
 *    only missed events (断線恢復);
 *  - item events are reduced into a map keyed by originalId (按原消息 id 归并);
 *  - events below a seen seq are ignored defensively.
 */
export function useBatchEvents(batchId: string | null): EventStreamState {
  const [connected, setConnected] = useState(false);
  const [items, setItems] = useState<Record<string, ItemState>>({});
  const [batch, setBatch] = useState<BatchEventData | undefined>(undefined);
  const lastSeqRef = useRef(0);
  const [lastSeq, setLastSeq] = useState(0);
  const esRef = useRef<EventSource | null>(null);
  const [resyncTick, setResyncTick] = useState(0);

  const applyEvent = useCallback((seq: number, kind: 'item' | 'batch', data: ItemEventData | BatchEventData) => {
    if (seq <= lastSeqRef.current) return; // dedupe replays
    lastSeqRef.current = seq;
    setLastSeq(seq);
    if (kind === 'item') {
      const item = data as ItemEventData;
      setItems((prev) => ({
        ...prev,
        [item.originalId]: {status: item.status, sends: item.sends, error: item.error, seq},
      }));
    } else {
      setBatch(data as BatchEventData);
    }
  }, []);

  const resync = useCallback(() => setResyncTick((n) => n + 1), []);

  // New batch => its seq space restarts at 1; clear merge state and cursor.
  useEffect(() => {
    lastSeqRef.current = 0;
    setLastSeq(0);
    setItems({});
    setBatch(undefined);
  }, [batchId]);

  useEffect(() => {
    if (!batchId) return;
    let closed = false;
    let reconnect: ReturnType<typeof setTimeout> | undefined;

    const connect = () => {
      if (closed) return;
      const es = new EventSource(`/api/batches/${batchId}/events?after=${lastSeqRef.current}`);
      esRef.current = es;

      es.onopen = () => setConnected(true);
      es.addEventListener('item', (message: MessageEvent) => {
        const seq = Number(message.lastEventId);
        applyEvent(seq, 'item', JSON.parse(message.data));
      });
      es.addEventListener('batch', (message: MessageEvent) => {
        const seq = Number(message.lastEventId);
        applyEvent(seq, 'batch', JSON.parse(message.data));
      });
      es.onerror = () => {
        setConnected(false);
        es.close();
        if (!closed) reconnect = setTimeout(connect, 600); // resume via ?after
      };
    };
    connect();

    return () => {
      closed = true;
      if (reconnect) clearTimeout(reconnect);
      esRef.current?.close();
    };
  }, [batchId, applyEvent, resyncTick]);

  return {connected, items, batch, lastSeq, resync};
}
