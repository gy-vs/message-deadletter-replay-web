import {useCallback, useEffect, useMemo, useRef, useState} from 'react';
import {
  ArrowRight,
  Ban,
  Filter,
  FlaskConical,
  Play,
  RefreshCw,
  RotateCcw,
  Snowflake,
  Wifi,
  WifiOff,
} from 'lucide-react';

/* ------------------------------- types ------------------------------- */

type ItemStatus = 'pending' | 'sending' | 'succeeded' | 'failed' | 'rejected' | 'cancelled';
type BatchStatus = 'preview' | 'running' | 'cancelling' | 'succeeded' | 'partial' | 'failed' | 'cancelled';

type DeadLetter = {
  id: string;
  topic: string;
  headers: Record<string, string>;
  body: string;
  failedAt: string;
  failures: {reason: string; detail?: string; at: string}[];
};

type DeadLetterList = {total: number; matched: number; filter: Record<string, string | null>; rows: DeadLetter[]};

type HeaderChange = {header: string; op: 'set' | 'remove'; value?: string};
type BatchItem = {
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
type BatchCounts = Record<ItemStatus, number> & {total: number};
type Batch = {
  id: string;
  status: BatchStatus;
  revision: number;
  lastSeq: number;
  frozenAt: string;
  filter: {q: string | null; topic: string | null; reason: string | null; matchedAt: string; candidateTotal: number};
  transformSummary: {targetTopic: string | null; keepsOriginalTarget: boolean; headerChanges: HeaderChange[]};
  cancelRequested: boolean;
  items: BatchItem[];
  counts: BatchCounts;
};

type SseEvent =
  | {kind: 'snapshot'; seq?: number; snapshot: Batch}
  | {kind: 'item'; seq: number; revision: number; item: BatchItem}
  | {kind: 'batch'; seq: number; revision: number; status: BatchStatus; cancelRequested: boolean; counts: BatchCounts};

type HeaderDraft = {header: string; value: string};

/* ------------------------------- api --------------------------------- */

async function jsonOrThrow(response: Response): Promise<any> {
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(payload.message ?? payload.error ?? `HTTP ${response.status}`) as Error & {
      code?: string;
      current?: Batch;
    };
    error.code = payload.error;
    error.current = payload.current;
    throw error;
  }
  return payload;
}

const api = {
  listDeadLetters: (filter: Record<string, string>) =>
    fetch(
      `/api/dead-letters?${new URLSearchParams(Object.entries(filter).filter(([, v]) => v.trim()))}`,
    ).then(jsonOrThrow) as Promise<DeadLetterList>,
  createBatch: (filter: Record<string, string>, transform: unknown) =>
    fetch('/api/batches', {
      method: 'POST',
      headers: {'content-type': 'application/json'},
      body: JSON.stringify({filter, transform}),
    }).then(jsonOrThrow) as Promise<Batch>,
  action: (id: string, name: 'start' | 'cancel' | 'retry', revision: number) =>
    fetch(`/api/batches/${id}/${name}`, {
      method: 'POST',
      headers: {'content-type': 'application/json'},
      body: JSON.stringify({revision}),
    }).then(jsonOrThrow) as Promise<Batch>,
  ingest: (letter: Record<string, unknown>) =>
    fetch('/api/dead-letters', {
      method: 'POST',
      headers: {'content-type': 'application/json'},
      body: JSON.stringify(letter),
    }).then(jsonOrThrow) as Promise<DeadLetter>,
};

/* --------------------------- status labels --------------------------- */

const STATUS_TEXT: Record<ItemStatus, string> = {
  pending: '待发送',
  sending: '发送中',
  succeeded: '已成功',
  failed: '失败',
  rejected: '目标拒绝',
  cancelled: '已取消',
};

const BATCH_STATUS_TEXT: Record<BatchStatus, string> = {
  preview: '预览待确认',
  running: '进行中',
  cancelling: '取消中',
  succeeded: '全部成功',
  partial: '部分成功',
  failed: '全部失败',
  cancelled: '已取消',
};

/* ------------------------------ app ---------------------------------- */

export default function App() {
  const [q, setQ] = useState('');
  const [topic, setTopic] = useState('');
  const [reason, setReason] = useState('');
  const [list, setList] = useState<DeadLetterList | null>(null);
  const [targetTopic, setTargetTopic] = useState('');
  const [headerDrafts, setHeaderDrafts] = useState<HeaderDraft[]>([{header: '', value: ''}]);
  const [batch, setBatch] = useState<Batch | null>(null);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [conflict, setConflict] = useState<string | null>(null);
  const [connection, setConnection] = useState<'connecting' | 'online' | 'offline'>('online');

  const lastSeqRef = useRef(-1);
  const batchRef = useRef<Batch | null>(null);
  batchRef.current = batch;

  const refreshList = useCallback(async () => {
    setList(await api.listDeadLetters({q, topic, reason}));
  }, [q, topic, reason]);

  useEffect(() => {
    refreshList().catch((err) => setNotice(err.message));
  }, [refreshList]);

  /* SSE: merge events by original message id, dedupe by seq, reconnect. */
  useEffect(() => {
    if (!batch) return;
    lastSeqRef.current = -1;
    setConnection('connecting');
    const source = new EventSource(`/api/batches/${batch.id}/events?after=${lastSeqRef.current}`);

    const apply = (raw: MessageEvent<string>) => {
      const event = JSON.parse(raw.data) as SseEvent;
      setConnection('online');
      if (event.kind === 'snapshot') {
        // Full snapshot: server sends it first on (re)connect — reconciles
        // disconnections and stale pages authoritatively.
        lastSeqRef.current = event.snapshot.lastSeq;
        setBatch(event.snapshot);
        return;
      }
      if (event.seq <= lastSeqRef.current) return; // replay/duplicate guard
      lastSeqRef.current = event.seq;
      setBatch((prev) => {
        if (!prev || prev.id !== batch.id) return prev;
        if (event.kind === 'item') {
          const index = prev.items.findIndex((it) => it.originalMessageId === event.item.originalMessageId);
          const items = index === -1 ? [...prev.items, event.item] : prev.items.map((it, i) => (i === index ? event.item : it));
          return {...prev, revision: event.revision, items, counts: countItems(items)};
        }
        return {
          ...prev,
          revision: event.revision,
          status: event.status,
          cancelRequested: event.cancelRequested,
          counts: event.counts,
        };
      });
    };
    source.onmessage = apply;
    source.onerror = () => setConnection((state) => (state === 'online' ? 'offline' : state));
    source.onopen = () => setConnection('online');
    return () => source.close();
  }, [batch?.id]);

  const transformFromDraft = useMemo(() => {
    const headers: Record<string, string | null> = {};
    for (const draft of headerDrafts) {
      const name = draft.header.trim();
      if (!name) continue;
      headers[name] = draft.value.trim() === '' ? null : draft.value;
    }
    return {topic: targetTopic.trim() || undefined, headers};
  }, [headerDrafts, targetTopic]);

  async function createBatch() {
    setBusy(true);
    setNotice(null);
    setConflict(null);
    try {
      const snapshot = await api.createBatch({q, topic, reason}, transformFromDraft);
      setBatch(snapshot);
    } catch (err) {
      setNotice((err as Error).message);
    } finally {
      setBusy(false);
    }
  }

  async function mutate(name: 'start' | 'cancel' | 'retry') {
    const current = batchRef.current;
    if (!current || busy) return;
    setBusy(true);
    setConflict(null);
    try {
      const snapshot = await api.action(current.id, name, current.revision);
      lastSeqRef.current = Math.max(lastSeqRef.current, snapshot.lastSeq);
      setBatch(snapshot);
    } catch (err) {
      const error = err as Error & {code?: string; current?: Batch};
      if (error.code === 'revision_conflict' && error.current) {
        // Another page/tab already advanced the batch: adopt its revision.
        lastSeqRef.current = Math.max(lastSeqRef.current, error.current.lastSeq);
        setBatch(error.current);
        setConflict('批次已被另一个页面更新，已为你加载最新状态（revision 冲突已阻止重复操作）。');
      } else {
        setNotice(error.message);
      }
    } finally {
      setBusy(false);
    }
  }

  async function simulateNewDeadLetter() {
    const n = (list?.total ?? 0) + 1;
    try {
      await api.ingest({
        topic: topic.trim() || 'orders.created',
        headers: {'x-dlq-sim': 'fail-once', 'x-trace': `tr-new-${n}`},
        body: `{"orderId":"o-new-${n}"}`,
        failures: [{reason: reason.trim() || 'timeout'}],
      });
      await refreshList();
    } catch (err) {
      setNotice((err as Error).message);
    }
  }

  const terminal = batch ? ['succeeded', 'partial', 'failed', 'cancelled'].includes(batch.status) : false;
  const mutable = batch ? batch.status === 'preview' : false;
  const frozenMatch = batch ? batch.items.length : 0;

  return (
    <main className="shell">
      <header className="topbar">
        <FlaskConical size={20} />
        <strong>消息交付工作台 · 死信重放</strong>
        <small>原始消息与失败历史不可变</small>
        <span className={`conn ${connection}`}>
          {connection === 'online' ? <Wifi size={14} /> : <WifiOff size={14} />}
          {connection === 'online' ? '实时连接' : connection === 'connecting' ? '连接中…' : '断线重连中…'}
        </span>
      </header>

      <section className="workspace">
        {/* filter + transform */}
        <aside className="pane">
          <h2>
            <Filter size={16} /> 筛选死信
          </h2>
          <label className="field">
            <span>关键字</span>
            <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="id / 主题 / 正文" />
          </label>
          <label className="field">
            <span>原始主题</span>
            <input value={topic} onChange={(e) => setTopic(e.target.value)} placeholder="orders.created" />
          </label>
          <label className="field">
            <span>失败原因</span>
            <input value={reason} onChange={(e) => setReason(e.target.value)} placeholder="timeout" />
          </label>
          <div className="muted">
            当前匹配 <b>{list?.matched ?? 0}</b> / 全部 {list?.total ?? 0} 条
          </div>
          <button className="ghost" onClick={simulateNewDeadLetter}>
            <RotateCcw size={13} /> 模拟筛选期间新增死信
          </button>

          <h2 className="gap">
            <ArrowRight size={16} /> 重放转换（冻结前可改）
          </h2>
          <label className="field">
            <span>目标主题（留空保持原主题）</span>
            <input value={targetTopic} onChange={(e) => setTargetTopic(e.target.value)} placeholder="orders.created.v2" />
          </label>
          <div className="header-editor">
            {headerDrafts.map((draft, i) => (
              <div className="header-row" key={i}>
                <input
                  value={draft.header}
                  placeholder="头部名"
                  onChange={(e) =>
                    setHeaderDrafts((rows) => rows.map((r, j) => (j === i ? {...r, header: e.target.value} : r)))
                  }
                />
                <input
                  value={draft.value}
                  placeholder="值（空=删除）"
                  onChange={(e) =>
                    setHeaderDrafts((rows) => rows.map((r, j) => (j === i ? {...r, value: e.target.value} : r)))
                  }
                />
              </div>
            ))}
            <button
              className="ghost"
              onClick={() => setHeaderDrafts((rows) => [...rows, {header: '', value: ''}])}
            >
              + 添加头部
            </button>
          </div>
          <button className="primary wide" disabled={busy || (list?.matched ?? 0) === 0} onClick={createBatch}>
            <Snowflake size={15} /> 冻结筛选结果并生成预览
          </button>
        </aside>

        {/* list or batch */}
        <section className="pane">
          {!batch ? (
            <>
              <h2>死信列表</h2>
              <div className="list">
                {list?.rows.map((row) => (
                  <div className="dead-card" key={row.id}>
                    <div className="dead-head">
                      <code>{row.id}</code>
                      <span className="pill">{row.topic}</span>
                    </div>
                    <pre>{row.body}</pre>
                    <div className="fail-history">
                      {row.failures.map((f, i) => (
                        <div key={i} className="fail-entry">
                          <Ban size={12} /> <code>{f.reason}</code>
                          {f.detail && <small> — {f.detail}</small>}
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
                {list && list.matched === 0 && <div className="muted">没有匹配的死信</div>}
              </div>
            </>
          ) : (
            <BatchView
              batch={batch}
              frozenMatch={frozenMatch}
              mutable={mutable}
              terminal={terminal}
              busy={busy}
              onStart={() => mutate('start')}
              onCancel={() => mutate('cancel')}
              onRetry={() => mutate('retry')}
              onDiscard={() => {
                setBatch(null);
                setConflict(null);
                refreshList();
              }}
            />
          )}
        </section>

        {/* inspection */}
        <aside className="pane">
          <h2>检查</h2>
          {notice && <div className="banner error">{notice}</div>}
          {conflict && <div className="banner warn">{conflict}</div>}
          {batch ? (
            <>
              <span className="pill">{batch.id}</span>
              <div className="kv">
                <div><span>批次状态</span><b>{BATCH_STATUS_TEXT[batch.status]}</b></div>
                <div><span>revision</span><code>{batch.revision}</code></div>
                <div><span>lastSeq</span><code>{batch.lastSeq}</code></div>
                <div><span>冻结时间</span><code>{batch.filter.matchedAt}</code></div>
                <div><span>冻结时死信总数</span><code>{batch.filter.candidateTotal}</code></div>
              </div>
              <pre className="json">{JSON.stringify(batch, null, 2)}</pre>
            </>
          ) : (
            <pre className="json">{JSON.stringify(list ?? null, null, 2)}</pre>
          )}
        </aside>
      </section>
    </main>
  );
}

/* --------------------------- batch view ------------------------------ */

function BatchView(props: {
  batch: Batch;
  frozenMatch: number;
  mutable: boolean;
  terminal: boolean;
  busy: boolean;
  onStart: () => void;
  onCancel: () => void;
  onRetry: () => void;
  onDiscard: () => void;
}) {
  const {batch, mutable, terminal, busy} = props;
  const counts = batch.counts;
  return (
    <>
      <div className="toolbar">
        <h2 className="no-margin">
          <Snowflake size={16} /> 批次 {batch.id}
        </h2>
        <span className={`status-badge ${batch.status}`}>{BATCH_STATUS_TEXT[batch.status]}</span>
        {batch.cancelRequested && <span className="status-badge cancelling">取消请求已生效：不再启动新发送</span>}
        <span className="spacer" />
        {mutable && (
          <button className="primary" disabled={busy} onClick={props.onStart}>
            <Play size={15} /> 确认并开始发送（{batch.items.length} 条）
          </button>
        )}
        {(batch.status === 'running' || batch.status === 'cancelling') && (
          <button className="danger" disabled={busy || batch.cancelRequested} onClick={props.onCancel}>
            <Ban size={15} /> 取消批次
          </button>
        )}
        {terminal && counts.failed > 0 && (batch.status === 'partial' || batch.status === 'failed') && (
          <button className="primary" disabled={busy} onClick={props.onRetry}>
            <RefreshCw size={15} /> 仅重试 {counts.failed} 条失败项
          </button>
        )}
        <button className="ghost" onClick={props.onDiscard}>返回筛选</button>
      </div>

      {mutable && (
        <div className="preview-note">
          确定预览：以下 {batch.items.length} 条为服务端在 <code>{batch.filter.matchedAt}</code> 冻结的集合。
          开始后筛选期间新增的死信<b>不会</b>进入本批次；目标主题与头部转换已冻结。
        </div>
      )}

      <FrozenSummary batch={batch} />

      <div className="count-row">
        {(['succeeded', 'failed', 'rejected', 'cancelled', 'sending', 'pending'] as ItemStatus[]).map((status) =>
          counts[status] > 0 ? (
            <span key={status} className={`chip ${status}`}>
              {STATUS_TEXT[status]} {counts[status]}
            </span>
          ) : null,
        )}
      </div>

      <div className="items">
        {batch.items.map((item) => (
          <ItemCard key={item.originalMessageId} item={item} mutable={mutable} />
        ))}
      </div>
    </>
  );
}

function FrozenSummary({batch}: {batch: Batch}) {
  const f = batch.filter;
  const t = batch.transformSummary;
  return (
    <div className="frozen">
      <div>
        <h3>冻结筛选</h3>
        <div className="kv small">
          <div><span>关键字</span><code>{f.q ?? '—'}</code></div>
          <div><span>主题</span><code>{f.topic ?? '—'}</code></div>
          <div><span>失败原因</span><code>{f.reason ?? '—'}</code></div>
          <div><span>冻结数量</span><b>{batch.items.length}</b></div>
          <div><span>当时死信池</span><code>{f.candidateTotal}</code></div>
        </div>
      </div>
      <div>
        <h3>转换摘要</h3>
        <div className="kv small">
          <div>
            <span>目标主题</span>
            {t.keepsOriginalTarget ? <em>保持原主题</em> : <code>{t.targetTopic}</code>}
          </div>
          {t.headerChanges.length === 0 && <div><span>头部</span><em>无变更</em></div>}
          {t.headerChanges.map((change) => (
            <div key={change.header}>
              <span>{change.op === 'remove' ? '删除头部' : '设置头部'}</span>
              <code>
                {change.header}
                {change.op === 'set' ? `=${change.value}` : ''}
              </code>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function ItemCard({item, mutable}: {item: BatchItem; mutable: boolean}) {
  return (
    <div className={`item ${item.status}`}>
      <div className="item-head">
        <code>{item.originalMessageId}</code>
        <span className={`chip ${item.status}`}>{STATUS_TEXT[item.status]}</span>
        {item.attempts > 0 && <small className="muted">尝试 {item.attempts} 次</small>}
        {item.duplicate && <small className="muted">幂等去重：未重复投递</small>}
      </div>
      <div className="route">
        <code>{item.originalTopic}</code>
        <ArrowRight size={13} />
        <code className="target">{item.targetTopic}</code>
      </div>
      <div className="idempotency">幂等键 <code>{item.idempotencyKey}</code></div>
      {item.error && <div className="error-detail">{item.error.code} — {item.error.detail}</div>}
      {mutable && (
        <details>
          <summary>转换后头部</summary>
          <pre>{JSON.stringify(item.headers, null, 2)}</pre>
        </details>
      )}
    </div>
  );
}

function countItems(items: BatchItem[]): BatchCounts {
  const counts: BatchCounts = {
    pending: 0, sending: 0, succeeded: 0, failed: 0, rejected: 0, cancelled: 0, total: items.length,
  };
  for (const item of items) counts[item.status] += 1;
  return counts;
}
