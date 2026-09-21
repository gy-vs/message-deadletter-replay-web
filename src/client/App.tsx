import {useCallback, useEffect, useMemo, useState} from 'react';
import {
  Ban,
  CheckCircle2,
  CircleDashed,
  FlaskConical,
  PauseCircle,
  Play,
  RefreshCw,
  RotateCcw,
  Snowflake,
  Wifi,
  WifiOff,
  XCircle,
} from 'lucide-react';
import type {BatchDetail, DeadLetter, FilterSpec, ItemStatus, TransformSpec} from '../shared/protocol';
import {api} from './api';
import {useBatchEvents} from './useBatchEvents';

const STATUS_META: Record<ItemStatus, {label: string; icon: typeof Play; tone: string}> = {
  pending: {label: '待发送', icon: CircleDashed, tone: 'muted'},
  inflight: {label: '发送中', icon: RefreshCw, tone: 'active'},
  succeeded: {label: '成功', icon: CheckCircle2, tone: 'ok'},
  failed: {label: '失败', icon: XCircle, tone: 'bad'},
  cancelled: {label: '已取消', icon: Ban, tone: 'muted'},
};

const BATCH_STATUS_LABEL: Record<BatchDetail['status'], string> = {
  pending: '待开始（已冻结）',
  running: '发送中',
  partial: '部分失败',
  succeeded: '全部成功',
  cancelled: '已取消',
};

type HeaderRows = {key: string; value: string}[];

function headersToRows(headers: Record<string, string>): HeaderRows {
  return Object.entries(headers).map(([key, value]) => ({key, value}));
}
function rowsToHeaders(rows: HeaderRows): Record<string, string> {
  const out: Record<string, string> = {};
  for (const row of rows) {
    const key = row.key.trim();
    if (key) out[key] = row.value;
  }
  return out;
}

export default function App() {
  // --- filter + transform draft -------------------------------------------
  const [letters, setLetters] = useState<DeadLetter[]>([]);
  const [topic, setTopic] = useState('');
  const [reason, setReason] = useState('');
  const [targetTopic, setTargetTopic] = useState('orders.v1.replay');
  const [headerRows, setHeaderRows] = useState<HeaderRows>([{key: 'x-replay-reason', value: 'manual-replay'}]);
  const [preview, setPreview] = useState<{count: number; total: number; ids: string[]} | null>(null);
  const [freezing, setFreezing] = useState(false);
  const [formError, setFormError] = useState<string | null>(null);

  // --- batch aggregate -----------------------------------------------------
  const [batch, setBatch] = useState<BatchDetail | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const stream = useBatchEvents(batch?.id ?? null);

  const filter = useMemo<FilterSpec>(() => ({topic: topic || undefined, reason: reason || undefined}), [topic, reason]);
  const transform = useMemo<TransformSpec>(
    () => ({targetTopic: targetTopic.trim(), headers: rowsToHeaders(headerRows)}),
    [targetTopic, headerRows],
  );

  const topics = useMemo(() => [...new Set(letters.map((l) => l.topic))].sort(), [letters]);
  const reasons = useMemo(() => [...new Set(letters.map((l) => l.reason))].sort(), [letters]);

  const refreshLetters = useCallback(async () => {
    const list = await api.listDeadLetters();
    setLetters(list);
  }, []);

  const refreshPreview = useCallback(async () => {
    const result = await api.preview(filter);
    setPreview({count: result.count, total: result.total, ids: result.matches.map((m) => m.id)});
  }, [filter]);

  useEffect(() => {
    refreshLetters();
  }, [refreshLetters]);

  useEffect(() => {
    refreshPreview();
  }, [refreshPreview]);

  const liveItems = (batch?.items ?? []).map((item) => {
    const ev = stream.items[item.originalId];
    return ev ? {...item, status: ev.status, sends: ev.sends, error: ev.error} : item;
  });
  const liveStatus: BatchDetail['status'] | undefined = stream.batch?.status ?? batch?.status;
  const liveCounts = stream.batch?.counts ?? batch?.counts;
  const liveRevision = stream.batch?.revision ?? batch?.revision ?? 0;
  useEffect(() => {
    if (!batch) return;
    const active = ['running', 'partial', 'pending'].includes(batch.status);
    if (!active) return;
    const timer = setInterval(async () => {
      const fresh = await api.getBatch(batch.id);
      setBatch(fresh);
    }, 2500);
    return () => clearInterval(timer);
  }, [batch?.id, batch?.status]); // eslint-disable-line react-hooks/exhaustive-deps

  const matchedLetters = useMemo(() => {
    const set = new Set(preview?.ids ?? []);
    return letters.filter((l) => set.has(l.id));
  }, [letters, preview]);

  // --- freeze / confirmation -----------------------------------------------

  async function freeze() {
    setFormError(null);
    setFreezing(true);
    try {
      // The server re-evaluates and freezes at this instant; any dead letter
      // that arrives later is drift, never a silent member of the batch.
      const detail = await api.createBatch(filter, transform);
      setBatch(detail);
      setSelectedId(detail.items[0]?.originalId ?? null);
      setActionError(null);
    } catch (error: any) {
      setFormError(error.body?.message ?? String(error));
    } finally {
      setFreezing(false);
    }
  }

  async function applyConfigToFrozen() {
    if (!batch) return;
    try {
      const updated = await api.updateTransform(batch.id, batch.revision, transform);
      setBatch(updated);
      setActionError(null);
    } catch (error: any) {
      handleActionError(error);
    }
  }

  function handleActionError(error: any) {
    if (error?.status === 409 && error.body?.error === 'revision_conflict' && error.body.current) {
      // Another page/tab moved first: adopt its frozen state, never overwrite.
      setBatch(error.body.current);
      setActionError(`配置/版本冲突：另一个页面已推进该批次（revision ${error.body.current.revision}），已同步其状态`);
      return;
    }
    setActionError(error?.body?.message ?? String(error));
  }

  async function act(kind: 'start' | 'retry' | 'cancel') {
    if (!batch) return;
    try {
      const fn = kind === 'start' ? api.start : kind === 'retry' ? api.retryFailed : api.cancel;
      const detail = await fn(batch.id, liveRevision);
      setBatch(detail);
      setActionError(null);
    } catch (error: any) {
      handleActionError(error);
    }
  }

  async function injectLetter() {
    const id = `dl-new-${Date.now()}`;
    await api.injectDeadLetter({id, topic: topic || 'orders.v1', reason: reason || 'rejected'});
    await refreshLetters();
    await refreshPreview();
  }

  const selected = batch?.items.find((i) => i.originalId === selectedId) ?? null;

  return (
    <main className="shell">
      <header className="topbar">
        <FlaskConical size={20} />
        <strong>死信重放工作台</strong>
        <small>筛选 → 冻结预览 → 重放</small>
        <span className={`conn ${stream.connected ? 'on' : 'off'}`}>
          {stream.connected ? <Wifi size={14} /> : <WifiOff size={14} />}
          {stream.connected ? '事件流已连接' : '事件流断开（轮询兜底）'}
        </span>
      </header>

      <section className="workspace">
        {/* 1. filter + transform */}
        <aside className="pane">
          <h2>1. 筛选死信</h2>
          <label className="field">
            原始主题
            <select value={topic} onChange={(e) => setTopic(e.target.value)}>
              <option value="">（全部）</option>
              {topics.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
          </label>
          <label className="field">
            失败原因
            <select value={reason} onChange={(e) => setReason(e.target.value)}>
              <option value="">（全部）</option>
              {reasons.map((r) => (
                <option key={r} value={r}>
                  {r}
                </option>
              ))}
            </select>
          </label>

          <div className="live-count">
            实时匹配（未冻结）：<strong>{preview?.count ?? '…'}</strong> / 共 {preview?.total ?? '…'} 条
            <button className="link" onClick={refreshPreview} title="重新计算">
              <RefreshCw size={12} />
            </button>
          </div>
          <ul className="preview-list">
            {matchedLetters.map((l) => (
              <li key={l.id}>
                <code>{l.id}</code>
                <small>{l.topic} · {l.reason}</small>
              </li>
            ))}
          </ul>
          <button className="ghost" onClick={injectLetter}>
            模拟筛选期间新增死信
          </button>

          <h2>2. 重放转换</h2>
          <label className="field">
            目标主题
            <input value={targetTopic} onChange={(e) => setTargetTopic(e.target.value)} placeholder="orders.v1.replay" />
          </label>
          <div className="field">
            头部补丁（空值=删除该头；原始头部不可变）
            <div className="headers">
              {headerRows.map((row, index) => (
                <div className="header-row" key={index}>
                  <input
                    value={row.key}
                    placeholder="header"
                    onChange={(e) =>
                      setHeaderRows((rows) => rows.map((r, i) => (i === index ? {...r, key: e.target.value} : r)))
                    }
                  />
                  <input
                    value={row.value}
                    placeholder="value（空=删除）"
                    onChange={(e) =>
                      setHeaderRows((rows) => rows.map((r, i) => (i === index ? {...r, value: e.target.value} : r)))
                    }
                  />
                  <button className="icon" onClick={() => setHeaderRows((rows) => rows.filter((_, i) => i !== index))}>
                    ×
                  </button>
                </div>
              ))}
              <button
                className="ghost"
                onClick={() => setHeaderRows((rows) => [...rows, {key: '', value: ''}])}
              >
                + 添加头部
              </button>
            </div>
          </div>

          <button className="primary wide" onClick={freeze} disabled={freezing || !targetTopic.trim()}>
            <Snowflake size={15} /> 冻结筛选结果并生成预览
          </button>
          {formError && <p className="error">{formError}</p>}
          <p className="hint">
            提示：可用头部 <code>x-sim: flaky</code>（首次失败）、<code>x-sim: fail-always</code>（恒失败），
            或目标主题 <code>topic.rejected</code>（目标拒绝）。
          </p>
        </aside>

        {/* 2. frozen preview / progress */}
        <section className="pane">
          {!batch ? (
            <EmptyState />
          ) : (
            <>
              <div className="batch-head">
                <h2>批次 {batch.id.slice(0, 8)}</h2>
                <span className={`status status-${liveStatus}`}>{BATCH_STATUS_LABEL[liveStatus!]}</span>
                <span className="rev">revision {stream.batch?.revision ?? batch.revision}</span>
              </div>

              {batch.status === 'pending' && (
                <div className="confirm">
                  <h3><Snowflake size={15} /> 开始前确认预览</h3>
                  <p className="frozen-note">
                    已冻结 <strong>{batch.frozenIds.length}</strong> 条（与冻结集合一致），
                    冻结时间 {new Date(batch.frozenAt).toLocaleTimeString()}。
                    目标 <code>{batch.transform.targetTopic}</code>，头部补丁{' '}
                    {Object.keys(batch.transform.headers).length} 项。
                  </p>
                  {batch.driftAdded > 0 && (
                    <p className="warn">
                      冻结后有 {batch.driftAdded} 条新的匹配死信到达 —— 不会进入本批次（需另建批次）。
                    </p>
                  )}
                  <div className="actions">
                    <button className="primary" onClick={() => act('start')}>
                      <Play size={15} /> 开始重放
                    </button>
                    <button onClick={applyConfigToFrozen}>
                      <RotateCcw size={15} /> 应用左侧修改到冻结配置
                    </button>
                    <button className="ghost" onClick={() => setBatch(null)}>放弃此批次</button>
                  </div>
                </div>
              )}

              <div className="counts">
                <Count label="总数" value={liveCounts!.total} />
                <Count label="成功" value={liveCounts!.succeeded} tone="ok" />
                <Count label="失败" value={liveCounts!.failed} tone="bad" />
                <Count label="发送中" value={liveCounts!.inflight} tone="active" />
                <Count label="已取消" value={liveCounts!.cancelled} />
              </div>

              {batch.driftAdded > 0 && (
                <p className="warn">
                  冻结后有 {batch.driftAdded} 条匹配新死信到达 —— 不属于本批次，需要时请新建批次。
                </p>
              )}

              {(liveStatus === 'succeeded' || liveStatus === 'cancelled') && (
                <div className="actions">
                  <button className="ghost" onClick={() => setBatch(null)}>
                    用当前筛选新建批次
                  </button>
                </div>
              )}

              {liveStatus === 'running' && (
                <div className="actions">
                  <button onClick={() => act('cancel')}>
                    <PauseCircle size={15} /> 取消（不再启动新发送，成功项保留）
                  </button>
                </div>
              )}
              {liveStatus === 'partial' && (
                <div className="actions">
                  <button className="primary" onClick={() => act('retry')}>
                    <RotateCcw size={15} /> 仅重试失败项（{liveCounts!.failed}）
                  </button>
                  <button onClick={() => act('cancel')}>
                    <Ban size={15} /> 取消
                  </button>
                </div>
              )}
              {actionError && <p className="error">{actionError}</p>}

              <ul className="items">
                {liveItems.map((item) => {
                  const meta = STATUS_META[item.status as ItemStatus];
                  const Icon = meta.icon;
                  return (
                    <li
                      key={item.originalId}
                      className={`item ${selectedId === item.originalId ? 'active' : ''} tone-${meta.tone}`}
                      onClick={() => setSelectedId(item.originalId)}
                    >
                      <Icon size={15} className={`tone-${meta.tone}`} />
                      <div className="item-main">
                        <code>{item.originalId}</code>
                        <small>
                          {item.originalTopic} → {item.targetTopic} · 发送 {item.sends} 次
                        </small>
                        {item.error && <small className="item-error">{item.error}</small>}
                      </div>
                      <span className={`badge tone-${meta.tone}`}>{meta.label}</span>
                    </li>
                  );
                })}
              </ul>
            </>
          )}
        </section>

        {/* 3. immutable inspector */}
        <aside className="pane inspector">
          <h2>原始消息（不可变）</h2>
          {!selected ? (
            <p className="hint">选择一条消息查看原始载荷与失败历史。</p>
          ) : (
            <ImmutableInspector
              id={selected.originalId}
              idempotencyKey={selected.idempotencyKey}
              targetTopic={batch!.transform.targetTopic}
              headerPatch={batch!.transform.headers}
              bump={batch!.revision}
            />
          )}
        </aside>
      </section>
    </main>
  );
}

function Count({label, value, tone}: {label: string; value: number; tone?: string}) {
  return (
    <div className={`count ${tone ? `tone-${tone}` : ''}`}>
      <strong>{value}</strong>
      <span>{label}</span>
    </div>
  );
}

function EmptyState() {
  return (
    <div className="empty">
      <Snowflake size={28} />
      <p>在左侧配置筛选条件与重放转换，然后冻结生成开始前预览。</p>
    </div>
  );
}

function ImmutableInspector({
  id,
  idempotencyKey,
  targetTopic,
  headerPatch,
  bump,
}: {
  id: string;
  idempotencyKey: string;
  targetTopic: string;
  headerPatch: Record<string, string>;
  bump: number;
}) {
  const [letter, setLetter] = useState<DeadLetter | null>(null);
  useEffect(() => {
    setLetter(null);
    api.listDeadLetters().then((all) => setLetter(all.find((l) => l.id === id) ?? null));
  }, [id]);

  const effective = useMemo(() => {
    if (!letter) return {};
    const merged: Record<string, string> = {...letter.headers};
    for (const [k, v] of Object.entries(headerPatch)) {
      if (v === '') delete merged[k];
      else merged[k] = v;
    }
    return merged;
  }, [letter, headerPatch, bump]);

  if (!letter) return <p className="hint">加载中…</p>;
  return (
    <div className="inspect-body">
      <span className="pill">{letter.id}</span>
      <dl>
        <dt>原始主题</dt>
        <dd><code>{letter.topic}</code></dd>
        <dt>重放目标</dt>
        <dd><code>{targetTopic}</code></dd>
        <dt>失败原因</dt>
        <dd>{letter.reason}（原尝试 {letter.attempts} 次）</dd>
        <dt>派生幂等键</dt>
        <dd><code>{idempotencyKey}</code></dd>
      </dl>

      <h4>原始载荷</h4>
      <pre className="readonly">{letter.payload}</pre>

      <h4>原始头部</h4>
      <pre className="readonly">{JSON.stringify(letter.headers, null, 2)}</pre>

      <h4>重放时生效头部（派生，不回写）</h4>
      <pre>{JSON.stringify(effective, null, 2)}</pre>

      <h4>失败历史（只追加，不可变）</h4>
      <ul className="history">
        {letter.history.map((h, i) => (
          <li key={i}>
            <small>{new Date(h.at).toLocaleString()}</small>
            <strong>{h.stage}</strong>
            <span>{h.detail}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}
