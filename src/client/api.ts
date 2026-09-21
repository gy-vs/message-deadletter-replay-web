import type {
  BatchDetail,
  BatchSummary,
  DeadLetter,
  FilterSpec,
  ItemEventData,
  BatchEventData,
  TransformSpec,
} from '../shared/protocol.js';

export interface RevisionConflict {
  error: 'revision_conflict';
  message: string;
  current: BatchDetail;
}

async function parse(response: Response): Promise<any> {
  if (response.status === 204) return null;
  return response.json().catch(() => null);
}

async function request<T>(input: string, init?: RequestInit): Promise<T> {
  const response = await fetch(input, {
    headers: {'content-type': 'application/json', ...(init?.headers ?? {})},
    ...init,
  });
  const body = await parse(response);
  if (!response.ok) {
    const error = new Error(body?.message ?? `${response.status}`) as Error & {
      status: number;
      body: any;
    };
    error.status = response.status;
    error.body = body;
    throw error;
  }
  return body as T;
}

export const api = {
  listDeadLetters: (filter: FilterSpec = {}) => {
    const params = new URLSearchParams();
    if (filter.topic) params.set('topic', filter.topic);
    if (filter.reason) params.set('reason', filter.reason);
    const suffix = params.toString() ? `?${params}` : '';
    return request<DeadLetter[]>(`/api/dead-letters${suffix}`);
  },
  preview: (filter: FilterSpec) =>
    request<{filter: FilterSpec; total: number; count: number; matches: {id: string; topic: string; reason: string}[]}>(
      '/api/dead-letters/preview',
      {method: 'POST', body: JSON.stringify(filter)},
    ),
  createBatch: (filter: FilterSpec, transform: TransformSpec) =>
    request<BatchDetail>('/api/batches', {method: 'POST', body: JSON.stringify({filter, transform})}),
  listBatches: () => request<BatchSummary[]>('/api/batches'),
  getBatch: (id: string) => request<BatchDetail>(`/api/batches/${id}`),
  getBatchPreview: (id: string) =>
    request<
      BatchSummary & {
        preview: {id: string; originalTopic: string; targetTopic: string; reason: string; headers: Record<string, string>}[];
        immutable: {id: string; payload: string; headers: Record<string, string>; history: {at: string; stage: string; detail: string}[]}[];
      }
    >(`/api/batches/${id}/preview`),
  updateTransform: (id: string, revision: number, transform: TransformSpec) =>
    request<BatchDetail>(`/api/batches/${id}`, {method: 'PATCH', body: JSON.stringify({revision, transform})}),
  start: (id: string, revision: number) =>
    request<BatchDetail>(`/api/batches/${id}/start`, {method: 'POST', body: JSON.stringify({revision})}),
  retryFailed: (id: string, revision: number) =>
    request<BatchDetail>(`/api/batches/${id}/retry`, {method: 'POST', body: JSON.stringify({revision})}),
  cancel: (id: string, revision: number) =>
    request<BatchDetail>(`/api/batches/${id}/cancel`, {method: 'POST', body: JSON.stringify({revision})}),
  injectDeadLetter: (input: {id?: string; topic: string; reason: string}) =>
    request<DeadLetter>('/api/dev/dead-letters', {method: 'POST', body: JSON.stringify(input)}),
};

export type {ItemEventData, BatchEventData};
