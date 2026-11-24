import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// -- 共享测点受控交付 --

async function postJson<T>(path: string, body: Record<string, unknown>): Promise<T> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  if (!response.ok) {
    const detail = (await response.json().catch(() => ({}))) as { detail?: string }
    throw new Error(detail.detail ?? '受控交付请求失败')
  }
  return (await response.json()) as T
}

export type GrantInput = {
  授权编号: string
  测点编号: string
  所属产线: string
  共享起始时间: string
  共享截止时间: string
  允许使用粒度: string
  授权依据?: string
}

export type DeliveryReceipt = {
  receipt_id: string
  export_id: string
  shard_id: string
  point: string
  line: string
  window_start: string
  window_end: string
  granularity: string
  record_count: number
  grant_id: string
  grant_version: string
  checksum: string
  delivered_at: string
}

export type ClaimResult = {
  export_id: string
  effective_grant: { grant_id: string; version: string; start: string; end: string; granularity: string }
  delivered_this_claim: number
  claimed_total: number
  deliverable_windows: number
  remaining_shards: number
  records_delivered_total: number
  receipts: DeliveryReceipt[]
  error?: string
}

export function registerSharedGrant(values: GrantInput) {
  return postJson<{ grant: Record<string, unknown>; ledger_marked: boolean }>('/api/shared-grants', { values })
}

export function correctSharedGrant(
  grantId: number,
  payload: { new_start: string; new_end: string; new_granularity?: string; basis?: string },
) {
  return postJson<{ old_grant: Record<string, unknown>; new_grant: Record<string, unknown> }>(
    `/api/shared-grants/${grantId}/correct`,
    payload,
  )
}

export function registerRawObservations(point: string, observations: Record<string, unknown>[]) {
  return postJson<{ point: string; registered: number }>('/api/raw-observations', { point, observations })
}

export function createControlledExport(payload: {
  point: string
  line: string
  start: string
  end: string
  granularity: string
  fmt?: string
}) {
  return postJson<{ export_id: string; entry_id: number }>('/api/controlled-exports', payload)
}

export function claimControlledShards(exportId: number, maxShards?: number) {
  return postJson<ClaimResult>(`/api/controlled-exports/${exportId}/claim`, { max_shards: maxShards ?? null })
}

export async function deliveryReceipts(exportId: number): Promise<{ receipt_count: number; receipts: DeliveryReceipt[] }> {
  const response = await request(`/api/controlled-exports/${exportId}/receipts`)
  if (!response.ok) throw new Error('交付凭据读取失败')
  return await response.json()
}

export async function reconcileControlledExport(exportId: number) {
  const response = await request(`/api/controlled-exports/${exportId}/reconcile`)
  if (!response.ok) throw new Error('交付对账失败')
  return await response.json()
}
