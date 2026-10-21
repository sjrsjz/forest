export type Phase = 'creating' | 'serving' | 'stopping' | 'destroying' | 'releasing' | 'failed'
export interface Release {
  version: string
  name: string | null
  description: string | null
}
export interface Instance {
  id: string
  release: Release
}
export type Lifecycle =
  | { phase: 'creating' | 'serving'; instance: Instance }
  | {
      phase: 'stopping'
      instance: Instance
      mode: 'drain' | 'at_yield' | 'force'
      replacement: Release | null
    }
  | { phase: 'destroying' | 'releasing'; instance: Instance; replacement: Release | null }
  | { phase: 'failed'; reason: string }
export interface Interface {
  path: string
  reentrant: boolean
  queued: number
}
export interface Module {
  object: string
  published: Release
  lifecycle: Lifecycle
  interfaces: Interface[]
  uses: { object: string; interface: string }[]
  subscriptions: { source: string; event: string; interface: string }[]
  budget: {
    requests_used: number
    requests_max: number
    input_bytes_used: number
    input_bytes_max: number
  }
  queued_next: number
}
export interface Host {
  object: string
  interfaces: Interface[]
  events: string[]
}
export interface Snapshot {
  sampled_at_ms: number
  parallelism: number
  memory_limit_bytes: number
  modules: Module[]
  hosts: Host[]
}
export interface Timed<T> {
  read_at_ms: number
  data: T
}
export interface ConfigKey {
  key: string
  schema: string
}
export type ValueTree =
  | { kind: 'scalar'; type_name: string; text: string }
  | { kind: 'bytes'; length: number; preview_hex: string }
  | { kind: 'sequence'; type_name: string; length: number; items: ValueTree[]; truncated: boolean }
  | { kind: 'record'; fields: { name: string; value: ValueTree }[]; truncated: boolean }
  | {
      kind: 'map'
      length: number
      entries: { key: ValueTree; value: ValueTree }[]
      truncated: boolean
    }
  | { kind: 'variant'; tag: string; body: ValueTree }
  | { kind: 'omitted' }
export interface ConfigEntry {
  schema: string
  current: ValueTree
  default: ValueTree
  is_default: boolean
}
export type Loadable<T> =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'ready'; value: T }
  | { status: 'error'; message: string; httpStatus?: number }
export const phaseLabel: Record<Phase, string> = {
  creating: '初始化',
  serving: '可接纳',
  stopping: '停止中',
  destroying: '清理中',
  releasing: '释放资源',
  failed: '失败',
}
export function bytes(value: number): string {
  if (value < 1024) return `${value} B`
  if (value < 1024 ** 2) return `${(value / 1024).toFixed(1)} KiB`
  return `${(value / 1024 ** 2).toFixed(1)} MiB`
}
export function time(value: number): string {
  return new Date(value).toLocaleTimeString('zh-CN', { hour12: false })
}
