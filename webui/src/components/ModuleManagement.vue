<script setup lang="ts">
import { computed, ref, shallowRef, watch, watchEffect } from 'vue'
import { command, HttpError, request } from '../api'
import type { Loadable, LoadPlan, LocalPackage, Snapshot, StopMode } from '../types'
import { phaseLabel } from '../types'

const props = defineProps<{
  open: boolean
  intent: 'load' | 'reload' | 'unload'
  target: string
  snapshot: Snapshot
}>()
const emit = defineEmits<{ close: []; updated: [object: string]; expired: [] }>()
const packages = shallowRef<Loadable<LocalPackage[]>>({ status: 'idle' })
const plan = shallowRef<Loadable<LoadPlan>>({ status: 'idle' })
const directory = ref('')
const mode = ref<StopMode>('drain')
const refresh = ref(0)
const busy = shallowRef<{ intent: 'load' | 'reload' | 'unload'; object: string } | null>(null)
const forcing = ref(false)
const result = ref('')
const error = ref('')
const details = ref<'summary' | 'manifest'>('summary')
const dialog = ref<HTMLDialogElement | null>(null)
watchEffect(() => {
  if (!dialog.value) return
  if (props.open) dialog.value.showModal()
  else dialog.value.close()
})
watch([() => props.open, () => props.intent, () => props.target], () => {
  if (!busy.value) {
    result.value = ''
    error.value = ''
    mode.value = 'drain'
    details.value = 'summary'
  }
})
const title = computed(
  () => ({ load: '本地包', reload: '重载模块', unload: '卸载模块' })[props.intent],
)
const entries = computed(() => (packages.value.status === 'ready' ? packages.value.value : []))
const candidates = computed(() =>
  props.intent === 'reload'
    ? entries.value.filter((p) => p.status === 'ready' && p.manifest.package.id === props.target)
    : entries.value,
)
const selected = computed(() => entries.value.find((p) => p.directory === directory.value))
const manifest = computed(() =>
  selected.value?.status === 'ready' ? selected.value.manifest : null,
)
const targetModule = computed(() => props.snapshot.modules.find((m) => m.object === props.target))
const operationModule = computed(() =>
  props.snapshot.modules.find((m) => m.object === (busy.value?.object ?? props.target)),
)
const users = computed(() =>
  props.snapshot.modules.filter(
    (m) => m.object !== props.target && m.uses.some((u) => u.object === props.target),
  ),
)
const canForce = computed(
  () =>
    operationModule.value &&
    ['creating', 'stopping', 'destroying'].includes(operationModule.value.lifecycle.phase),
)
const canSubmit = computed(() => {
  if (busy.value) return false
  if (props.intent === 'load')
    return plan.value.status === 'ready' && plan.value.value.order.length > 0
  if (props.intent === 'reload')
    return !!manifest.value && targetModule.value?.lifecycle.phase === 'serving'
  return (
    !!targetModule.value &&
    ['serving', 'failed'].includes(targetModule.value.lifecycle.phase) &&
    !users.value.length
  )
})
function message(e: unknown): string {
  if (e instanceof HttpError && e.status === 401) emit('expired')
  return e instanceof Error ? e.message : String(e)
}
watch(
  [() => props.open, () => props.intent, () => props.target, refresh],
  async ([open], _, cleanup) => {
    if (!open) return
    if (props.intent === 'unload') return
    const controller = new AbortController()
    cleanup(() => controller.abort())
    packages.value = { status: 'loading' }
    try {
      const value = await request<LocalPackage[]>('/api/modules/packages', {
        signal: controller.signal,
      })
      if (controller.signal.aborted) return
      packages.value = { status: 'ready', value }
      if (!candidates.value.some((p) => p.directory === directory.value))
        directory.value = candidates.value[0]?.directory ?? ''
    } catch (e) {
      if (!controller.signal.aborted) packages.value = { status: 'error', message: message(e) }
    }
  },
  { immediate: true },
)
watch(
  [manifest, () => props.open, () => props.intent],
  async ([declaration, open, intent], _, cleanup) => {
    plan.value = { status: 'idle' }
    if (!declaration || !open || intent !== 'load') return
    const controller = new AbortController()
    cleanup(() => controller.abort())
    plan.value = { status: 'loading' }
    try {
      const value = await request<LoadPlan>('/api/modules/plan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ directory: directory.value }),
        signal: controller.signal,
      })
      if (!controller.signal.aborted) plan.value = { status: 'ready', value }
    } catch (e) {
      if (!controller.signal.aborted) plan.value = { status: 'error', message: message(e) }
    }
  },
)
async function submit() {
  const intent = props.intent
  const object = intent === 'load' ? manifest.value!.package.id : props.target
  busy.value = { intent, object }
  result.value = ''
  error.value = ''
  try {
    const input =
      intent === 'load'
        ? { directory: directory.value }
        : intent === 'reload'
          ? { object, directory: directory.value, mode: mode.value }
          : { object, mode: mode.value }
    await command(intent, input)
    result.value = { load: '已加载', reload: '已重载', unload: '已卸载' }[intent]
    emit('updated', intent === 'unload' ? '' : object)
    refresh.value++
  } catch (e) {
    error.value = message(e)
  } finally {
    busy.value = null
  }
}
async function force() {
  forcing.value = true
  error.value = ''
  try {
    await command('force', { object: operationModule.value!.object })
    emit('updated', operationModule.value?.object ?? '')
  } catch (e) {
    error.value = message(e)
  } finally {
    forcing.value = false
  }
}
</script>

<template>
  <dialog
    ref="dialog"
    class="management-dialog"
    :aria-label="title"
    @click.self="emit('close')"
    @cancel.prevent="emit('close')"
  >
    <section class="management-panel">
      <header class="management-heading">
        <div>
          <h2>{{ title }}</h2>
          <code v-if="target && intent !== 'load'">{{ target }}</code>
        </div>
        <div class="button-group">
          <button
            v-if="intent !== 'unload'"
            :disabled="packages.status === 'loading' || !!busy"
            @click="refresh++"
          >
            刷新目录
          </button>
          <button aria-label="关闭模块管理" @click="emit('close')">×</button>
        </div>
      </header>
      <div class="management-body scroll-area">
        <template v-if="intent !== 'unload'">
          <p v-if="packages.status === 'loading'" class="muted">读取本地包…</p>
          <p v-else-if="packages.status === 'error'" class="notice danger" role="alert">
            {{ packages.message }}
          </p>
          <label class="package-select">
            包目录
            <select
              v-model="directory"
              :disabled="!!busy || packages.status !== 'ready'"
              aria-label="包目录"
            >
              <option v-if="!candidates.length" value="">暂无可用包</option>
              <option v-for="p in candidates" :key="p.directory" :value="p.directory">
                {{ p.directory
                }}{{ p.status === 'ready' ? ` · ${p.manifest.package.version}` : ' · 无效' }}
              </option>
            </select>
          </label>
          <p v-if="selected?.status === 'invalid'" class="notice danger" role="alert">
            {{ selected.message }}
          </p>
          <template v-if="manifest">
            <div class="mini-tabs">
              <button :class="{ active: details === 'summary' }" @click="details = 'summary'">
                声明概览
              </button>
              <button :class="{ active: details === 'manifest' }" @click="details = 'manifest'">
                Manifest
              </button>
            </div>
            <pre v-if="details === 'manifest'" class="manifest-text">{{
              JSON.stringify(manifest, null, 2)
            }}</pre>
            <div v-else class="package-summary">
              <h3>{{ manifest.package.name || manifest.package.id }}</h3>
              <code>{{ manifest.package.id }}</code>
              <p v-if="manifest.package.description" class="muted">
                {{ manifest.package.description }}
              </p>
              <dl class="properties">
                <div>
                  <dt>候选版本</dt>
                  <dd>{{ manifest.package.version }}</dd>
                </div>
                <div v-if="targetModule">
                  <dt>当前版本</dt>
                  <dd>{{ targetModule.published.version }}</dd>
                </div>
                <div>
                  <dt>Wasm</dt>
                  <dd>
                    <code>{{ manifest.module }}</code>
                  </dd>
                </div>
                <div>
                  <dt>配置项</dt>
                  <dd>{{ manifest.configs.length }}</dd>
                </div>
              </dl>
              <h3>提供接口</h3>
              <ul class="plain-list">
                <li v-for="i in manifest.provides" :key="i.path">
                  <code>{{ i.path }}</code>
                </li>
                <li v-if="!manifest.provides.length" class="muted">无</li>
              </ul>
              <h3>接口依赖</h3>
              <ul class="plain-list">
                <li v-for="u in manifest.uses" :key="`${u.object}:${u.interface}`">
                  <code>{{ u.object }} → {{ u.interface }}</code>
                </li>
                <li v-if="!manifest.uses.length" class="muted">无</li>
              </ul>
              <h3>事件接收</h3>
              <ul class="plain-list">
                <li
                  v-for="s in manifest.subscriptions"
                  :key="`${s.source}:${s.event}:${s.interface}`"
                >
                  <code>{{ s.source }} · {{ s.event }} → {{ s.interface }}</code>
                </li>
                <li v-if="!manifest.subscriptions.length" class="muted">无</li>
              </ul>
            </div>
          </template>
          <section v-if="intent === 'load'" class="load-plan">
            <h3>装载顺序</h3>
            <p v-if="plan.status === 'loading'" class="muted">规划中…</p>
            <p v-else-if="plan.status === 'error'" class="notice danger" role="alert">
              {{ plan.message }}
            </p>
            <template v-else-if="plan.status === 'ready'">
              <ol v-if="plan.value.order.length">
                <li v-for="p in plan.value.order" :key="p.manifest.package.id">
                  <code>{{ p.manifest.package.id }}</code>
                  <span class="muted">{{ p.manifest.package.version }}</span>
                </li>
              </ol>
              <p v-else class="muted">已加载</p>
            </template>
          </section>
        </template>
        <template v-if="intent === 'unload'">
          <p v-if="!targetModule" class="muted">模块已卸载</p>
          <section v-if="users.length" class="dependent-list">
            <h3>依赖此模块</h3>
            <ul class="plain-list">
              <li v-for="m in users" :key="m.object">
                <code>{{ m.object }}</code>
              </li>
            </ul>
          </section>
        </template>
        <fieldset
          v-if="
            intent !== 'load' &&
            targetModule &&
            ['serving', 'failed'].includes(targetModule.lifecycle.phase)
          "
          class="stop-modes"
          :disabled="!!busy"
        >
          <legend>停止方式</legend>
          <label>
            <input v-model="mode" type="radio" value="drain" name="stop-mode" />
            排空后停止
            <code>Drain</code>
          </label>
          <label>
            <input v-model="mode" type="radio" value="at_yield" name="stop-mode" />
            主动让出后停止
            <code>AtYield</code>
          </label>
          <label>
            <input v-model="mode" type="radio" value="force" name="stop-mode" />
            强制停止
            <code>Force</code>
          </label>
        </fieldset>
      </div>
      <footer class="management-footer">
        <div class="management-feedback" aria-live="polite">
          <p v-if="error" class="management-error" role="alert">{{ error }}</p>
          <p v-else-if="busy">
            {{ { load: '加载中…', reload: '重载中…', unload: '卸载中…' }[busy.intent] }}
            <code>{{ busy.object }}</code>
            <span v-if="operationModule">{{ phaseLabel[operationModule.lifecycle.phase] }}</span>
          </p>
          <p v-else-if="result" class="management-success">{{ result }}</p>
          <p v-else-if="targetModule">{{ phaseLabel[targetModule.lifecycle.phase] }}</p>
        </div>
        <div class="button-group">
          <button v-if="canForce" class="danger-button" :disabled="forcing" @click="force">
            强制中断
          </button>
          <button
            :class="intent === 'unload' ? 'danger-button' : 'primary'"
            :disabled="!canSubmit"
            @click="submit"
          >
            {{ { load: '加载', reload: '执行重载', unload: '卸载' }[intent] }}
          </button>
        </div>
      </footer>
    </section>
  </dialog>
</template>
