<script setup lang="ts">
import { computed, onUnmounted, ref, shallowRef, watch } from 'vue'
import { HttpError, query, request } from '../api'
import type { ConfigEntry, ConfigKey, Loadable, Snapshot, Timed } from '../types'
import { time } from '../types'
import ValueTree from './ValueTree.vue'
const props = defineProps<{ selected: string; snapshot: Snapshot }>()
const emit = defineEmits<{ expired: [] }>()
const entities = shallowRef<Loadable<Timed<string[]>>>({ status: 'loading' })
const keys = shallowRef<Loadable<Timed<ConfigKey[]>>>({ status: 'idle' })
const entry = shallowRef<Loadable<Timed<ConfigEntry>>>({ status: 'idle' })
const object = ref(props.selected)
const selectedKey = ref('')
const search = ref('')
const follow = ref(false)
const refreshKeys = ref(0)
const refreshEntry = ref(0)
const activeTab = ref<'current' | 'default' | 'schema'>('current')
const loadingEntry = ref(false)
const entityController = new AbortController()
const loaded = computed(() => new Set(props.snapshot.modules.map((m) => m.object)))
const filteredKeys = computed(() =>
  keys.value.status === 'ready'
    ? keys.value.value.data.filter((k) =>
        k.key.toLocaleLowerCase().includes(search.value.toLocaleLowerCase()),
      )
    : [],
)
const download = computed(() =>
  query('config-value', {
    object: object.value,
    key: selectedKey.value,
    value: activeTab.value === 'default' ? 'default' : 'current',
  }),
)
const current = computed(() => (entry.value.status === 'ready' ? entry.value.value : null))
function failure<T>(error: unknown): Loadable<T> {
  if (error instanceof HttpError && error.status === 401) emit('expired')
  return {
    status: 'error',
    message: error instanceof Error ? error.message : String(error),
    ...(error instanceof HttpError ? { httpStatus: error.status } : {}),
  }
}
async function loadEntities() {
  try {
    entities.value = {
      status: 'ready',
      value: await request<Timed<string[]>>('/api/inspect/config-entities', {
        signal: entityController.signal,
      }),
    }
  } catch (e) {
    if (!entityController.signal.aborted) entities.value = failure(e)
  }
}
void loadEntities()
function refreshDirectory() {
  refreshKeys.value++
  void loadEntities()
}
watch(
  () => props.selected,
  (value) => {
    object.value = value
  },
)
watch(
  [object, refreshKeys],
  async ([target], _, cleanup) => {
    const controller = new AbortController()
    cleanup(() => controller.abort())
    selectedKey.value = ''
    entry.value = { status: 'idle' }
    if (!target) {
      keys.value = { status: 'idle' }
      return
    }
    keys.value = { status: 'loading' }
    try {
      const value = await request<Timed<ConfigKey[]>>(query('config-keys', { object: target }), {
        signal: controller.signal,
      })
      if (!controller.signal.aborted) {
        keys.value = { status: 'ready', value }
        selectedKey.value = value.data[0]?.key ?? ''
      }
    } catch (e) {
      if (!controller.signal.aborted) keys.value = failure(e)
    }
  },
  { immediate: true },
)
watch(
  [object, selectedKey, follow, refreshEntry],
  ([target, key], _, cleanup) => {
    const controller = new AbortController()
    let timer: ReturnType<typeof setTimeout> | undefined
    cleanup(() => {
      controller.abort()
      clearTimeout(timer)
    })
    loadingEntry.value = false
    entry.value = { status: 'idle' }
    if (!target || !key) return
    async function load() {
      loadingEntry.value = true
      if (entry.value.status !== 'ready') entry.value = { status: 'loading' }
      try {
        const value = await request<Timed<ConfigEntry>>(
          query('config-entry', { object: target, key }),
          { signal: controller.signal },
        )
        if (!controller.signal.aborted) entry.value = { status: 'ready', value }
      } catch (e) {
        if (!controller.signal.aborted) entry.value = failure(e)
      } finally {
        if (!controller.signal.aborted) {
          loadingEntry.value = false
          if (follow.value) timer = setTimeout(load, 1000)
        }
      }
    }
    void load()
  },
  { immediate: true },
)
onUnmounted(() => entityController.abort())
</script>

<template>
  <section class="config-view">
    <div class="config-toolbar">
      <label class="entity-label">
        配置对象
        <select v-model="object" aria-label="配置对象">
          <option
            v-if="
              selected && (entities.status !== 'ready' || !entities.value.data.includes(selected))
            "
            :value="selected"
          >
            {{ selected }} · 暂无配置
          </option>
          <template v-if="entities.status === 'ready'">
            <option v-for="id in entities.value.data" :key="id" :value="id">
              {{ id }}{{ loaded.has(id) ? '' : ' · 仅持久化数据' }}
            </option>
          </template>
          <option v-if="!object" value="">请选择对象</option>
        </select>
      </label>
      <button class="subtle" @click="refreshDirectory">刷新目录</button>
    </div>
    <div v-if="entities.status === 'error'" class="notice danger" role="alert">
      {{ entities.message }}
    </div>
    <div class="config-split">
      <aside class="config-key-panel">
        <div class="search-wrap">
          <input v-model="search" placeholder="筛选配置路径…" aria-label="筛选配置路径" />
        </div>
        <div class="key-list scroll-area">
          <p v-if="keys.status === 'loading'" class="muted inset">正在读取配置键…</p>
          <div v-else-if="keys.status === 'error'" class="notice" role="status">
            <template v-if="keys.httpStatus === 404">
              <strong>暂无配置表</strong>
            </template>
            <template v-else>{{ keys.message }}</template>
          </div>
          <template v-else-if="keys.status === 'ready'">
            <button
              v-for="item in filteredKeys"
              :key="item.key"
              class="config-key"
              :class="{ active: selectedKey === item.key }"
              @click="selectedKey = item.key"
            >
              <code>{{ item.key }}</code>
              <span>{{ item.schema }}</span>
            </button>
            <p v-if="!keys.value.data.length" class="muted inset">暂无配置项</p>
            <p v-else-if="!filteredKeys.length" class="muted inset">无匹配项</p>
          </template>
          <p v-else class="muted inset">选择配置对象</p>
        </div>
        <div v-if="keys.status === 'ready'" class="panel-footer">
          {{ filteredKeys.length }} / {{ keys.value.data.length }} 项
        </div>
      </aside>
      <div class="config-entry-panel">
        <template v-if="selectedKey">
          <div class="entry-toolbar">
            <code class="entry-path">{{ selectedKey }}</code>
            <div class="button-group">
              <label class="follow">
                <input v-model="follow" type="checkbox" />
                自动刷新
              </label>
              <button :disabled="loadingEntry" @click="refreshEntry++">刷新</button>
            </div>
          </div>
          <div class="entry-meta">
            <span v-if="current" class="tag" :class="{ neutral: current.data.is_default }">
              {{ current.data.is_default ? '等于默认值' : '区别于默认值' }}
            </span>
            <span v-if="current" class="muted">读取于 {{ time(current.read_at_ms) }}</span>
            <a :href="download" download="forest-value.txt">下载完整值 ↗</a>
          </div>
          <div class="mini-tabs">
            <button :class="{ active: activeTab === 'current' }" @click="activeTab = 'current'">
              当前值
            </button>
            <button :class="{ active: activeTab === 'default' }" @click="activeTab = 'default'">
              默认值
            </button>
            <button :class="{ active: activeTab === 'schema' }" @click="activeTab = 'schema'">
              Schema
            </button>
          </div>
          <div class="entry-value scroll-area">
            <div v-if="entry.status === 'loading'" class="muted">正在读取…</div>
            <div v-else-if="entry.status === 'error'" class="notice danger" role="alert">
              {{ entry.message }}
            </div>
            <template v-else-if="current">
              <pre v-if="activeTab === 'schema'" class="schema-text">{{ current.data.schema }}</pre>
              <ValueTree
                v-else
                :key="`${object}:${selectedKey}:${activeTab}`"
                :value="activeTab === 'current' ? current.data.current : current.data.default"
              />
            </template>
          </div>
        </template>
        <div v-else class="empty-state">
          <strong>选择一个配置项</strong>
        </div>
      </div>
    </div>
  </section>
</template>
