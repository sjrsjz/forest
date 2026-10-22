<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, ref, watch } from 'vue'
import { HttpError, request } from '../api'

type Level = 'Error' | 'Warn' | 'Info' | 'Debug' | 'Trace'
interface Entry {
  sequence: number
  timestamp_ms: number
  level: Level
  source: string
  message: string
}
interface Batch {
  cursor: number
  reset: boolean
  entries: Entry[]
}
const props = defineProps<{ selected: string }>()
const emit = defineEmits<{ expired: [] }>()
const entries = ref<Entry[]>([])
const level = ref<Level | ''>('')
const query = ref('')
const currentOnly = ref(false)
const following = ref(true)
const connection = ref<'connecting' | 'live' | 'retrying'>('connecting')
const viewport = ref<HTMLElement>()
let source: EventSource | undefined
let checkingSession = false
const labels: Record<Level, string> = {
  Error: '错误',
  Warn: '警告',
  Info: '信息',
  Debug: '调试',
  Trace: '追踪',
}
const visible = computed(() => {
  const search = query.value.toLocaleLowerCase()
  return entries.value.filter(
    (entry) =>
      (!level.value || entry.level === level.value) &&
      (!currentOnly.value || entry.source === props.selected) &&
      (!search || `${entry.source} ${entry.message}`.toLocaleLowerCase().includes(search)),
  )
})
const clock = new Intl.DateTimeFormat('zh-CN', {
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
})
function timestamp(value: number) {
  return `${clock.format(value)}.${String(value % 1000).padStart(3, '0')}`
}
function scrolled() {
  const element = viewport.value
  if (element)
    following.value = element.scrollHeight - element.scrollTop - element.clientHeight < 24
}
async function scrollToLatest() {
  await nextTick()
  if (following.value && viewport.value) viewport.value.scrollTop = viewport.value.scrollHeight
}
watch([visible, following], scrollToLatest, { flush: 'post' })
onMounted(() => {
  source = new EventSource('/api/inspect/logs')
  source.addEventListener('logs', (event: MessageEvent<string>) => {
    const batch = JSON.parse(event.data) as Batch
    entries.value = (batch.reset ? batch.entries : entries.value.concat(batch.entries)).slice(-2000)
    connection.value = 'live'
  })
  source.onerror = async () => {
    connection.value = 'retrying'
    if (checkingSession) return
    checkingSession = true
    try {
      await request<void>('/api/session')
    } catch (error) {
      if (error instanceof HttpError && error.status === 401) emit('expired')
    } finally {
      checkingSession = false
    }
  }
})
onUnmounted(() => source?.close())
</script>

<template>
  <section class="log-panel">
    <div class="log-toolbar">
      <input v-model="query" aria-label="搜索日志" placeholder="搜索日志内容或来源…" />
      <select v-model="level" aria-label="日志级别">
        <option value="">全部级别</option>
        <option v-for="(label, value) in labels" :key="value" :value="value">{{ label }}</option>
      </select>
      <label>
        <input v-model="currentOnly" type="checkbox" />
        仅当前对象
      </label>
      <label>
        <input v-model="following" type="checkbox" />
        自动滚动
      </label>
      <span class="log-connection" :class="connection">
        {{ { connecting: '连接中', live: '实时日志', retrying: '正在重连' }[connection] }}
      </span>
    </div>
    <div ref="viewport" class="log-viewport scroll-area" aria-label="运行日志" @scroll="scrolled">
      <div
        v-for="entry in visible"
        :key="entry.sequence"
        class="log-entry"
        :class="entry.level.toLowerCase()"
      >
        <time :title="new Date(entry.timestamp_ms).toLocaleString()">
          {{ timestamp(entry.timestamp_ms) }}
        </time>
        <span class="log-level">{{ labels[entry.level] }}</span>
        <code class="log-source">{{ entry.source }}</code>
        <pre>{{ entry.message }}</pre>
      </div>
      <p v-if="!visible.length" class="muted inset">
        {{ entries.length ? '没有匹配的日志。' : '暂无日志。' }}
      </p>
    </div>
    <footer class="log-footer">
      <span>{{ visible.length }} / {{ entries.length }} 条</span>
      <button v-if="!following" @click="following = true">回到最新</button>
    </footer>
  </section>
</template>

<style scoped>
.log-panel {
  display: flex;
  flex: 1;
  flex-direction: column;
  min-height: 0;
  min-width: 0;
}
.log-toolbar {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 12px;
  padding: 14px 20px;
  border-bottom: 1px solid var(--line);
}
.log-toolbar > input {
  flex: 1;
  min-width: 160px;
}
.log-toolbar label {
  display: flex;
  align-items: center;
  gap: 5px;
  white-space: nowrap;
  color: var(--muted);
  font-size: 12px;
}
.log-connection {
  font-size: 11px;
  color: var(--muted);
}
.log-connection.live {
  color: #328368;
}
.log-viewport {
  flex: 1;
  padding: 8px 12px;
  overscroll-behavior: contain;
  overflow-anchor: none;
}
.log-entry {
  display: grid;
  grid-template-columns: 92px 36px minmax(120px, 190px) minmax(0, 1fr);
  align-items: baseline;
  gap: 10px;
  padding: 5px 8px;
  border-bottom: 1px solid #f1f3f6;
  line-height: 1.6;
}
.log-entry time {
  font-family: Consolas, monospace;
  font-size: 11px;
  color: var(--muted);
}
.log-level {
  color: #64728a;
  font-size: 11px;
}
.log-source {
  color: #71819a;
  overflow-wrap: anywhere;
  font-size: 11px;
}
.log-entry pre {
  line-height: 1.6;
  font-size: 12px;
}
.log-entry.error {
  background: #fff5f5;
}
.log-entry.error .log-level {
  color: #bd4149;
}
.log-entry.warn .log-level {
  color: #a36f23;
}
.log-entry.debug,
.log-entry.trace {
  color: #8490a1;
}
.log-footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  min-height: 38px;
  padding: 5px 20px;
  border-top: 1px solid var(--line);
  color: var(--muted);
  font-size: 11px;
}
.log-footer button {
  padding: 3px 8px;
  font-size: 11px;
}
@media (max-width: 1050px) {
  .log-entry {
    grid-template-columns: 86px 30px minmax(0, 1fr);
  }
  .log-source {
    grid-column: 3;
  }
  .log-entry pre {
    grid-column: 3;
  }
}
</style>
