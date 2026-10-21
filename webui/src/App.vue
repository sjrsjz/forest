<script setup lang="ts">
import { computed, onMounted, ref, shallowRef, watch } from 'vue'
import { HttpError, request } from './api'
import type { Phase, Snapshot } from './types'
import { phaseLabel, time } from './types'
import ModuleOverview from './components/ModuleOverview.vue'
import DependencyGraph from './components/DependencyGraph.vue'
import ConfigPanel from './components/ConfigPanel.vue'
import ModuleManagement from './components/ModuleManagement.vue'

const session = ref<'checking' | 'out' | 'in'>('checking')
const password = ref('')
const loginError = ref('')
const loginBusy = ref(false)
const snapshot = shallowRef<Snapshot | null>(null)
const connection = ref<'connecting' | 'live' | 'retrying'>('connecting')
const search = ref('')
const phase = ref<Phase | ''>('')
const selected = ref('')
const tab = ref<'status' | 'graph' | 'config'>('status')
const managementOpen = ref(false)
const managementIntent = ref<'load' | 'reload' | 'unload'>('load')
const managementTarget = ref('')
function manage(intent: 'load' | 'reload' | 'unload') {
  managementIntent.value = intent
  managementTarget.value = intent === 'load' ? '' : selected.value
  managementOpen.value = true
}
function acceptSnapshot(next: Snapshot) {
  snapshot.value = next
  if (
    !next.modules.some((m) => m.object === selected.value) &&
    !next.hosts.some((h) => h.object === selected.value)
  )
    selected.value = next.modules[0]?.object ?? next.hosts[0]?.object ?? ''
}
async function managed(object: string) {
  try {
    const next = await request<Snapshot>('/api/inspect/snapshot')
    if (object) selected.value = object
    acceptSnapshot(next)
  } catch (e) {
    if (e instanceof HttpError && e.status === 401) session.value = 'out'
  }
}
const selectedModule = computed(() =>
  snapshot.value?.modules.find((m) => m.object === selected.value),
)
const selectedHost = computed(() => snapshot.value?.hosts.find((h) => h.object === selected.value))
const modules = computed(
  () =>
    snapshot.value?.modules.filter(
      (m) =>
        (!phase.value || m.lifecycle.phase === phase.value) &&
        `${m.object} ${m.published.name ?? ''}`
          .toLocaleLowerCase()
          .includes(search.value.toLocaleLowerCase()),
    ) ?? [],
)
const hosts = computed(
  () =>
    snapshot.value?.hosts.filter((h) =>
      h.object.toLocaleLowerCase().includes(search.value.toLocaleLowerCase()),
    ) ?? [],
)
const title = computed(
  () => selectedModule.value?.published.name || selected.value || '框架运行状态',
)
const connectionText = computed(
  () => ({ connecting: '连接中', live: '实时更新', retrying: '正在重连' })[connection.value],
)
async function checkSession() {
  try {
    await request<void>('/api/session')
    session.value = 'in'
  } catch (e) {
    session.value = 'out'
    if (!(e instanceof HttpError && e.status === 401))
      loginError.value = '暂时无法连接宿主，请稍后重试。'
  }
}
async function login() {
  loginBusy.value = true
  loginError.value = ''
  try {
    await request<void>('/api/session', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ password: password.value }),
    })
    password.value = ''
    session.value = 'in'
  } catch (e) {
    loginError.value = e instanceof Error ? e.message : String(e)
  } finally {
    loginBusy.value = false
  }
}
async function logout() {
  try {
    await request<void>('/api/session', { method: 'DELETE' })
    session.value = 'out'
  } catch (e) {
    loginError.value = e instanceof Error ? e.message : String(e)
  }
}
watch(session, (state, _, cleanup) => {
  if (state !== 'in') {
    snapshot.value = null
    managementOpen.value = false
    return
  }
  connection.value = 'connecting'
  const source = new EventSource('/api/inspect/events')
  let verifying = false
  source.addEventListener('snapshot', (event: MessageEvent<string>) => {
    acceptSnapshot(JSON.parse(event.data) as Snapshot)
    connection.value = 'live'
  })
  source.onerror = async () => {
    connection.value = 'retrying'
    if (verifying) return
    verifying = true
    try {
      await request<void>('/api/session')
    } catch (e) {
      if (e instanceof HttpError && e.status === 401) session.value = 'out'
    } finally {
      verifying = false
    }
  }
  cleanup(() => source.close())
})
onMounted(checkSession)
</script>

<template>
  <div v-if="session !== 'in'" class="login-screen">
    <form class="login-box" @submit.prevent="login">
      <div class="brand login-brand">
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M12 3v18M12 8 6 4M12 12l7-5M12 17l-7-5" />
        </svg>
        <span>FOREST</span>
      </div>
      <p class="eyebrow">宿主调试台</p>
      <h1>登录调试台</h1>
      <label class="login-field">
        管理口令
        <input
          v-model="password"
          type="password"
          autocomplete="current-password"
          autofocus
          required
          placeholder="输入管理口令"
          :disabled="session === 'checking'"
        />
      </label>
      <p v-if="loginError" class="login-error" role="alert">{{ loginError }}</p>
      <button class="primary login-submit" :disabled="loginBusy || session === 'checking'">
        {{ session === 'checking' ? '正在连接…' : loginBusy ? '正在登录…' : '进入调试台' }}
      </button>
    </form>
  </div>
  <div v-else class="console-shell">
    <header class="topbar">
      <div class="brand">
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M12 3v18M12 8 6 4M12 12l7-5M12 17l-7-5" />
        </svg>
        <strong>FOREST</strong>
        <span class="brand-subtitle">宿主调试台</span>
      </div>
      <div class="topbar-status">
        <span class="connection" :class="connection">
          <i />
          {{ connectionText }}
        </span>
        <span v-if="snapshot" class="sample-time">{{ time(snapshot.sampled_at_ms) }}</span>
        <button class="text-button" @click="logout">退出</button>
      </div>
    </header>
    <aside class="sidebar">
      <div class="sidebar-search">
        <input v-model="search" aria-label="搜索模块" placeholder="搜索名称或 Token…" />
        <select v-model="phase" aria-label="生命周期筛选">
          <option value="">全部生命周期</option>
          <option v-for="(label, key) in phaseLabel" :key="key" :value="key">{{ label }}</option>
        </select>
        <button class="package-open" @click="manage('load')">本地包</button>
      </div>
      <div class="sidebar-objects scroll-area">
        <div class="sidebar-heading">
          WASM 模块
          <span>{{ snapshot?.modules.length ?? 0 }}</span>
        </div>
        <button
          v-for="m in modules"
          :key="m.object"
          class="object-button"
          :class="{ selected: selected === m.object }"
          @click="selected = m.object"
        >
          <div>
            <strong>{{ m.published.name || m.object.split('/').at(-1) }}</strong>
            <span
              class="phase-dot"
              :class="m.lifecycle.phase"
              :title="phaseLabel[m.lifecycle.phase]"
            />
          </div>
          <code>{{ m.object }}</code>
          <small>
            {{ phaseLabel[m.lifecycle.phase] }}
            <span>v{{ m.published.version }}</span>
          </small>
        </button>
        <p v-if="snapshot && !modules.length" class="muted inset">没有匹配的模块。</p>
        <details class="host-list" open>
          <summary class="sidebar-heading">
            宿主对象
            <span>{{ snapshot?.hosts.length ?? 0 }}</span>
          </summary>
          <button
            v-for="h in hosts"
            :key="h.object"
            class="object-button host-object"
            :class="{ selected: selected === h.object }"
            @click="selected = h.object"
          >
            <strong>{{ h.object.split('/').at(-1) }}</strong>
            <code>{{ h.object }}</code>
            <small>原生接口 / 事件源</small>
          </button>
        </details>
      </div>
      <footer class="sidebar-footer">
        <span v-if="snapshot">并行推进 {{ snapshot.parallelism }}</span>
      </footer>
    </aside>
    <main class="workspace">
      <template v-if="snapshot">
        <div class="workspace-heading">
          <div>
            <p class="eyebrow">{{ selectedHost ? 'HOST OBJECT' : 'WASM MODULE' }}</p>
            <h1>{{ title }}</h1>
            <code class="object-path">{{ selected || '尚无登记对象' }}</code>
          </div>
          <div v-if="selectedModule" class="module-actions">
            <span class="phase-badge" :class="selectedModule.lifecycle.phase">
              {{ phaseLabel[selectedModule.lifecycle.phase] }}
            </span>
            <button
              :disabled="selectedModule.lifecycle.phase !== 'serving'"
              @click="manage('reload')"
            >
              重载
            </button>
            <button @click="manage('unload')">
              {{ ['serving', 'failed'].includes(selectedModule.lifecycle.phase) ? '卸载' : '管理' }}
            </button>
          </div>
          <span v-else-if="selectedHost" class="phase-badge registered">已注册</span>
        </div>
        <nav class="tabs" aria-label="对象视图">
          <button :class="{ active: tab === 'status' }" @click="tab = 'status'">运行状态</button>
          <button :class="{ active: tab === 'graph' }" @click="tab = 'graph'">依赖关系</button>
          <button :class="{ active: tab === 'config' }" @click="tab = 'config'">配置表</button>
          <span class="tabs-tail">
            {{ snapshot.modules.length }} 个模块 · {{ snapshot.hosts.length }} 个宿主对象
          </span>
        </nav>
        <div class="workspace-content">
          <ModuleOverview
            v-if="tab === 'status'"
            :module="selectedModule"
            :host="selectedHost"
            :snapshot="snapshot"
          />
          <DependencyGraph v-else-if="tab === 'graph'" :snapshot="snapshot" :selected="selected" />
          <ConfigPanel
            v-else
            :snapshot="snapshot"
            :selected="selected"
            @expired="session = 'out'"
          />
        </div>
      </template>
      <div v-else class="empty-state">
        <span class="loading-mark" />
        <strong>等待宿主快照</strong>
      </div>
    </main>
    <ModuleManagement
      v-if="snapshot"
      :open="managementOpen"
      :intent="managementIntent"
      :target="managementTarget"
      :snapshot="snapshot"
      @close="managementOpen = false"
      @updated="managed"
      @expired="session = 'out'"
    />
  </div>
</template>
