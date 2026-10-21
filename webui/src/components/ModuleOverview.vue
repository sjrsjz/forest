<script setup lang="ts">
import type { Module, Host, Snapshot } from '../types'
import { bytes, phaseLabel } from '../types'
defineProps<{ module?: Module; host?: Host; snapshot: Snapshot }>()
const modeLabel = { drain: '排空旧集合', at_yield: '主动边界停止', force: '强制中断' }
</script>

<template>
  <div class="overview scroll-area">
    <template v-if="module">
      <div v-if="module.lifecycle.phase === 'failed'" class="notice danger">
        <strong>模块已失败</strong>
        <pre>{{ module.lifecycle.reason }}</pre>
      </div>
      <div v-else-if="module.lifecycle.phase !== 'serving'" class="notice">
        <strong>{{ phaseLabel[module.lifecycle.phase] }}</strong>
        <span v-if="module.lifecycle.phase === 'stopping'">
          · {{ modeLabel[module.lifecycle.mode] }}
        </span>
        <span v-if="'replacement' in module.lifecycle && module.lifecycle.replacement">
          · 候选 {{ module.lifecycle.replacement.version }}
        </span>
      </div>
      <div class="stats-strip">
        <div>
          <span class="eyebrow">占用请求名额</span>
          <strong>
            {{ module.budget.requests_used }}
            <small>/ {{ module.budget.requests_max }}</small>
          </strong>
          <meter min="0" :max="module.budget.requests_max" :value="module.budget.requests_used" />
        </div>
        <div>
          <span class="eyebrow">当前接口排队</span>
          <strong>
            {{ module.interfaces.reduce((n, i) => n + i.queued, 0) }}
            <small>条</small>
          </strong>
        </div>
        <div>
          <span class="eyebrow">下一实例队列</span>
          <strong>
            {{ module.queued_next }}
            <small>条</small>
          </strong>
        </div>
      </div>
      <section class="detail-section">
        <h3>执行实例</h3>
        <dl class="properties">
          <div>
            <dt>发布标记</dt>
            <dd>
              <code>{{ module.published.version }}</code>
            </dd>
          </div>
          <template v-if="module.lifecycle.phase !== 'failed'">
            <div>
              <dt>实例编号</dt>
              <dd>
                <code>{{ module.lifecycle.instance.id }}</code>
              </dd>
            </div>
            <div>
              <dt>实例工件</dt>
              <dd>
                <code>{{ module.lifecycle.instance.release.version }}</code>
              </dd>
            </div>
          </template>
          <div>
            <dt>输入容量</dt>
            <dd>
              {{ bytes(module.budget.input_bytes_used) }} /
              {{ bytes(module.budget.input_bytes_max) }}
            </dd>
          </div>
          <div>
            <dt>Wasm 内存上限</dt>
            <dd>{{ bytes(snapshot.memory_limit_bytes) }}</dd>
          </div>
        </dl>
        <p v-if="module.published.description" class="muted">{{ module.published.description }}</p>
      </section>
      <section class="detail-section">
        <div class="section-title">
          <h3>提供接口</h3>
          <span class="count">{{ module.interfaces.length }}</span>
        </div>
        <table>
          <thead>
            <tr>
              <th>接口路径</th>
              <th>重入方式</th>
              <th class="number">排队</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="i in module.interfaces" :key="i.path">
              <td>
                <code>{{ i.path }}</code>
              </td>
              <td>{{ i.reentrant ? '可重入' : '独占至完成' }}</td>
              <td class="number">{{ i.queued }}</td>
            </tr>
            <tr v-if="!module.interfaces.length">
              <td colspan="3" class="muted">未声明接口</td>
            </tr>
          </tbody>
        </table>
      </section>
      <section class="detail-section">
        <div class="section-title">
          <h3>接口依赖</h3>
          <span class="count">{{ module.uses.length }}</span>
        </div>
        <table>
          <thead>
            <tr>
              <th>目标对象</th>
              <th>接口路径</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="u in module.uses" :key="`${u.object}:${u.interface}`">
              <td>
                <code>{{ u.object }}</code>
              </td>
              <td>
                <code>{{ u.interface }}</code>
              </td>
            </tr>
            <tr v-if="!module.uses.length">
              <td colspan="2" class="muted">未声明外部接口依赖</td>
            </tr>
          </tbody>
        </table>
      </section>
      <section class="detail-section">
        <div class="section-title">
          <h3>宿主事件接收</h3>
          <span class="count">{{ module.subscriptions.length }}</span>
        </div>
        <table>
          <thead>
            <tr>
              <th>来源 / 事件</th>
              <th>接收接口</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="s in module.subscriptions" :key="`${s.source}:${s.event}:${s.interface}`">
              <td>
                <code>{{ s.source }}</code>
                <small class="subline">{{ s.event }}</small>
              </td>
              <td>
                <code>{{ s.interface }}</code>
              </td>
            </tr>
            <tr v-if="!module.subscriptions.length">
              <td colspan="2" class="muted">未订阅宿主事件</td>
            </tr>
          </tbody>
        </table>
      </section>
    </template>
    <template v-else-if="host">
      <section class="detail-section">
        <h3>注册接口</h3>
        <table>
          <thead>
            <tr>
              <th>接口路径</th>
              <th>重入方式</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="i in host.interfaces" :key="i.path">
              <td>
                <code>{{ i.path }}</code>
              </td>
              <td>{{ i.reentrant ? '可重入' : '串行' }}</td>
            </tr>
            <tr v-if="!host.interfaces.length">
              <td colspan="2" class="muted">无注册接口</td>
            </tr>
          </tbody>
        </table>
      </section>
      <section class="detail-section">
        <h3>宿主事件</h3>
        <ul class="plain-list">
          <li v-for="e in host.events" :key="e">
            <code>{{ e }}</code>
          </li>
          <li v-if="!host.events.length" class="muted">无事件定义</li>
        </ul>
      </section>
    </template>
    <div v-else class="empty-state">
      <strong>选择一个对象</strong>
    </div>
  </div>
</template>
