<script setup lang="ts">
import type { ValueTree } from '../types'
withDefaults(defineProps<{ value: ValueTree; depth?: number }>(), { depth: 0 })
</script>

<template>
  <div v-if="value.kind === 'scalar'" class="value-scalar">
    <span class="type-label">{{ value.type_name }}</span>
    <pre>{{ value.text }}</pre>
  </div>
  <div v-else-if="value.kind === 'bytes'" class="value-scalar">
    <span class="type-label">bytes · {{ value.length }} B</span>
    <code>{{ value.preview_hex || '∅' }}{{ value.length > 32 ? ' …' : '' }}</code>
  </div>
  <div v-else-if="value.kind === 'omitted'" class="muted">… 更多内容见完整值下载</div>
  <details v-else-if="value.kind === 'sequence'" :open="depth < 2" class="value-branch">
    <summary>
      <span class="type-label">{{ value.type_name }}</span>
      {{ value.length }} 项
    </summary>
    <div v-for="(item, index) in value.items" :key="index" class="value-child">
      <span class="value-key">{{ index }}</span>
      <ValueTree :value="item" :depth="depth + 1" />
    </div>
    <p v-if="value.truncated" class="muted">… 已省略其余项</p>
  </details>
  <details v-else-if="value.kind === 'record'" :open="depth < 2" class="value-branch">
    <summary>
      <span class="type-label">record</span>
      {{ value.fields.length }} 个字段
    </summary>
    <div v-for="field in value.fields" :key="field.name" class="value-child">
      <code class="value-key">{{ field.name }}</code>
      <ValueTree :value="field.value" :depth="depth + 1" />
    </div>
    <p v-if="value.truncated" class="muted">… 已省略其余字段</p>
  </details>
  <details v-else-if="value.kind === 'map'" :open="depth < 2" class="value-branch">
    <summary>
      <span class="type-label">map</span>
      {{ value.length }} 对
    </summary>
    <div v-for="(pair, index) in value.entries" :key="index" class="value-child">
      <div>
        <span class="type-label">键</span>
        <ValueTree :value="pair.key" :depth="depth + 1" />
      </div>
      <div>
        <span class="type-label">值</span>
        <ValueTree :value="pair.value" :depth="depth + 1" />
      </div>
    </div>
    <p v-if="value.truncated" class="muted">… 已省略其余键值对</p>
  </details>
  <details v-else :open="depth < 2" class="value-branch">
    <summary>
      <span class="type-label">variant</span>
      {{ value.tag }}
    </summary>
    <ValueTree :value="value.body" :depth="depth + 1" />
  </details>
</template>
