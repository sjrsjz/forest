<script setup lang="ts">
import { onUnmounted, ref } from 'vue'
import { HttpError, request } from '../api'
import { formText, type ConfigForm } from '../config-form'
import ConfigControl from './ConfigControl.vue'

const props = defineProps<{ object: string; keyPath: string }>()
const emit = defineEmits<{ saved: []; close: []; expired: [] }>()
const draft = ref('')
const form = ref<ConfigForm | null>(null)
const textMode = ref(false)
const loading = ref(true)
const saving = ref(false)
const error = ref('')
const controller = new AbortController()

function failure(e: unknown) {
  if (e instanceof HttpError && e.status === 401) emit('expired')
  error.value = e instanceof Error ? e.message : String(e)
}
async function load() {
  try {
    form.value = await request<ConfigForm>(
      `/api/config/form?${new URLSearchParams({ object: props.object, key: props.keyPath })}`,
      { signal: controller.signal },
    )
  } catch (e) {
    if (!controller.signal.aborted) failure(e)
  } finally {
    loading.value = false
  }
}
function editText() {
  draft.value = formText(form.value!)
  textMode.value = true
}
async function save() {
  saving.value = true
  error.value = ''
  try {
    await request<void>('/api/config/set', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        object: props.object,
        key: props.keyPath,
        value: textMode.value ? draft.value : formText(form.value!),
      }),
    })
    emit('saved')
  } catch (e) {
    failure(e)
  } finally {
    saving.value = false
  }
}
void load()
onUnmounted(() => controller.abort())
</script>

<template>
  <form class="config-editor" @submit.prevent="save">
    <div v-if="error" class="notice danger" role="alert">{{ error }}</div>
    <ConfigControl
      v-if="form && !textMode"
      :control="form"
      :label="''"
      hide-label
      :disabled="saving"
    />
    <label v-if="textMode" for="config-draft">当前值 · Forest 文本</label>
    <textarea
      v-if="textMode"
      id="config-draft"
      v-model="draft"
      :disabled="loading || saving"
      spellcheck="false"
      aria-label="配置值"
    />
    <div class="button-group editor-actions">
      <button type="submit" class="primary" :disabled="!form || loading || saving">
        {{ saving ? '正在保存…' : '保存' }}
      </button>
      <button type="button" :disabled="saving" @click="emit('close')">取消</button>
      <button v-if="form && !textMode" type="button" :disabled="saving" @click="editText">
        Forest 文本
      </button>
    </div>
  </form>
</template>
