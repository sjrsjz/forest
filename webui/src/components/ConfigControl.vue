<script setup lang="ts">
import { ref, toRaw, useId } from 'vue'
import type { ConfigForm } from '../config-form'

const props = withDefaults(
  defineProps<{
    control: ConfigForm
    label: string
    disabled: boolean
    hideLabel?: boolean
  }>(),
  { hideLabel: false },
)
const id = useId()
const multiline = ref(props.control.kind === 'string' && props.control.value.includes('\n'))
function add(list: Extract<ConfigForm, { kind: 'list' }>) {
  list.items.push(structuredClone(toRaw(list.template)))
}
</script>

<template>
  <fieldset v-if="control.kind === 'record'" class="config-fields" :disabled="disabled">
    <legend v-if="label && !hideLabel">{{ label }}</legend>
    <ConfigControl
      v-for="field in control.fields"
      :key="field.name"
      :control="field.control"
      :label="field.name"
      :disabled="disabled"
    />
  </fieldset>
  <div v-else-if="control.kind === 'tuple'" class="config-tuple">
    <ConfigControl
      v-for="(item, index) in control.items"
      :key="index"
      :control="item"
      :label="control.items.length === 1 ? label : `参数 ${index + 1}`"
      :hide-label="control.items.length === 1"
      :disabled="disabled"
    />
  </div>
  <div v-else class="config-field" :class="{ 'is-unlabeled': hideLabel || !label }">
    <label
      v-if="label && !hideLabel"
      class="config-field-label"
      :for="control.kind === 'list' ? undefined : id"
    >
      {{ label }}
    </label>
    <div class="config-field-body" :class="{ 'is-optional': control.kind === 'optional' }">
      <template v-if="control.kind === 'optional'">
        <label class="config-checkbox">
          <input
            :id="id"
            v-model="control.enabled"
            type="checkbox"
            :disabled="disabled"
            :aria-label="`${label} · 自定义`"
          />
          自定义
        </label>
        <ConfigControl
          v-if="control.enabled"
          :control="control.inner"
          :label="label"
          hide-label
          :disabled="disabled"
        />
      </template>
      <template v-else-if="control.kind === 'variant'">
        <select :id="id" v-model="control.selected" :disabled="disabled" :aria-label="label">
          <option v-for="item in control.cases" :key="item.name" :value="item.name">
            {{ item.name }}
          </option>
        </select>
        <template v-for="item in control.cases" :key="item.name">
          <ConfigControl
            v-if="item.name === control.selected"
            :control="item.payload"
            :label="label"
            hide-label
            :disabled="disabled"
          />
        </template>
      </template>
      <template v-else-if="control.kind === 'list'">
        <div class="config-list" :aria-label="label">
          <div v-for="(item, index) in control.items" :key="index" class="config-list-row">
            <span class="config-list-index">{{ index + 1 }}</span>
            <ConfigControl
              :control="item"
              :label="`${label} ${index + 1}`"
              hide-label
              :disabled="disabled"
            />
            <button
              type="button"
              class="text-button"
              :aria-label="`删除 ${label} ${index + 1}`"
              :disabled="disabled"
              @click="control.items.splice(index, 1)"
            >
              移除
            </button>
          </div>
          <button type="button" class="config-list-add" :disabled="disabled" @click="add(control)">
            添加一项
          </button>
        </div>
      </template>
      <label v-else-if="control.kind === 'bool'" class="config-checkbox">
        <input
          :id="id"
          v-model="control.value"
          type="checkbox"
          :disabled="disabled"
          :aria-label="label"
        />
        {{ control.value ? '开启' : '关闭' }}
      </label>
      <template v-else-if="control.kind === 'string'">
        <textarea
          v-if="multiline"
          :id="id"
          v-model="control.value"
          rows="6"
          :aria-label="label || '当前值'"
          :disabled="disabled"
          autocomplete="off"
          spellcheck="false"
        />
        <input
          v-else
          :id="id"
          v-model="control.value"
          type="text"
          :aria-label="label || '当前值'"
          :disabled="disabled"
          autocomplete="off"
          spellcheck="false"
        />
        <button
          v-if="!multiline"
          type="button"
          class="text-button multiline-toggle"
          :aria-label="`多行编辑 ${label || '当前值'}`"
          title="多行编辑"
          :disabled="disabled"
          @click="multiline = true"
        >
          ↵
        </button>
      </template>
      <textarea
        v-else-if="control.kind === 'text'"
        :id="id"
        v-model="control.value"
        rows="4"
        :aria-label="label || '当前值'"
        :disabled="disabled"
        autocomplete="off"
        spellcheck="false"
      />
      <input
        v-else
        :id="id"
        v-model="control.value"
        type="text"
        inputmode="numeric"
        pattern="-?[0-9]+"
        :aria-label="label || '当前值'"
        :disabled="disabled"
        autocomplete="off"
      />
    </div>
  </div>
</template>
