<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'

const props = defineProps<{
  storageKey: string
  label: string
  initial: number
  min: number
  max: number
  minMain: number
}>()
const container = ref<HTMLElement>()
const available = ref(Infinity)
const preferred = ref(readWidth())
const dragging = ref(false)
const maximum = computed(() =>
  Math.max(0, Math.min(props.max, available.value - props.minMain - 6)),
)
const minimum = computed(() => Math.min(props.min, maximum.value))
const width = computed(() => Math.min(maximum.value, Math.max(minimum.value, preferred.value)))
let observer: ResizeObserver
let startX = 0
let startWidth = 0

function readWidth() {
  try {
    const saved = localStorage.getItem(props.storageKey)
    const value = saved === null ? props.initial : Number(saved)
    return Number.isFinite(value) ? value : props.initial
  } catch {
    return props.initial
  }
}
function remember() {
  try {
    localStorage.setItem(props.storageKey, String(preferred.value))
  } catch {
    /* Optional browser preference. */
  }
}
function resize(value: number) {
  preferred.value = Math.min(maximum.value, Math.max(minimum.value, value))
}
function start(event: PointerEvent) {
  if (event.button !== 0) return
  startX = event.clientX
  startWidth = width.value
  dragging.value = true
  ;(event.currentTarget as HTMLElement).setPointerCapture(event.pointerId)
  event.preventDefault()
}
function move(event: PointerEvent) {
  if (dragging.value) resize(startWidth + event.clientX - startX)
}
function finish() {
  if (dragging.value) {
    dragging.value = false
    remember()
  }
}
function reset() {
  preferred.value = props.initial
  remember()
}
function keydown(event: KeyboardEvent) {
  const step = event.shiftKey ? 40 : 10
  switch (event.key) {
    case 'ArrowLeft':
      resize(width.value - step)
      break
    case 'ArrowRight':
      resize(width.value + step)
      break
    case 'Home':
      resize(minimum.value)
      break
    case 'End':
      resize(maximum.value)
      break
    default:
      return
  }
  event.preventDefault()
  remember()
}
onMounted(() => {
  available.value = container.value!.getBoundingClientRect().width
  observer = new ResizeObserver(([entry]) => {
    available.value = entry!.contentRect.width
  })
  observer.observe(container.value!)
})
onUnmounted(() => observer.disconnect())
</script>

<template>
  <div
    ref="container"
    class="split-pane"
    :class="{ 'is-resizing': dragging }"
    :style="{ gridTemplateColumns: `${width}px 6px minmax(0, 1fr)` }"
  >
    <div class="split-leading"><slot name="side" /></div>
    <div
      class="pane-divider"
      role="separator"
      tabindex="0"
      aria-orientation="vertical"
      :aria-label="label"
      :aria-valuenow="Math.round(width)"
      :aria-valuemin="Math.round(minimum)"
      :aria-valuemax="Math.round(maximum)"
      title="拖动调整宽度，双击恢复"
      @pointerdown="start"
      @pointermove="move"
      @pointerup="finish"
      @pointercancel="finish"
      @lostpointercapture="finish"
      @keydown="keydown"
      @dblclick="reset"
    />
    <div class="split-trailing"><slot /></div>
  </div>
</template>
