<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import type { Snapshot } from '../types'
import { phaseLabel } from '../types'
const props = defineProps<{ snapshot: Snapshot; selected: string }>()
const svg = ref('')
const failure = ref('')
const busy = ref(false)
const zoom = ref(1)
let generation = 0
// Labels are encoded before entering Mermaid's grammar. Stable objects never
// become syntax identifiers, directives, HTML, or click handlers.
const label = (text: string) =>
  Array.from(text)
    .map((c) => `#${c.codePointAt(0)};`)
    .join('')
const definition = computed(() => {
  const nodes = new Map<string, { id: string; type: string; title: string }>()
  const add = (object: string, type: string, title: string) => {
    if (!nodes.has(object)) nodes.set(object, { id: `n${nodes.size}`, type, title })
  }
  for (const m of props.snapshot.modules)
    add(
      m.object,
      m.lifecycle.phase === 'failed' ? 'failed' : 'module',
      `${m.published.name || m.object}\n${m.object}\n${phaseLabel[m.lifecycle.phase]}`,
    )
  for (const h of props.snapshot.hosts) add(h.object, 'host', `${h.object}\n宿主对象`)
  const edges: string[] = []
  for (const m of props.snapshot.modules) {
    for (const use of m.uses) {
      add(use.object, 'missing', `${use.object}\n未登记`)
      edges.push(
        `${nodes.get(m.object)!.id} -->|"${label(use.interface)}"| ${nodes.get(use.object)!.id}`,
      )
    }
    for (const s of m.subscriptions) {
      add(s.source, 'missing', `${s.source}\n未登记`)
      edges.push(
        `${nodes.get(s.source)!.id} -.->|"${label(`${s.event} → ${s.interface}`)}"| ${nodes.get(m.object)!.id}`,
      )
    }
  }
  return [
    'flowchart TB',
    ...Array.from(
      nodes,
      ([object, n]) =>
        `${n.id}["${label(n.title)}"]:::${n.type === 'failed' ? 'failed' : object === props.selected ? 'selected' : n.type}`,
    ),
    ...edges,
    'classDef module fill:#fff,stroke:#93a3b8,color:#263449,stroke-width:1px',
    'classDef host fill:#edf1f6,stroke:#aab4c2,color:#526174',
    'classDef selected fill:#edf2ff,stroke:#4169db,color:#2345a2,stroke-width:2px',
    'classDef failed fill:#fff1f0,stroke:#d76b67,color:#a72f30',
    'classDef missing fill:#fff7e9,stroke:#c89138,color:#946621',
  ].join('\n')
})
watch(
  definition,
  async (source) => {
    const current = ++generation
    busy.value = true
    try {
      const { default: mermaid } = await import('mermaid')
      mermaid.initialize({
        startOnLoad: false,
        securityLevel: 'strict',
        theme: 'base',
        flowchart: {
          htmlLabels: true,
          useMaxWidth: false,
          curve: 'basis',
          rankSpacing: 150,
          nodeSpacing: 70,
          diagramPadding: 24,
        },
        themeVariables: {
          fontFamily: 'Segoe UI, Microsoft YaHei, sans-serif',
          fontSize: '13px',
          lineColor: '#94a3b8',
          primaryColor: '#fff',
          edgeLabelBackground: '#f7f9fc',
        },
      })
      const result = await mermaid.render(`forest_graph_${current}`, source)
      if (current === generation) {
        svg.value = result.svg
        failure.value = ''
      }
    } catch (error) {
      if (current === generation)
        failure.value = error instanceof Error ? error.message : String(error)
    } finally {
      if (current === generation) busy.value = false
    }
  },
  { immediate: true },
)
</script>

<template>
  <section class="graph-view">
    <div class="section-toolbar">
      <div>
        <strong>声明依赖图</strong>
        <p class="muted">实线：接口调用　虚线：事件接收</p>
      </div>
      <div class="button-group">
        <button aria-label="缩小" @click="zoom = Math.max(0.35, zoom - 0.15)">−</button>
        <button @click="zoom = 1">{{ Math.round(zoom * 100) }}%</button>
        <button aria-label="放大" @click="zoom = Math.min(2.5, zoom + 0.15)">＋</button>
      </div>
    </div>
    <div v-if="failure" class="notice danger" role="alert">{{ failure }}</div>
    <div class="graph-canvas">
      <span v-if="busy && !svg" class="muted">正在绘制关系图…</span>
      <div class="graph-svg" :style="{ zoom }" v-html="svg" />
    </div>
    <div class="graph-legend">
      <span>
        <i class="legend-dot module" />
        Wasm 模块
      </span>
      <span>
        <i class="legend-dot host" />
        宿主对象
      </span>
    </div>
  </section>
</template>
