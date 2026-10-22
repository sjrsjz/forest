export type ConfigForm =
  | { kind: 'string'; value: string }
  | { kind: 'bool'; value: boolean }
  | { kind: 'integer'; value: string; bits: 32 | 64 }
  | { kind: 'record'; fields: { name: string; control: ConfigForm }[] }
  | { kind: 'tuple'; items: ConfigForm[] }
  | { kind: 'list'; items: ConfigForm[]; template: ConfigForm }
  | { kind: 'optional'; enabled: boolean; inner: ConfigForm }
  | { kind: 'variant'; selected: string; cases: { name: string; payload: ConfigForm }[] }
  | { kind: 'text'; value: string }

// Render the existing Forest language, retaining integer width and tuple/record
// payloads. Neither JSON numbers nor a second configuration protocol are used.
export function formText(form: ConfigForm): string {
  switch (form.kind) {
    case 'string':
      return JSON.stringify(form.value)
    case 'bool':
      return String(form.value)
    case 'integer':
      return form.value + (form.bits === 64 ? 'i64' : 'i32')
    case 'record':
      return `{${form.fields.map((f) => `${JSON.stringify(f.name)}: ${formText(f.control)}`).join(', ')}}`
    case 'tuple':
      return `(${form.items.map(formText).join(', ')})`
    case 'list':
      return `[${form.items.map(formText).join(', ')}]`
    case 'optional':
      return form.enabled ? `some ${formText(form.inner)}` : 'null'
    case 'variant': {
      const selected = form.cases.find((c) => c.name === form.selected)!
      return `${JSON.stringify(form.selected)}::${formText(selected.payload)}`
    }
    case 'text':
      return form.value
  }
}
