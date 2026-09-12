// Render common closed object schemas; retain JSON editing for richer schemas.
// All labels and values are data, never HTML or executable schema expressions.
const aliases = {label: '结果显示名称', multiplier: '增加量倍率', increment: '本次增加量'};
const clone = (value) => JSON.parse(JSON.stringify(value));
export function defaults(schema) {
  if (schema.default !== undefined) return clone(schema.default);
  if (schema.enum?.length) return clone(schema.enum[0]);
  if (schema.type === 'object') return Object.fromEntries(Object.entries(schema.properties || {})
    .filter(([key, value]) => schema.required?.includes(key) || value.default !== undefined)
    .map(([key, value]) => [key, defaults(value)]));
  if (schema.type === 'array') return [];
  if (schema.type === 'boolean') return false;
  if (schema.type === 'null') return null;
  if (['integer', 'number'].includes(schema.type)) return schema.minimum ?? 0;
  return '';
}
export function schemaForm(container, schema, initial) {
  container.replaceChildren();
  const value = initial === undefined ? defaults(schema) : initial;
  const simple = schema.type === 'object' && schema.additionalProperties === false &&
    !['$ref', 'oneOf', 'anyOf', 'allOf', 'if', 'patternProperties', 'dependentSchemas'].some((k) => k in schema);
  if (!simple) {
    const label = document.createElement('label'); label.textContent = schema.title || '配置内容（JSON）';
    const input = document.createElement('textarea'); input.rows = 8; input.name = 'json';
    input.value = JSON.stringify(value, null, 2); input.required = true; label.append(input); container.append(label);
    return () => JSON.parse(input.value);
  }
  const readers = [];
  for (const [name, field] of Object.entries(schema.properties || {})) {
    const label = document.createElement('label'); label.textContent = field.title || aliases[name] || name;
    const required = schema.required?.includes(name);
    const original = value && Object.hasOwn(value, name) ? value[name] : defaults(field);
    let input, read;
    if (field.enum) {
      input = document.createElement('select');
      field.enum.forEach((entry, index) => { const option = document.createElement('option');
        option.value = String(index); option.textContent = typeof entry === 'string' ? entry : JSON.stringify(entry);
        input.append(option); });
      input.value = String(field.enum.findIndex((entry) => JSON.stringify(entry) === JSON.stringify(original)));
      read = () => clone(field.enum[Number(input.value)]);
    } else if (['string', 'integer', 'number', 'boolean'].includes(field.type) && !field.$ref) {
      input = document.createElement('input');
      input.type = field.type === 'boolean' ? 'checkbox' : field.type === 'string' ? 'text' : 'number';
      input.value = original ?? ''; input.checked = !!original;
      if (field.type === 'integer') input.step = 1;
      if (field.type === 'number') input.step = 'any';
      for (const [key, attribute] of [['minimum', 'min'], ['maximum', 'max'], ['minLength', 'minLength'], ['maxLength', 'maxLength']]) {
        if (field[key] !== undefined) input[attribute] = field[key];
      }
      read = () => field.type === 'boolean' ? input.checked : field.type === 'string' ? input.value : Number(input.value);
    } else {
      input = document.createElement('textarea'); input.rows = 4;
      input.value = JSON.stringify(original, null, 2); read = () => JSON.parse(input.value);
    }
    input.name = name; input.required = !!required && input.type !== 'checkbox'; label.append(input);
    let include;
    if (!required) {
      const toggle = document.createElement('label'); toggle.className = 'check';
      include = document.createElement('input'); include.type = 'checkbox';
      include.checked = !!value && Object.hasOwn(value, name); input.disabled = !include.checked;
      include.addEventListener('change', () => { input.disabled = !include.checked; });
      toggle.append(include, document.createTextNode(`设置 ${field.title || name}（可选）`)); container.append(toggle);
    }
    if (field.description) { const hint = document.createElement('span'); hint.className = 'hint';
      hint.textContent = field.description; label.append(hint); }
    container.append(label); readers.push(() => !include || include.checked ? [name, read()] : null);
  }
  return () => Object.fromEntries(readers.map((read) => read()).filter(Boolean));
}
