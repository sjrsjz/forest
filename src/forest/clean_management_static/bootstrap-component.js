function titleFor(name) {
  return name.replaceAll('_', ' ').replace(/\b\w/g, value => value.toUpperCase());
}

function scalarEditor(schema, value, name) {
  const input = document.createElement(schema.enum ? 'select' : 'input');
  input.name = name;
  if (schema.enum) {
    for (const choice of schema.enum) {
      const option = document.createElement('option');
      option.value = choice;
      option.textContent = choice;
      input.append(option);
    }
    input.value = value;
  } else {
    input.type = ['integer', 'number'].includes(schema.type) ? 'number' : 'text';
    input.value = value ?? '';
    if (schema.minimum !== undefined) input.min = schema.minimum;
    if (schema.maximum !== undefined) input.max = schema.maximum;
  }
  return {
    element: input,
    read: () => {
      if (schema.type === 'integer') return Number.parseInt(input.value, 10);
      if (schema.type === 'number') return Number(input.value);
      return input.value;
    },
  };
}

function editorFor(schema, value, name) {
  if (schema.const !== undefined) {
    const code = document.createElement('code');
    code.textContent = String(schema.const);
    return {element: code, read: () => schema.const};
  }
  if (schema.type === 'boolean') {
    const input = document.createElement('input');
    input.type = 'checkbox';
    input.name = name;
    input.checked = Boolean(value);
    return {element: input, read: () => input.checked};
  }
  if (schema.type === 'array') {
    const input = document.createElement('textarea');
    input.name = name;
    input.rows = 3;
    input.value = JSON.stringify(Array.isArray(value) ? value : [], null, 2);
    return {
      element: input,
      read: () => JSON.parse(input.value),
    };
  }
  if (schema.type === 'object') {
    if (!schema.properties) {
      const input = document.createElement('textarea');
      input.name = name;
      input.rows = 6;
      input.value = JSON.stringify(value ?? {}, null, 2);
      return {element: input, read: () => JSON.parse(input.value)};
    }
    const fieldset = document.createElement('fieldset');
    const readers = new Map();
    for (const [property, childSchema] of Object.entries(schema.properties)) {
      const row = document.createElement('label');
      row.className = 'bootstrap-field';
      const caption = document.createElement('span');
      caption.textContent = titleFor(property);
      const child = editorFor(childSchema, value?.[property], `${name}.${property}`);
      row.append(caption, child.element);
      fieldset.append(row);
      readers.set(property, child.read);
    }
    return {
      element: fieldset,
      read: () => Object.fromEntries([...readers].map(([key, read]) => [key, read()])),
    };
  }
  return scalarEditor(schema, value, name);
}

export async function mountBootstrap({root, contribution}) {
  root.querySelector('[data-label]').textContent = contribution.label;
  root.querySelector('[data-component]').textContent = contribution.component_id;
  root.querySelector('[data-description]').textContent = contribution.description;
  root.querySelector('[data-schema]').textContent = contribution.schema_digest;
  const editor = editorFor(
    contribution.configuration_schema,
    contribution.initial_value,
    contribution.component_id,
  );
  root.querySelector('[data-fields]').append(editor.element);
  return {read: editor.read};
}
