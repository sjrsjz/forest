// Erased configuration is a distinct state, never an implicit default value.
export function clearConfiguration(form) {
  form.reset();
  for (const field of form.elements) {
    if (field.type === 'checkbox' || field.type === 'radio') field.checked = false;
    else if (field.tagName === 'SELECT') field.selectedIndex = -1;
    else if (['INPUT','TEXTAREA'].includes(field.tagName)) field.value = '';
  }
}

export function replacementButton(status, id, start) {
  const button = document.createElement('button');
  button.type = 'button'; button.id = id; button.className = 'subtle hidden';
  button.textContent = '填写新配置'; button.addEventListener('click', start);
  status.after(button);
  return button;
}
