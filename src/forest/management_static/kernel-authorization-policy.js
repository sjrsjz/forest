import {randomId} from './random-id.js';

const PROTOCOL = 'forest.web-operation-policy/1';

function splitSubjects(value) {
  const items = value.split(',').map(item => item.trim()).filter(Boolean);
  return items.length ? [...new Set(items)] : null;
}

function addRow(rows, template, rule = {}) {
  const row = template.content.firstElementChild.cloneNode(true);
  row.querySelector('[data-field="enabled"]').checked = rule.enabled ?? true;
  row.querySelector('[data-field="component"]').value = rule.target_component_id ?? '';
  row.querySelector('[data-field="action"]').value = rule.action_key ?? '';
  row.querySelector('[data-field="roles"]').value = (rule.allowed_roles ?? ['ADMIN']).join(',');
  row.querySelector('[data-field="subjects"]').value = (rule.allowed_subject_refs ?? []).join(', ');
  row.querySelector('[data-action="remove"]').onclick = () => row.remove();
  rows.append(row);
}

function collect(rows) {
  const rules = [...rows.querySelectorAll('tr')].map(row => ({
    target_component_id: row.querySelector('[data-field="component"]').value.trim(),
    action_key: row.querySelector('[data-field="action"]').value.trim(),
    enabled: row.querySelector('[data-field="enabled"]').checked,
    allowed_roles: row.querySelector('[data-field="roles"]').value.split(','),
    allowed_subject_refs: splitSubjects(row.querySelector('[data-field="subjects"]').value),
  }));
  if (rules.some(rule => !rule.target_component_id || !rule.action_key)) {
    throw new Error('目标组件和动作不能为空。');
  }
  const identities = rules.map(rule => `${rule.target_component_id}\u0000${rule.action_key}`);
  if (new Set(identities).size !== identities.length) {
    throw new Error('目标组件与动作组合不能重复。');
  }
  return {protocol: PROTOCOL, rules};
}

export async function mount({root, component, api, notice, refresh}) {
  const base = `/management/v1/modules/${encodeURIComponent(component.component_id)}/configuration`;
  const rows = root.querySelector('#policy-rows');
  const template = root.querySelector('#policy-row-template');
  const current = await api(base);
  root.querySelector('#policy-revision').textContent = `期望 r${current.desired.revision} · 生效 r${current.effective.revision}`;
  for (const rule of current.desired.value.rules) addRow(rows, template, rule);
  root.querySelector('#policy-add').onclick = () => addRow(rows, template);
  root.querySelector('#policy-save').onclick = async event => {
    const button = event.currentTarget;
    button.disabled = true;
    notice('');
    try {
      const value = collect(rows);
      const key = randomId();
      const plan = await api(`${base}/plans`, {
        method: 'POST',
        headers: {'idempotency-key': `${key}:plan`},
        body: JSON.stringify({expected_desired_revision: current.desired.revision, value}),
      });
      const applied = await api(`${base}/apply`, {
        method: 'POST',
        headers: {'idempotency-key': `${key}:apply`},
        body: JSON.stringify({plan_id: plan.plan_id, expected_desired_revision: current.desired.revision}),
      });
      notice(`策略 r${applied.configuration.revision} 已提交；重建请求 ${applied.rebuild.request_id} 正在处理。`);
      await refresh();
    } catch (error) {
      notice(error.message);
    } finally {
      button.disabled = false;
    }
  };
}
