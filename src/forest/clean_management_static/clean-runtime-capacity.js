const PROTOCOL = 'forest.kernel-resource-policy/1';
const FIELDS = [
  'max_workers', 'max_frame_bytes', 'max_inflight', 'max_queue_items',
  'max_state_inflight', 'max_owned_tasks', 'max_owned_resources',
];

export async function mount({root, component, api, notice, refresh}) {
  const base = `/management/v1/modules/${encodeURIComponent(component.component_id)}/configuration`;
  const current = await api(base);
  const form = root.querySelector('#runtime-capacity-form');
  root.querySelector('#runtime-capacity-revision').textContent =
    `期望 r${current.desired.revision} · 生效 r${current.effective.revision}`;
  for (const field of FIELDS) form.elements.namedItem(field).value = current.desired.value.worker_budget[field];
  root.querySelector('#runtime-capacity-save').onclick = async event => {
    const button = event.currentTarget;
    if (!form.reportValidity()) return;
    button.disabled = true;
    notice('');
    try {
      const worker_budget = Object.fromEntries(FIELDS.map(field => {
        const value = Number(form.elements.namedItem(field).value);
        if (!Number.isSafeInteger(value) || value < 1) throw new Error(`${field} 必须为正整数`);
        return [field, value];
      }));
      const key = crypto.randomUUID();
      const plan = await api(`${base}/plans`, {
        method: 'POST', headers: {'idempotency-key': `${key}:plan`},
        body: JSON.stringify({expected_desired_revision: current.desired.revision,
          value: {protocol: PROTOCOL, worker_budget}}),
      });
      const applied = await api(`${base}/apply`, {
        method: 'POST', headers: {'idempotency-key': `${key}:apply`},
        body: JSON.stringify({plan_id: plan.plan_id,
          expected_desired_revision: current.desired.revision}),
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
