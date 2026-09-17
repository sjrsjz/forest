const $ = id => document.getElementById(id);

export function createModulePanel(api, busy, notice, {setBranding}) {
  let scope = null, configuration = null, pendingPlan = null, applyKey = null, readSeq = 0;
  const base = () => `/scopes/deployment/${encodeURIComponent(scope.scope_id)}/modules/deployment.configuration`;

  async function load() {
    if (!scope) return;
    const seq = ++readSeq;
    const value = await api(`${base()}/queries/configuration`);
    if (seq !== readSeq || !scope) return value;
    configuration = value;
    const values = value.effective?.values || value.defaults;
    const fields = $('config-form').elements;
    fields.display_name.value = values.display_name;
    fields.session_ttl_minutes.value = values.session_ttl_minutes;
    $('revision').textContent = value.head_revision;
    $('desired').textContent = value.desired?.revision || '默认';
    $('effective').textContent = value.effective?.revision || '默认';
    $('config-state').textContent = value.effective ? '已生效' : '默认配置';
    pendingPlan = null; applyKey = null; $('preview').classList.add('hidden');
    setBranding(values.display_name);
    return value;
  }

  $('config-form').addEventListener('input', () => {
    pendingPlan = null; applyKey = null; $('preview').classList.add('hidden');
  });
  $('config-form').addEventListener('submit', event => {
    event.preventDefault();
    busy(event.currentTarget.querySelector('button'), async () => {
      const fields = $('config-form').elements;
      pendingPlan = await api(`${base()}/actions/configuration.plan`, {
        expected_revision: configuration.head_revision,
        values: {display_name: fields.display_name.value,
          session_ttl_minutes: Number(fields.session_ttl_minutes.value)},
      }, {'Idempotency-Key': crypto.randomUUID()});
      applyKey = crypto.randomUUID(); $('diff').replaceChildren();
      for (const change of pendingPlan.diff) {
        const item = document.createElement('li');
        item.textContent = `${change.field === 'display_name' ? '显示名称' : '会话时长'}：${change.before} → ${change.after}`;
        $('diff').append(item);
      }
      $('effect').textContent = pendingPlan.effect;
      $('preview').classList.remove('hidden');
    });
  });
  $('apply').addEventListener('click', () => busy($('apply'), async () => {
    if (!pendingPlan) return;
    await api(`${base()}/actions/configuration.apply`, {
      plan_id: pendingPlan.plan_id, plan_digest: pendingPlan.plan_digest,
      expected_revision: pendingPlan.expected_revision,
    }, {'Idempotency-Key': applyKey});
    await load(); notice('配置已提交并生效。', true);
  }));
  $('reload').addEventListener('click', () => busy($('reload'), load));

  return {
    async select(row) { scope = row; await load(); },
    reset() {
      readSeq++; scope = null; configuration = null; pendingPlan = null; applyKey = null;
      $('config-form').reset(); $('diff').replaceChildren();
      $('preview').classList.add('hidden');
    },
    invalidateContent() { readSeq++; configuration = null; pendingPlan = null; },
  };
}
