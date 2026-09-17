const $ = id => document.getElementById(id);
const params = values => new URLSearchParams(values).toString();

export function createModulePanel(api, busy, notice) {
  let scope = null, epoch = 0, poll = null, stageKey = null;
  let activationPlan = null, activationRetry = null, activationKey = null;
  let eligible = [];
  const base = () => `/scopes/deployment/${encodeURIComponent(scope.scope_id)}/modules/deployment.restore`;

  function closeActivation() {
    activationPlan = activationRetry = null; activationKey = null;
    $('deployment-restore-activation-form').reset();
    $('deployment-restore-activation').classList.add('hidden');
  }

  async function openActivation(restoreId, target) {
    activationPlan = await api(`${base()}/actions/activation.plan`, {
      restore_id:restoreId, target_ordinal:target.ordinal,
    }, {'Idempotency-Key':crypto.randomUUID()});
    activationRetry = null; activationKey = null;
    $('deployment-restore-activation-target').textContent = `Bot ${target.bot_id} · 恢复 ${restoreId}`;
    $('deployment-restore-activation-warning').textContent =
      `存储代际 ${activationPlan.before.storage_generation} → ${activationPlan.after.storage_generation}。切换期间 Bot 会暂停接纳。`;
    $('deployment-restore-activation-form').querySelector('button[type="submit"]').textContent = '封存旧库并切换';
    $('deployment-restore-activation').classList.remove('hidden');
  }

  function openRetry(row) {
    activationPlan = null; activationRetry = row.activation_id; activationKey = null;
    $('deployment-restore-activation-target').textContent = `激活任务 ${row.activation_id} · Bot ${row.bot_id}`;
    $('deployment-restore-activation-warning').textContent = `上次激活被阻塞：${row.error_code || '未知错误'}。`;
    $('deployment-restore-activation-form').querySelector('button[type="submit"]').textContent = '重试激活';
    $('deployment-restore-activation').classList.remove('hidden');
  }

  function render(rows, activations) {
    const select = $('deployment-restore-create-form').elements.backup;
    const previous = select.value; select.replaceChildren();
    for (const row of eligible) {
      const option = document.createElement('option'); option.value = row.backup_id;
      option.textContent = `${row.backup_id} · 验证有效至 ${row.verification_expires_at}`;
      select.append(option);
    }
    if (eligible.some(row => row.backup_id === previous)) select.value = previous;
    $('deployment-restore-create-form').querySelector('button').disabled = eligible.length === 0;
    const list = $('deployment-restore-list'); list.replaceChildren();
    $('deployment-restore-empty').classList.toggle('hidden', rows.length !== 0);
    for (const row of rows) {
      const card = document.createElement('article'); card.className = 'audit-row';
      const title = document.createElement('h3'); title.textContent = `${row.state} · ${row.restore_id}`;
      const meta = document.createElement('p'); meta.className = 'muted';
      meta.textContent = `${row.database_completed}/${row.database_total} 个数据库 · 来源 ${row.backup_id}${row.error_code ? ` · ${row.error_code}` : ''}`;
      card.append(title, meta);
      if (row.targets) {
        const targets = document.createElement('ul');
        for (const target of row.targets) {
          const item = document.createElement('li');
          item.textContent = `${target.database_scope === 'deployment' ? '部署副本' : `Bot ${target.bot_id}`} · ${target.state}`;
          if (target.database_scope === 'bot' && target.state === 'STAGED') {
            const active = activations.find(value =>
              value.restore_id === row.restore_id && value.target_ordinal === target.ordinal);
            if (!active) {
              const button = document.createElement('button'); button.type = 'button';
              button.className = 'danger'; button.textContent = '预览并激活';
              button.addEventListener('click', () => busy(button, () => openActivation(row.restore_id, target)));
              item.append(document.createTextNode(' '), button);
            } else if (active.state === 'BLOCKED') {
              const button = document.createElement('button'); button.type = 'button';
              button.className = 'danger'; button.textContent = '重试激活';
              button.addEventListener('click', () => openRetry(active));
              item.append(document.createTextNode(' '), button);
            } else item.append(document.createTextNode(` · 激活 ${active.state}`));
          }
          targets.append(item);
        }
        card.append(targets);
      }
      list.append(card);
    }
  }

  async function load() {
    if (!scope) return;
    const own = epoch;
    const [stageData, activationData] = await Promise.all([
      api(`${base()}/queries/stages`), api(`${base()}/queries/activations`),
    ]);
    const detailed = await Promise.all(stageData.restore_stages.map((row, index) =>
      index < 20 && ['STAGED', 'FAILED'].includes(row.state)
        ? api(`${base()}/queries/stage?${params({restore_id:row.restore_id})}`) : row));
    if (own !== epoch) return;
    eligible = stageData.eligible_backups || [];
    render(detailed, activationData.restore_activations || []);
  }

  $('deployment-restore-refresh').addEventListener('click', event => busy(event.currentTarget, load));
  $('deployment-restore-create-form').addEventListener('input', () => { stageKey = null; });
  $('deployment-restore-create-form').addEventListener('submit', event => {
    event.preventDefault(); const form = event.currentTarget;
    const source = eligible.find(row => row.backup_id === form.elements.backup.value);
    if (!source || !form.reportValidity()) return;
    busy(form.querySelector('button'), async () => {
      stageKey ||= crypto.randomUUID();
      try {
        await api(`${base()}/actions/stage.create`, {
          backup_id:source.backup_id, verification_id:source.verification_id,
          current_password:form.elements.current_password.value,
        }, {'Idempotency-Key':stageKey});
      } finally { form.elements.current_password.value = ''; }
      stageKey = null; await load(); notice('隔离恢复任务已接纳。', true);
    });
  });
  $('deployment-restore-activation-cancel').addEventListener('click', closeActivation);
  $('deployment-restore-activation-form').addEventListener('input', () => { activationKey = null; });
  $('deployment-restore-activation-form').addEventListener('submit', event => {
    event.preventDefault(); const form = event.currentTarget;
    if ((!activationPlan && !activationRetry) || !form.reportValidity()) return;
    busy(form.querySelector('button'), async () => {
      activationKey ||= crypto.randomUUID();
      try {
        if (activationRetry) await api(`${base()}/actions/activation.retry`, {
          activation_id:activationRetry, current_password:form.elements.current_password.value,
        }, {'Idempotency-Key':activationKey});
        else await api(`${base()}/actions/activation.apply`, {
          plan_id:activationPlan.plan_id, plan_digest:activationPlan.plan_digest,
          current_password:form.elements.current_password.value,
        }, {'Idempotency-Key':activationKey});
      } finally { form.elements.current_password.value = ''; }
      closeActivation(); await load(); notice('恢复激活操作已接纳。', true);
    });
  });

  function reset() {
    epoch++; scope = null; eligible = []; stageKey = null; closeActivation();
    if (poll !== null) clearInterval(poll); poll = null;
    $('deployment-restore-list').replaceChildren();
  }
  async function select(value) {
    reset(); scope = value; await load();
    poll = setInterval(() => load().catch(error => notice(error.message)), 1500);
  }
  function invalidateContent(value) { if (scope && value === base()) reset(); }
  return {select, reset, refresh:load, invalidateContent};
}
