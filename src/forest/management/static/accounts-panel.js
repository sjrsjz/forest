const $ = (id) => document.getElementById(id);

export function accountsPanel(api, busy, notice, sessionEnded) {
  let rows = [], selected = null, plan = null, createKey = null, applyKey = null;
  let readSeq = 0, currentAccountId = null;

  function resetPlan() {
    plan = null; applyKey = null;
    $('account-preview').classList.add('hidden');
    $('account-preview-diff').textContent = '';
    $('account-apply-form').reset();
  }

  function renderList(currentId) {
    $('account-list').replaceChildren();
    for (const row of rows) {
      const button = document.createElement('button');
      button.className = 'account-row subtle';
      button.textContent = `${row.username}${row.id === currentId ? '（当前）' : ''} · ` +
        `${row.role === 'admin' ? '管理员' : '查看者'} · ` +
        `${row.disabled ? '已停用' : `活跃会话 ${row.active_sessions}`}`;
      button.addEventListener('click', () => select(row.id));
      $('account-list').append(button);
    }
  }

  function select(id) {
    selected = rows.find((row) => row.id === id) || null;
    resetPlan();
    $('account-detail').classList.toggle('hidden', !selected);
    if (!selected) return;
    $('account-detail-name').textContent = selected.username;
    $('account-detail-meta').textContent =
      `修订 ${selected.revision} · ${selected.disabled ? '已停用' : '已启用'} · 活跃会话 ${selected.active_sessions}`;
    const form = $('account-plan-form');
    form.elements.role.value = selected.role;
    form.elements.disabled.checked = selected.disabled;
    form.elements.new_password.value = '';
  }

  async function load(currentId) {
    if (currentId) currentAccountId = currentId;
    const seq = ++readSeq;
    const data = await api('/accounts');
    if (seq !== readSeq) return data;
    rows = data.accounts;
    renderList(currentAccountId);
    if (selected) select(selected.id);
    return data;
  }

  $('account-refresh').addEventListener('click', () => busy($('account-refresh'), () => load()));
  $('account-create-form').addEventListener('input', () => { createKey = null; });
  $('account-create-form').addEventListener('submit', (event) => {
    event.preventDefault(); const form = event.currentTarget;
    if (!form.reportValidity()) return;
    busy(form.querySelector('button'), async () => {
      createKey ||= crypto.randomUUID();
      const body = Object.fromEntries(new FormData(form));
      await api('/accounts', body, {'Idempotency-Key': createKey});
      form.reset(); createKey = null; await load();
      notice('管理账号已创建；对方可使用新密码登录。', true);
    });
  });
  $('account-plan-form').addEventListener('input', resetPlan);
  $('account-plan-form').addEventListener('submit', (event) => {
    event.preventDefault(); const form = event.currentTarget;
    if (!selected || !form.reportValidity()) return;
    busy(form.querySelector('button'), async () => {
      const body = {
        expected_revision: selected.revision,
        role: form.elements.role.value,
        disabled: form.elements.disabled.checked,
      };
      if (form.elements.new_password.value) body.new_password = form.elements.new_password.value;
      plan = await api(`/accounts/${encodeURIComponent(selected.id)}/change-plans`, body);
      form.elements.new_password.value = '';
      applyKey = crypto.randomUUID();
      $('account-preview-diff').textContent = JSON.stringify({
        当前: plan.before,
        待应用: plan.after,
        影响: '角色、停用状态或密码变化会撤销该账号的全部会话',
        过期时间: plan.expires_at,
      }, null, 2);
      $('account-preview').classList.remove('hidden');
    });
  });
  $('account-apply-form').addEventListener('submit', (event) => {
    event.preventDefault(); const form = event.currentTarget;
    if (!selected || !plan || !form.reportValidity()) return;
    busy(form.querySelector('button'), async () => {
      const result = await api(`/accounts/${encodeURIComponent(selected.id)}/management-requests`, {
        expected_revision: plan.expected_revision,
        plan_id: plan.plan_id,
        plan_digest: plan.plan_digest,
        current_password: form.elements.current_password.value,
      }, {'Idempotency-Key': applyKey});
      form.reset(); resetPlan();
      if (result.reauthentication_required) {
        sessionEnded('账号已更新，全部旧会话已撤销，请重新登录。');
        return;
      }
      await load(); select(result.id);
      notice(`账号已更新，并撤销 ${result.revoked_sessions} 个会话。`, true);
    });
  });

  return {
    load,
    reset() {
      rows = []; selected = null; createKey = null;
      readSeq++; currentAccountId = null;
      resetPlan(); $('account-list').replaceChildren();
      $('account-detail').classList.add('hidden'); $('account-create-form').reset();
    },
  };
}
