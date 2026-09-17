const $ = id => document.getElementById(id);

export function createModulePanel(api, busy, notice) {
  let scope = null, epoch = 0, poll = null, plan = null, retry = null, applyKey = null;
  const base = () => `/scopes/host/${encodeURIComponent(scope.scope_id)}/modules/host.storage`;

  function closeApply() {
    plan = retry = null; applyKey = null;
    $('host-storage-apply-form').reset(); $('host-storage-apply').classList.add('hidden');
  }
  function showPlan(value) {
    retry = null; plan = value; applyKey = null;
    $('host-storage-plan').textContent = JSON.stringify(value, null, 2);
    $('host-storage-apply-form').querySelector('button[type="submit"]').textContent = '撤销可见性并回收';
    $('host-storage-apply').classList.remove('hidden');
  }
  function showRetry(row) {
    plan = null; retry = row.cleanup_id; applyKey = null;
    $('host-storage-plan').textContent = JSON.stringify(row, null, 2);
    $('host-storage-apply-form').querySelector('button[type="submit"]').textContent = '重试回收';
    $('host-storage-apply').classList.remove('hidden');
  }
  function render(generations, cleanups) {
    const generationList = $('host-storage-generations'); generationList.replaceChildren();
    $('host-storage-generation-empty').classList.toggle('hidden', generations.length !== 0);
    for (const row of generations) {
      const card = document.createElement('article'); card.className = 'audit-row';
      const title = document.createElement('h3');
      title.textContent = `${row.display_name} · 第 ${row.storage_generation} 代 · ${row.state}`;
      const meta = document.createElement('p'); meta.className = 'muted';
      meta.textContent = `${row.source_kind}${row.retire_after ? ` · 可回收时间 ${row.retire_after}` : ''}`;
      card.append(title, meta);
      if (row.state === 'SUPERSEDED') {
        const button = document.createElement('button'); button.type = 'button';
        button.className = 'danger'; button.textContent = '预览回收';
        button.addEventListener('click', () => {
          const form = $('host-storage-plan-form');
          form.elements.resource_kind.value = 'BOT_STORAGE_GENERATION';
          form.elements.resource_id.value = `${row.bot_id}:${row.storage_generation}`;
          form.requestSubmit();
        });
        card.append(button);
      }
      generationList.append(card);
    }
    const cleanupList = $('host-storage-cleanups'); cleanupList.replaceChildren();
    $('host-storage-cleanup-empty').classList.toggle('hidden', cleanups.length !== 0);
    for (const row of cleanups) {
      const card = document.createElement('article'); card.className = 'audit-row';
      const title = document.createElement('h3'); title.textContent = `${row.state} · ${row.cleanup_id}`;
      const meta = document.createElement('p'); meta.className = 'muted';
      meta.textContent = `${row.resource_kind} · ${row.resource_id}${row.error_code ? ` · ${row.error_code}` : ''}`;
      card.append(title, meta);
      if (row.state === 'BLOCKED') {
        const button = document.createElement('button'); button.type = 'button';
        button.className = 'danger'; button.textContent = '重试';
        button.addEventListener('click', () => showRetry(row)); card.append(button);
      }
      cleanupList.append(card);
    }
  }
  async function load() {
    if (!scope) return;
    const own = epoch;
    const [generationData, cleanupData] = await Promise.all([
      api(`${base()}/queries/generations`), api(`${base()}/queries/cleanups`),
    ]);
    if (own !== epoch) return;
    render(generationData.storage_generations, cleanupData.cleanups);
  }

  $('host-storage-refresh').addEventListener('click', event => busy(event.currentTarget, load));
  $('host-storage-plan-form').addEventListener('submit', event => {
    event.preventDefault(); const form = event.currentTarget;
    if (!form.reportValidity()) return;
    busy(form.querySelector('button'), async () => showPlan(await api(
      `${base()}/actions/cleanup.plan`, {
        resource_kind:form.elements.resource_kind.value,
        resource_id:form.elements.resource_id.value.trim(),
      }, {'Idempotency-Key':crypto.randomUUID()})));
  });
  $('host-storage-apply-cancel').addEventListener('click', closeApply);
  $('host-storage-apply-form').addEventListener('input', () => { applyKey = null; });
  $('host-storage-apply-form').addEventListener('submit', event => {
    event.preventDefault(); const form = event.currentTarget;
    if ((!plan && !retry) || !form.reportValidity()) return;
    busy(form.querySelector('button'), async () => {
      applyKey ||= crypto.randomUUID();
      try {
        if (retry) await api(`${base()}/actions/cleanup.retry`, {
          cleanup_id:retry, current_password:form.elements.current_password.value,
        }, {'Idempotency-Key':applyKey});
        else await api(`${base()}/actions/cleanup.apply`, {
          plan_id:plan.plan_id, plan_digest:plan.plan_digest,
          current_password:form.elements.current_password.value,
        }, {'Idempotency-Key':applyKey});
      } finally { form.elements.current_password.value = ''; }
      closeApply(); await load(); notice('资源回收操作已接纳。', true);
    });
  });

  function reset() {
    epoch++; scope = null; closeApply();
    if (poll !== null) clearInterval(poll); poll = null;
    $('host-storage-generations').replaceChildren(); $('host-storage-cleanups').replaceChildren();
  }
  async function select(value) {
    reset(); scope = value; await load();
    poll = setInterval(() => load().catch(error => notice(error.message)), 1500);
  }
  function invalidateContent(value) { if (scope && value === base()) reset(); }
  return {select, reset, refresh:load, invalidateContent};
}
