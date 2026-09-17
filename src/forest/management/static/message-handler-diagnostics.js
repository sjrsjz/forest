const $ = id => document.getElementById(id);

export function messageHandlerDiagnosticsPanel(api, busy, notice) {
  let bot = null, epoch = 0, readSeq = 0, recoveryKey = null;
  let limitRevision = 0, limitPlan = null, limitApplyKey = null, limitReadSeq = 0;
  const valid = own => bot !== null && own === epoch;
  const base = () => `/scopes/bot/${encodeURIComponent(bot.bot_id)}/modules/message.router`;
  const query = (operation, values = {}) => `${base()}/queries/${operation}?${new URLSearchParams(values)}`;
  const action = operation => `${base()}/actions/${operation}`;

  function reset() {
    epoch++; readSeq++; limitReadSeq++; bot = null; recoveryKey = null;
    limitRevision = 0; limitPlan = null; limitApplyKey = null;
    $('bot-handler-diagnostics').open = false;
    $('bot-handler-diagnostics').classList.add('hidden');
    $('handler-diagnostic-binding').replaceChildren();
    $('handler-diagnostic-summary').replaceChildren();
    $('handler-diagnostic-chains').replaceChildren();
    $('handler-diagnostic-attempts').replaceChildren();
    $('handler-diagnostic-status').textContent = '';
    $('handler-limit-form').reset();
    $('handler-limit-password').value = '';
    $('handler-limit-status').textContent = '';
    $('handler-limit-preview').classList.add('hidden');
  }

  const limitNames = [
    'max_event_bytes', 'max_pending_bot', 'max_pending_binding',
    'max_pending_principal', 'max_handlers_per_chain', 'principal_burst',
    'principal_refill_seconds', 'deadline_seconds',
  ];

  function clearLimitPlan() {
    limitPlan = null; limitApplyKey = null;
    $('handler-limit-preview').classList.add('hidden');
  }

  function limitValues() {
    const data = new FormData($('handler-limit-form')), values = {};
    for (const name of limitNames) values[name] = Number(data.get(name));
    return values;
  }

  async function loadLimits() {
    if (!bot) return;
    const own = epoch, seq = ++limitReadSeq;
    const data = await api(query('limits'));
    if (!valid(own) || seq !== limitReadSeq) return;
    limitRevision = data.revision; clearLimitPlan();
    if (data.values === null) {
      $('handler-limit-status').textContent =
        `限制正文不可用（${data.values_payload_state}），请先恢复或重新配置。`;
      return;
    }
    for (const name of limitNames) {
      $('handler-limit-form').elements.namedItem(name).value = data.values[name];
    }
    $('handler-limit-status').textContent =
      `当前修订 ${data.revision} · ${data.values_payload_state}`;
  }

  async function loadBindings() {
    const own = epoch, data = await api(query('bindings'));
    if (!valid(own)) return;
    const select = $('handler-diagnostic-binding'), previous = select.value;
    select.replaceChildren();
    const all = document.createElement('option');
    all.value = ''; all.textContent = '全部 Binding'; select.append(all);
    for (const binding of data.bindings.filter(row => row.registration_stage === 'REGISTERED')) {
      const option = document.createElement('option'); option.value = binding.binding_id;
      option.textContent = `${binding.display_name || binding.self_id} · ${binding.self_id}`;
      select.append(option);
    }
    if ([...select.options].some(option => option.value === previous)) select.value = previous;
  }

  function addRow(root, titleText, detailText) {
    const item = document.createElement('article'); item.className = 'audit-row';
    const title = document.createElement('h3'); title.textContent = titleText;
    const detail = document.createElement('p'); detail.className = 'hint';
    detail.textContent = detailText; item.append(title, detail); root.append(item);
  }

  function empty(root, text) {
    if (!root.childElementCount) root.textContent = text;
  }

  function render(data) {
    const summary = $('handler-diagnostic-summary'); summary.replaceChildren();
    for (const [label, value] of [
      ['等待', data.summary.pending], ['已领取', data.summary.claimed],
      ['Kernel 已接纳', data.summary.claimed_call_admitted],
      ['待提交完成回执', data.summary.claimed_call_completed],
      ['已回复', data.summary.replied], ['失败', data.summary.failed],
      ['已取消', data.summary.cancelled],
    ]) {
      const item = document.createElement('span'); item.className = 'role-badge';
      item.textContent = `${label} ${value}`; summary.append(item);
    }
    const chains = $('handler-diagnostic-chains'); chains.replaceChildren();
    for (const row of data.chains) {
      addRow(
        chains, `${row.status} · ${row.handler_id || '已终结'}`,
        `${row.binding_id} / ${row.conversation_id} · ingress ${row.ingress_seq} · ` +
        `route ${row.current_index}/${row.route_count} · attempt ${row.attempt_count}` +
        `${row.call_status ? ` · call ${row.call_status}` : ''}` +
        `${row.last_error_code ? ` · ${row.last_error_code}` : ''}` +
        `${row.terminal_reason ? ` · ${row.terminal_reason}` : ''}`,
      );
    }
    empty(chains, '没有 Handler dispatch trace。');
    const attempts = $('handler-diagnostic-attempts'); attempts.replaceChildren();
    for (const row of data.attempts) {
      addRow(
        attempts, `${row.handler_id} · attempt ${row.attempt_no}`,
        `${row.component_id} · route ${row.route_index} · call ${row.call_status || 'NOT_ADMITTED'}` +
        `${row.disposition ? ` · ${row.disposition}` : ''}` +
        `${row.failure_error_code || row.call_error_code ?
          ` · ${row.failure_error_code || row.call_error_code}` : ''} · ${row.created_at}`,
      );
    }
    empty(attempts, '没有 Handler attempt。');
    $('handler-diagnostic-status').textContent =
      `读取 ${data.chains.length} 条Dispatch trace和 ${data.attempts.length} 次尝试；` +
      '页面不读取群消息、用户标识、插件返回正文或异常正文。';
  }

  async function refresh() {
    if (!bot) return;
    const own = epoch, seq = ++readSeq;
    await Promise.all([loadBindings(), loadLimits()]);
    if (!valid(own) || seq !== readSeq) return;
    const params = new URLSearchParams({limit: '50'});
    const binding = $('handler-diagnostic-binding').value;
    if (binding) params.set('binding_id', binding);
    const data = await api(query('diagnostics', params));
    if (valid(own) && seq === readSeq) render(data);
  }

  $('handler-diagnostic-binding').addEventListener('change', event =>
    busy(event.currentTarget, refresh));
  $('handler-diagnostic-refresh').addEventListener('click', event =>
    busy(event.currentTarget, refresh));
  $('handler-diagnostic-recover').addEventListener('click', event => {
    recoveryKey ||= crypto.randomUUID();
    const own = epoch, key = recoveryKey;
    busy(event.currentTarget, async () => {
      const result = await api(
        action('recovery'), {limit: 128}, {'Idempotency-Key': key},
      );
      if (!valid(own)) return;
      recoveryKey = null; await refresh();
      if (valid(own)) notice(
        `已唤醒 Actor 恢复扫描：检查 ${result.inspected} 条，` +
        `待提交 ${result.completed_to_finalize} 条，Kernel 处理中 ${result.admitted_waiting} 条。`,
        true,
      );
    });
  });

  $('handler-limit-reload').addEventListener('click', event =>
    busy(event.currentTarget, loadLimits));
  $('handler-limit-form').addEventListener('input', () => {
    limitReadSeq++; clearLimitPlan();
  });
  $('handler-limit-form').addEventListener('submit', event => {
    event.preventDefault();
    busy(event.submitter, async () => {
      clearLimitPlan();
      const own = epoch, key = crypto.randomUUID();
      const values = limitValues(), draft = JSON.stringify(values);
      const plan = await api(
        action('limit.plan'),
        {
          expected_revision: limitRevision,
          values,
          current_password: $('handler-limit-password').value,
        },
        {'Idempotency-Key': key},
      );
      if (!valid(own) || JSON.stringify(limitValues()) !== draft) return;
      limitPlan = plan; limitApplyKey = crypto.randomUUID();
      $('handler-limit-diff').textContent = JSON.stringify({
        before: plan.before_values,
        before_payload_state: plan.before_values_payload_state,
        after: plan.values,
      }, null, 2);
      $('handler-limit-preview').classList.remove('hidden');
    });
  });
  $('handler-limit-apply').addEventListener('click', event => {
    if (!limitPlan) return;
    busy(event.currentTarget, async () => {
      const own = epoch, plan = limitPlan;
      const result = await api(
        action('limit.apply'),
        {
          expected_revision: plan.expected_revision,
          plan_id: plan.plan_id,
          plan_digest: plan.plan_digest,
          current_password: $('handler-limit-password').value,
        },
        {'Idempotency-Key': limitApplyKey},
      );
      if (!valid(own)) return;
      $('handler-limit-password').value = '';
      clearLimitPlan(); await loadLimits();
      if (valid(own)) notice(`Handler 容量限制已应用为修订 ${result.revision}。`, true);
    });
  });

  async function select(row) {
    reset(); bot = row; $('bot-handler-diagnostics').classList.remove('hidden'); await refresh();
  }
  function invalidateContent(scope) {
    if (!bot || !scope.startsWith(base())) return;
    readSeq++; limitReadSeq++; recoveryKey = null; clearLimitPlan();
  }
  return {select, reset, refresh, invalidateContent};
}
