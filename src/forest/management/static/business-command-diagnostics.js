const $ = id => document.getElementById(id);

export function businessCommandDiagnosticsPanel(api, busy, notice) {
  let bot = null, epoch = 0, readSeq = 0, recoveryKey = null;
  const valid = own => bot !== null && own === epoch;
  const base = () => `/bots/${encodeURIComponent(bot.bot_id)}`;

  function reset() {
    epoch++; readSeq++; bot = null; recoveryKey = null;
    $('bot-command-diagnostics').classList.add('hidden');
    $('command-diagnostic-binding').replaceChildren();
    $('command-diagnostic-summary').replaceChildren();
    $('command-diagnostic-requests').replaceChildren();
    $('command-diagnostic-classifications').replaceChildren();
    $('command-diagnostic-notices').replaceChildren();
    $('command-diagnostic-status').textContent = '';
  }

  async function loadBindings() {
    const own = epoch, data = await api(`${base()}/bindings`);
    if (!valid(own)) return;
    const select = $('command-diagnostic-binding'), previous = select.value;
    select.replaceChildren();
    const all = document.createElement('option'); all.value = ''; all.textContent = '全部 Binding';
    select.append(all);
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

  function empty(root, value) {
    if (!root.childElementCount) root.textContent = value;
  }

  function render(data) {
    const summary = $('command-diagnostic-summary'); summary.replaceChildren();
    const s = data.summary;
    for (const [label, value] of [
      ['待执行命令', s.pending_requests], ['已领取命令', s.claimed_requests],
      ['失败命令', s.failed_requests], ['待分类', s.pending_classifications],
      ['待发送安全通知', s.pending_notices], ['已领取通知', s.claimed_notices],
    ]) {
      const item = document.createElement('span'); item.className = 'role-badge';
      item.textContent = `${label} ${value}`; summary.append(item);
    }
    const requests = $('command-diagnostic-requests'); requests.replaceChildren();
    for (const row of data.requests) {
      addRow(
        requests, `${row.command_name} · ${row.status}`,
        `${row.binding_id} / ${row.conversation_id} · ingress ${row.ingress_seq} · ` +
        `attempt ${row.execution_attempt_count}` +
        `${row.last_error_code ? ` · ${row.last_error_code}` : ''} · ${row.created_at}`,
      );
    }
    empty(requests, '没有业务命令请求。');
    const classifications = $('command-diagnostic-classifications');
    classifications.replaceChildren();
    for (const row of data.classifications) {
      addRow(
        classifications,
        `${row.event_id} · ${row.business_classification_pending ? '等待重试' : row.trigger_decision}`,
        `${row.binding_id} / ${row.conversation_id} · ingress ${row.ingress_seq} · ` +
        `attempt ${row.business_classification_attempt_count}` +
        `${row.business_classification_error_code ? ` · ${row.business_classification_error_code}` : ''}`,
      );
    }
    empty(classifications, '没有失败或等待中的分类。');
    const notices = $('command-diagnostic-notices'); notices.replaceChildren();
    for (const row of data.notices) {
      addRow(
        notices, `${row.kind} · ${row.status}`,
        `${row.binding_id} / ${row.conversation_id} · ingress ${row.ingress_seq} · ` +
        `${row.error_code}${row.response_seq ? ` · reply ${row.response_seq}` : ''}`,
      );
    }
    empty(notices, '没有业务命令安全通知。');
    $('command-diagnostic-status').textContent =
      `读取 ${data.requests.length} 条命令、${data.classifications.length} 条分类记录和 ` +
      `${data.notices.length} 条通知；页面不读取消息、参数或异常正文。`;
  }

  async function refresh() {
    if (!bot) return;
    const own = epoch, seq = ++readSeq;
    await loadBindings();
    if (!valid(own) || seq !== readSeq) return;
    const params = new URLSearchParams({limit: '50'});
    const binding = $('command-diagnostic-binding').value;
    if (binding) params.set('binding_id', binding);
    const data = await api(`${base()}/business-commands/diagnostics?${params}`);
    if (!valid(own) || seq !== readSeq) return;
    render(data);
  }

  $('command-diagnostic-binding').addEventListener('change', event =>
    busy(event.currentTarget, refresh));
  $('command-diagnostic-refresh').addEventListener('click', event =>
    busy(event.currentTarget, refresh));
  $('command-diagnostic-recover').addEventListener('click', event => {
    recoveryKey ||= crypto.randomUUID();
    const own = epoch, key = recoveryKey;
    busy(event.currentTarget, async () => {
      const result = await api(
        `${base()}/business-commands/recovery`, {limit: 128}, {'Idempotency-Key': key},
      );
      if (!valid(own)) return;
      recoveryKey = null; await refresh();
      if (valid(own)) notice(
        `恢复扫描完成：分类完成 ${result.classification_completed}，超时终结 ${result.classification_expired}。`,
        true,
      );
    });
  });

  async function select(row) {
    reset(); bot = row; $('bot-command-diagnostics').classList.remove('hidden'); await refresh();
  }
  function invalidateContent(scope) {
    if (!bot || !scope.startsWith(base())) return;
    readSeq++; recoveryKey = null;
  }
  return {select, reset, refresh, invalidateContent};
}
