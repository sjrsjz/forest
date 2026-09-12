const $ = (id) => document.getElementById(id);

export function agentPanel(api, busy, notice) {
  const childDefaults = {children_enabled:false, max_children:4, child_concurrency:2,
    child_timeout:90, child_max_output_tokens:1024, run_token_budget:32768};
  let bot = null, epoch = 0, edit = 0, readSeq = 0, statusSeq = 0;
  let configuration = null, pending = null, planKey = null, applyKey = null;
  const form = $('agent-config');
  const base = () => `/bots/${encodeURIComponent(bot.bot_id)}/agent`;
  const post = (path, body, key) => api(path, body, {'Idempotency-Key':key});
  const valid = (own) => bot !== null && own === epoch;
  function clearPlan() {
    edit++; pending = null; planKey = null; applyKey = null;
    $('agent-preview').classList.add('hidden');
  }
  function reset() {
    epoch++; readSeq++; statusSeq++; bot = null; configuration = null;
    clearPlan(); form.reset(); form.querySelector('fieldset').disabled = true;
    $('bot-agent').classList.add('hidden'); $('agent-runs').replaceChildren();
    $('agent-config-status').textContent = '';
  }
  function showConfiguration(row) {
    row = {...row, values:{...childDefaults, ...row.values}};
    configuration = row; clearPlan(); form.reset();
    for (const [name, value] of Object.entries(row.values)) {
      const field = form.elements[name]; if (!field) continue;
      if (field.type === 'checkbox') field.checked = value;
      else if (name === 'allowed_tools') field.value = JSON.stringify(value, null, 2);
      else field.value = Array.isArray(value) ? value.join('\n') : value;
    }
    form.querySelector('fieldset').disabled = false;
    $('agent-config-status').textContent = `配置修订 ${row.revision} · ${row.has_api_key ? '已保存 API 密钥' : '无 API 密钥'} · ${row.values.enabled ? 'Agent 已启用' : 'Agent 未启用'}`;
  }
  async function read() {
    if (!bot) return;
    const own = epoch, sequence = ++readSeq, edited = edit;
    const row = await api(`${base()}/configuration`);
    if (!valid(own) || sequence !== readSeq) return;
    if (edited !== edit) { notice('读取期间配置已编辑，已保留草稿。'); return; }
    showConfiguration(row);
  }
  function values() {
    const result = structuredClone(configuration.values);
    for (const [name, old] of Object.entries(result)) {
      const field = form.elements[name]; if (!field) continue;
      if (typeof old === 'boolean') result[name] = field.checked;
      else if (typeof old === 'number') result[name] = Number(field.value);
      else if (name === 'allowed_tools') result[name] = JSON.parse(field.value || '[]');
      else if (Array.isArray(old)) result[name] = field.value.split(/\r?\n/).map(x => x.trim()).filter(Boolean);
      else result[name] = field.value;
    }
    return result;
  }
  form.addEventListener('input', clearPlan);
  form.addEventListener('change', clearPlan);
  form.addEventListener('submit', event => {
    event.preventDefault(); const own = epoch, edited = edit;
    if (!configuration) return;
    let normalized;
    try { normalized = values(); } catch { notice('工具授权需要合法的 JSON 数组。'); return; }
    const payload = {expected_revision: configuration.revision, values: normalized,
      api_key: form.elements.api_key.value || null, clear_api_key: form.elements.clear_api_key.checked};
    planKey ||= crypto.randomUUID(); const key = planKey, prefix = base();
    busy(event.submitter, async () => {
      const result = await post(`${prefix}/change-plans`, payload, key);
      if (!valid(own) || edited !== edit || key !== planKey) return;
      pending = result; applyKey = crypto.randomUUID();
      $('agent-config-diff').textContent = JSON.stringify({before:result.before_values, after:result.values,
        api_key:result.credentials_changed ? '将更新密钥' : '保留密钥'}, null, 2);
      $('agent-preview').classList.remove('hidden');
    });
  });
  $('agent-apply').addEventListener('click', event => {
    if (!pending) return;
    const own = epoch, edited = edit, plan = pending, key = applyKey, prefix = base();
    busy(event.currentTarget, async () => {
      const result = await post(`${prefix}/management-requests`, {
        expected_revision:plan.expected_revision, plan_id:plan.plan_id, plan_digest:plan.plan_digest}, key);
      if (!valid(own)) return;
      if (edited === edit && pending === plan) {
        showConfiguration({revision:result.revision, values:plan.values,
          secret_revision:result.secret_revision,
          has_api_key:plan.credentials_changed ? !payloadCleared() : configuration.has_api_key});
      }
      notice('Agent 配置已保存。新对话使用新配置；停用和密钥轮换会撤销旧运行的后续调用。', true);
    });
  });
  function payloadCleared() { return form.elements.clear_api_key.checked; }
  $('agent-reload').addEventListener('click', event => busy(event.currentTarget, read));
  async function refresh() {
    if (!bot) return;
    const own = epoch, sequence = ++statusSeq, prefix = base();
    const data = await api(`${prefix}/runs`);
    if (!valid(own) || sequence !== statusSeq) return;
    const root = $('agent-runs'); root.replaceChildren();
    for (const row of data.runs || []) {
      const item = document.createElement('details'), label = document.createElement('summary');
      label.textContent = `${row.channel_id || row.conversation_id} · ${row.phase} · ${row.run_id}`;
      const text = document.createElement('pre'); text.textContent = JSON.stringify(row, null, 2);
      item.append(label, text);
      if (!['COMPLETED','FAILED','CANCELLED'].includes(row.phase)) {
        const reason = document.createElement('input'); reason.placeholder = '取消原因'; reason.maxLength = 2000;
        const cancel = document.createElement('button'); cancel.textContent = '取消此运行';
        let cancelKey = crypto.randomUUID(); reason.addEventListener('input', () => { cancelKey = crypto.randomUUID(); });
        cancel.addEventListener('click', () => busy(cancel, async () => {
          if (!reason.value.trim()) { notice('请填写取消原因。'); return; }
          await post(`${prefix}/cancellations`, {run_id:row.run_id,
            expected_checkpoint_seq:row.checkpoint_seq, reason:reason.value.trim()}, cancelKey);
          if (valid(own)) await refresh();
        })); item.append(reason, cancel);
      }
      for (const tool of [...(row.tools || []), ...(data.unknown_tools || []).filter(tool => tool.run_id === row.run_id)]) {
        if (tool.status !== 'UNKNOWN') continue;
        const title = document.createElement('p'); title.textContent = `工具 ${tool.tool_name} 结果未知。核对实际执行结果后再登记，系统不会重放此次调用。`;
        const state = document.createElement('select');
        for (const [value, name] of [['SUCCEEDED','确认成功'],['FAILED','确认失败']]) {
          const option = document.createElement('option'); option.value = value; option.textContent = name; state.append(option);
        }
        const result = document.createElement('textarea'); result.placeholder = '核实后的结果（JSON 对象）'; result.value = '{}';
        const button = document.createElement('button'); button.textContent = '登记核实结果'; let key = crypto.randomUUID();
        for (const field of [state, result]) field.addEventListener('input', () => { key = crypto.randomUUID(); });
        button.addEventListener('click', () => busy(button, async () => {
          let parsed; try { parsed = JSON.parse(result.value); } catch { notice('结果需要合法 JSON。'); return; }
          await post(`${prefix}/resolutions`, {run_id:row.run_id,
            tool_call_id:tool.tool_call_id, expected_checkpoint_seq:row.checkpoint_seq,
            arguments_digest:tool.arguments_digest, status:state.value, result:parsed}, key);
          if (valid(own)) await refresh();
        })); item.append(title, state, result, button);
      }
      root.append(item);
    }
    for (const approval of data.approvals || []) {
      if (approval.decision != null || (approval.tool_status && approval.tool_status !== 'WAIT_APPROVAL')) continue;
      const item = document.createElement('div'); item.className = 'card';
      const text = document.createElement('pre'); text.textContent = JSON.stringify(approval, null, 2);
      item.append(text);
      for (const [decision, label] of [['APPROVE','批准本次参数'],['REJECT','拒绝本次调用']]) {
        const button = document.createElement('button'); button.textContent = label;
        const key = crypto.randomUUID();
        button.addEventListener('click', () => busy(button, async () => {
          await post(`${prefix}/approvals`, {approval_id:approval.approval_id,
            arguments_digest:approval.arguments_digest, decision}, key);
          if (valid(own)) await refresh();
        })); item.append(button);
      }
      root.append(item);
    }
    if (data.children?.length) {
      const section = document.createElement('section'), title = document.createElement('h3');
      title.textContent = '私有子分析'; section.append(title);
      const note = document.createElement('p'); note.className = 'hint';
      note.textContent = '子分析结果只交回父运行，不直接发送到群。取消父运行会停止接纳其后续结果；已发出的模型请求可能仍产生费用。';
      section.append(note);
      const states = {PENDING:'待执行', PREPARED:'输入已固定', RUNNING:'分析中', DISPATCHING:'请求已发出',
        RESPONDED:'模型已返回', SUCCEEDED:'已完成', COMPLETED:'已完成', FAILED:'失败', CANCELLED:'已取消', UNKNOWN:'结果待核实',
        QUEUED:'排队中', RETRY:'等待重试', RESULT_READY:'等待父运行采纳', ACCEPTED:'父运行已采纳'};
      for (const child of data.children) {
        const item = document.createElement('details'), label = document.createElement('summary');
        label.textContent = `${states[child.status] || child.status} · ${child.child_id}`;
        const facts = document.createElement('dl');
        for (const [name, value] of [['父运行',child.parent_run_id], ['委托主体',child.principal_ref],
          ['来源触发',child.authority_trigger_id], ['选定上下文单元',child.source_unit_ids?.length ?? 0],
          ['期限',child.deadline_at], ['错误',child.error_code || '—']]) {
          const key = document.createElement('dt'), detail = document.createElement('dd');
          key.textContent = name; detail.textContent = String(value ?? '—'); facts.append(key, detail);
        }
        item.append(label, facts); section.append(item);
      }
      root.append(section);
    }
    if (data.run_budgets?.length) {
      const section = document.createElement('details'), title = document.createElement('summary');
      title.textContent = '父运行与子分析共用预算'; section.append(title);
      for (const budget of data.run_budgets) {
        const row = document.createElement('p');
        row.textContent = `${budget.run_id} · 已预留 ${budget.reserved_tokens ?? '—'} token · 已计入 ${budget.charged_tokens ?? '—'} token`;
        section.append(row);
      }
      root.append(section);
    }
    if (data.budget?.length) {
      const budget = document.createElement('details'), title = document.createElement('summary');
      title.textContent = '最近每日预算与预留';
      const text = document.createElement('pre'); text.textContent = JSON.stringify(data.budget, null, 2);
      budget.append(title, text); root.append(budget);
    }
    if (!root.childNodes.length) root.textContent = '尚无 Agent 运行记录。';
  }
  $('agent-refresh').addEventListener('click', event => busy(event.currentTarget, refresh));
  async function select(row) {
    reset(); bot = row; $('bot-agent').classList.remove('hidden');
    await read(); await refresh();
  }
  return {reset, select, refresh};
}
