import {clearConfiguration, replacementButton} from './configuration-forms.js';

const $ = (id) => document.getElementById(id);

export function agentPanel(api, busy, notice) {
  let bot = null, epoch = 0, edit = 0, readSeq = 0, statusSeq = 0;
  let configuration = null, pending = null, planKey = null, applyKey = null;
  const form = $('agent-config');
  const replace = replacementButton($('agent-config-status'), 'agent-replace-configuration', () => {
    if (!configuration?.defaults) return;
    showConfiguration({...configuration, values:structuredClone(configuration.defaults), values_payload_state:'DRAFT'});
  });
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
    $('agent-protocol-version').textContent = '—';
    $('agent-protocol-digest').textContent = '—';
    $('agent-core-prompt').textContent = '';
    replace.classList.add('hidden');
  }
  function showConfiguration(row) {
    configuration = row; clearPlan(); form.reset();
    $('agent-protocol-version').textContent = row.protocol
      ? `${row.protocol.protocol_id} v${row.protocol.protocol_version} · ${row.protocol.parser_revision}` : '—';
    $('agent-protocol-digest').textContent = row.protocol?.core_prompt_digest || '—';
    $('agent-core-prompt').textContent = row.protocol?.core_prompt || '';
    replace.classList.add('hidden');
    if (row.values_payload_state === 'UNAVAILABLE' || row.values === null) {
      clearConfiguration(form); form.querySelector('fieldset').disabled = true;
      $('agent-config-status').textContent = `配置修订 ${row.revision} 的正文已清理。原修订和密钥记录仍保留；填写完整新配置后可保存替换。`;
      replace.disabled = !row.defaults; replace.classList.remove('hidden'); return;
    }
    configuration = row;
    for (const [name, value] of Object.entries(row.values)) {
      const field = form.elements[name]; if (!field) continue;
      if (field.type === 'checkbox') field.checked = value;
      else if (name === 'allowed_tools') field.value = JSON.stringify(value, null, 2);
      else if (name === 'preset_context') field.value = value.join('\n\n');
      else field.value = Array.isArray(value) ? value.join('\n') : value;
    }
    form.querySelector('fieldset').disabled = false;
    $('agent-config-status').textContent = row.values_payload_state === 'DRAFT'
      ? `新配置草稿 · 保存后替换修订 ${row.revision}。当前显示的是新模板。`
      : `配置修订 ${row.revision} · ${row.has_api_key ? '已保存 API 密钥' : '无 API 密钥'} · ${row.values.enabled ? 'Agent 已启用' : 'Agent 未启用'}`;
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
      else if (name === 'preset_context') result[name] = field.value.trim() ? [field.value] : [];
      else if (Array.isArray(old)) result[name] = field.value.split(/\r?\n/).map(x => x.trim()).filter(Boolean);
      else result[name] = field.value;
    }
    return result;
  }
  form.addEventListener('input', clearPlan);
  form.addEventListener('change', clearPlan);
  form.addEventListener('submit', event => {
    event.preventDefault(); const own = epoch, edited = edit;
    if (!configuration?.values || configuration.values_payload_state === 'UNAVAILABLE') return;
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
          protocol:configuration.protocol,
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
    if (data.run_budgets?.length) {
      const section = document.createElement('details'), title = document.createElement('summary');
      title.textContent = '每次运行预算'; section.append(title);
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
  function invalidateContent(scope) {
    if (!bot || scope !== base()) return;
    epoch++; readSeq++; statusSeq++; configuration = null; clearPlan();
    clearConfiguration(form); form.querySelector('fieldset').disabled = true;
    $('agent-config-diff').replaceChildren(); $('agent-runs').replaceChildren();
    $('agent-config-status').textContent = '相关内容已不可用，请重新加载当前配置。';
    replace.classList.add('hidden');
  }
  return {reset, select, refresh, invalidateContent};
}

export const createModulePanel = agentPanel;
