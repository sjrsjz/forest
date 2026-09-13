import {clearConfiguration, replacementButton} from './configuration-forms.js';

const $ = (id) => document.getElementById(id);
const status = (row) => `${({ONLINE:'在线',OFFLINE:'离线',DISABLED:'未启用',DRAINING:'正在切换',RESERVED:'待完成登记'})[row.state || row.stage] || row.state}${row.error_code ? ` · ${row.error_code}` : ''}`;

export function bindingsPanel(api, busy, notice) {
  let bot = null, selected = null, epoch = 0, edit = 0;
  let pending = null, planKey = null, applyKey = null, createKey = null;
  let refreshSequence = 0, configSequence = 0;
  let replacementRequired = false, replacementDraft = false;
  const replace = replacementButton($('binding-state'), 'binding-replace-configuration', () => {
    if (!selected?.defaults) return;
    clearPlan(); fill(selected.defaults); replacementRequired = false; replacementDraft = true;
    replace.classList.add('hidden'); state(selected);
  });
  const deliveryNodes = new Map(), resolutionRequests = new Map();
  const base = () => `/bots/${encodeURIComponent(bot.bot_id)}`;
  const path = () => `${base()}/bindings/${encodeURIComponent(selected.binding_id)}`;
  const valid = (value) => value === epoch && bot !== null;
  function clearPlan() { edit++; pending = null; planKey = null; applyKey = null; $('binding-preview').classList.add('hidden'); }
  function state(row) {
    const missing = row.desired_values_payload_state === 'UNAVAILABLE' ? '（正文已清理）' : '';
    const effective = row.config_payload_state === 'UNAVAILABLE' ? '（正文已清理）' : '';
    $('binding-state').textContent = `${status(row)} · 已保存 ${row.revision}${missing} / 已生效 ${row.effective_revision}${effective}${replacementDraft ? ' · 正在填写独立新配置草稿' : replacementRequired ? ' · 可填写完整新配置后保存替换' : ''}`;
  }
  function fill(values) {
    const form = $('binding-config'); clearConfiguration(form);
    for (const field of form.elements) field.disabled = false;
    for (const [name, value] of Object.entries(values)) {
      const field = form.elements[name]; if (!field) continue;
      if (field.type === 'checkbox') field.checked = value;
      else field.value = Array.isArray(value) ? value.join(', ') : value;
    }
  }
  function reset() {
    epoch++; bot = null; selected = null; createKey = null; clearPlan();
    refreshSequence++; configSequence++; deliveryNodes.clear(); resolutionRequests.clear();
    $('binding-create').reset(); $('binding-config').reset();
    $('binding-detail').classList.add('hidden'); $('bot-bindings').classList.add('hidden');
    for (const id of ['binding-list','conversation-list','delivery-list']) $(id).replaceChildren();
    replacementRequired = false; replacementDraft = false; replace.classList.add('hidden');
  }
  async function readSelected() {
    if (!selected) return;
    const own = epoch, current = selected.binding_id, requested = ++configSequence, edited = edit;
    const row = await api(`${path()}/configuration`);
    if (!valid(own) || selected?.binding_id !== current || requested !== configSequence) return;
    if (edited !== edit) {
      state(row); notice('读取期间有新的配置输入，已保留。需要丢弃草稿时可再次重新加载。'); return;
    }
    selected = row; clearPlan(); replacementDraft = false;
    replacementRequired = row.desired_values_payload_state === 'UNAVAILABLE' || (row.revision > 0 && row.desired_values === null);
    state(row);
    $('binding-name').textContent = `${row.display_name ?? '名称已清理'} · ${row.self_id}`;
    $('binding-webhook').value = row.webhook_url;
    replace.classList.toggle('hidden', !replacementRequired); replace.disabled = !row.defaults;
    if (replacementRequired) {
      const form = $('binding-config'); clearConfiguration(form);
      for (const field of form.elements) field.disabled = true;
    } else {
      fill(row.desired_values ?? row.defaults);
    }
    $('binding-detail').classList.remove('hidden');
  }
  async function choose(row) {
    epoch++; selected = row; clearPlan(); $('binding-config').reset();
    $('binding-detail').classList.add('hidden'); await readSelected();
  }
  async function refresh() {
    if (!bot) return;
    const own = epoch, prefix = base(), requested = ++refreshSequence;
    const [data, queues, sends] = await Promise.all([api(`${prefix}/bindings`), api(`${prefix}/conversations`), api(`${prefix}/delivery`)]);
    if (!valid(own) || requested !== refreshSequence) return;
    $('binding-list').replaceChildren();
    if (!data.bindings.length) $('binding-list').textContent = '尚未登记账号。';
    for (const row of data.bindings) {
      const button = document.createElement('button'); button.className = 'bot-row subtle';
      button.textContent = `${row.display_name ?? '名称已清理'} · ${row.self_id} · ${status(row)}`; button.dataset.bindingId = row.binding_id;
      button.addEventListener('click', () => busy(button, async () => {
        const own = epoch;
        if (row.registration_stage === 'RESERVED') {
          await api(`${prefix}/bindings/${row.binding_id}/registration`, {});
          if (!valid(own)) return; await refresh();
        }
        if (valid(own)) await choose(row);
      }));
      $('binding-list').append(button);
      // A list read can finish after a newer configuration read or save.
      // Keep the known desired/effective heads while that older list catches up.
      if (selected?.binding_id === row.binding_id && row.revision >= selected.revision &&
          row.effective_revision >= selected.effective_revision) state(row);
    }
    $('conversation-list').replaceChildren();
    if (!queues.conversations.length) $('conversation-list').textContent = '收到消息后显示群聊与私聊。';
    for (const row of queues.conversations) {
      const item = document.createElement('p');
      item.textContent = `${row.channel_kind === 'group' ? '群' : '私聊'} ${row.channel_id} · 已接收 ${row.next_ingress_seq - 1} · 已推入上下文 ${row.context_applied_seq} · 待回复触发 ${row.pending_triggers}`;
      $('conversation-list').append(item);
    }
    // Keep each live form node: periodic polling must not steal focus, reset an
    // unfinished reason, or re-enable a request whose response is still pending.
    const visible = new Set();
    if (!deliveryNodes.size) $('delivery-list').replaceChildren();
    if (!sends.parts.length) $('delivery-list').textContent = '暂无发送记录。';
    for (const row of sends.parts) {
      const part = `${row.intent_id}:${row.part_index}`; visible.add(part);
      let node = deliveryNodes.get(part);
      if (!node) {
        const item = document.createElement('div'), label = document.createElement('p');
        const details = document.createElement('details'), summary = document.createElement('summary'), content = document.createElement('pre');
        summary.textContent = '查看固定发送内容与时间'; details.append(summary, content); item.append(label, details);
        node = {item, label, content, form:null, revision:null}; deliveryNodes.set(part, node);
        $('delivery-list').append(item);
      }
      node.label.textContent = `账号 ${row.reply_route.self_id} · ${row.reply_route.channel_kind === 'group' ? '群' : '私聊'} ${row.reply_route.channel_id} · ${row.send_lane} · ${row.state} · 分片 ${row.part_index + 1} · 尝试 ${row.attempt_count}${row.error_code ? ` · ${row.error_code}` : ''}`;
      const text = row.payload_state === 'UNAVAILABLE' ? '发送内容已不可用，发送记录仍保留。' : (row.payload || []).map(segment => {
        if (segment.type === 'text') return segment.text;
        if (segment.type === 'mention') return `[提及 ${segment.external_user_id}]`;
        if (segment.type === 'reply') return `[引用消息 ${segment.platform_message_id}]`;
        return `[${segment.type || '未知类型'}：${JSON.stringify(segment)}]`;
      }).join('');
      node.content.textContent = `${text}\n\n意图：${row.intent_id}\n分片操作：${row.part_operation_id || part}\n来源操作：${row.root_operation_id || row.operation_id}\n尝试：${row.attempt_id || '尚未许可'}\n创建：${row.created_at || '—'}\n取得许可：${row.permitted_at || '—'}\n交给传输：${row.started_at || '—'}\n最后观察：${row.observed_at || row.updated_at || '—'}\n语义摘要：${row.payload_digest || '—'}\n传输摘要：${row.wire_digest || '—'}\n平台回执：${JSON.stringify(row.receipt || {})}`;
      if (row.state !== 'UNKNOWN') {
        node.form?.remove(); node.form = null; node.revision = row.revision;
      } else if (!node.form || node.revision !== row.revision) {
        node.form?.remove(); node.form = resolutionForm(row, prefix, own, part);
        node.revision = row.revision; node.item.append(node.form);
      }
    }
    for (const [part, node] of deliveryNodes) if (!visible.has(part)) { node.item.remove(); deliveryNodes.delete(part); }
  }
  function resolutionForm(row, prefix, own, part) {
    const form = document.createElement('form'); form.className = 'actions'; form.dataset.part = part;
    const select = document.createElement('select'); select.name = 'decision'; select.required = true; select.setAttribute('aria-label','核对结论');
    for (const [value,text] of [['','请选择核对结论'],['SKIP','跳过此条'],['SENT','平台已收到'],['NOT_SENT','确认未发送并重试']]) {
      const option = document.createElement('option'); option.value = value; option.textContent = text; select.append(option);
    }
    const reason = document.createElement('input'); reason.name = 'reason'; reason.required = true; reason.maxLength = 2000; reason.placeholder = '核对依据'; reason.setAttribute('aria-label','核对依据');
    const button = document.createElement('button'); button.type = 'submit'; button.textContent = '提交核对结果';
    const help = document.createElement('p'); help.className = 'muted';
    help.textContent = '请展开核对固定内容和时间。“未发送”会重新派发相同内容；“跳过”仅放行后续消息，原未知尝试继续留账。';
    form.append(select,reason,button,help);
    const key = `${part}:${row.revision}`;
    let request = resolutionRequests.get(key);
    if (request) { select.value = request.body.decision; reason.value = request.body.reason; }
    function locked() {
      const pending = [...resolutionRequests.values()].some(item => item.part === part && item.inflight);
      for (const control of form.elements) control.disabled = pending;
    }
    locked();
    form.addEventListener('input', () => { resolutionRequests.delete(key); request = null; });
    form.addEventListener('submit', event => {
      event.preventDefault();
      if (!valid(own) || [...resolutionRequests.values()].some(item => item.part === part && item.inflight)) return;
      request ||= {part, key:crypto.randomUUID(), body:Object.freeze({decision:select.value, reason:reason.value, expected_revision:row.revision}), inflight:false};
      resolutionRequests.set(key, request); request.inflight = true; locked();
      busy(button, async () => {
        try {
          await api(`${prefix}/delivery/${row.intent_id}/${row.part_index}/resolution`, request.body, {'Idempotency-Key':request.key});
          if (valid(own)) { await refresh(); notice('核对结果已保存。', true); }
        } finally {
          request.inflight = false;
          if (valid(own)) {
            const current = deliveryNodes.get(part)?.form;
            if (current) for (const control of current.elements) control.disabled = false;
          }
        }
      });
    });
    return form;
  }
  $('binding-create').addEventListener('input', () => { createKey = null; });
  $('binding-create').addEventListener('submit', (event) => {
    event.preventDefault(); const form = event.currentTarget;
    busy(form.querySelector('button'), async () => {
      const own = epoch; createKey ||= crypto.randomUUID();
      const row = await api(`${base()}/bindings`, Object.fromEntries(new FormData(form)), {'Idempotency-Key':createKey});
      if (!valid(own)) return;
      createKey = null; form.reset(); await refresh(); if (valid(own)) await choose(row);
    });
  });
  $('binding-config').addEventListener('input', clearPlan);
  $('binding-config').addEventListener('submit', (event) => {
    event.preventDefault(); const form = event.currentTarget;
    if (!selected || replacementRequired || !form.reportValidity()) return;
    busy(form.querySelector('button'), async () => {
      const own = epoch, edited = edit, values = {};
      for (const name of ['enabled','trigger_mentions','private_trigger']) values[name] = form.elements[name].checked;
      for (const name of ['api_base_url','response_mode','response_template','trigger_prefix']) values[name] = form.elements[name].value;
      values.admin_user_ids = form.elements.admin_user_ids.value.split(/[,，\s]+/).filter(Boolean);
      const body = {expected_revision:selected.revision, values};
      for (const name of ['access_token','webhook_secret']) if (form.elements[name].value) body[name] = form.elements[name].value;
      planKey ||= crypto.randomUUID(); const plan = await api(`${path()}/change-plans`, body, {'Idempotency-Key':planKey});
      if (!valid(own) || edited !== edit) return;
      pending = plan; applyKey = crypto.randomUUID();
      $('binding-diff').textContent = `连接：${values.enabled ? '启用' : '停用'}\nAPI：${values.api_base_url}\n回复：${values.response_mode === 'component' ? '模板回复' : '静默记录'}\n模板：${values.response_template}\n触发：@ ${values.trigger_mentions ? '开启' : '关闭'}；前缀 ${values.trigger_prefix || '关闭'}；私聊 ${values.private_trigger ? '开启' : '关闭'}\n管理员：${values.admin_user_ids.join('、') || '未设置'}\n凭据：${plan.credentials_changed ? '更新' : '保持已有'}`;
      $('binding-preview').classList.remove('hidden');
    });
  });
  $('apply-binding').addEventListener('click', () => busy($('apply-binding'), async () => {
    if (!pending) return; const own = epoch, edited = edit;
    const result = await api(`${path()}/management-requests`, {expected_revision:pending.expected_revision, plan_id:pending.plan_id, plan_digest:pending.plan_digest}, {'Idempotency-Key':applyKey});
    if (!valid(own)) return;
    const preserved = edited !== edit;
    if (preserved) { selected = {...selected, revision:result.revision}; clearPlan(); }
    else await readSelected();
    if (!valid(own)) return; await refresh();
    if (valid(own)) notice(preserved ? '上一份配置已保存；提交期间的新输入已保留，可继续预览。' : '连接配置已保存，正在校验账号并切换连接。', true);
  }));
  $('reload-binding').addEventListener('click', () => busy($('reload-binding'), readSelected));
  $('refresh-bindings').addEventListener('click', () => busy($('refresh-bindings'), refresh));
  function invalidateContent(scope) {
    if (!bot || ![`${base()}/bindings`, `${base()}/delivery`].includes(scope)) return;
    refreshSequence++;
    if (scope === `${base()}/bindings`) {
      epoch++; configSequence++; clearPlan(); selected = null;
      $('binding-config').reset(); $('binding-diff').replaceChildren();
      $('binding-detail').classList.add('hidden'); $('binding-list').replaceChildren();
      replacementRequired = false; replacementDraft = false; replace.classList.add('hidden');
    } else {
      $('delivery-list').replaceChildren(); deliveryNodes.clear(); resolutionRequests.clear();
    }
  }
  return {reset, refresh, invalidateContent, async select(row) {reset(); if (row.stage !== 'READY') return; bot = row; $('bot-bindings').classList.remove('hidden'); await refresh();}};
}
