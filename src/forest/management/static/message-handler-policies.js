const $ = (id) => document.getElementById(id);

export function messageHandlerPoliciesPanel(api, busy, notice) {
  let bot = null, epoch = 0, readSeq = 0, pending = null, applyKey = null;
  const base = () => `/bots/${encodeURIComponent(bot.bot_id)}`;
  const password = () => $('handler-policy-password').value;
  const post = (path, body, key) => api(path, body, {'Idempotency-Key':key});
  const valid = (own) => bot !== null && own === epoch;
  function clearPlan() {
    pending = null; applyKey = null; $('handler-policy-preview').classList.add('hidden');
  }
  function reset() {
    epoch++; readSeq++; bot = null; clearPlan();
    $('bot-handler-policies').classList.add('hidden');
    $('handler-policy-binding').replaceChildren();
    $('handler-policy-list').replaceChildren();
    $('handler-policy-status').textContent = '';
    $('handler-policy-password').value = '';
  }
  async function loadBindings() {
    const own = epoch, data = await api(`${base()}/bindings`);
    if (!valid(own)) return;
    const select = $('handler-policy-binding'), previous = select.value;
    select.replaceChildren();
    for (const binding of data.bindings.filter(row => row.registration_stage === 'REGISTERED')) {
      const option = document.createElement('option');
      option.value = binding.binding_id;
      option.textContent = `${binding.display_name || binding.self_id} · ${binding.self_id}`;
      select.append(option);
    }
    if ([...select.options].some(option => option.value === previous)) select.value = previous;
  }
  async function read() {
    if (!bot) return;
    const own = epoch, sequence = ++readSeq;
    await loadBindings();
    if (!valid(own) || sequence !== readSeq) return;
    const binding = $('handler-policy-binding').value;
    clearPlan(); $('handler-policy-list').replaceChildren();
    if (!binding) {
      $('handler-policy-status').textContent = '还没有已登记的 Binding。'; return;
    }
    const data = await api(`${base()}/bindings/${encodeURIComponent(binding)}/message-handler-policies`);
    if (!valid(own) || sequence !== readSeq || binding !== $('handler-policy-binding').value) return;
    $('handler-policy-status').textContent =
      `Binding 策略 · 处理器注册表修订 ${data.registry_revision} · ${data.handlers.length} 条有效处理器`;
    for (const handler of data.handlers) renderHandler(binding, handler);
    if (!data.handlers.length) $('handler-policy-list').textContent = '当前 Bot 没有已启用的普通消息处理器。';
  }
  function renderHandler(binding, handler) {
    const form = document.createElement('form'); form.className = 'card';
    const title = document.createElement('h3'); title.textContent = handler.handler_id;
    const detail = document.createElement('p'); detail.className = 'hint';
    detail.textContent = `${handler.component_id} · ${handler.handler} · 优先级 ${handler.priority} · 策略修订 ${handler.revision}`;
    const modeLabel = document.createElement('label'); modeLabel.textContent = '允许用户';
    const mode = document.createElement('select'); mode.name = 'mode';
    for (const [value, label] of [
      ['DISABLED','禁用，不认领消息'], ['EVERYONE','所有可信平台用户'],
      ['ADMINS','当前 Binding 管理员'], ['USER_SET','指定平台用户'],
    ]) {
      const option = document.createElement('option'); option.value = value; option.textContent = label;
      mode.append(option);
    }
    mode.value = handler.values?.mode || 'DISABLED'; modeLabel.append(mode);
    const usersLabel = document.createElement('label'); usersLabel.textContent = '指定 QQ 号';
    const users = document.createElement('input'); users.name = 'user_ids'; users.maxLength = 1400;
    users.placeholder = '多个账号用逗号分隔'; users.value = (handler.values?.user_ids || []).join(',');
    users.disabled = mode.value !== 'USER_SET'; usersLabel.append(users);
    const button = document.createElement('button'); button.type = 'submit'; button.textContent = '预览策略';
    const changed = () => { clearPlan(); users.disabled = mode.value !== 'USER_SET'; };
    form.addEventListener('input', changed); form.addEventListener('change', changed);
    form.addEventListener('submit', event => {
      event.preventDefault();
      if (!password()) { notice('请输入当前管理账号密码后再预览。'); return; }
      const userIds = users.value.split(',').map(value => value.trim()).filter(Boolean);
      const values = {mode:mode.value, user_ids:mode.value === 'USER_SET' ? userIds : []};
      const own = epoch, key = crypto.randomUUID();
      busy(button, async () => {
        const plan = await post(
          `${base()}/bindings/${encodeURIComponent(binding)}/components/` +
          `${encodeURIComponent(handler.component_id)}/message-handlers/` +
          `${encodeURIComponent(handler.handler_id)}/policy-change-plans`,
          {expected_revision:handler.revision, values, current_password:password()}, key,
        );
        if (!valid(own)) return;
        pending = {binding, handler, plan}; applyKey = crypto.randomUUID();
        $('handler-policy-diff').textContent = JSON.stringify({
          handler:`${plan.component_id}/${plan.handler_id}`, before:plan.before_values,
          before_state:plan.before_values_payload_state, after:plan.values,
        }, null, 2);
        $('handler-policy-preview').classList.remove('hidden');
      });
    });
    form.append(title, detail, modeLabel, usersLabel, button);
    if (handler.values_payload_state === 'UNAVAILABLE') {
      const warning = document.createElement('p'); warning.className = 'notice';
      warning.textContent = '当前策略正文已不可用；保存会建立完整的新修订。'; form.prepend(warning);
    }
    $('handler-policy-list').append(form);
  }
  $('handler-policy-binding').addEventListener('change', event => busy(event.currentTarget, read));
  $('handler-policy-reload').addEventListener('click', event => busy(event.currentTarget, read));
  $('handler-policy-apply').addEventListener('click', event => {
    if (!pending) return;
    if (!password()) { notice('请输入当前管理账号密码后再应用。'); return; }
    const own = epoch, frozen = pending, key = applyKey;
    busy(event.currentTarget, async () => {
      const {binding, handler, plan} = frozen;
      await post(
        `${base()}/bindings/${encodeURIComponent(binding)}/components/` +
        `${encodeURIComponent(handler.component_id)}/message-handlers/` +
        `${encodeURIComponent(handler.handler_id)}/policy-management-requests`,
        {expected_revision:plan.expected_revision, plan_id:plan.plan_id,
          plan_digest:plan.plan_digest, current_password:password()}, key,
      );
      if (!valid(own)) return;
      $('handler-policy-password').value = '';
      await read();
      if (valid(own)) notice('普通消息处理策略已应用。新消息使用新修订。', true);
    });
  });
  async function select(row) {
    reset(); bot = row; $('bot-handler-policies').classList.remove('hidden'); await read();
  }
  function invalidateContent(scope) {
    if (!bot || !scope.startsWith(base())) return;
    clearPlan(); readSeq++;
  }
  return {reset, select, refresh:read, invalidateContent};
}
