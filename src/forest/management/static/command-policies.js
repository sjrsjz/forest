const $ = (id) => document.getElementById(id);

export function commandPoliciesPanel(api, busy, notice) {
  let bot = null, epoch = 0, readSeq = 0, pending = null, applyKey = null;
  const base = () => `/bots/${encodeURIComponent(bot.bot_id)}`;
  const password = () => $('command-policy-password').value;
  const post = (path, body, key) => api(path, body, {'Idempotency-Key':key});
  const valid = (own) => bot !== null && own === epoch;
  function clearPlan() {
    pending = null; applyKey = null; $('command-policy-preview').classList.add('hidden');
  }
  function reset() {
    epoch++; readSeq++; bot = null; clearPlan();
    $('bot-command-policies').classList.add('hidden');
    $('command-policy-binding').replaceChildren();
    $('command-policy-list').replaceChildren();
    $('command-policy-status').textContent = '';
    $('command-policy-password').value = '';
  }
  async function loadBindings() {
    const own = epoch, data = await api(`${base()}/bindings`);
    if (!valid(own)) return;
    const select = $('command-policy-binding'), previous = select.value;
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
    const binding = $('command-policy-binding').value;
    clearPlan(); $('command-policy-list').replaceChildren();
    if (!binding) {
      $('command-policy-status').textContent = '还没有已登记的 Binding。'; return;
    }
    const data = await api(`${base()}/bindings/${encodeURIComponent(binding)}/command-policies`);
    if (!valid(own) || sequence !== readSeq || binding !== $('command-policy-binding').value) return;
    $('command-policy-status').textContent =
      `Binding 策略 · 命令注册表修订 ${data.registry_revision} · ${data.commands.length} 条有效命令`;
    for (const command of data.commands) renderCommand(binding, command);
    if (!data.commands.length) $('command-policy-list').textContent = '当前 Bot 没有已启用的业务命令。';
  }
  function renderCommand(binding, command) {
    const form = document.createElement('form'); form.className = 'card';
    const title = document.createElement('h3'); title.textContent = command.name;
    const detail = document.createElement('p'); detail.className = 'hint';
    detail.textContent = `${command.component_id} · ${command.permission} · 策略修订 ${command.revision}`;
    const modeLabel = document.createElement('label'); modeLabel.textContent = '允许用户';
    const mode = document.createElement('select'); mode.name = 'mode';
    for (const [value, label] of [
      ['DISABLED','禁用，不认领消息'], ['EVERYONE','所有可信平台用户'],
      ['ADMINS','当前 Binding 管理员'], ['USER_SET','指定平台用户'],
    ]) {
      const option = document.createElement('option'); option.value = value; option.textContent = label;
      mode.append(option);
    }
    mode.value = command.values?.mode || 'DISABLED'; modeLabel.append(mode);
    const usersLabel = document.createElement('label'); usersLabel.textContent = '指定 QQ 号';
    const users = document.createElement('input'); users.name = 'user_ids'; users.maxLength = 1400;
    users.placeholder = '多个账号用逗号分隔'; users.value = (command.values?.user_ids || []).join(',');
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
          `${base()}/bindings/${encodeURIComponent(binding)}/commands/` +
          `${encodeURIComponent(command.command_id)}/policy-change-plans`,
          {expected_revision:command.revision, values, current_password:password()}, key,
        );
        if (!valid(own)) return;
        pending = {binding, command, plan}; applyKey = crypto.randomUUID();
        $('command-policy-diff').textContent = JSON.stringify({
          command:plan.name, before:plan.before_values,
          before_state:plan.before_values_payload_state, after:plan.values,
        }, null, 2);
        $('command-policy-preview').classList.remove('hidden');
      });
    });
    form.append(title, detail, modeLabel, usersLabel, button);
    if (command.values_payload_state === 'UNAVAILABLE') {
      const warning = document.createElement('p'); warning.className = 'notice';
      warning.textContent = '当前策略正文已不可用；保存会建立完整的新修订。'; form.prepend(warning);
    }
    $('command-policy-list').append(form);
  }
  $('command-policy-binding').addEventListener('change', event => busy(event.currentTarget, read));
  $('command-policy-reload').addEventListener('click', event => busy(event.currentTarget, read));
  $('command-policy-apply').addEventListener('click', event => {
    if (!pending) return;
    if (!password()) { notice('请输入当前管理账号密码后再应用。'); return; }
    const own = epoch, frozen = pending, key = applyKey;
    busy(event.currentTarget, async () => {
      const {binding, command, plan} = frozen;
      await post(
        `${base()}/bindings/${encodeURIComponent(binding)}/commands/` +
        `${encodeURIComponent(command.command_id)}/policy-management-requests`,
        {expected_revision:plan.expected_revision, plan_id:plan.plan_id,
          plan_digest:plan.plan_digest, current_password:password()}, key,
      );
      if (!valid(own)) return;
      $('command-policy-password').value = '';
      await read();
      if (valid(own)) notice('业务命令策略已应用。新消息使用新修订。', true);
    });
  });
  async function select(row) {
    reset(); bot = row; $('bot-command-policies').classList.remove('hidden'); await read();
  }
  function invalidateContent(scope) {
    if (!bot || !scope.startsWith(base())) return;
    clearPlan(); readSeq++;
  }
  return {reset, select, refresh:read, invalidateContent};
}
