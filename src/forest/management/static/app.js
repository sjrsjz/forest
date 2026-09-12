const $ = (id) => document.getElementById(id);
const csrf = () => document.cookie.split('; ').find((v) => v.startsWith('forest_csrf='))?.split('=')[1] || '';
let configuration = null;
let pendingPlan = null;
let applyKey = null;
let activeView = 'loading';

function notice(text, success = false) {
  $('notice').textContent = text;
  $('notice').className = `notice${success ? ' success' : ''}`;
}
function show(id) {
  if (activeView === 'bots' && id !== 'bots') selectionEpoch++;
  activeView = id;
  for (const name of ['loading', 'setup', 'login', 'configuration', 'bots']) $(name).classList.toggle('hidden', name !== id);
  $('main-nav').classList.toggle('hidden', !['configuration', 'bots'].includes(id));
}
async function api(path, body, extraHeaders = {}) {
  const response = await fetch(`/management/v1${path}`, {
    method: body === undefined ? 'GET' : 'POST', credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrf(), ...extraHeaders },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  const data = await response.json().catch(() => ({ code: 'INVALID_RESPONSE', message: '服务暂时无法响应，请稍后重试。' }));
  if (!response.ok || data.code === 'INVALID_RESPONSE') {
    const error = new Error(`${data.message || '操作未完成'} (${data.code || response.status})`);
    error.code = data.code;
    throw error;
  }
  return data;
}
async function busy(button, work) {
  button.disabled = true;
  try { await work(); } catch (error) { notice(error.message); }
  finally { button.disabled = false; }
}
async function readConfiguration() {
  configuration = await api('/configuration');
  const values = configuration.effective?.values || configuration.defaults;
  const fields = $('config-form').elements;
  fields.display_name.value = values.display_name;
  fields.session_ttl_minutes.value = values.session_ttl_minutes;
  $('page-title').textContent = values.display_name;
  document.title = `${values.display_name} · 管理后台`;
  $('revision').textContent = configuration.head_revision;
  $('desired').textContent = configuration.desired?.revision || '默认';
  $('effective').textContent = configuration.effective?.revision || '默认';
  $('config-state').textContent = configuration.effective ? '已生效' : '默认配置';
  pendingPlan = null; applyKey = null; $('preview').classList.add('hidden');
  show('configuration');
}
async function start() {
  try {
    const status = await api('/setup');
    if (status.setup_required) {
      show('setup');
      $('dsn-label').classList.toggle('hidden', status.managed_database_available);
      $('managed-db').classList.toggle('hidden', !status.managed_database_available);
      if (status.stage === 'FROZEN') notice('已有固定初始化计划，服务正在自动恢复。可用原信息重试或稍后刷新查看结果。');
      return;
    }
    if (!status.database_ready) { show('loading'); notice('部署已初始化，但数据库未就绪。服务会自动重试，恢复数据库后请刷新页面。'); return; }
    try {
      const user = await api('/session');
      $('username').textContent = user.username; $('identity').classList.remove('hidden');
      await readConfiguration();
    } catch (error) {
      if (error.code === 'UNAUTHENTICATED') show('login'); else throw error;
    }
  } catch (error) { notice(error.message); }
}
$('setup-form').addEventListener('submit', (event) => {
  event.preventDefault();
  const form = event.currentTarget;
  busy(form.querySelector('button'), async () => {
    const values = Object.fromEntries(new FormData(form));
    if (!values.dsn) delete values.dsn;
    await api('/setup', values);
    form.reset(); show('login'); notice('初始化完成，请使用刚创建的管理账号登录。', true);
  });
});
$('login-form').addEventListener('submit', (event) => {
  event.preventDefault();
  const form = event.currentTarget;
  busy(form.querySelector('button'), async () => {
    const user = await api('/login', Object.fromEntries(new FormData(form)));
    form.reset(); $('notice').classList.add('hidden'); $('username').textContent = user.username;
    $('identity').classList.remove('hidden'); await readConfiguration();
  });
});
$('logout').addEventListener('click', () => busy($('logout'), async () => {
  await api('/logout', {}); configuration = null; pendingPlan = null; applyKey = null;
  selectedBot = null; botRows = []; botConfig = null; packageRows = []; selectionEpoch++;
  clearBotPlan(); createKey = null; invokeKey = null; $('bot-list').replaceChildren();
  $('bot-detail').classList.add('hidden'); $('create-bot-form').reset();
  $('config-form').reset(); $('diff').replaceChildren(); $('identity').classList.add('hidden');
  $('notice').classList.add('hidden'); show('login');
}));
$('config-form').addEventListener('input', () => { pendingPlan = null; applyKey = null; $('preview').classList.add('hidden'); });
$('config-form').addEventListener('submit', (event) => {
  event.preventDefault();
  busy(event.currentTarget.querySelector('button'), async () => {
    const fields = $('config-form').elements;
    pendingPlan = await api('/change-plans', { expected_revision: configuration.head_revision,
      values: { display_name: fields.display_name.value, session_ttl_minutes: Number(fields.session_ttl_minutes.value) } });
    applyKey = crypto.randomUUID(); $('diff').replaceChildren();
    for (const change of pendingPlan.diff) {
      const li = document.createElement('li');
      li.textContent = `${change.field === 'display_name' ? '显示名称' : '会话时长'}：${change.before} → ${change.after}`;
      $('diff').append(li);
    }
    $('effect').textContent = pendingPlan.effect;
    $('preview').classList.remove('hidden');
    $('notice').classList.add('hidden');
  });
});
$('apply').addEventListener('click', () => busy($('apply'), async () => {
  if (!pendingPlan) return;
  await api('/management-requests', { plan_id: pendingPlan.plan_id, plan_digest: pendingPlan.plan_digest,
    expected_revision: pendingPlan.expected_revision }, { 'Idempotency-Key': applyKey });
  await readConfiguration(); notice('配置已提交并生效。', true);
}));
$('reload').addEventListener('click', () => busy($('reload'), readConfiguration));
let botRows = [], selectedBot = null, botConfig = null, packageRows = [];
let botPlan = null, botApplyKey = null, createKey = null, invokeKey = null, renameKey = null, retryKey = null;
let selectionEpoch = 0, formEpoch = 0;
const selectedPackage = () => packageRows.find((p) => p.package_digest === $('bot-package').value);
const componentPath = () => `/bots/${encodeURIComponent(selectedBot.bot_id)}/components/${encodeURIComponent(selectedPackage().component_id)}`;
function clearBotPlan() { formEpoch++; botPlan = null; botApplyKey = null; $('bot-config-preview').classList.add('hidden'); }
function botStatus(state) {
  return ({ PENDING: '等待创建存储', PROVISIONING: '正在创建存储', READY: '存储就绪', FAILED: '创建未完成' })[state] || state;
}
async function readBots() {
  const data = await api('/bots'); botRows = data.bots;
  $('bot-list').replaceChildren();
  if (!botRows.length) $('bot-list').textContent = '还没有 Bot。从右侧创建第一个。';
  for (const row of botRows) {
    const button = document.createElement('button'); button.className = 'bot-row subtle';
    button.textContent = `${row.display_name} · ${botStatus(row.stage)}`;
    button.dataset.botId = row.bot_id;
    button.addEventListener('click', () => busy(button, () => selectBot(row.bot_id)));
    $('bot-list').append(button);
  }
  const mode = $('create-bot-form').elements.mode;
  mode.options[0].disabled = !data.managed_available;
  if (!data.managed_available) mode.value = 'external';
  $('bot-dsn-label').classList.toggle('hidden', mode.value !== 'external');
}
async function loadPackages() {
  if (packageRows.length) return;
  packageRows = (await api('/packages')).packages;
  $('bot-package').replaceChildren();
  for (const item of packageRows) {
    const option = document.createElement('option'); option.value = item.package_digest;
    option.textContent = item.package_id === 'forest.counter' ? '计数器（内置演示组件）' : item.package_id;
    $('bot-package').append(option);
  }
}
function updateBotState(state) {
  $('bot-desired').textContent = state.desired_revision;
  $('bot-effective').textContent = state.effective_revision;
  const component = state.runtime.components.find((c) => c.component_id === selectedPackage().component_id);
  const error = component?.error_code || state.runtime.error_code;
  $('bot-runtime-status').textContent = error ? `组件暂未就绪：${error}` :
    component?.worker_state === 'ACTIVE' && component.loaded_revision === state.desired_revision ? '组件已生效并运行中' :
    state.desired_revision ? '已保存，等待组件启动或切换…' : '保存组件配置后即可启动';
}
async function readBotConfig() {
  const epoch = selectionEpoch;
  const state = await api(`${componentPath()}/configuration`);
  if (epoch !== selectionEpoch) return;
  botConfig = state; clearBotPlan(); updateBotState(state);
  const values = state.desired_values?.config || { label: selectedBot.display_name, multiplier: 1 };
  $('bot-config-fields').replaceChildren();
  for (const [name, schema] of Object.entries(selectedPackage().config_schema.properties)) {
    const label = document.createElement('label'); label.textContent = ({label: '结果显示名称', multiplier: '增加量倍率'})[name] || name;
    const input = document.createElement('input'); input.name = name; input.required = true;
    input.type = schema.type === 'integer' ? 'number' : 'text';
    if (schema.minimum !== undefined) input.min = schema.minimum;
    if (schema.maximum !== undefined) input.max = schema.maximum;
    if (schema.maxLength !== undefined) input.maxLength = schema.maxLength;
    input.value = values[name] ?? ''; label.append(input); $('bot-config-fields').append(label);
  }
  $('bot-grants').checked = !!state.desired_values;
}
async function selectBot(id) {
  const epoch = ++selectionEpoch;
  clearBotPlan(); invokeKey = null; $('bot-invoke-result').textContent = '';
  renameKey = null; retryKey = null;
  const row = await api(`/bots/${encodeURIComponent(id)}`);
  if (epoch !== selectionEpoch) return;
  selectedBot = row; $('bot-detail').classList.remove('hidden');
  $('bot-name').textContent = row.display_name;
  $('rename-bot-form').elements.display_name.value = row.display_name;
  $('bot-status').textContent = `${botStatus(row.stage)}${row.error_code ? ` · ${row.error_code}` : ''}`;
  $('retry-bot').classList.toggle('hidden', row.stage !== 'FAILED');
  $('bot-components').classList.toggle('hidden', row.stage !== 'READY');
  const job = await api(`/provision-jobs/${encodeURIComponent(row.job_id)}`);
  if (epoch !== selectionEpoch) return;
  $('bot-job-events').replaceChildren();
  for (const entry of job.events) {
    const li = document.createElement('li'); li.textContent = `${entry.step} · ${entry.created_at}`;
    $('bot-job-events').append(li);
  }
  if (row.stage === 'READY') { await loadPackages(); await readBotConfig(); }
}
$('nav-deployment').addEventListener('click', () => busy($('nav-deployment'), readConfiguration));
$('nav-bots').addEventListener('click', () => busy($('nav-bots'), async () => { show('bots'); await readBots(); }));
$('refresh-bots').addEventListener('click', () => busy($('refresh-bots'), readBots));
$('create-bot-form').addEventListener('input', () => {
  createKey = null;
  $('bot-dsn-label').classList.toggle('hidden', $('create-bot-form').elements.mode.value !== 'external');
});
$('create-bot-form').addEventListener('submit', (event) => {
  event.preventDefault(); const form = event.currentTarget;
  busy(form.querySelector('button'), async () => {
    createKey ||= crypto.randomUUID(); const values = Object.fromEntries(new FormData(form));
    if (values.mode === 'managed' || !values.dsn) delete values.dsn;
    const row = await api('/bots', {...values, expected_revision: 0}, {'Idempotency-Key': createKey});
    form.reset(); createKey = null; await readBots(); await selectBot(row.bot_id);
    notice('Bot 创建任务已接纳。存储进度会自动更新。', true);
  });
});
$('rename-bot-form').addEventListener('submit', (event) => {
  event.preventDefault(); const form = event.currentTarget;
  busy(form.querySelector('button'), async () => {
    renameKey ||= crypto.randomUUID();
    const row = await api(`/bots/${selectedBot.bot_id}/rename`, {display_name: form.elements.display_name.value,
      expected_revision: selectedBot.revision}, {'Idempotency-Key': renameKey});
    await readBots(); await selectBot(row.bot_id); notice('Bot 名称已更新。', true);
  });
});
$('rename-bot-form').addEventListener('input', () => { renameKey = null; });
$('retry-bot').addEventListener('click', () => busy($('retry-bot'), async () => {
  retryKey ||= crypto.randomUUID();
  await api(`/bots/${selectedBot.bot_id}/retry`, {expected_revision: selectedBot.revision}, {'Idempotency-Key': retryKey});
  await readBots(); await selectBot(selectedBot.bot_id);
}));
$('bot-package').addEventListener('change', () => { selectionEpoch++; clearBotPlan(); busy($('reload-bot-config'), readBotConfig); });
$('reload-bot-config').addEventListener('click', () => busy($('reload-bot-config'), readBotConfig));
$('bot-config-form').addEventListener('input', clearBotPlan);
$('bot-config-form').addEventListener('submit', (event) => {
  event.preventDefault(); const form = event.currentTarget;
  busy(form.querySelector('button'), async () => {
    const epoch = selectionEpoch, edit = formEpoch, pkg = selectedPackage(); const config = {};
    for (const [name, schema] of Object.entries(pkg.config_schema.properties)) {
      config[name] = schema.type === 'integer' ? Number(form.elements[name].value) : form.elements[name].value;
    }
    const plan = await api(`${componentPath()}/change-plans`, {expected_revision: botConfig.desired_revision,
      values: {package_digest: pkg.package_digest, config, grants: [...pkg.grants].sort()}}, {'Idempotency-Key': crypto.randomUUID()});
    if (epoch !== selectionEpoch || edit !== formEpoch) return;
    botPlan = plan; botApplyKey = crypto.randomUUID();
    $('bot-config-diff').textContent = Object.entries(config).map(([name, value]) => `${({label:'结果显示名称',multiplier:'增加量倍率'})[name] || name}：${value}`).join('\n');
    $('bot-config-preview').classList.remove('hidden');
  });
});
$('bot-config-apply').addEventListener('click', () => busy($('bot-config-apply'), async () => {
  if (!botPlan) return;
  const epoch = selectionEpoch;
  await api(`${componentPath()}/management-requests`, {plan_id:botPlan.plan_id, plan_digest:botPlan.plan_digest,
    expected_revision:botPlan.expected_revision}, {'Idempotency-Key':botApplyKey});
  if (epoch !== selectionEpoch) return;
  await readBotConfig(); notice('配置已保存，正在启动或切换组件。', true);
}));
$('bot-invoke-form').addEventListener('input', () => { invokeKey = null; });
$('bot-invoke-form').addEventListener('submit', (event) => {
  event.preventDefault(); const form = event.currentTarget;
  busy(form.querySelector('button'), async () => {
    invokeKey ||= crypto.randomUUID(); const epoch = selectionEpoch;
    const data = await api(`${componentPath()}/invoke`, {service:'counter.increment',
      payload:{increment:Number(form.elements.increment.value)}}, {'Idempotency-Key':invokeKey});
    if (epoch !== selectionEpoch) return;
    invokeKey = null;
    $('bot-invoke-result').textContent = `${data.result.label}：${data.result.value}（状态修订 ${data.result.revision}）`;
  });
});
let polling = false;
setInterval(async () => {
  if (activeView !== 'bots' || polling) return;
  polling = true;
  try {
    await readBots();
    if (selectedBot) {
      const row = botRows.find((r) => r.bot_id === selectedBot.bot_id);
      if (row && row.stage !== selectedBot.stage) await selectBot(row.bot_id);
      else if (selectedBot.stage === 'READY') {
        const epoch = selectionEpoch, state = await api(`${componentPath()}/configuration`);
        if (epoch === selectionEpoch) updateBotState(state);
      }
    }
  } catch (error) { if (activeView === 'bots') notice(error.message); }
  finally { polling = false; }
}, 2500);
start();
