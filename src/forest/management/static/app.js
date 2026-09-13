import { bindingsPanel } from './bindings.js';
import { agentPanel } from './agent-panel.js';
import { memoryPanel } from './memory.js';
import { jobsPanel } from './jobs-panel.js';
import { schemaForm } from './schema-form.js';
import { controlReceipts } from './control-receipts.js';
import { replacementButton } from './configuration-forms.js';

const $ = (id) => document.getElementById(id);
const csrf = () => document.cookie.split('; ').find((v) => v.startsWith('forest_csrf='))?.split('=')[1] || '';
let configuration = null;
let pendingPlan = null;
let applyKey = null;
let activeView = 'loading';
const readReceipt = controlReceipts(invalidateControl);

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
  return readReceipt(path, () => fetchApi(path, body, extraHeaders));
}
async function fetchApi(path, body, extraHeaders) {
  const response = await fetch(`/management/v1${path}`, {
    method: body === undefined ? 'GET' : 'POST', credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrf(), ...extraHeaders },
    ...(body === undefined ? {} : { body: body instanceof Blob ? body : JSON.stringify(body) }),
  });
  const data = await response.json().catch(() => ({ code: 'INVALID_RESPONSE', message: '服务暂时无法响应，请稍后重试。' }));
  if (!response.ok || data?.code === 'INVALID_RESPONSE') {
    const error = new Error(`${data?.message || '操作未完成'} (${data?.code || response.status})`);
    error.code = data?.code;
    error.details = data?.details;
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
  bindingPanel.reset();
  modelPanel.reset();
  memoriesPanel.reset();
  scheduledPanel.reset();
  selectedBot = null; botRows = []; botConfig = null; packageRows = []; selectionEpoch++; packageReadSeq++;
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
let selectionEpoch = 0, formEpoch = 0, configReadSeq = 0, packageReadSeq = 0;
let readConfigFields, readInvokeFields, historyBefore = null, importKey = null;
let componentReplacementRequired = false, componentReplacementDraft = false;
let componentSourceRevision = null;
const replaceComponent = replacementButton($('bot-runtime-status'), 'component-replace-configuration', () => {
  if (!botConfig?.defaults || !selectedPackage()) return;
  clearBotPlan(); componentReplacementRequired = false; componentReplacementDraft = true;
  componentSourceRevision = null;
  replaceComponent.classList.add('hidden'); renderComponent(undefined, []); updateBotState(botConfig);
});
const bindingPanel = bindingsPanel(api, busy, notice);
const modelPanel = agentPanel(api, busy, notice);
const memoriesPanel = memoryPanel(api, busy, notice);
const scheduledPanel = jobsPanel(api, busy, notice);
const selectedPackage = () => packageRows.find((p) => p.package_digest === $('bot-package').value);
const componentPath = () => `/bots/${encodeURIComponent(selectedBot.bot_id)}/components/${encodeURIComponent(selectedPackage().component_id)}`;
function clearBotPlan() { formEpoch++; botPlan = null; botApplyKey = null; $('bot-config-preview').classList.add('hidden'); }
function invalidateControl(scope) {
  if (selectedBot && selectedPackage() && scope === componentPath()) {
    selectionEpoch++; configReadSeq++; clearBotPlan(); resetComponent();
    $('bot-config-diff').replaceChildren(); $('bot-invoke-result').replaceChildren();
    $('bot-runtime-status').textContent = '相关内容已不可用，请重新加载当前配置。';
  }
  for (const panel of [bindingPanel, modelPanel, memoriesPanel, scheduledPanel]) panel.invalidateContent(scope);
}
function resetComponent() {
  botConfig = null; readConfigFields = null; readInvokeFields = null; invokeKey = null;
  componentReplacementRequired = false; componentReplacementDraft = false;
  componentSourceRevision = null;
  replaceComponent.classList.add('hidden'); $('bot-grants').checked = false;
  $('bot-config-fields').replaceChildren(); $('bot-invoke-fields').replaceChildren();
  $('bot-service').replaceChildren(); $('bot-history').replaceChildren();
  $('bot-grants').disabled = true;
  $('bot-config-form').querySelector('button[type="submit"]').disabled = true;
  $('bot-invoke-form').querySelector('button[type="submit"]').disabled = true;
}
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
async function loadPackages(force = false) {
  if (packageRows.length && !force) return;
  const seq = ++packageReadSeq;
  const response = await api('/packages');
  if (seq !== packageReadSeq) return;
  const previous = $('bot-package').value;
  packageRows = response.packages;
  $('bot-package').replaceChildren();
  for (const item of packageRows) {
    const option = document.createElement('option'); option.value = item.package_digest;
    option.textContent = `${item.package_id} · ${item.version}`;
    $('bot-package').append(option);
  }
  if (packageRows.some((p) => p.package_digest === previous)) $('bot-package').value = previous;
  $('package-list').replaceChildren();
  for (const pkg of packageRows) {
    const row = document.createElement('p'); row.textContent = `${pkg.package_id} · ${pkg.version} `;
    const link = document.createElement('a'); link.textContent = '下载插件包';
    link.href = `/management/v1/packages/${encodeURIComponent(pkg.package_digest)}/archive`;
    row.append(link); $('package-list').append(row);
  }
}
function updateBotState(state) {
  if (botConfig && (state.desired_revision < botConfig.desired_revision ||
      state.effective_revision < botConfig.effective_revision)) return;
  $('bot-desired').textContent = state.desired_revision;
  $('bot-effective').textContent = state.effective_revision;
  const component = state.runtime.components.find((c) => c.component_id === selectedPackage().component_id);
  const error = component?.error_code || state.runtime.error_code;
  $('bot-runtime-status').textContent = error ? `组件暂未就绪：${error}` :
    component?.worker_state === 'ACTIVE' && component.loaded_revision === state.desired_revision ? '组件已生效并运行中' :
    state.desired_revision ? '已保存，等待组件启动或切换…' : '保存组件配置后即可启动';
  const unavailable = [];
  if (state.desired_values_payload_state === 'UNAVAILABLE') unavailable.push('已保存配置正文已清理');
  if (state.effective_values_payload_state === 'UNAVAILABLE') unavailable.push('已生效配置正文已清理');
  if (componentReplacementDraft) unavailable.push('正在填写独立新配置草稿');
  if (unavailable.length) $('bot-runtime-status').textContent += ` · ${unavailable.join(' · ')}`;
  $('bot-operations').replaceChildren();
  for (const op of state.operations || []) {
    const row = document.createElement('li');
    row.textContent = `${op.created_at} · ${op.phase}${op.error_code ? ` · ${op.error_code}` : ''}`;
    $('bot-operations').append(row);
  }
}
function renderComponent(config, grants = []) {
  const pkg = selectedPackage();
  readConfigFields = schemaForm($('bot-config-fields'), pkg.config_schema, config);
  $('bot-grants').required = pkg.grants.length > 0;
  $('bot-grants').disabled = false;
  $('bot-config-form').querySelector('button[type="submit"]').disabled = false;
  $('bot-grants').checked = pkg.grants.every((g) => grants.includes(g));
  $('bot-grants-label').textContent = pkg.grants.length ? `允许组件使用：${pkg.grants.join('、')}` : '此组件未申请额外能力';
  $('bot-service').replaceChildren();
  for (const service of pkg.services) {
    const option = document.createElement('option'); option.value = service.name; option.textContent = service.name;
    $('bot-service').append(option);
  }
  renderService();
}
function renderService() {
  const service = selectedPackage().services.find((s) => s.name === $('bot-service').value);
  $('bot-invoke-form').classList.toggle('hidden', !service);
  readInvokeFields = service ? schemaForm($('bot-invoke-fields'), service.input_schema) : null;
  $('bot-invoke-form').querySelector('button[type="submit"]').disabled = !service;
  invokeKey = null; $('bot-invoke-result').textContent = '';
}
async function readBotConfig() {
  const epoch = selectionEpoch, edit = formEpoch, seq = ++configReadSeq;
  const state = await api(`${componentPath()}/configuration`);
  if (epoch !== selectionEpoch || seq !== configReadSeq) return;
  if (edit !== formEpoch) {
    updateBotState(state); notice('读取期间有新的配置输入，已保留。需要丢弃草稿时可再次重新加载。'); return;
  }
  botConfig = state; clearBotPlan(); componentReplacementDraft = false;
  componentSourceRevision = state.desired_values_payload_state === 'ACTIVE' && state.desired_values !== null
    ? state.desired_revision : null;
  componentReplacementRequired = state.desired_values_payload_state === 'UNAVAILABLE' ||
    (state.desired_revision > 0 && state.desired_values === null);
  replaceComponent.classList.toggle('hidden', !componentReplacementRequired);
  replaceComponent.disabled = !state.defaults;
  updateBotState(state);
  if (componentReplacementRequired) {
    readConfigFields = null; $('bot-config-fields').replaceChildren(); $('bot-grants').checked = false;
    $('bot-grants').disabled = true; $('bot-config-form').querySelector('button[type="submit"]').disabled = true;
  } else {
    renderComponent(state.desired_values?.config, state.desired_values?.grants);
  }
  $('bot-history').replaceChildren(); historyBefore = null;
}
async function selectBot(id) {
  const epoch = ++selectionEpoch;
  bindingPanel.reset();
  modelPanel.reset();
  memoriesPanel.reset();
  scheduledPanel.reset();
  clearBotPlan(); invokeKey = null; $('bot-invoke-result').textContent = '';
  resetComponent();
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
  if (row.stage === 'READY') {
    await loadPackages(); if (epoch !== selectionEpoch) return;
    await readBotConfig(); if (epoch !== selectionEpoch) return;
    await bindingPanel.select(row);
    if (epoch !== selectionEpoch) return;
    await modelPanel.select(row);
    if (epoch !== selectionEpoch) return;
    await scheduledPanel.select(row);
    if (epoch !== selectionEpoch) return;
    await memoriesPanel.select(row);
  }
}
$('nav-deployment').addEventListener('click', () => busy($('nav-deployment'), readConfiguration));
$('nav-bots').addEventListener('click', () => busy($('nav-bots'), async () => { show('bots'); await readBots(); await loadPackages(); }));
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
$('bot-package').addEventListener('change', () => {
  selectionEpoch++; clearBotPlan(); resetComponent();
  busy($('reload-bot-config'), readBotConfig);
});
$('reload-bot-config').addEventListener('click', () => busy($('reload-bot-config'), readBotConfig));
$('bot-config-form').addEventListener('input', clearBotPlan);
$('bot-config-form').addEventListener('submit', (event) => {
  event.preventDefault(); const form = event.currentTarget;
  if (!botConfig || componentReplacementRequired || !readConfigFields || !form.reportValidity()) return;
  busy(form.querySelector('button'), async () => {
    const epoch = selectionEpoch, edit = formEpoch, pkg = selectedPackage(), config = readConfigFields();
    const plan = await api(`${componentPath()}/change-plans`, {expected_revision: botConfig.desired_revision,
      ...(componentSourceRevision === null ? {} : {source_revision: componentSourceRevision}),
      values: {package_digest: pkg.package_digest, config, grants: [...pkg.grants].sort()}}, {'Idempotency-Key': crypto.randomUUID()});
    if (epoch !== selectionEpoch || edit !== formEpoch) return;
    botPlan = plan; botApplyKey = crypto.randomUUID();
    $('bot-config-diff').textContent = JSON.stringify({当前: botConfig.desired_values,
      待应用: {插件: `${pkg.package_id} ${pkg.version}`, config, grants: pkg.grants},
      状态格式: pkg.state_schema}, null, 2);
    $('bot-config-preview').classList.remove('hidden');
  });
});
$('bot-config-apply').addEventListener('click', () => busy($('bot-config-apply'), async () => {
  if (!botPlan) return;
  const epoch = selectionEpoch, edit = formEpoch;
  await api(`${componentPath()}/management-requests`, {plan_id:botPlan.plan_id, plan_digest:botPlan.plan_digest,
    expected_revision:botPlan.expected_revision}, {'Idempotency-Key':botApplyKey});
  if (epoch !== selectionEpoch || edit !== formEpoch) return;
  await readBotConfig(); notice('配置已保存，正在启动或切换组件。', true);
}));
$('bot-invoke-form').addEventListener('input', () => { invokeKey = null; });
$('bot-service').addEventListener('change', renderService);
$('bot-invoke-form').addEventListener('submit', (event) => {
  event.preventDefault(); const form = event.currentTarget;
  busy(form.querySelector('button'), async () => {
    invokeKey ||= crypto.randomUUID(); const epoch = selectionEpoch, sentKey = invokeKey;
    const data = await api(`${componentPath()}/invoke`, {service: $('bot-service').value,
      payload: readInvokeFields()}, {'Idempotency-Key':sentKey});
    if (epoch !== selectionEpoch || invokeKey !== sentKey) return;
    invokeKey = null;
    $('bot-invoke-result').textContent = data.result?.label !== undefined && data.result?.revision !== undefined ?
      `${data.result.label}：${data.result.value}（状态修订 ${data.result.revision}）` : JSON.stringify(data.result, null, 2);
  });
});
$('package-import-form').addEventListener('input', () => { importKey = null; });
$('package-import-form').addEventListener('submit', (event) => {
  event.preventDefault(); const form = event.currentTarget;
  busy(form.querySelector('button'), async () => {
    const file = form.elements.archive.files[0];
    if (!file || file.size > 32 * 1024 * 1024) throw new Error('请选择不超过 32 MiB 的插件 ZIP 包。');
    importKey ||= crypto.randomUUID(); const sentKey = importKey;
    const pkg = await api('/packages', file, {'Content-Type': 'application/zip', 'Idempotency-Key': sentKey});
    if (importKey === sentKey) { importKey = null; form.reset(); }
    await loadPackages(true);
    notice(`已导入 ${pkg.package_id} ${pkg.version}。选择 Bot 和插件版本，预览配置后即可启用。`, true);
  });
});
async function readHistory(append = false) {
  if (!selectedBot || !botConfig) return;
  const epoch = selectionEpoch, path = componentPath();
  const data = await api(`${path}/configuration/history${append && historyBefore ? `?before=${historyBefore}` : ''}`);
  if (epoch !== selectionEpoch) return;
  if (!append) $('bot-history').replaceChildren();
  historyBefore = data.next_before;
  $('bot-history-more').classList.toggle('hidden', !historyBefore);
  for (const revision of data.revisions) {
    const row = document.createElement('div');
    const unavailable = revision.values_payload_state === 'UNAVAILABLE' || !revision.values;
    const pkg = !unavailable && packageRows.find((p) => p.package_digest === revision.values.package_digest);
    const text = document.createElement('p');
    text.textContent = `修订 ${revision.revision} · ${revision.created_at} · ${unavailable ? '配置正文已清理' : pkg?.version || '插件包不可用'}`;
    const button = document.createElement('button'); button.className = 'subtle';
    button.textContent = '载入此配置'; button.disabled = !pkg;
    button.addEventListener('click', () => {
      if (epoch !== selectionEpoch) return;
      $('bot-package').value = pkg.package_digest;
      clearBotPlan(); componentReplacementRequired = false; componentReplacementDraft = true;
      componentSourceRevision = revision.revision;
      replaceComponent.classList.remove('hidden'); replaceComponent.disabled = !botConfig.defaults;
      renderComponent(revision.values.config, revision.values.grants);
      updateBotState(botConfig);
      notice(`已载入修订 ${revision.revision}。预览并应用后会创建新的配置修订；持久状态按兼容性迁移。`, true);
    });
    row.append(text, button); $('bot-history').append(row);
  }
}
$('read-bot-history').addEventListener('click', () => busy($('read-bot-history'), () => readHistory()));
$('bot-history-more').addEventListener('click', () => busy($('bot-history-more'), () => readHistory(true)));
let polling = false;
setInterval(async () => {
  if (activeView !== 'bots' || polling) return;
  polling = true;
  try {
    await readBots();
    if (selectedBot) {
      const row = botRows.find((r) => r.bot_id === selectedBot.bot_id);
      if (row && row.stage !== selectedBot.stage) await selectBot(row.bot_id);
      else if (selectedBot.stage === 'READY' && selectedPackage()) {
        const epoch = selectionEpoch, state = await api(`${componentPath()}/configuration`);
        if (epoch === selectionEpoch) { updateBotState(state); await bindingPanel.refresh(); }
      }
    }
  } catch (error) { if (activeView === 'bots') notice(error.message); }
  finally { polling = false; }
}, 2500);
start();
