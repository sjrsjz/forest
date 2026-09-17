import { controlReceipts } from './control-receipts.js';
import { backupPanel as createBackupPanel } from './backup-panel.js';
import { managementContributions } from './management-contributions.js';

const $ = (id) => document.getElementById(id);
const csrf = () => document.cookie.split('; ').find((v) => v.startsWith('forest_csrf='))?.split('=')[1] || '';
let activeView = 'loading';
let currentUser = null;
const readReceipt = controlReceipts(invalidateControl);

function notice(text, success = false) {
  $('notice').textContent = text;
  $('notice').className = `notice${success ? ' success' : ''}`;
}
function setBranding(displayName = 'Forest') {
  $('page-title').textContent = displayName;
  document.title = `${displayName} · 管理后台`;
}
function show(id) {
  if (activeView === 'bots' && id !== 'bots') selectionEpoch++;
  activeView = id;
  for (const name of ['loading', 'setup', 'login', 'configuration', 'backups', 'bots']) $(name).classList.toggle('hidden', name !== id);
  $('main-nav').classList.toggle('hidden', !['configuration', 'backups', 'bots'].includes(id));
}
async function api(path, body, extraHeaders = {}) {
  return readReceipt(path, () => fetchApi(path, body, extraHeaders));
}
async function fetchApi(path, body, extraHeaders) {
  if (currentUser?.role === 'viewer' && body !== undefined && path !== '/logout') {
    const error = new Error('当前账号是查看者，不能提交管理变更。 (PERMISSION_DENIED)');
    error.code = 'PERMISSION_DENIED'; throw error;
  }
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
async function uploadBackupArtifact(path, artifact) {
  if (currentUser?.role === 'viewer') {
    const error = new Error('当前账号不能导入备份。(PERMISSION_DENIED)');
    error.code = 'PERMISSION_DENIED'; throw error;
  }
  const response = await fetch(`/management/v1${path}`, {
    method: 'PUT', credentials: 'same-origin', body: artifact,
    headers: {
      'Content-Type': 'application/vnd.forest.backup',
      'X-CSRF-Token': csrf(),
    },
  });
  const data = await response.json().catch(() => ({
    code: 'INVALID_RESPONSE', message: '服务暂时无法响应，请稍后重试。',
  }));
  if (!response.ok || data?.code === 'INVALID_RESPONSE') {
    const error = new Error(`${data?.message || '上传未完成'} (${data?.code || response.status})`);
    error.code = data?.code; throw error;
  }
  return data;
}
function setIdentity(user) {
  currentUser = user;
  document.body.classList.toggle('viewer', user?.role === 'viewer');
  $('username').textContent = user?.username || '';
  $('role').textContent = user?.role === 'admin' ? '管理员' : '查看者';
  $('nav-backups').classList.toggle('hidden', user?.role !== 'admin');
  if (user?.display_name) setBranding(user.display_name);
  $('identity').classList.toggle('hidden', !user);
}
async function busy(button, work) {
  button.disabled = true;
  try { await work(); } catch (error) { notice(error.message); }
  finally { button.disabled = false; }
}
async function readDeploymentWorkspace() {
  show('configuration');
  await readDeploymentModules();
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
      setIdentity(user);
      await readDeploymentWorkspace();
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
    form.reset(); $('notice').classList.add('hidden'); setIdentity(user);
    await readDeploymentWorkspace();
  });
});
$('logout').addEventListener('click', () => busy($('logout'), async () => {
  await api('/logout', {});
  runtimeViews.reset();
  deploymentViews.reset();
  backupsPanel.reset();
  selectedBot = null; selectionEpoch++;
  $('bot-detail').classList.add('hidden');
  setIdentity(null); setBranding();
  $('notice').classList.add('hidden'); show('login');
}));
let selectedBot = null, selectionEpoch = 0;
function sessionEnded(message) {
  deploymentViews.reset();
  setIdentity(null); show('login'); notice(message, true);
}
const backupsPanel = createBackupPanel(api, busy, notice, uploadBackupArtifact);

const deploymentViews = managementContributions({
  api, busy, notice, scopeKind: 'deployment', scopeIdentity: row => row.scope_id,
  nav: $('deployment-module-nav'), registry: $('deployment-module-registry'),
  root: $('deployment-module-views'), sessionEnded, setBranding, openScope,
});
async function readDeploymentModules() {
  deploymentViews.reset();
  if (!currentUser?.deployment_id) {
    deploymentViews.update([]); return;
  }
  const scope = {scope_kind:'deployment', scope_id:currentUser.deployment_id,
    account_id:currentUser.account_id};
  const result = await api(`/scopes/deployment/${encodeURIComponent(scope.scope_id)}/modules`);
  const modules = currentUser.role === 'admin' ? (result.modules || [])
    : (result.modules || []).filter(row =>
      row.management_contract?.operations?.some(operation =>
        operation.kind === 'query' && operation.principal !== 'admin'));
  deploymentViews.update(modules);
  await deploymentViews.select(scope);
}

// A Bot is a runtime scope. The deployment catalog only supplies its identity;
// every runtime surface below is mounted from the selected Bot authority.
const runtimeViews = managementContributions({
  api, busy, notice,
  nav: $('bot-module-nav'),
  registry: $('bot-module-registry'),
  root: $('bot-module-views'),
});
function invalidateControl(scope) {
  runtimeViews.invalidateContent(scope);
  deploymentViews.invalidateContent(scope);
}
function botStatus(state) {
  return ({ PENDING: '等待创建存储', PROVISIONING: '正在创建存储', READY: '存储就绪', FAILED: '创建未完成' })[state] || state;
}
async function openBotWorkspace(row, modules) {
  const epoch = ++selectionEpoch;
  runtimeViews.reset();
  selectedBot = row; $('bot-detail').classList.remove('hidden');
  show('bots');
  $('bot-name').textContent = row.display_name;
  $('bot-status').textContent = `${botStatus(row.stage)}${row.error_code ? ` · ${row.error_code}` : ''}`;
  if (row.stage === 'READY') {
    runtimeViews.update(modules || []);
    await runtimeViews.select(row);
    if (epoch !== selectionEpoch) return;
  } else {
    runtimeViews.update([]);
  }
}
async function openScope(target) {
  if (target?.kind !== 'bot' || !target.id || target.row?.bot_id !== target.id) {
    throw new Error('当前壳层无法打开该管理作用域。');
  }
  const projection = await api(
    `/scopes/${encodeURIComponent(target.kind)}/${encodeURIComponent(target.id)}/modules`
  );
  if (projection.scope?.kind !== target.kind || projection.scope?.id !== target.id) {
    throw new Error('管理作用域解析结果与请求不一致。');
  }
  await openBotWorkspace(target.row, projection.modules);
}
$('nav-deployment').addEventListener('click', () =>
  busy($('nav-deployment'), readDeploymentWorkspace));
$('nav-bots').addEventListener('click', () => show('bots'));
$('nav-backups').addEventListener('click', () => busy($('nav-backups'), async () => {
  show('backups'); await backupsPanel.load();
}));
let backupPolling = false;
setInterval(async () => {
  if (activeView !== 'backups' || backupPolling || !backupsPanel.hasRunning()) return;
  backupPolling = true;
  try { await backupsPanel.load(); }
  catch (error) { if (activeView === 'backups') notice(error.message); }
  finally { backupPolling = false; }
}, 1000);
start();
