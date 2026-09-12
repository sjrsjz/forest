const $ = (id) => document.getElementById(id);
const csrf = () => document.cookie.split('; ').find((v) => v.startsWith('forest_csrf='))?.split('=')[1] || '';
let configuration = null;
let pendingPlan = null;
let applyKey = null;

function notice(text, success = false) {
  $('notice').textContent = text;
  $('notice').className = `notice${success ? ' success' : ''}`;
}
function show(id) {
  for (const name of ['loading', 'setup', 'login', 'configuration']) $(name).classList.toggle('hidden', name !== id);
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
start();
