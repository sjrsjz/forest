let token = sessionStorage.getItem('forest.management.token') || '';
let apiBase = '/management/v1';
let scopes = [];
const $ = id => document.getElementById(id);

function scopedPath(path) {
  const prefix = '/management/v1';
  return path.startsWith(`${prefix}/`) ? `${apiBase}${path.slice(prefix.length)}` : path;
}

function scopeBase(scope) {
  return `/management/v1/scopes/${encodeURIComponent(scope.scope_kind)}/${encodeURIComponent(scope.scope_id)}`;
}

function scopeKey(scope) {
  return `${scope.scope_kind}:${scope.scope_id}`;
}

async function api(path, options = {}) {
  const response = await fetch(scopedPath(path), {
    credentials: 'omit',
    ...options,
    headers: {
      'content-type': 'application/json',
      'x-forest-management-token': token,
      ...(options.headers || {}),
    },
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(body.message || body.code || `HTTP ${response.status}`);
    error.code = body.code;
    error.status = response.status;
    throw error;
  }
  return body;
}

function notice(message) {
  $('notice').textContent = message;
  $('notice').classList.toggle('hidden', !message);
}

function showOverview() {
  $('overview').classList.remove('hidden');
  $('module-view').classList.add('hidden');
  document.querySelectorAll('#module-nav button').forEach(item => item.classList.remove('selected'));
}

function renderScopes(rows) {
  const previousBase = apiBase;
  scopes = rows;
  $('scope-picker').classList.remove('hidden');
  const select = $('scope-select');
  const selected = select.value || scopeKey(scopes[0]);
  select.replaceChildren();
  for (const scope of scopes) {
    const option = document.createElement('option');
    option.value = scopeKey(scope);
    option.textContent = scope.scope_kind === 'deployment'
      ? `部署 · ${scope.label}`
      : `${scope.label} · ${scope.observed_state}`;
    option.disabled = !scope.available;
    select.append(option);
  }
  if ([...select.options].some(option => option.value === selected && !option.disabled)) {
    select.value = selected;
  }
  const active = scopes.find(scope => scopeKey(scope) === select.value && scope.available);
  if (active) apiBase = scopeBase(active);
  return previousBase !== apiBase;
}

async function openModule(row, button) {
  document.querySelectorAll('#module-nav button').forEach(
    item => item.classList.toggle('selected', item === button),
  );
  $('overview').classList.add('hidden');
  $('module-view').classList.remove('hidden');
  $('component-title').textContent = row.component_id;
  $('component-kind').textContent = `${row.state} · GENERATION`;
  const nav = $('contribution-nav');
  const root = $('contribution-root');
  nav.replaceChildren();
  root.replaceChildren();
  const contributions = row.management_contributions || [];
  if (!contributions.length) {
    const paragraph = document.createElement('p');
    paragraph.textContent = '此 Generation 没有发布管理工作区。';
    paragraph.className = 'muted';
    root.append(paragraph);
    return;
  }
  for (const [index, contribution] of contributions.entries()) {
    const tab = document.createElement('button');
    tab.textContent = contribution.label;
    nav.append(tab);
    tab.onclick = async () => {
      nav.querySelectorAll('button').forEach(
        item => item.classList.toggle('selected', item === tab),
      );
      root.replaceChildren();
      try {
        const template = await fetch(contribution.surface.template).then(response => {
          if (!response.ok) throw new Error('管理模板读取失败');
          return response.text();
        });
        root.innerHTML = template;
        const style = contribution.surface.style;
        if (style && !document.querySelector(`link[data-module-style="${CSS.escape(style)}"]`)) {
          const link = document.createElement('link');
          link.rel = 'stylesheet';
          link.href = style;
          link.dataset.moduleStyle = style;
          document.head.append(link);
        }
        const module = await import(contribution.surface.module);
        await module.mount({root, component: row, api, notice, refresh});
      } catch (error) {
        notice(error.message);
      }
    };
    if (index === 0) tab.click();
  }
}

async function refresh() {
  let snapshot;
  try {
    snapshot = await api('/management/v1/runtime');
  } catch (error) {
    if (
      apiBase !== '/management/v1'
      && ['BOT_MANAGEMENT_UNAVAILABLE', 'BOT_MANAGEMENT_SCOPE_NOT_FOUND'].includes(error.code)
    ) {
      apiBase = '/management/v1';
      return refresh();
    }
    throw error;
  }
  if (snapshot.scopes && renderScopes(snapshot.scopes)) return refresh();
  const runtime = snapshot.runtime;
  $('bot-id').textContent = runtime.bot_id;
  $('runtime-state').textContent = runtime.graph_state;
  $('graph-id').textContent = runtime.graph_id;
  $('module-count').textContent = snapshot.modules.length;
  const nav = $('module-nav');
  const table = $('module-table');
  nav.replaceChildren();
  table.replaceChildren();
  for (const row of snapshot.modules) {
    const button = document.createElement('button');
    button.textContent = row.component_id;
    button.onclick = () => openModule(row, button);
    nav.append(button);
    const item = document.createElement('div');
    item.className = 'module-row';
    const name = document.createElement('strong');
    name.textContent = row.component_id;
    const state = document.createElement('span');
    state.className = 'state';
    state.textContent = row.state;
    const generation = document.createElement('code');
    generation.textContent = row.generation_id || '—';
    item.append(name, state, generation);
    item.onclick = () => openModule(row, button);
    table.append(item);
  }
}

$('scope-select').onchange = async event => {
  const scopeId = event.currentTarget.value;
  const scope = scopes.find(item => scopeKey(item) === scopeId && item.available);
  if (!scope) return;
  apiBase = scopeBase(scope);
  notice('');
  showOverview();
  try {
    await refresh();
  } catch (error) {
    notice(error.message);
  }
};

$('login-form').onsubmit = async event => {
  event.preventDefault();
  token = new FormData(event.currentTarget).get('token');
  apiBase = '/management/v1';
  try {
    await refresh();
    sessionStorage.setItem('forest.management.token', token);
    $('login').classList.add('hidden');
    $('console').classList.remove('hidden');
    $('login-error').textContent = '';
  } catch (error) {
    $('login-error').textContent = error.message;
  }
};

$('logout').onclick = () => {
  sessionStorage.removeItem('forest.management.token');
  location.reload();
};
$('back').onclick = showOverview;

if (token) {
  refresh()
    .then(() => {
      $('login').classList.add('hidden');
      $('console').classList.remove('hidden');
    })
    .catch(() => sessionStorage.removeItem('forest.management.token'));
}
