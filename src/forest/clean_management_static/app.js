let apiBase = '/management/v1';
let scopes = [];
const $ = id => document.getElementById(id);

function cookie(name) {
  const prefix = `${encodeURIComponent(name)}=`;
  const item = document.cookie.split('; ').find(value => value.startsWith(prefix));
  return item ? decodeURIComponent(item.slice(prefix.length)) : '';
}

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
    credentials: 'same-origin',
    ...options,
    headers: {
      'content-type': 'application/json',
      ...(options.method && options.method !== 'GET'
        ? {'x-forest-csrf': cookie('forest_management_csrf')}
        : {}),
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

let activeSandboxCleanup = null;

async function fetchText(path) {
  const response = await fetch(scopedPath(path), {credentials: 'same-origin'});
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.message || body.code || `HTTP ${response.status}`);
  }
  return response.text();
}

async function sha256Hex(text) {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
  return [...new Uint8Array(digest)].map(value => value.toString(16).padStart(2, '0')).join('');
}

async function mountSandboxedPackage(root, row, contribution) {
  const surface = contribution.surface;
  const html = await fetchText(surface.artifact);
  if (await sha256Hex(html) !== surface.sha256) {
    throw new Error('模块管理 UI 摘要校验失败');
  }
  const base = `/management/v1/modules/${encodeURIComponent(row.component_id)}/configuration`;
  const configuration = await api(base);
  let draft = structuredClone(configuration.desired.value);
  let plan = null;
  let messages = [];
  const operationReplies = new Map();
  const nonce = crypto.randomUUID();
  const channel = new MessageChannel();
  const blobUrl = URL.createObjectURL(new Blob([html], {type: 'text/html'}));
  const shell = document.createElement('div');
  shell.className = 'sandbox-shell';
  const controls = document.createElement('div');
  controls.className = 'sandbox-controls';
  const state = document.createElement('span');
  state.textContent = '等待模块 UI';
  const discard = document.createElement('button');
  discard.className = 'quiet';
  discard.textContent = '丢弃草稿';
  const apply = document.createElement('button');
  apply.textContent = '应用计划';
  apply.disabled = true;
  controls.append(state, discard, apply);
  const frame = document.createElement('iframe');
  frame.className = 'package-ui-frame';
  frame.sandbox = 'allow-scripts';
  frame.referrerPolicy = 'no-referrer';
  frame.title = contribution.label;
  shell.append(controls, frame);
  root.append(shell);

  const send = (type, payload = {}) => channel.port1.postMessage({
    protocol: 'forest.management-ui-channel/1', type, nonce, ...payload,
  });
  const fail = error => {
    const message = error instanceof Error ? error.message : String(error);
    state.textContent = message;
    send('error', {message});
  };
  channel.port1.onmessage = async event => {
    const message = event.data;
    let encoded;
    try { encoded = JSON.stringify(message); } catch { return; }
    const now = Date.now();
    messages = messages.filter(value => now - value < 1000);
    if (
      !message || message.protocol !== 'forest.management-ui-channel/1'
      || message.nonce !== nonce || encoded.length > 262144 || messages.length >= 20
    ) return;
    messages.push(now);
    try {
      if (message.type === 'ready') {
        state.textContent = 'UI 已隔离连接';
        send('initialize', {
          component: {
            component_id: row.component_id,
            generation_id: row.generation_id,
            package: row.artifact,
          },
          configuration,
        });
      } else if (message.type === 'set-draft') {
        if (!message.value || Array.isArray(message.value) || typeof message.value !== 'object') {
          throw new Error('模块 UI 草稿必须是对象');
        }
        draft = structuredClone(message.value);
        plan = null;
        apply.disabled = true;
        state.textContent = '草稿未生成计划';
      } else if (message.type === 'validate' || message.type === 'request-plan') {
        const key = crypto.randomUUID();
        plan = await api(`${base}/plans`, {
          method: 'POST',
          headers: {'idempotency-key': `${key}:plan`},
          body: JSON.stringify({
            expected_desired_revision: configuration.desired.revision,
            value: draft,
          }),
        });
        apply.dataset.key = key;
        apply.disabled = false;
        state.textContent = '计划已校验，等待管理员应用';
        send('plan-ready', {plan_id: plan.plan_id});
      } else if (message.type === 'discard-draft') {
        discard.click();
      } else if (message.type === 'resize') {
        send('resize-assigned', {height: frame.clientHeight});
      } else if (message.type === 'query' || message.type === 'action') {
        const requestId = message.request_id;
        const operationId = message.operation_id;
        if (
          typeof requestId !== 'string' || !/^[a-zA-Z0-9._:-]{1,128}$/.test(requestId)
          || typeof operationId !== 'string'
          || !/^[a-z0-9][a-z0-9._-]{0,127}$/.test(operationId)
          || !message.value || Array.isArray(message.value) || typeof message.value !== 'object'
        ) throw new Error('模块 UI 操作请求无效');
        const prior = operationReplies.get(requestId);
        if (prior) {
          send(prior.type, prior.payload);
          return;
        }
        const kind = message.type === 'query' ? 'queries' : 'actions';
        const options = {method: 'POST', body: JSON.stringify(message.value)};
        if (message.type === 'action') {
          options.headers = {'idempotency-key': `package-ui:${nonce}:${requestId}`};
        }
        try {
          const value = await api(
            `/management/v1/modules/${encodeURIComponent(row.component_id)}/${kind}/${encodeURIComponent(operationId)}`,
            options,
          );
          const reply = {
            type: message.type === 'query' ? 'query-result' : 'action-result',
            payload: {request_id: requestId, operation_id: operationId, value},
          };
          if (operationReplies.size >= 64) {
            operationReplies.delete(operationReplies.keys().next().value);
          }
          operationReplies.set(requestId, reply);
          send(reply.type, reply.payload);
        } catch (error) {
          send('operation-error', {
            request_id: requestId,
            operation_id: operationId,
            message: error instanceof Error ? error.message : String(error),
          });
        }
      }
    } catch (error) {
      fail(error);
    }
  };
  channel.port1.start();
  discard.onclick = () => {
    draft = structuredClone(configuration.desired.value);
    plan = null;
    apply.disabled = true;
    state.textContent = '草稿已恢复';
    send('reset', {value: draft});
  };
  apply.onclick = async () => {
    if (!plan) return;
    apply.disabled = true;
    try {
      const result = await api(`${base}/apply`, {
        method: 'POST',
        headers: {'idempotency-key': `${apply.dataset.key}:apply`},
        body: JSON.stringify({
          plan_id: plan.plan_id,
          expected_desired_revision: configuration.desired.revision,
        }),
      });
      state.textContent = `配置 r${result.configuration.revision} 已生效`;
      send('applied', {revision: result.configuration.revision});
      await refresh();
    } catch (error) {
      apply.disabled = false;
      fail(error);
    }
  };
  frame.onload = () => frame.contentWindow.postMessage({
    protocol: 'forest.management-ui-connect/1', type: 'connect', nonce,
  }, '*', [channel.port2]);
  frame.src = blobUrl;
  activeSandboxCleanup = () => {
    channel.port1.close();
    URL.revokeObjectURL(blobUrl);
  };
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
  if (activeSandboxCleanup) {
    activeSandboxCleanup();
    activeSandboxCleanup = null;
  }
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
        if (contribution.surface.kind === 'sandboxed-package') {
          await mountSandboxedPackage(root, row, contribution);
          return;
        }
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

$('setup-form').onsubmit = async event => {
  event.preventDefault();
  const form = new FormData(event.currentTarget);
  apiBase = '/management/v1';
  try {
    await api('/management/v1/setup/complete', {
      method: 'POST',
      body: JSON.stringify({
        pairing_code: form.get('pairing_code'),
        username: form.get('username'),
        display_name: form.get('display_name'),
        password: form.get('password'),
      }),
    });
    const username = form.get('username');
    event.currentTarget.reset();
    $('setup').classList.add('hidden');
    $('login').classList.remove('hidden');
    $('login-form').elements.username.value = username;
    $('setup-error').textContent = '';
  } catch (error) {
    $('setup-error').textContent = error.message;
  }
};

$('login-form').onsubmit = async event => {
  event.preventDefault();
  const form = new FormData(event.currentTarget);
  apiBase = '/management/v1';
  try {
    await api('/management/v1/auth/login', {
      method: 'POST',
      body: JSON.stringify({
        username: form.get('username'),
        password: form.get('password'),
      }),
    });
    await refresh();
    $('login').classList.add('hidden');
    $('console').classList.remove('hidden');
    $('login-error').textContent = '';
  } catch (error) {
    $('login-error').textContent = error.message;
  }
};

$('logout').onclick = async () => {
  try {
    await api('/management/v1/auth/logout', {method: 'POST', body: '{}'});
  } finally {
    location.reload();
  }
};
$('back').onclick = showOverview;

async function start() {
  try {
    await api('/management/v1/auth/session');
    await refresh();
    $('console').classList.remove('hidden');
    return;
  } catch (_error) {
    // An absent session is expected before login.
  }
  try {
    const status = await api('/management/v1/setup/status');
    (status.setup_required ? $('setup') : $('login')).classList.remove('hidden');
  } catch (error) {
    $('login').classList.remove('hidden');
    $('login-error').textContent = error.message;
  }
}

start();
