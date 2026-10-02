let apiBase = '/management/v1';
let scopes = [];
let workspaceEpoch = 0;
let currentRole = null;
const $ = id => document.getElementById(id);

function cookie(name) {
  const prefix = `${encodeURIComponent(name)}=`;
  const item = document.cookie.split('; ').find(value => value.startsWith(prefix));
  return item ? decodeURIComponent(item.slice(prefix.length)) : '';
}

function scopedPath(path) {
  const prefix = '/management/v1';
  return path === `${prefix}/runtime` || path.startsWith(`${prefix}/modules/`)
    ? `${apiBase}${path.slice(prefix.length)}` : path;
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

async function watchConfigurationRebuild(requestId, componentId, revision, scope, epoch) {
  const endpoint = '/management/v1/modules/component.worker-host/queries/configuration-rebuild';
  const deadline = Date.now() + 180000;
  while (apiBase === scope && workspaceEpoch === epoch && Date.now() < deadline) {
    await new Promise(resolve => setTimeout(resolve, 2000));
    if (apiBase !== scope || workspaceEpoch !== epoch) return;
    let result;
    try {
      result = await api(endpoint, {
        method: 'POST', body: JSON.stringify({request_id: requestId}),
      });
    } catch (error) {
      if ([404, 409, 429].includes(error.status) || error.status >= 500) continue;
      notice(`配置 r${revision} 已提交；重建 ${requestId} 状态查询失败：${error.message}`);
      return;
    }
    if (result.request_id !== requestId
      || result.target_component_id !== componentId
      || result.desired_revision !== revision) {
      notice(`配置 r${revision} 已提交；重建 ${requestId} 回执身份不符`);
      return;
    }
    if (result.state === 'REBUILD_FAILED') {
      notice(`配置 r${revision} 已提交，但重建失败：${result.error_code || requestId}`);
      return;
    }
    if (result.state === 'EFFECTIVE') {
      await refresh();
      notice(`配置 r${revision} 已生效`);
      return;
    }
  }
  if (apiBase === scope && workspaceEpoch === epoch) {
    notice(`配置 r${revision} 已提交；重建 ${requestId} 仍在进行，可在组件控制页查询`);
  }
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

async function mountSandboxedPackage(root, row, contribution, epoch) {
  const surface = contribution.surface;
  const html = await fetchText(surface.artifact);
  if (epoch !== workspaceEpoch) return;
  if (await sha256Hex(html) !== surface.sha256) {
    throw new Error('模块管理 UI 摘要校验失败');
  }
  if (epoch !== workspaceEpoch) return;
  const base = `/management/v1/modules/${encodeURIComponent(row.component_id)}/configuration`;
  const configuration = await api(base);
  if (epoch !== workspaceEpoch) return;
  let draft = structuredClone(configuration.desired.value);
  let plan = null;
  let messages = [];
  let assetChunkMessages = [];
  let resizeMessages = [];
  const operationReplies = new Map();
  const nonce = crypto.randomUUID();
  const channel = new MessageChannel();
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
  const editable = row.management_contract?.configuration?.operations?.includes('configuration.plan')
    && row.management_contract?.configuration?.operations?.includes('configuration.apply');
  discard.hidden = !editable;
  apply.hidden = !editable;
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
    if (
      !message || message.protocol !== 'forest.management-ui-channel/1'
      || message.nonce !== nonce || encoded.length > 262144
    ) return;
    const now = Date.now();
    const resize = message.type === 'resize';
    const assetChunk = message.type === 'asset-chunk';
    const recent = (resize ? resizeMessages : assetChunk ? assetChunkMessages : messages)
      .filter(value => now - value < 1000);
    if (resize) resizeMessages = recent;
    else if (assetChunk) assetChunkMessages = recent;
    else messages = recent;
    if (recent.length >= (resize ? 30 : assetChunk ? 64 : 20)) {
      if (!resize && typeof message.request_id === 'string') {
        send('operation-error', {
          request_id: message.request_id,
          operation_id: message.operation_id,
          message: '模块 UI 操作过快，请稍后重试',
        });
      }
      return;
    }
    recent.push(now);
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
          const stableKey = message.idempotency_key;
          if (stableKey !== undefined && (
            typeof stableKey !== 'string'
            || !/^[a-zA-Z0-9._:-]{1,96}$/.test(stableKey)
          )) throw new Error('模块动作幂等键无效');
          options.headers = {'idempotency-key': stableKey
            ? `package-ui:${stableKey}` : `package-ui:${nonce}:${requestId}`};
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
      } else if (['asset-begin', 'asset-chunk', 'asset-finalize'].includes(message.type)) {
        const requestId = message.request_id;
        const assetId = message.asset_id;
        const uploadId = message.upload_id || message.value?.upload_id;
        if (
          typeof requestId !== 'string' || !/^[a-zA-Z0-9._:-]{1,128}$/.test(requestId)
          || typeof assetId !== 'string' || !/^[a-z0-9][a-z0-9._-]{0,127}$/.test(assetId)
          || !row.management_contract?.assets?.some(asset => asset.asset_id === assetId)
          || typeof uploadId !== 'string' || !/^[a-zA-Z0-9._:-]{1,128}$/.test(uploadId)
        ) throw new Error('模块资产请求无效');
        const prior = operationReplies.get(requestId);
        if (prior) { send(prior.type, prior.payload); return; }
        const assetBase = `/management/v1/modules/${encodeURIComponent(row.component_id)}`
          + `/assets/${encodeURIComponent(assetId)}/uploads`;
        let path, options;
        if (message.type === 'asset-begin') {
          if (!message.value || Array.isArray(message.value)
              || typeof message.value !== 'object') throw new Error('上传声明无效');
          path = assetBase;
          options = {method: 'POST', body: JSON.stringify(message.value)};
        } else if (message.type === 'asset-chunk') {
          if (!Number.isInteger(message.chunk_index) || message.chunk_index < 0
              || message.chunk_index >= 32 || !(message.content instanceof ArrayBuffer)
              || message.content.byteLength < 1 || message.content.byteLength > 262144) {
            throw new Error('上传分块无效');
          }
          path = `${assetBase}/${encodeURIComponent(uploadId)}/chunks/${message.chunk_index}`;
          options = {method: 'PUT', body: message.content,
            headers: {'content-type': 'application/octet-stream'}};
        } else {
          path = `${assetBase}/${encodeURIComponent(uploadId)}/finalize`;
          options = {method: 'POST', body: '{}'};
        }
        try {
          const value = await api(path, options);
          const reply = {type: 'asset-result',
            payload: {request_id: requestId, asset_id: assetId, value}};
          if (operationReplies.size >= 64) {
            operationReplies.delete(operationReplies.keys().next().value);
          }
          operationReplies.set(requestId, reply);
          send(reply.type, reply.payload);
        } catch (error) {
          send('operation-error', {request_id: requestId, operation_id: assetId,
            message: error instanceof Error ? error.message : String(error)});
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
    let result;
    try {
      result = await api(`${base}/apply`, {
        method: 'POST',
        headers: {'idempotency-key': `${apply.dataset.key}:apply`},
        body: JSON.stringify({
          plan_id: plan.plan_id,
          expected_desired_revision: configuration.desired.revision,
        }),
      });
    } catch (error) {
      apply.disabled = false;
      fail(error);
      return;
    }
    const revision = result.configuration.revision;
    const requestId = result.rebuild?.request_id;
    state.textContent = `配置 r${revision} 已提交，等待组件图生效`;
    send('desired-committed', {revision, request_id: requestId});
    const submittedScope = apiBase;
    try {
      await refresh();
    } catch (error) {
      notice(`配置 r${revision} 已提交；页面刷新失败：${error.message}`);
      return;
    }
    if (apiBase !== submittedScope) return;
    if (requestId) {
      notice(`配置 r${revision} 已提交；重建请求 ${requestId} 等待生效`);
      const scope = submittedScope, epoch = workspaceEpoch;
      void watchConfigurationRebuild(requestId, row.component_id, revision, scope, epoch).catch(error => {
        if (apiBase === scope && workspaceEpoch === epoch)
          notice(`配置 r${revision} 已提交；重建 ${requestId} 状态查询失败：${error.message}`);
      });
    } else {
      notice(`配置 r${revision} 已提交；请查询目标组件的生效修订`);
    }
  };
  frame.onload = () => frame.contentWindow.postMessage({
    protocol: 'forest.management-ui-connect/1', type: 'connect', nonce,
  }, '*', [channel.port2]);
  frame.src = scopedPath(surface.artifact);
  activeSandboxCleanup = () => {
    channel.port1.close();
    frame.onload = null;
    frame.src = 'about:blank';
  };
}

function unmountPackage() {
  workspaceEpoch += 1;
  if (activeSandboxCleanup) activeSandboxCleanup();
  activeSandboxCleanup = null;
  $('contribution-root').replaceChildren();
}

function showOverview() {
  unmountPackage();
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
  unmountPackage();
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
      unmountPackage();
      const epoch = workspaceEpoch;
      nav.querySelectorAll('button').forEach(
        item => item.classList.toggle('selected', item === tab),
      );
      try {
        if (contribution.surface.kind === 'sandboxed-package') {
          await mountSandboxedPackage(root, row, contribution, epoch);
          return;
        }
        const template = await fetch(contribution.surface.template).then(response => {
          if (!response.ok) throw new Error('管理模板读取失败');
          return response.text();
        });
        if (epoch !== workspaceEpoch) return;
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
        if (epoch !== workspaceEpoch) return;
        await module.mount({root, component: row, api, notice, refresh});
      } catch (error) {
        if (epoch === workspaceEpoch) notice(error.message);
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
  unmountPackage();
  const runtime = snapshot.runtime;
  $('bot-id').textContent = runtime.bot_id || runtime.scope_id;
  $('runtime-state').textContent = runtime.graph_state;
  $('graph-id').textContent = runtime.graph_id;
  $('module-count').textContent = snapshot.modules.length;
  const graphControl = $('graph-control');
  const graphForm = $('graph-replace-form');
  const graphModules = snapshot.modules.filter(row => row.artifact?.scope === 'deployment');
  graphControl.classList.toggle('hidden', runtime.scope_kind !== 'deployment'
    || currentRole !== 'ADMIN' || !graphModules.length);
  if (!graphControl.classList.contains('hidden')) {
    const selector = graphForm.elements.component_id;
    const selected = selector.value;
    selector.replaceChildren(...graphModules.map(row => {
      const option = document.createElement('option');
      option.value = row.component_id;
      option.textContent = `${row.component_id} · ${row.artifact.package_version}`;
      return option;
    }));
    selector.value = graphModules.some(row => row.component_id === selected)
      ? selected : graphModules[0].component_id;
    graphForm.dataset.revision = String(runtime.graph_revision);
    $('graph-replace-revision').textContent = String(runtime.graph_revision);
    if (!graphForm.elements.package_digest.value) {
      graphForm.elements.package_digest.value = graphModules.find(
        row => row.component_id === selector.value
      ).artifact.package_digest;
    }
    selector.onchange = () => {
      graphForm.elements.package_digest.value = graphModules.find(
        row => row.component_id === selector.value
      ).artifact.package_digest;
      delete graphForm.dataset.operationKey;
    };
  }
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

$('graph-replace-form').oninput = () => {
  delete $('graph-replace-form').dataset.operationKey;
};

$('graph-replace-form').onsubmit = async event => {
  event.preventDefault();
  const form = event.currentTarget;
  const output = $('graph-replace-status');
  const button = form.querySelector('button[type="submit"]');
  const scope = apiBase;
  const epoch = workspaceEpoch;
  const body = {
    component_id: form.elements.component_id.value,
    package_digest: form.elements.package_digest.value.trim(),
    expected_graph_revision: Number(form.dataset.revision),
  };
  form.dataset.operationKey ||= Array.from(
    crypto.getRandomValues(new Uint8Array(16)), value => value.toString(16).padStart(2, '0')
  ).join('');
  button.disabled = true;
  try {
    const saved = await api('/management/v1/modules/$runtime/actions/module.replace', {
      method: 'POST',
      headers: {'idempotency-key': form.dataset.operationKey},
      body: JSON.stringify(body),
    });
    output.textContent = `请求 ${saved.operation_id}：${saved.state}`;
    const deadline = Date.now() + 180000;
    while (apiBase === scope && workspaceEpoch === epoch && Date.now() < deadline) {
      if (['SUCCEEDED', 'FAILED'].includes(saved.state)) {
        delete form.dataset.operationKey;
        if (saved.state === 'SUCCEEDED') await refresh();
        break;
      }
      await new Promise(resolve => setTimeout(resolve, 1000));
      const status = await api('/management/v1/modules/$runtime/queries/module.replace.status', {
        method: 'POST', body: JSON.stringify({operation_id: saved.operation_id}),
      });
      output.textContent = `请求 ${saved.operation_id}：${status.state}`
        + (status.error_code ? ` · ${status.error_code}` : '');
      if (['SUCCEEDED', 'FAILED'].includes(status.state)) {
        delete form.dataset.operationKey;
        if (status.state === 'SUCCEEDED') await refresh();
        break;
      }
    }
  } catch (error) {
    output.textContent = error.message;
  } finally {
    button.disabled = false;
  }
};

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
    const session = await api('/management/v1/auth/login', {
      method: 'POST',
      body: JSON.stringify({
        username: form.get('username'),
        password: form.get('password'),
      }),
    });
    currentRole = session.subject.role;
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
    const session = await api('/management/v1/auth/session');
    currentRole = session.subject.role;
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
