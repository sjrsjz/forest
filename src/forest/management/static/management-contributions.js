const CONTRIBUTION_PROTOCOL = 'forest.management-contribution/1';
const TRUSTED_KIND = 'trusted-module';
const TRUSTED_VIEW_PROTOCOL = 'forest.trusted-management-view/1';

function trustedAsset(value, suffix) {
  return typeof value === 'string' && value.startsWith('/static/') && value.endsWith(suffix)
    && !value.includes('..') && !value.includes('\\');
}

function contributionKey(componentId, contributionId) {
  return `${componentId}:${contributionId}`;
}

function availableContributions(descriptors, scopeKind) {
  const rows = [];
  for (const descriptor of descriptors) {
    for (const contribution of descriptor.management_contributions || []) {
      if (contribution.protocol !== CONTRIBUTION_PROTOCOL
          || !contribution.scope_kinds?.includes(scopeKind)) continue;
      rows.push({descriptor, contribution,
        key: contributionKey(descriptor.component_id, contribution.contribution_id)});
    }
  }
  return rows.sort((left, right) =>
    left.contribution.group.localeCompare(right.contribution.group)
      || left.contribution.order - right.contribution.order
      || left.contribution.label.localeCompare(right.contribution.label)
      || left.key.localeCompare(right.key));
}

export function managementContributions({
  api, busy, notice, nav, registry, root, packageElements = [], scopeKind = 'bot',
  scopeIdentity = row => row.bot_id,
}) {
  const views = new Map();
  let descriptors = [];
  let scope = null;
  let active = 'package';

  function show(key) {
    const available = key === 'package' || views.has(key);
    if (!available) return;
    active = key;
    for (const element of packageElements) {
      element.classList.toggle('bot-module-surface-inactive', key !== 'package');
    }
    for (const [identity, view] of views) {
      view.container.classList.toggle('bot-module-surface-inactive', identity !== key);
    }
    for (const button of nav.querySelectorAll('button')) {
      button.classList.toggle('selected', button.dataset.contributionKey === key);
    }
  }

  async function load(entry) {
    const view = views.get(entry.key);
    if (!view || view.controller) return view?.controller || null;
    if (view.loading) return view.loading;
    const spec = entry.contribution.surface;
    view.loading = (async () => {
      if (spec?.kind !== TRUSTED_KIND || spec.protocol !== TRUSTED_VIEW_PROTOCOL
          || !trustedAsset(spec.template, '.html')
          || !trustedAsset(spec.module, '.js')
          || (spec.style != null && !trustedAsset(spec.style, '.css'))) {
        throw new Error(`模块 ${entry.descriptor.component_id} 的管理贡献无效。`);
      }
      const response = await fetch(spec.template, {credentials: 'same-origin'});
      if (!response.ok) throw new Error(`模块 ${entry.descriptor.component_id} 的管理模板无法读取。`);
      view.container.innerHTML = await response.text();
      if (spec.style) {
        const stylesheet = document.createElement('link');
        stylesheet.rel = 'stylesheet'; stylesheet.href = spec.style;
        view.container.prepend(stylesheet);
      }
      const implementation = await import(spec.module);
      if (typeof implementation.createModulePanel !== 'function') {
        throw new Error(`模块 ${entry.descriptor.component_id} 未导出 createModulePanel。`);
      }
      view.controller = implementation.createModulePanel(api, busy, notice);
      if (!view.controller || ['select', 'reset', 'invalidateContent'].some(
        name => typeof view.controller[name] !== 'function'
      )) {
        throw new Error(`模块 ${entry.descriptor.component_id} 的管理控制器生命周期不完整。`);
      }
      return view.controller;
    })().catch(error => {
      view.container.replaceChildren();
      const message = document.createElement('p');
      message.className = 'notice'; message.textContent = error.message;
      view.container.append(message);
      throw error;
    }).finally(() => { view.loading = null; });
    return view.loading;
  }

  function addButton(key, label, entry = null) {
    const button = document.createElement('button');
    button.type = 'button'; button.className = 'subtle';
    button.dataset.contributionKey = key; button.textContent = label;
    button.addEventListener('click', () => busy(button, async () => {
      if (entry) {
        const controller = await load(entry);
        const view = views.get(key);
        const selectedId = scope ? scopeIdentity(scope) : null;
        if (scope && view.selectedScopeId !== selectedId) {
          await controller.select(scope); view.selectedScopeId = selectedId;
        }
      }
      show(key);
    }));
    nav.append(button);
  }

  function update(runtimeModules, packageRow = null) {
    descriptors = runtimeModules || [];
    const entries = availableContributions(descriptors, scopeKind);
    const wanted = new Set(entries.map(entry => entry.key));
    for (const [key, view] of views) {
      if (wanted.has(key)) continue;
      view.controller?.reset(); view.container.remove(); views.delete(key);
    }
    for (const entry of entries) {
      if (views.has(entry.key)) continue;
      const container = document.createElement('div');
      container.className = 'runtime-module-view bot-module-surface-inactive';
      container.dataset.contributionKey = entry.key;
      root.append(container);
      views.set(entry.key, {
        entry, container, controller: null, loading: null, selectedScopeId: null,
      });
    }

    registry.replaceChildren(); nav.replaceChildren();
    const rows = [...(packageRow ? [packageRow] : []), ...descriptors];
    for (const row of rows) {
      const item = document.createElement('div'); item.className = 'module-registry-row';
      const identity = document.createElement('strong'); identity.textContent = row.component_id;
      const detail = document.createElement('span');
      detail.textContent = row.requires?.length ? `依赖 ${row.requires.join('、')}` : '无启动依赖';
      item.append(identity, detail); registry.append(item);
    }
    if (packageRow) addButton('package', packageRow.component_id);
    for (const entry of entries) addButton(entry.key, entry.contribution.label, entry);
    const available = active === 'package' ? Boolean(packageRow) : views.has(active);
    if (!available) active = packageRow ? 'package' : (entries[0]?.key || 'package');
    show(active);
  }

  async function select(row) {
    scope = row;
    const selectedId = scopeIdentity(row);
    for (const view of views.values()) {
      const controller = await load(view.entry);
      if (!scope || scopeIdentity(scope) !== selectedId) return;
      await controller.select(row); view.selectedScopeId = selectedId;
    }
  }

  function reset() {
    scope = null;
    for (const view of views.values()) {
      view.controller?.reset(); view.selectedScopeId = null;
    }
  }

  function invalidateContent(targetScope) {
    for (const view of views.values()) view.controller?.invalidateContent(targetScope);
  }

  return {update, select, reset, invalidateContent, show};
}
