const TRUSTED_KIND = 'trusted-module';
const TRUSTED_PROTOCOL = 'forest.trusted-management-view/1';

function trustedAsset(value, suffix) {
  return typeof value === 'string' && value.startsWith('/static/') && value.endsWith(suffix)
    && !value.includes('..') && !value.includes('\\');
}

export function runtimeModuleViews({api, busy, notice, nav, registry, root, packageElements}) {
  const views = new Map();
  let descriptors = [];
  let bot = null;
  let active = 'package';

  function show(componentId) {
    const available = componentId === 'package' || views.has(componentId);
    if (!available) return;
    active = componentId;
    for (const element of packageElements) {
      element.classList.toggle('bot-module-surface-inactive', componentId !== 'package');
    }
    for (const [id, view] of views) {
      view.container.classList.toggle('bot-module-surface-inactive', id !== componentId);
    }
    for (const button of nav.querySelectorAll('button')) {
      button.classList.toggle('selected', button.dataset.moduleId === componentId);
    }
  }

  async function load(descriptor) {
    const componentId = descriptor.component_id;
    const view = views.get(componentId);
    if (!view || view.controller) return view?.controller || null;
    if (view.loading) return view.loading;
    const spec = descriptor.management_view;
    view.loading = (async () => {
      if (spec?.kind !== TRUSTED_KIND || spec.protocol !== TRUSTED_PROTOCOL
          || !trustedAsset(spec.template, '.html')
          || !trustedAsset(spec.module, '.js')
          || (spec.style != null && !trustedAsset(spec.style, '.css'))) {
        throw new Error(`模块 ${componentId} 的管理视图声明无效。`);
      }
      const response = await fetch(spec.template, {credentials: 'same-origin'});
      if (!response.ok) throw new Error(`模块 ${componentId} 的管理模板无法读取。`);
      view.container.innerHTML = await response.text();
      if (spec.style) {
        const stylesheet = document.createElement('link');
        stylesheet.rel = 'stylesheet'; stylesheet.href = spec.style;
        view.container.prepend(stylesheet);
      }
      const implementation = await import(spec.module);
      if (typeof implementation.createModulePanel !== 'function') {
        throw new Error(`模块 ${componentId} 未导出 createModulePanel。`);
      }
      view.controller = implementation.createModulePanel(api, busy, notice);
      if (!view.controller || ['select', 'reset', 'invalidateContent'].some(
        name => typeof view.controller[name] !== 'function'
      )) {
        throw new Error(`模块 ${componentId} 的管理控制器生命周期不完整。`);
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

  function addButton(componentId, label, descriptor = null) {
    const button = document.createElement('button');
    button.type = 'button'; button.className = 'subtle';
    button.dataset.moduleId = componentId; button.textContent = label;
    button.addEventListener('click', () => busy(button, async () => {
      if (descriptor) {
        const controller = await load(descriptor);
        const view = views.get(componentId);
        if (bot && view.selectedBotId !== bot.bot_id) {
          await controller.select(bot); view.selectedBotId = bot.bot_id;
        }
      }
      show(componentId);
    }));
    nav.append(button);
  }

  function update(runtimeModules, packageRow = null) {
    descriptors = runtimeModules || [];
    const wanted = new Set(
      descriptors.filter(row => row.management_view).map(row => row.component_id)
    );
    for (const [componentId, view] of views) {
      if (wanted.has(componentId)) continue;
      view.controller?.reset(); view.container.remove(); views.delete(componentId);
    }
    for (const descriptor of descriptors) {
      if (!descriptor.management_view || views.has(descriptor.component_id)) continue;
      const container = document.createElement('div');
      container.className = 'runtime-module-view bot-module-surface-inactive';
      container.dataset.moduleId = descriptor.component_id;
      root.append(container);
      views.set(descriptor.component_id, {
        container, controller: null, loading: null, selectedBotId: null,
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
    for (const descriptor of descriptors) {
      if (descriptor.management_view) {
        addButton(
          descriptor.component_id,
          descriptor.management_view.label || descriptor.component_id,
          descriptor,
        );
      }
    }
    const available = active === 'package' ? Boolean(packageRow) : views.has(active);
    if (!available) active = packageRow ? 'package' : (views.keys().next().value || 'package');
    show(active);
  }

  async function select(row) {
    bot = row;
    for (const descriptor of descriptors) {
      if (!descriptor.management_view) continue;
      const controller = await load(descriptor);
      if (!bot || bot.bot_id !== row.bot_id) return;
      await controller.select(row);
      const view = views.get(descriptor.component_id);
      if (view) view.selectedBotId = row.bot_id;
    }
  }

  function reset() {
    bot = null;
    for (const view of views.values()) {
      view.controller?.reset(); view.selectedBotId = null;
    }
  }

  function invalidateContent(scope) {
    for (const view of views.values()) view.controller?.invalidateContent(scope);
  }

  return {update, select, reset, invalidateContent, show};
}
