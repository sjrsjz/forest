// Host-owned composition of trusted install-time module surfaces.
export function createBootstrapEditorBroker({notice, isCurrent}) {
  let active = null;

  function surfacePath(value, suffix) {
    return typeof value === 'string' && value.startsWith('/static/')
      && value.endsWith(suffix) && !value.includes('\\')
      && !value.split('/').includes('..');
  }

  function validateCatalog(catalog) {
    if (catalog?.protocol !== 'forest.bootstrap-configuration-catalog/1'
      || !Array.isArray(catalog.items) || catalog.items.length < 1
      || catalog.items.length > 64) throw new Error('Bootstrap 贡献目录无效');
    const seen = new Set();
    for (const item of catalog.items) {
      const surface = item?.surface;
      const packageDigest = /^sha256:[a-f0-9]{64}$/.test(item?.package_digest || '');
      const trusted = surface?.kind === 'trusted-module'
        && surface.protocol === 'forest.trusted-management-view/1'
        && surfacePath(surface.template, '.html')
        && surfacePath(surface.module, '.js')
        && (surface.style === null || surfacePath(surface.style, '.css'));
      const sandboxed = surface?.kind === 'sandboxed-bootstrap'
        && surface.protocol === 'forest.bootstrap-ui/1'
        && /^[a-f0-9]{64}$/.test(surface.sha256 || '')
        && surface.artifact === `/management/v1/bootstrap-ui/${item.component_id}/`
          + `${item.package_digest?.slice(7)}/${surface.sha256}`;
      if (!/^[a-z0-9][a-z0-9._-]{0,127}$/.test(item?.component_id || '')
        || seen.has(item.component_id)
        || item.protocol !== 'forest.bootstrap-configuration-contribution/1'
        || !packageDigest || !(trusted || sandboxed)) {
        throw new Error('Bootstrap 贡献身份或界面地址无效');
      }
      seen.add(item.component_id);
    }
  }

  function cleanup(session) {
    session.dialog.remove();
    for (const link of session.styles) link.remove();
    for (const port of session.ports) port.close();
    if (active === session) active = null;
  }

  function close() {
    if (!active) return;
    const session = active;
    session.cancelled = true;
    session.reject?.(new Error('已取消模块设置'));
    cleanup(session);
  }

  async function open(catalog) {
    if (active || !isCurrent()) throw new Error('模块设置窗口不可用');
    validateCatalog(catalog);
    const dialog = document.createElement('dialog');
    dialog.className = 'bootstrap-editor-dialog';
    const form = document.createElement('form');
    const header = document.createElement('header');
    const title = document.createElement('h2');
    title.textContent = '创建期模块设置';
    const hint = document.createElement('small');
    hint.textContent = '每个模块自己的编辑器返回该模块的配置值';
    header.append(title, hint);
    const panels = document.createElement('div');
    panels.className = 'bootstrap-editor-panels';
    const footer = document.createElement('footer');
    const cancel = document.createElement('button');
    cancel.type = 'button'; cancel.textContent = '取消';
    const apply = document.createElement('button');
    apply.type = 'submit'; apply.textContent = '使用这些模块设置';
    footer.append(cancel, apply);
    form.append(header, panels, footer);
    dialog.append(form);
    document.body.append(dialog);
    const session = {dialog, styles: [], ports: [], editors: new Map(), cancelled: false, reject: null};
    active = session;
    try {
      for (const contribution of catalog.items) {
        if (session.cancelled || !isCurrent()) throw new Error('模块设置页面已切换');
        const surface = contribution.surface;
        if (surface.kind === 'sandboxed-bootstrap') {
          const panel = document.createElement('article');
          panel.className = 'bootstrap-editor-panel';
          const heading = document.createElement('h3');
          heading.textContent = contribution.label;
          const frame = document.createElement('iframe');
          frame.className = 'bootstrap-package-frame';
          frame.setAttribute('sandbox', 'allow-scripts allow-forms');
          frame.src = surface.artifact;
          panel.append(heading, frame);
          panels.append(panel);
          await new Promise((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error('Bootstrap 包界面加载超时')), 10000);
            frame.onload = () => { clearTimeout(timeout); resolve(); };
            frame.onerror = () => {
              clearTimeout(timeout);
              reject(new Error('Bootstrap 包界面加载失败'));
            };
          });
          const channel = new MessageChannel();
          session.ports.push(channel.port1);
          let pending = null;
          const ready = new Promise((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error('Bootstrap 包界面未就绪')), 10000);
            channel.port1.onmessage = event => {
              const reply = event.data;
              if (reply?.protocol === 'forest.bootstrap-ui.ready/1') {
                clearTimeout(timeout); resolve();
              } else if (reply?.protocol === 'forest.bootstrap-ui.value/1'
                && pending && reply.request_id === pending.id) {
                const answer = pending;
                pending = null;
                clearTimeout(answer.timeout);
                answer.resolve(reply.value);
              }
            };
          });
          frame.contentWindow.postMessage({
            protocol: 'forest.bootstrap-ui.init/1',
            initial_value: structuredClone(contribution.initial_value),
          }, '*', [channel.port2]);
          await ready;
          session.editors.set(contribution.component_id, {
            read: () => new Promise((resolve, reject) => {
              if (pending) return reject(new Error('Bootstrap 包界面正在读取'));
              const id = crypto.randomUUID();
              const timeout = setTimeout(() => {
                pending = null;
                reject(new Error('Bootstrap 包界面读取超时'));
              }, 10000);
              pending = {id, timeout, resolve};
              channel.port1.postMessage({protocol: 'forest.bootstrap-ui.read/1', request_id: id});
            }),
          });
          continue;
        }
        const response = await fetch(surface.template, {credentials: 'same-origin'});
        if (!response.ok) throw new Error('Bootstrap 组件模板读取失败');
        const template = await response.text();
        if (session.cancelled || !isCurrent()) throw new Error('模块设置页面已切换');
        const panel = document.createElement('article');
        panel.className = 'bootstrap-editor-panel';
        const heading = document.createElement('h3');
        heading.textContent = contribution.label;
        const body = document.createElement('div');
        body.className = 'bootstrap-module';
        body.innerHTML = template;
        panel.append(heading, body);
        panels.append(panel);
        if (surface.style) {
          const link = document.createElement('link');
          link.rel = 'stylesheet'; link.href = surface.style;
          document.head.append(link);
          session.styles.push(link);
        }
        const module = await import(surface.module);
        if (session.cancelled || !isCurrent()) throw new Error('模块设置页面已切换');
        const editor = await module.mountBootstrap({root: body, contribution, notice});
        if (!editor || typeof editor.read !== 'function') {
          throw new Error(`Bootstrap 组件 ${contribution.component_id} 未返回配置读取器`);
        }
        session.editors.set(contribution.component_id, editor);
      }
      if (session.cancelled || !isCurrent()) throw new Error('模块设置页面已切换');
      return await new Promise((resolve, reject) => {
        session.reject = reject;
        cancel.onclick = close;
        dialog.oncancel = event => { event.preventDefault(); close(); };
        form.onsubmit = async event => {
          event.preventDefault();
          try {
            const value = Object.fromEntries(
              await Promise.all([...session.editors].map(async ([id, editor]) =>
                [id, structuredClone(await editor.read())])),
            );
            resolve(value);
          } catch (error) {
            notice(error instanceof Error ? error.message : String(error));
          }
        };
        dialog.showModal();
      });
    } finally {
      cleanup(session);
    }
  }

  return {open, close};
}
