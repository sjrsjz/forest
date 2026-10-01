export async function mount({root, component, api, notice}) {
  const base = `/management/v1/modules/${component.component_id}`;
  const call = (id, value) => api(`${base}/actions/${id}`, {
    method: 'POST',
    headers: {'idempotency-key': crypto.randomUUID()},
    body: JSON.stringify(value),
  });
  const query = (id, value = {}) => api(`${base}/queries/${id}`, {
    method: 'POST',
    body: JSON.stringify(value),
  });
  const jobsRoot = root.querySelector('[data-list]');
  const componentsRoot = root.querySelector('[data-bootstrap-components]');
  const componentEditors = new Map();

  async function renderBootstrapCatalog(items) {
    componentsRoot.replaceChildren();
    componentEditors.clear();
    for (const contribution of items) {
      const panel = document.createElement('article');
      panel.className = 'bootstrap-module';
      componentsRoot.append(panel);
      const template = await fetch(contribution.surface.template).then(response => {
        if (!response.ok) throw new Error('Bootstrap 组件模板读取失败');
        return response.text();
      });
      panel.innerHTML = template;
      const style = contribution.surface.style;
      if (style && !document.querySelector(`link[data-module-style="${CSS.escape(style)}"]`)) {
        const link = document.createElement('link');
        link.rel = 'stylesheet';
        link.href = style;
        link.dataset.moduleStyle = style;
        document.head.append(link);
      }
      const module = await import(contribution.surface.module);
      const editor = await module.mountBootstrap({root: panel, contribution, notice});
      if (!editor || typeof editor.read !== 'function') {
        throw new Error(`Bootstrap 组件 ${contribution.component_id} 未返回配置读取器`);
      }
      componentEditors.set(contribution.component_id, editor);
    }
  }

  async function load() {
    try {
      const [catalog, jobs] = await Promise.all([
        query('provisioning.bootstrap-catalog'),
        query('provisioning.jobs.list'),
      ]);
      await renderBootstrapCatalog(catalog.items);
      jobsRoot.replaceChildren();
      for (const job of jobs.items) {
        const row = document.createElement('div');
        row.className = 'job-row';
        for (const value of [
          job.job_id,
          job.bot_id,
          job.state,
          `#${job.attempt_no}`,
          job.last_error_code || '—',
        ]) {
          const item = document.createElement('code');
          item.textContent = value;
          row.append(item);
        }
        jobsRoot.append(row);
      }
    } catch (error) {
      notice(error.message);
    }
  }

  function values(form) {
    return Object.fromEntries(new FormData(form));
  }

  root.querySelector('[data-storage]').onsubmit = async event => {
    event.preventDefault();
    const value = values(event.currentTarget);
    value.storage_generation = Number(value.storage_generation);
    try {
      await call('storage.create-local', value);
      notice('本地 PostgreSQL Storage Profile 已创建');
    } catch (error) {
      notice(error.message);
    }
  };

  root.querySelector('[data-bootstrap]').onsubmit = async event => {
    event.preventDefault();
    const value = Object.fromEntries(
      [...event.currentTarget.querySelectorAll(':scope > .deploy-form [name]')]
        .map(input => [input.name, input.value]),
    );
    value.storage_generation = Number(value.storage_generation);
    try {
      value.components = Object.fromEntries(
        [...componentEditors].map(([componentId, editor]) => [componentId, editor.read()]),
      );
      await call('bootstrap.create', value);
      notice('Bootstrap Profile 已按模块配置冻结');
    } catch (error) {
      notice(error.message);
    }
  };

  root.querySelector('[data-job]').onsubmit = async event => {
    event.preventDefault();
    const value = values(event.currentTarget);
    value.storage_generation = Number(value.storage_generation);
    try {
      await call('provisioning.enqueue', value);
      await load();
    } catch (error) {
      notice(error.message);
    }
  };
  root.querySelector('[data-refresh]').onclick = load;
  await load();
}
