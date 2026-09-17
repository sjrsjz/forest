const $ = id => document.getElementById(id);

export function createModulePanel(api, busy, notice) {
  let scope = null, importKey = null, readSeq = 0;
  const base = () => `/scopes/deployment/${encodeURIComponent(scope.scope_id)}/modules/deployment.package-registry`;

  async function load() {
    if (!scope) return;
    const seq = ++readSeq;
    const result = await api(`${base()}/queries/packages`);
    if (seq !== readSeq || !scope) return;
    const list = $('deployment-package-list'); list.replaceChildren();
    if (!result.packages.length) list.textContent = '还没有可用包工件。';
    for (const pkg of result.packages) {
      const row = document.createElement('p');
      row.textContent = `${pkg.package_id} · ${pkg.version} `;
      const link = document.createElement('a'); link.textContent = '下载原工件';
      const params = new URLSearchParams({package_digest:pkg.package_digest});
      link.href = `/management/v1${base()}/downloads/package.archive?${params}`;
      row.append(link); list.append(row);
    }
    for (const [digest, code] of Object.entries(result.errors || {})) {
      const error = document.createElement('p'); error.className = 'notice';
      error.textContent = `${digest} · ${code}`; list.append(error);
    }
  }

  $('refresh-deployment-packages').addEventListener('click', event =>
    busy(event.currentTarget, load));
  $('deployment-package-import-form').addEventListener('input', () => { importKey = null; });
  $('deployment-package-import-form').addEventListener('submit', event => {
    event.preventDefault(); const form = event.currentTarget;
    busy(form.querySelector('button'), async () => {
      const file = form.elements.archive.files[0];
      if (!file || file.size > 32 * 1024 * 1024) {
        throw new Error('请选择不超过 32 MiB 的插件 ZIP 包。');
      }
      importKey ||= crypto.randomUUID(); const sent = importKey;
      const pkg = await api(`${base()}/uploads/package.import`, file, {
        'Content-Type':'application/zip', 'Idempotency-Key':sent,
      });
      if (importKey === sent) { importKey = null; form.reset(); }
      await load(); notice(`已导入 ${pkg.package_id} ${pkg.version}。`, true);
    });
  });

  return {
    async select(row) { scope = row; await load(); },
    reset() {
      readSeq++; scope = null; importKey = null;
      $('deployment-package-list').replaceChildren();
      $('deployment-package-import-form').reset();
    },
    invalidateContent() { readSeq++; },
  };
}
