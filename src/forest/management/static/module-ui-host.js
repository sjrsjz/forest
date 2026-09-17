const PROTOCOL = 'forest.management-ui/1';
const MAX_MESSAGE_BYTES = 128 * 1024;
const MAX_HEIGHT = 720;

function hex(bytes) {
  return [...new Uint8Array(bytes)].map(value => value.toString(16).padStart(2, '0')).join('');
}

function plainObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value) &&
    Object.getPrototypeOf(value) === Object.prototype;
}

export async function mountModuleUi(container, pkg, values, onDraft) {
  if (pkg.management_ui?.protocol !== PROTOCOL) throw new Error('模块管理界面协议不兼容。');
  if (typeof pkg.management_ui_url !== 'string' ||
      !pkg.management_ui_url.startsWith('/management/v1/scopes/')) {
    throw new Error('模块管理界面工件地址无效。');
  }
  const response = await fetch(
    pkg.management_ui_url,
    {credentials: 'same-origin'},
  );
  if (!response.ok) throw new Error(`模块管理界面读取失败 (${response.status})`);
  const sourceBytes = await response.arrayBuffer();
  const actual = hex(await crypto.subtle.digest('SHA-256', sourceBytes));
  if (actual !== pkg.management_ui.sha256) throw new Error('模块管理界面摘要不匹配。');
  const source = new TextDecoder('utf-8', {fatal: true}).decode(sourceBytes);
  const csp = `<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; img-src data: blob:; font-src data:; connect-src 'none'; form-action 'none'; base-uri 'none'">`;
  const html = /<head(?:\s[^>]*)?>/i.test(source)
    ? source.replace(/<head(?:\s[^>]*)?>/i, match => `${match}${csp}`)
    : source.replace(/<html(?:\s[^>]*)?>/i, match => `${match}<head>${csp}</head>`);
  const url = URL.createObjectURL(new Blob([html], {type: 'text/html;charset=utf-8'}));
  const frame = document.createElement('iframe');
  frame.className = 'module-ui-frame';
  frame.title = `${pkg.component_id} 配置界面`;
  frame.sandbox = 'allow-scripts';
  frame.referrerPolicy = 'no-referrer';
  frame.src = url;
  container.replaceChildren(frame);
  container.classList.remove('hidden');
  const channel = new MessageChannel();
  const nonce = hex(crypto.getRandomValues(new Uint8Array(24)));
  let destroyed = false;
  let rateWindow = performance.now(), rateCount = 0;
  channel.port1.onmessage = event => {
    if (destroyed || !plainObject(event.data)) return;
    const now = performance.now();
    if (now - rateWindow >= 1000) { rateWindow = now; rateCount = 0; }
    if (++rateCount > 30) return;
    let size;
    try { size = new TextEncoder().encode(JSON.stringify(event.data)).byteLength; }
    catch { return; }
    if (size > MAX_MESSAGE_BYTES || event.data.protocol !== PROTOCOL || event.data.nonce !== nonce) return;
    if (event.data.type === 'module:set-draft' && plainObject(event.data.values)) {
      onDraft(structuredClone(event.data.values));
    } else if (event.data.type === 'module:resize' && Number.isFinite(event.data.height)) {
      frame.style.height = `${Math.max(120, Math.min(MAX_HEIGHT, Math.ceil(event.data.height)))}px`;
    }
  };
  frame.addEventListener('load', () => {
    if (destroyed) return;
    frame.contentWindow.postMessage({
      protocol: PROTOCOL, type: 'forest:init', nonce,
      component: {component_id: pkg.component_id, package_digest: pkg.package_digest},
      schema: structuredClone(pkg.config_schema), values: structuredClone(values || {}),
    }, '*', [channel.port2]);
  }, {once: true});
  return {
    destroy() {
      if (destroyed) return;
      destroyed = true; channel.port1.close(); frame.remove(); URL.revokeObjectURL(url);
      container.classList.add('hidden');
    },
  };
}
