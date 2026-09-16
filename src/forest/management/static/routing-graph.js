const $ = id => document.getElementById(id);

export function createModulePanel(api, busy, notice) {
  let bot = null, epoch = 0, snapshot = null;
  const path = () => `/bots/${encodeURIComponent(bot.bot_id)}/runtime-modules/routing.graph/queries/snapshot`;

  function reset() {
    epoch++; bot = null; snapshot = null;
    $('bot-route-graph').classList.add('hidden');
    $('route-graph-summary').replaceChildren();
    $('route-graph-nodes').replaceChildren();
    $('route-graph-edges').replaceChildren();
    $('route-graph-status').textContent = '';
    $('route-graph-filter').reset();
  }

  const text = value => value == null ? '—' : String(value);
  function render() {
    if (!snapshot) return;
    const query = new FormData($('route-graph-filter'));
    const kind = query.get('kind'), needle = String(query.get('query') || '').toLowerCase();
    const visible = snapshot.nodes.filter(node =>
      (!kind || node.kind === kind) && (!needle || JSON.stringify(node).toLowerCase().includes(needle))
    );
    const counts = new Map();
    for (const node of snapshot.nodes) counts.set(node.kind, (counts.get(node.kind) || 0) + 1);
    $('route-graph-summary').replaceChildren(...[
      `摘要 ${snapshot.graph_digest.slice(0, 18)}…`,
      ...[...counts].map(([name, count]) => `${name} ${count}`),
    ].map(value => { const item = document.createElement('span'); item.textContent = value; return item; }));
    $('route-graph-nodes').replaceChildren(...visible.map(node => {
      const item = document.createElement('article'); item.className = 'route-node';
      const title = document.createElement('strong'); title.textContent = node.node_id;
      const owner = document.createElement('small');
      owner.textContent = `组件 ${text(node.component_id)} · 代际 ${text(node.generation_id)} · epoch ${text(node.route_epoch)}`;
      const detail = document.createElement('small');
      detail.textContent = node.contract ? `契约 ${node.contract}` : `类型 ${node.kind}`;
      item.append(title, owner, detail); return item;
    }));
    $('route-graph-edges').replaceChildren(...snapshot.edges.map(edge => {
      const item = document.createElement('article'); item.className = 'route-edge'; item.dataset.status = edge.status;
      const title = document.createElement('strong'); title.textContent = `${edge.kind} · ${edge.status}`;
      const route = document.createElement('small'); route.textContent = `${edge.source} → ${text(edge.target)}`;
      const contract = document.createElement('small');
      contract.textContent = edge.contract_range ? `要求 ${edge.contract_range}` : edge.edge_id;
      item.append(title, route, contract); return item;
    }));
    $('route-graph-status').textContent = `显示 ${visible.length}/${snapshot.nodes.length} 个节点，${snapshot.edges.length} 条边。`;
  }

  async function refresh() {
    if (!bot) return;
    const own = epoch, data = await api(path());
    if (!bot || own !== epoch) return;
    snapshot = data; render();
  }

  $('route-graph-filter').addEventListener('input', render);
  $('route-graph-refresh').addEventListener('click', event => busy(event.currentTarget, refresh));

  async function select(row) {
    reset(); bot = row; const own = epoch;
    $('bot-route-graph').classList.remove('hidden');
    try { await refresh(); } catch (error) { if (bot && own === epoch) notice(error.message); }
  }

  function invalidateContent(scope) {
    if (bot && scope.startsWith(`/bots/${encodeURIComponent(bot.bot_id)}/`)) {
      snapshot = null; $('route-graph-status').textContent = '路由事实已变化，请刷新快照。';
    }
  }

  return {select, reset, invalidateContent};
}
