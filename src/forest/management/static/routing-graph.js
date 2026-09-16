const $ = id => document.getElementById(id);

export function createModulePanel(api, busy, notice) {
  let bot = null, epoch = 0, snapshot = null, traces = [], traceScanTruncated = false;
  const path = () => `/bots/${encodeURIComponent(bot.bot_id)}/runtime-modules/routing.graph/queries/snapshot`;
  const operationPath = name => `/bots/${encodeURIComponent(bot.bot_id)}/runtime-modules/routing.graph/queries/${name}`;

  function reset() {
    epoch++; bot = null; snapshot = null; traces = []; traceScanTruncated = false;
    $('bot-route-graph').classList.add('hidden');
    $('route-graph-summary').replaceChildren();
    $('route-graph-nodes').replaceChildren();
    $('route-graph-edges').replaceChildren();
    $('route-graph-status').textContent = '';
    $('route-graph-filter').reset();
    $('dispatch-trace-filter').reset();
    $('dispatch-trace-filter').elements.limit.value = '30';
    $('dispatch-trace-status').textContent = '';
    $('dispatch-traces').replaceChildren();
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

  async function frozenGraph(digest) {
    if (!bot) return;
    const own = epoch;
    const data = await api(`${operationPath('snapshot-version')}?graph_digest=${encodeURIComponent(digest)}`);
    if (!bot || own !== epoch) return;
    snapshot = data; render();
    $('route-graph-status').textContent = `正在查看消息冻结图 ${digest.slice(0, 18)}…，捕获于 ${text(data.captured_at)}。`;
  }

  function renderTraces() {
    $('dispatch-traces').replaceChildren(...traces.map(trace => {
      const article = document.createElement('article'); article.className = 'dispatch-trace';
      const header = document.createElement('header');
      const title = document.createElement('strong');
      title.textContent = `${trace.channel_id} · #${trace.ingress_seq} · ${trace.status}`
        + (trace.causal_truncated ? ' · \u56e0\u679c\u5df2\u622a\u65ad' : '');
      const graph = document.createElement('button'); graph.type = 'button'; graph.className = 'subtle'; graph.textContent = '查看冻结图';
      if (trace.graph_digest) {
        graph.addEventListener('click', event => busy(event.currentTarget, () => frozenGraph(trace.graph_digest)));
      } else {
        graph.disabled = true; graph.textContent = '旧记录无冻结图';
      }
      header.append(title, graph);
      const identity = document.createElement('small'); identity.textContent = `${trace.event_id} · ${trace.received_at}`;
      article.append(header, identity);
      for (const entry of trace.entries) {
        const row = document.createElement('div'); row.className = 'dispatch-step';
        const route = document.createElement('span'); route.textContent = `${entry.kind} · ${entry.node_id}`;
        const state = document.createElement('strong'); state.textContent = entry.status;
        const owner = document.createElement('small'); owner.textContent = `组件 ${text(entry.component_id)} · 代际 ${text(entry.generation_id)}`;
        const reason = document.createElement('small');
        const cause = entry.references && entry.references.caused_by;
        reason.textContent = entry.reason || (cause
          ? `\u7531 ${cause.kind} · ${cause.id} \u89e6\u53d1 · ${JSON.stringify(entry.references)}`
          : JSON.stringify(entry.references || {}));
        row.append(route, state, owner, reason); article.append(row);
      }
      return article;
    }));
    $('dispatch-trace-status').textContent = traces.length ? `显示 ${traces.length} 条消息执行记录。` : '没有符合条件的执行记录。';
  }

  async function queryTraces() {
    if (!bot) return;
    const own = epoch, data = new FormData($('dispatch-trace-filter')), query = new URLSearchParams();
    for (const [key, value] of data) if (String(value).trim()) query.set(key, String(value).trim());
    const result = await api(`${operationPath('dispatch-traces')}?${query}`);
    if (!bot || own !== epoch) return;
    traces = result.traces || [];
    traceScanTruncated = Boolean(result.candidate_scan_truncated);
    renderTraces();
    if (traceScanTruncated) $('dispatch-trace-status').textContent += ' \u5019\u9009\u626b\u63cf\u5df2\u5230\u4e0a\u9650\uff0c\u8bf7\u589e\u52a0\u6d88\u606f\u3001\u7fa4\u6216\u65f6\u95f4\u7b5b\u9009\u3002';
  }

  $('route-graph-filter').addEventListener('input', render);
  $('route-graph-refresh').addEventListener('click', event => busy(event.currentTarget, refresh));
  $('dispatch-trace-filter').addEventListener('submit', event => {
    event.preventDefault(); busy(event.submitter, queryTraces);
  });

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
