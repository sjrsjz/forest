function queryFrom(form) {
  const data = new FormData(form);
  const query = {limit: Number(data.get('limit'))};
  for (const key of [
    'action', 'producer_component_id', 'origin_kind',
    'resource_kind', 'resource_id', 'operation_kind', 'operation_id',
  ]) {
    const value = data.get(key);
    if (value) query[key] = value;
  }
  return query;
}

function identity(kind, id) {
  return kind && id ? `${kind}:${id}` : '—';
}

function appendRow(rows, item) {
  const row = document.createElement('article');
  row.className = 'audit-row';
  const time = document.createElement('time');
  time.dateTime = item.created_at;
  time.textContent = new Date(item.created_at).toLocaleString();
  const severity = document.createElement('strong');
  severity.dataset.severity = item.severity;
  severity.textContent = item.severity;
  const action = document.createElement('code');
  action.textContent = item.action;
  const producer = document.createElement('code');
  producer.textContent = item.producer_component_id;
  const actor = document.createElement('span');
  actor.textContent = identity(item.actor_kind, item.actor_id);
  const resource = document.createElement('span');
  resource.textContent = identity(item.resource_kind, item.resource_id);
  const operation = document.createElement('span');
  operation.textContent = identity(item.operation_kind, item.operation_id);
  const auditId = document.createElement('code');
  auditId.className = 'audit-id';
  auditId.textContent = item.audit_id;
  row.append(time, severity, action, producer, actor, resource, operation, auditId);
  rows.append(row);
}

export async function mount({root, component, api, notice}) {
  const form = root.querySelector('#audit-filters');
  const rows = root.querySelector('#audit-rows');
  const summary = root.querySelector('#audit-summary');
  const more = root.querySelector('#audit-more');
  const path = `/management/v1/modules/${encodeURIComponent(component.component_id)}/queries/audit.records.query`;
  let nextBefore = null;
  let lastQuery = null;
  let loaded = 0;

  async function load(before = null) {
    const query = before ? {...lastQuery, before} : queryFrom(form);
    const page = await api(path, {method: 'POST', body: JSON.stringify(query)});
    if (!before) {
      rows.replaceChildren();
      loaded = 0;
      lastQuery = {...query};
    }
    for (const item of page.records) appendRow(rows, item);
    loaded += page.records.length;
    nextBefore = page.next_before;
    summary.textContent = `已载入 ${loaded} 条脱敏审计记录`;
    more.classList.toggle('hidden', !nextBefore);
  }

  form.onsubmit = event => {
    event.preventDefault();
    load().catch(error => notice(error.message));
  };
  more.onclick = () => load(nextBefore).catch(error => notice(error.message));
  await load();
}
