function iso(value) {
  return value ? new Date(value).toISOString() : null;
}

function appendField(list, name, value) {
  const dt = document.createElement('dt');
  const dd = document.createElement('dd');
  dt.textContent = name;
  dd.textContent = value ?? '—';
  list.append(dt, dd);
}

function sourceFilters(form) {
  const data = new FormData(form);
  const filters = {limit: Number(data.get('limit'))};
  for (const key of ['trunk_id', 'author_subject_ref', 'modality', 'unit_kind']) {
    if (data.get(key)) filters[key] = data.get(key);
  }
  if (data.get('occurred_from')) filters.occurred_from = iso(data.get('occurred_from'));
  if (data.get('occurred_to')) filters.occurred_to = iso(data.get('occurred_to'));
  return filters;
}

function appendSourceRow(rows, item, select) {
  const row = document.createElement('button');
  row.type = 'button';
  row.className = 'memory-row';
  const time = document.createElement('time');
  time.dateTime = item.occurred_at;
  time.textContent = new Date(item.occurred_at).toLocaleString();
  const source = document.createElement('code');
  source.textContent = item.external_group_id ? `群 ${item.external_group_id}` : item.source_id;
  const author = document.createElement('code');
  author.textContent = item.author_subject_ref;
  const kind = document.createElement('span');
  kind.textContent = item.unit_kind;
  const modality = document.createElement('span');
  modality.className = 'memory-modality';
  modality.textContent = item.modality;
  row.append(time, source, author, kind, modality);
  row.onclick = () => select(item, row);
  rows.append(row);
}

function appendRelation(detail, relation) {
  const box = document.createElement('section');
  box.className = 'relation';
  const heading = document.createElement('strong');
  heading.textContent = `${relation.relation_kind} · 权重 ${relation.weight_micros}`;
  const edge = document.createElement('div');
  edge.className = 'relation-edge';
  edge.textContent = `${relation.left_kind}:${relation.left_id} → ${relation.right_kind}:${relation.right_id}`;
  const evidenceHeading = document.createElement('small');
  evidenceHeading.textContent = `来源证据 ${relation.evidence.length}`;
  box.append(heading, edge, evidenceHeading);
  for (const evidence of relation.evidence) {
    const source = document.createElement('div');
    source.className = 'relation-evidence';
    const group = evidence.external_group_id ? `群 ${evidence.external_group_id}` : evidence.source_kind;
    source.textContent = `${group} · ${evidence.trunk_id ?? '无主干'} · ${evidence.author_subject_ref} · ${evidence.source_unit_id}#${evidence.part_id}`;
    box.append(source);
  }
  detail.append(box);
}

export async function mount({root, component, api, notice}) {
  const form = root.querySelector('#memory-filters');
  const rows = root.querySelector('#memory-rows');
  const summary = root.querySelector('#memory-summary');
  const more = root.querySelector('#memory-more');
  const detail = root.querySelector('#memory-detail');
  const path = `/management/v1/modules/${encodeURIComponent(component.component_id)}/queries/`;
  let page = null;
  let lastFilters = null;
  let loadedCount = 0;

  const trunks = await api(`${path}memory.trunks.list`, {method: 'POST', body: '{}'});
  for (const trunk of trunks) {
    const option = document.createElement('option');
    option.value = trunk.trunk_id;
    option.textContent = `${trunk.external_group_id || trunk.trunk_id} · ${trunk.eligible_sources} 条可召回`;
    form.elements.trunk_id.append(option);
  }

  async function select(item, row) {
    rows.querySelectorAll('.memory-row').forEach(candidate => {
      candidate.classList.toggle('selected', candidate === row);
    });
    detail.replaceChildren();
    const title = document.createElement('h3');
    title.textContent = item.source_unit_id;
    const list = document.createElement('dl');
    appendField(list, '来源群', item.external_group_id ? `onebot:group:${item.external_group_id}` : item.source_id);
    appendField(list, '作者', item.author_subject_ref);
    appendField(list, '主干', item.trunk_id);
    appendField(list, '消息类型', item.unit_kind);
    appendField(list, '模态', item.modality);
    appendField(list, '发生时间', item.occurred_at);
    appendField(list, '内容部件', item.parts.map(part => `${part.part_index}:${part.part_kind}:${part.media_type}`).join(' · '));
    detail.append(title, list);
    try {
      const relations = await api(`${path}memory.relations.query`, {
        method: 'POST',
        body: JSON.stringify({
          endpoint_kind: 'SOURCE_UNIT',
          endpoint_id: item.source_unit_id,
          limit: 100,
          active: true,
          eligible_only: true,
        }),
      });
      const relationHeading = document.createElement('h4');
      relationHeading.textContent = `关联关系 ${relations.items.length}${relations.has_more ? '+' : ''}`;
      detail.append(relationHeading);
      for (const relation of relations.items) appendRelation(detail, relation);
      if (!relations.items.length) {
        const empty = document.createElement('p');
        empty.className = 'muted';
        empty.textContent = '当前来源没有可见关联。';
        detail.append(empty);
      }
    } catch (error) {
      notice(error.message);
    }
  }

  async function load(cursor = null) {
    const filters = cursor ? {...lastFilters} : sourceFilters(form);
    if (cursor) {
      Object.assign(filters, {
        snapshot_accepted_at: page.snapshot_accepted_at,
        after_occurred_at: cursor.occurred_at,
        after_source_unit_id: cursor.source_unit_id,
      });
    }
    const next = await api(`${path}memory.sources.query`, {
      method: 'POST',
      body: JSON.stringify(filters),
    });
    if (!cursor) {
      rows.replaceChildren();
      detail.replaceChildren();
      loadedCount = 0;
      lastFilters = {...filters};
    }
    page = next;
    loadedCount += next.items.length;
    for (const item of next.items) appendSourceRow(rows, item, select);
    summary.textContent = `已载入 ${loadedCount} 条 · 快照 ${new Date(next.snapshot_accepted_at).toLocaleString()}`;
    more.classList.toggle('hidden', !next.has_more);
  }

  form.onsubmit = event => {
    event.preventDefault();
    load().catch(error => notice(error.message));
  };
  more.onclick = () => load(page.next_cursor).catch(error => notice(error.message));
  await load();
}
