const $ = (id) => document.getElementById(id);

export function auditPanel(api, busy) {
  let records = [], nextBefore = null, readSeq = 0;

  function query(before = null) {
    const form = $('audit-filter-form');
    const params = new URLSearchParams({limit: '50'});
    for (const name of ['action', 'resource_prefix', 'actor_id']) {
      const value = form.elements[name].value.trim();
      if (value) params.set(name, value);
    }
    if (before) params.set('before', before);
    return `/audit?${params}`;
  }

  function render() {
    const target = $('audit-list'); target.replaceChildren();
    $('audit-empty').classList.toggle('hidden', records.length !== 0);
    for (const row of records) {
      const card = document.createElement('article'); card.className = 'audit-row';
      const title = document.createElement('h3');
      title.textContent = `${row.action} · ${row.resource_key}`;
      const meta = document.createElement('p'); meta.className = 'muted';
      meta.textContent = `${row.created_at} · ${row.actor.username} (${row.actor.account_id})`;
      const refs = document.createElement('p'); refs.className = 'hint';
      refs.textContent = `audit ${row.audit_id} · request ${row.request_id || '—'}`;
      const details = document.createElement('pre');
      details.textContent = JSON.stringify(row.details, null, 2);
      card.append(title, meta, refs, details);
      if (row.details_redacted) {
        const redacted = document.createElement('p'); redacted.className = 'hint';
        redacted.textContent = '未列入公开审计字段的详情已由服务端移除。';
        card.append(redacted);
      }
      target.append(card);
    }
    $('audit-more').classList.toggle('hidden', !nextBefore);
  }

  async function load({append = false} = {}) {
    const seq = ++readSeq;
    const cursor = append ? nextBefore : null;
    if (append && !cursor) return;
    const data = await api(query(cursor));
    if (seq !== readSeq) return data;
    records = append ? records.concat(data.records) : data.records;
    nextBefore = data.next_before;
    render(); return data;
  }

  $('audit-filter-form').addEventListener('submit', (event) => {
    event.preventDefault();
    busy(event.currentTarget.querySelector('button'), () => load());
  });
  $('audit-refresh').addEventListener('click', () => busy($('audit-refresh'), () => load()));
  $('audit-more').addEventListener('click', () => busy($('audit-more'), () => load({append: true})));

  return {
    load,
    reset() {
      readSeq++; records = []; nextBefore = null;
      $('audit-filter-form').reset(); $('audit-list').replaceChildren();
      $('audit-empty').classList.remove('hidden'); $('audit-more').classList.add('hidden');
    },
  };
}
