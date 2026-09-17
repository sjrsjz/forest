const $ = id => document.getElementById(id);

export function createModulePanel(api, busy) {
  let bot = null, epoch = 0, readSeq = 0, records = [], nextBefore = null;
  const valid = own => bot !== null && own === epoch;
  const base = () => `/scopes/bot/${encodeURIComponent(bot.bot_id)}/modules/kernel.audit`;

  function query(before = null) {
    const params = new URLSearchParams({limit: '50'});
    const action = $('bot-audit-action').value.trim();
    if (action) params.set('action', action);
    if (before) params.set('before', before);
    return `${base()}/queries/records?${params}`;
  }

  function render() {
    const root = $('bot-audit-list'); root.replaceChildren();
    $('bot-audit-empty').classList.toggle('hidden', records.length !== 0);
    for (const row of records) {
      const item = document.createElement('article'); item.className = 'audit-row';
      const title = document.createElement('h3');
      title.textContent = `${row.action} · ${row.origin_kind}`;
      const meta = document.createElement('p'); meta.className = 'muted';
      meta.textContent = row.created_at;
      const refs = document.createElement('p'); refs.className = 'hint';
      refs.textContent = `audit ${row.audit_id} · operation ${row.operation_id || '—'} · ` +
        (row.has_receipt ? '有持久回执' : '无控制回执');
      item.append(title, meta, refs); root.append(item);
    }
    $('bot-audit-more').classList.toggle('hidden', !nextBefore);
  }

  async function refresh({append = false} = {}) {
    if (!bot || (append && !nextBefore)) return;
    const own = epoch, seq = ++readSeq;
    const data = await api(query(append ? nextBefore : null));
    if (!valid(own) || seq !== readSeq) return;
    records = append ? records.concat(data.records || []) : (data.records || []);
    nextBefore = data.next_before; render();
  }

  $('bot-audit-filter').addEventListener('submit', event => {
    event.preventDefault(); busy(event.currentTarget.querySelector('button'), refresh);
  });
  $('bot-audit-refresh').addEventListener('click', event => busy(event.currentTarget, refresh));
  $('bot-audit-more').addEventListener('click', event =>
    busy(event.currentTarget, () => refresh({append: true})));

  function reset() {
    epoch++; readSeq++; bot = null; records = []; nextBefore = null;
    $('bot-audit-filter').reset(); $('bot-audit-list').replaceChildren();
    $('bot-audit-empty').classList.add('hidden'); $('bot-audit-more').classList.add('hidden');
  }
  async function select(row) { reset(); bot = row; await refresh(); }
  function invalidateContent(scope) {
    if (!bot || scope !== base()) return;
    epoch++; readSeq++; records = []; nextBefore = null; render();
  }
  return {select, reset, refresh, invalidateContent};
}
