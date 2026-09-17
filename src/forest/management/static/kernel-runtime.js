const $ = id => document.getElementById(id);

export function createModulePanel(api, busy, notice) {
  let bot = null, state = null, epoch = 0, readSeq = 0, write = null, key = null;
  const valid = own => bot !== null && own === epoch;
  const base = () => `/scopes/bot/${encodeURIComponent(bot.bot_id)}/modules/kernel.runtime`;

  function render(value) {
    state = value;
    const supervisor = value.supervisor || {};
    $('kernel-runtime-status').textContent = supervisor.error_code || '运行投影已同步';
    $('kernel-runtime-host').textContent = supervisor.attached ? '已挂载' : '恢复中';
    $('kernel-runtime-storage').textContent = supervisor.storage_generation ?? '—';
    $('kernel-runtime-admission').textContent = value.data_admission ? '正在处理业务' : '业务已暂停';
    $('kernel-runtime-epoch').textContent = value.maintenance_epoch;
    $('kernel-runtime-maintenance-toggle').textContent =
      value.data_admission ? '暂停业务处理' : '恢复业务处理';
  }

  async function refresh() {
    if (!bot || write) return;
    const own = epoch, seq = ++readSeq;
    const value = await api(`${base()}/queries/runtime`);
    if (!valid(own) || seq !== readSeq) return;
    render(value);
  }

  $('kernel-runtime-maintenance-form').addEventListener('input', () => { key = null; });
  $('kernel-runtime-refresh').addEventListener('click', event => busy(event.currentTarget, refresh));
  $('kernel-runtime-maintenance-form').addEventListener('submit', event => {
    event.preventDefault();
    const form = event.currentTarget;
    if (!state || !form.reportValidity() || write) return;
    busy($('kernel-runtime-maintenance-toggle'), async () => {
      const own = epoch;
      key ||= crypto.randomUUID();
      const sentKey = key;
      readSeq++;
      write = {own, key: sentKey};
      try {
        const value = await api(`${base()}/actions/maintenance`, {
          expected_maintenance_epoch: state.maintenance_epoch,
          data_admission: !state.data_admission,
          reason: form.elements.reason.value,
        }, {'Idempotency-Key': sentKey});
        if (!valid(own) || key !== sentKey) return;
        key = null; form.reset(); render(value);
        notice(value.data_admission ? 'Bot 已恢复业务处理。' : 'Bot 已暂停业务处理。', true);
      } catch (error) {
        if (valid(own) && error.code === 'MAINTENANCE_CONFLICT') {
          write = null; await refresh();
        }
        throw error;
      } finally {
        if (write?.own === own && write?.key === sentKey) write = null;
      }
    });
  });

  function reset() {
    epoch++; readSeq++; bot = null; state = null; write = null; key = null;
    $('kernel-runtime-maintenance-form').reset();
    for (const id of ['kernel-runtime-status', 'kernel-runtime-host',
      'kernel-runtime-storage', 'kernel-runtime-admission', 'kernel-runtime-epoch']) {
      $(id).textContent = '—';
    }
  }
  async function select(row) { reset(); bot = row; await refresh(); }
  function invalidateContent(scope) {
    if (!bot || scope !== base()) return;
    epoch++; readSeq++; state = null; key = null;
  }
  return {select, reset, refresh, invalidateContent};
}
