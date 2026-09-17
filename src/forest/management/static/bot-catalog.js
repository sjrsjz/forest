const $ = id => document.getElementById(id);
const statusText = state => ({PENDING:'等待创建存储',PROVISIONING:'正在创建存储',READY:'存储就绪',FAILED:'创建未完成'})[state] || state;

export function createModulePanel(api, busy, notice, {openScope}) {
  let scope = null, rows = [], selected = null, createKey = null, renameKey = null;
  let retryKey = null, readSeq = 0;
  const base = () => `/scopes/deployment/${encodeURIComponent(scope.scope_id)}/modules/deployment.bot-catalog`;
  const query = (operation, params = {}) => `${base()}/queries/${operation}?${new URLSearchParams(params)}`;

  function renderList(managedAvailable) {
    $('bot-list').replaceChildren();
    if (!rows.length) $('bot-list').textContent = '还没有 Bot。从右侧创建第一个。';
    for (const row of rows) {
      const button = document.createElement('button'); button.className = 'bot-row subtle';
      button.textContent = `${row.display_name} · ${statusText(row.stage)}`;
      button.addEventListener('click', () => busy(button, () => selectBot(row.bot_id)));
      $('bot-list').append(button);
    }
    const mode = $('create-bot-form').elements.mode;
    mode.options[0].disabled = !managedAvailable;
    if (!managedAvailable) mode.value = 'external';
    $('bot-dsn-label').classList.toggle('hidden', mode.value !== 'external');
  }

  async function load() {
    if (!scope) return;
    const seq = ++readSeq, data = await api(query('bots'));
    if (seq !== readSeq || !scope) return data;
    rows = data.bots || []; renderList(data.managed_available); return data;
  }

  async function selectBot(botId) {
    const seq = ++readSeq;
    const row = await api(query('bot', {bot_id: botId}));
    if (seq !== readSeq || !scope) return;
    selected = row; renameKey = null; retryKey = null;
    $('catalog-bot-detail').classList.remove('hidden');
    $('catalog-bot-name').textContent = row.display_name;
    $('catalog-bot-status').textContent = `${statusText(row.stage)}${row.error_code ? ` · ${row.error_code}` : ''}`;
    $('rename-bot-form').elements.display_name.value = row.display_name;
    $('retry-bot').classList.toggle('hidden', row.stage !== 'FAILED');
    const job = await api(query('provision-job', {job_id: row.job_id}));
    if (seq !== readSeq) return;
    $('bot-job-events').replaceChildren();
    for (const event of job.events) {
      const item = document.createElement('li');
      item.textContent = `${event.step} · ${event.created_at}`; $('bot-job-events').append(item);
    }
    if (row.stage === 'READY') await openScope({kind:'bot', id:row.bot_id, row});
  }

  $('refresh-bots').addEventListener('click', event => busy(event.currentTarget, load));
  $('create-bot-form').addEventListener('input', () => {
    createKey = null;
    $('bot-dsn-label').classList.toggle('hidden', $('create-bot-form').elements.mode.value !== 'external');
  });
  $('create-bot-form').addEventListener('submit', event => {
    event.preventDefault(); const form = event.currentTarget;
    busy(form.querySelector('button'), async () => {
      createKey ||= crypto.randomUUID(); const values = Object.fromEntries(new FormData(form));
      if (values.mode === 'managed' || !values.dsn) delete values.dsn;
      const row = await api(`${base()}/actions/bot.create`, {...values, expected_revision:0},
        {'Idempotency-Key':createKey});
      form.reset(); createKey = null; await load(); await selectBot(row.bot_id);
      notice('Bot 创建任务已接纳。可在目录中刷新查看存储进度。', true);
    });
  });
  $('rename-bot-form').addEventListener('input', () => { renameKey = null; });
  $('rename-bot-form').addEventListener('submit', event => {
    event.preventDefault(); const form = event.currentTarget;
    busy(form.querySelector('button'), async () => {
      renameKey ||= crypto.randomUUID();
      const row = await api(`${base()}/actions/bot.rename`, {bot_id:selected.bot_id,
        display_name:form.elements.display_name.value, expected_revision:selected.revision},
        {'Idempotency-Key':renameKey});
      await load(); await selectBot(row.bot_id); notice('Bot 名称已更新。', true);
    });
  });
  $('retry-bot').addEventListener('click', event => busy(event.currentTarget, async () => {
    retryKey ||= crypto.randomUUID();
    const row = await api(`${base()}/actions/bot.retry`, {bot_id:selected.bot_id,
      expected_revision:selected.revision}, {'Idempotency-Key':retryKey});
    await load(); await selectBot(row.bot_id);
  }));

  return {
    async select(row) { scope = row; await load(); },
    reset() {
      readSeq++; scope = null; rows = []; selected = null; createKey = null;
      renameKey = null; retryKey = null; $('bot-list').replaceChildren();
      $('catalog-bot-detail').classList.add('hidden'); $('create-bot-form').reset();
    },
    invalidateContent() { readSeq++; },
  };
}
