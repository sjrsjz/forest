const $ = id => document.getElementById(id);
const labels = {PENDING:'排队中', INFLIGHT:'处理中', RETRY_WAIT:'等待重试', ACKED:'已确认', DEAD:'死信', RETIRED:'已退役'};

export function componentJobsPanel(api, busy, notice) {
  let bot = null, epoch = 0, readSeq = 0, retirementPlan = null, retirementApplyKey = null;
  const valid = own => bot !== null && own === epoch;
  const base = () => `/bots/${encodeURIComponent(bot.bot_id)}/component-jobs`;
  const post = (path, body, key) => api(path, body, {'Idempotency-Key':key});
  function facts(root, entries) {
    const list = document.createElement('dl');
    for (const [name, value] of entries) {
      const term = document.createElement('dt'), detail = document.createElement('dd');
      term.textContent = name; detail.textContent = String(value ?? '—'); list.append(term, detail);
    }
    root.append(list);
  }
  function clearPlan() {
    retirementPlan = null; retirementApplyKey = null;
    $('component-job-retirement-preview').classList.add('hidden');
    $('component-job-retirement-diff').replaceChildren();
  }
  async function previewRetirement(row, button) {
    const own = epoch, prefix = base(), key = crypto.randomUUID();
    await busy(button, async () => {
      const result = await post(prefix + '/retirement-plans', {
        component_id:row.component_id, handler_id:row.handler_id,
        expected_revision:row.management_revision,
      }, key);
      if (!valid(own)) return;
      retirementPlan = result; retirementApplyKey = crypto.randomUUID();
      $('component-job-retirement-diff').textContent = JSON.stringify({
        component_id:result.component_id, handler_id:result.handler_id,
        expected_revision:result.expected_revision, impact:result.impact,
        expires_at:result.expires_at,
      }, null, 2);
      $('component-job-retirement-preview').classList.remove('hidden');
      $('component-job-retirement-preview').scrollIntoView({behavior:'smooth', block:'nearest'});
    });
  }
  function renderHandlers(rows) {
    const root = $('component-job-handlers'); root.replaceChildren();
    for (const row of rows) {
      const item = document.createElement('article'); item.className = 'job-occurrence';
      const title = document.createElement('h3');
      title.textContent = `${row.component_id} · ${row.handler_id} · ${row.active ? '启用' : '已退役'}`;
      item.append(title);
      facts(item, [['最大尝试',row.max_attempts], ['单次超时（秒）',row.timeout_seconds],
        ['最大并发',row.max_concurrency], ['管理修订',row.management_revision],
        ['当前代际',row.generation_id], ['更新时间',row.updated_at], ['退役时间',row.retired_at]]);
      if (row.active) {
        const button = document.createElement('button'); button.type = 'button';
        button.className = 'subtle'; button.textContent = '预览处理器退役';
        button.addEventListener('click', () => previewRetirement(row, button)); item.append(button);
      }
      root.append(item);
    }
    if (!rows.length) root.textContent = '当前 Bot 没有组件持久任务处理器。';
  }
  function renderJobs(rows) {
    const root = $('component-job-items'); root.replaceChildren();
    for (const row of rows) {
      const item = document.createElement('article'); item.className = 'job-occurrence';
      const title = document.createElement('h3');
      title.textContent = `${labels[row.status] || row.status} · #${row.job_seq} · ${row.handler_id}`;
      item.append(title);
      facts(item, [['任务',row.job_id], ['组件',row.component_id], ['调度操作',row.operation_id],
        ['处理尝试',row.attempt_count], ['调度次数',row.dispatch_count],
        ['最大尝试',row.max_attempts], ['回放轮次',row.replay_epoch],
        ['正文',row.payload_state === 'ACTIVE' ? '可用' : '不可用'],
        ['错误码',row.last_error_code], ['首次可用',row.first_available_at], ['创建时间',row.created_at]]);
      if (row.replay_allowed) {
        const button = document.createElement('button'); button.type = 'button';
        button.textContent = '重放死信'; const key = crypto.randomUUID(), own = epoch, prefix = base();
        button.addEventListener('click', () => busy(button, async () => {
          await post(prefix + '/replays', {
            job_id:row.job_id, expected_revision:row.management_revision,
          }, key);
          if (!valid(own)) return;
          notice('组件任务已进入新的重放轮次；原任务身份保持不变。', true); await refresh();
        })); item.append(button);
      }
      root.append(item);
    }
    if (!rows.length) root.textContent = '当前筛选条件下没有组件持久任务。';
  }
  async function refresh() {
    if (!bot) return;
    const own = epoch, seq = ++readSeq, status = $('component-job-status-filter').value;
    const query = new URLSearchParams({limit:'200'}); if (status) query.set('status', status);
    const result = await api(base() + '?' + query.toString());
    if (!valid(own) || seq !== readSeq) return;
    renderHandlers(result.handlers || []); renderJobs(result.jobs || []);
    $('component-job-status').textContent = `${result.handlers?.length || 0} 个处理器 · ${result.jobs?.length || 0} 个任务`;
  }
  $('component-job-refresh').addEventListener('click', event => busy(event.currentTarget, refresh));
  $('component-job-status-filter').addEventListener('change', event => busy(event.currentTarget, refresh));
  $('component-job-retirement-apply').addEventListener('click', event => {
    if (!retirementPlan || !bot) return;
    const own = epoch, plan = retirementPlan, key = retirementApplyKey, prefix = base();
    busy(event.currentTarget, async () => {
      const result = await post(prefix + '/retirement-requests', {
        expected_revision:plan.expected_revision, plan_id:plan.plan_id,
        plan_digest:plan.plan_digest,
      }, key);
      if (!valid(own) || plan !== retirementPlan) return;
      clearPlan();
      notice(`任务处理器已退役，${result.retired_job_count} 个待处理任务已终结。`, true);
      await refresh();
    });
  });
  function reset() {
    epoch++; readSeq++; bot = null; clearPlan();
    $('bot-component-jobs').classList.add('hidden'); $('component-job-status').textContent = '';
    $('component-job-handlers').replaceChildren(); $('component-job-items').replaceChildren();
    $('component-job-status-filter').value = '';
  }
  async function select(row) {
    reset(); bot = row; const own = epoch; $('bot-component-jobs').classList.remove('hidden');
    await refresh(); if (!valid(own)) return;
  }
  function invalidateContent(scope) {
    if (!bot || scope !== base()) return;
    epoch++; readSeq++; clearPlan();
    $('component-job-status').textContent = '相关管理正文已不可用，请重新读取组件任务状态。';
  }
  return {select, reset, refresh, invalidateContent};
}
