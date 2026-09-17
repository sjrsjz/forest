const $ = id => document.getElementById(id);
const labels = {PENDING:'排队中', INFLIGHT:'处理中', RETRY_WAIT:'等待重试', ACKED:'已确认', DEAD:'死信', RETIRED:'已退役'};

export function eventsPanel(api, busy, notice) {
  let bot = null, epoch = 0, readSeq = 0, retirementPlan = null;
  let retirementApplyKey = null;
  const valid = own => bot !== null && own === epoch;
  const base = () => `/scopes/bot/${encodeURIComponent(bot.bot_id)}/modules/routing.graph`;
  const query = (operation, values = {}) => `${base()}/queries/${operation}?${new URLSearchParams(values)}`;
  const action = operation => `${base()}/actions/${operation}`;
  const post = (operation, body, key) => api(action(operation), body, {'Idempotency-Key':key});
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
    $('event-retirement-preview').classList.add('hidden');
    $('event-retirement-diff').replaceChildren();
  }
  async function previewRetirement(row, button) {
    const own = epoch, key = crypto.randomUUID();
    await busy(button, async () => {
      const result = await post('event.retirement.plan', {
        subscriber_id:row.subscriber_id,
        contract_major:row.contract_major,
        expected_revision:row.management_revision,
      }, key);
      if (!valid(own)) return;
      retirementPlan = result; retirementApplyKey = crypto.randomUUID();
      $('event-retirement-diff').textContent = JSON.stringify({
        subscriber_id:result.subscriber_id,
        contract_major:result.contract_major,
        expected_revision:result.expected_revision,
        impact:result.impact,
        expires_at:result.expires_at,
      }, null, 2);
      $('event-retirement-preview').classList.remove('hidden');
      $('event-retirement-preview').scrollIntoView({behavior:'smooth', block:'nearest'});
    });
  }
  function renderSubscriptions(rows) {
    const root = $('event-subscriptions'); root.replaceChildren();
    for (const row of rows) {
      const item = document.createElement('article'); item.className = 'job-occurrence';
      const title = document.createElement('h3');
      title.textContent = `${row.subscriber_id} · ${row.active ? '启用' : '已退役'}`; item.append(title);
      facts(item, [['Topic',row.topic], ['契约',row.contract], ['组件',row.component_id],
        ['最大尝试',row.max_attempts], ['管理修订',row.management_revision],
        ['当前代际',row.generation_id], ['更新时间',row.updated_at], ['退役时间',row.retired_at]]);
      if (row.active) {
        const button = document.createElement('button'); button.type = 'button';
        button.className = 'subtle'; button.textContent = '预览退役影响';
        button.addEventListener('click', () => previewRetirement(row, button)); item.append(button);
      }
      root.append(item);
    }
    if (!rows.length) root.textContent = '当前 Bot 没有持久事件订阅。';
  }
  function renderDeliveries(rows) {
    const root = $('event-deliveries'); root.replaceChildren();
    for (const row of rows) {
      const item = document.createElement('article'); item.className = 'job-occurrence';
      const title = document.createElement('h3');
      title.textContent = `${labels[row.status] || row.status} · #${row.event_seq} · ${row.topic}`; item.append(title);
      facts(item, [['投递',row.delivery_id], ['事件',row.event_id], ['订阅',row.subscriber_id],
        ['契约',row.contract], ['发布组件',row.publisher_component_id],
        ['处理组件',row.component_id], ['处理尝试',row.attempt_count],
        ['调度次数',row.dispatch_count], ['回放轮次',row.replay_epoch],
        ['正文',row.payload_state === 'ACTIVE' ? '可用' : '不可用'],
        ['错误码',row.last_error_code], ['发布时间',row.published_at]]);
      if (row.replay_allowed) {
        const button = document.createElement('button'); button.type = 'button';
        button.textContent = '回放死信'; const key = crypto.randomUUID(), own = epoch;
        button.addEventListener('click', () => busy(button, async () => {
          await post('event.replay', {
            delivery_id:row.delivery_id, expected_revision:row.management_revision,
          }, key);
          if (!valid(own)) return;
          notice('死信已进入新的回放轮次；事件身份保持不变。', true); await refresh();
        })); item.append(button);
      }
      root.append(item);
    }
    if (!rows.length) root.textContent = '当前筛选条件下没有事件投递。';
  }
  async function refresh() {
    if (!bot) return;
    const own = epoch, seq = ++readSeq, status = $('event-status-filter').value;
    const values = {limit:'200'}; if (status) values.status = status;
    const result = await api(query('events', values));
    if (!valid(own) || seq !== readSeq) return;
    renderSubscriptions(result.subscriptions || []); renderDeliveries(result.deliveries || []);
    $('event-status').textContent = `${result.subscriptions?.length || 0} 个订阅 · ${result.deliveries?.length || 0} 条投递`;
  }
  $('event-refresh').addEventListener('click', event => busy(event.currentTarget, refresh));
  $('event-status-filter').addEventListener('change', event => busy(event.currentTarget, refresh));
  $('event-retirement-apply').addEventListener('click', event => {
    if (!retirementPlan || !bot) return;
    const own = epoch, plan = retirementPlan, key = retirementApplyKey;
    busy(event.currentTarget, async () => {
      const result = await post('event.retirement.apply', {
        expected_revision:plan.expected_revision,
        plan_id:plan.plan_id,
        plan_digest:plan.plan_digest,
      }, key);
      if (!valid(own) || plan !== retirementPlan) return;
      clearPlan();
      notice(`订阅已退役，${result.retired_delivery_count} 条待处理投递已终结。`, true);
      await refresh();
    });
  });
  function reset() {
    epoch++; readSeq++; bot = null; clearPlan();
    $('bot-events').open = false;
    $('bot-events').classList.add('hidden'); $('event-status').textContent = '';
    $('event-subscriptions').replaceChildren(); $('event-deliveries').replaceChildren();
    $('event-status-filter').value = '';
  }
  async function select(row) {
    reset(); bot = row; const own = epoch; $('bot-events').classList.remove('hidden');
    await refresh(); if (!valid(own)) return;
  }
  function invalidateContent(scope) {
    if (!bot || !scope.startsWith(base())) return;
    epoch++; readSeq++; clearPlan();
    $('event-status').textContent = '相关管理正文已不可用，请重新读取事件状态。';
  }
  return {select, reset, refresh, invalidateContent};
}
