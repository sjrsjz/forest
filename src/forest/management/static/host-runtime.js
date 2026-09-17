const $ = id => document.getElementById(id);

export function createModulePanel(api) {
  let epoch = 0;

  function clear() {
    for (const id of [
      'host-runtime-id', 'host-runtime-incarnation',
      'host-runtime-activation', 'host-runtime-deployment',
    ]) $(id).textContent = '—';
  }

  async function select(scope) {
    const own = ++epoch;
    const root = `/scopes/host/${encodeURIComponent(scope.scope_id)}/modules/host.runtime`;
    const state = await api(`${root}/queries/runtime`);
    if (own !== epoch) return;
    $('host-runtime-id').textContent = state.host_id;
    $('host-runtime-incarnation').textContent = state.incarnation;
    $('host-runtime-activation').textContent = state.module_activation_id;
    $('host-runtime-deployment').textContent = state.deployment_id;
  }

  function reset() { epoch++; clear(); }
  function invalidateContent(scope) {
    if (!scope.startsWith('/scopes/host/')) return;
    reset();
  }
  return {select, reset, refresh:select, invalidateContent};
}
