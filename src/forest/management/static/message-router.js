import {messageHandlerDiagnosticsPanel} from './message-handler-diagnostics.js';
import {messageHandlerPoliciesPanel} from './message-handler-policies.js';

export function createModulePanel(api, busy, notice) {
  const policies = messageHandlerPoliciesPanel(api, busy, notice);
  const diagnostics = messageHandlerDiagnosticsPanel(api, busy, notice);

  async function select(bot) {
    await policies.select(bot);
    await diagnostics.select(bot);
  }

  function reset() {
    policies.reset();
    diagnostics.reset();
  }

  function invalidateContent(scope) {
    policies.invalidateContent(scope);
    diagnostics.invalidateContent(scope);
  }

  return {select, reset, invalidateContent};
}
