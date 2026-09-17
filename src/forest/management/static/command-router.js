import {businessCommandDiagnosticsPanel} from './business-command-diagnostics.js';
import {commandPoliciesPanel} from './command-policies.js';

export function createModulePanel(api, busy, notice) {
  const policies = commandPoliciesPanel(api, busy, notice);
  const diagnostics = businessCommandDiagnosticsPanel(api, busy, notice);

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
