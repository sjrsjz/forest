from pathlib import Path

STATIC = Path(__file__).resolve().parents[1] / "src/forest/management/static"


def test_control_shell_contains_no_module_owned_product_pages():
    index = (STATIC / "index.html").read_text(encoding="utf-8")
    app = (STATIC / "app.js").read_text(encoding="utf-8")
    style = (STATIC / "style.css").read_text(encoding="utf-8")
    assert 'id="bot-agent"' not in index
    assert 'id="bot-memory"' not in index
    assert 'id="bot-jobs"' not in index
    assert 'id="bot-bindings"' not in index
    assert 'id="bot-events"' not in index
    assert 'id="bot-component-jobs"' not in index
    assert 'id="bot-command-policies"' not in index
    assert 'id="bot-command-diagnostics"' not in index
    assert 'id="bot-handler-policies"' not in index
    assert 'id="bot-handler-diagnostics"' not in index
    assert 'id="bot-audit"' not in index
    assert 'id="audit"' not in index
    assert 'id="nav-audit"' not in index
    assert 'id="nav-accounts"' not in index
    assert 'id="accounts"' not in index
    assert 'id="config-form"' not in index
    assert 'id="bot-list"' not in index
    assert 'id="create-bot-form"' not in index
    assert 'id="rename-bot-form"' not in index
    assert 'id="retry-bot"' not in index
    assert 'id="bot-job-events"' not in index
    assert 'id="package-import-form"' not in index
    assert 'id="package-list"' not in index
    assert "jobsPanel" not in app
    assert "eventsPanel" not in app
    assert "componentJobsPanel" not in app
    assert "commandPoliciesPanel" not in app
    assert "businessCommandDiagnosticsPanel" not in app
    assert "messageHandlerPoliciesPanel" not in app
    assert "messageHandlerDiagnosticsPanel" not in app
    assert "botAuditPanel" not in app
    assert "bot-audit-panel" not in app
    assert "auditPanel" not in app
    assert "moduleSurfaces" not in app
    assert "'agent.runtime'" not in app
    assert "'memory.graph'" not in app
    assert "managementContributions" in app
    assert "runtime-module-views" not in app
    assert "/scopes/bot/" in (STATIC / "agent-panel.js").read_text(encoding="utf-8")
    assert "/runtime-modules/" not in "".join(
        path.read_text(encoding="utf-8") for path in STATIC.glob("*.js")
    )
    assert "bot-agent" not in style and "memory-association" not in style
    assert "#job-" not in style


def test_official_module_templates_and_controllers_own_their_surfaces():
    app = (STATIC / "app.js").read_text(encoding="utf-8")
    agent = (STATIC / "modules/agent.runtime.html").read_text(encoding="utf-8")
    memory = (STATIC / "modules/memory.graph.html").read_text(encoding="utf-8")
    routing = (STATIC / "modules/routing.graph.html").read_text(encoding="utf-8")
    jobs = (STATIC / "modules/scheduler.jobs.html").read_text(encoding="utf-8")
    onebot = (STATIC / "modules/adapter.onebot.html").read_text(encoding="utf-8")
    command = (STATIC / "modules/command.router.html").read_text(encoding="utf-8")
    message = (STATIC / "modules/message.router.html").read_text(encoding="utf-8")
    audit = (STATIC / "modules/kernel.audit.html").read_text(encoding="utf-8")
    deployment_audit = (STATIC / "modules/deployment.audit.html").read_text(encoding="utf-8")
    deployment_accounts = (STATIC / "modules/deployment.accounts.html").read_text(
        encoding="utf-8"
    )
    deployment_configuration = (
        STATIC / "modules/deployment.configuration.html"
    ).read_text(encoding="utf-8")
    deployment_catalog = (
        STATIC / "modules/deployment.bot-catalog.html"
    ).read_text(encoding="utf-8")
    deployment_packages = (
        STATIC / "modules/deployment.package-registry.html"
    ).read_text(encoding="utf-8")
    host = (STATIC / "management-contributions.js").read_text(encoding="utf-8")
    agent_style = (STATIC / "modules/agent.runtime.css").read_text(encoding="utf-8")
    memory_style = (STATIC / "modules/memory.graph.css").read_text(encoding="utf-8")
    routing_style = (STATIC / "modules/routing.graph.css").read_text(encoding="utf-8")
    jobs_style = (STATIC / "modules/scheduler.jobs.css").read_text(encoding="utf-8")
    onebot_style = (STATIC / "modules/adapter.onebot.css").read_text(encoding="utf-8")
    assert 'id="bot-agent"' in agent and 'id="bot-memory"' not in agent
    assert 'id="bot-memory"' in memory and 'id="bot-agent"' not in memory
    assert 'id="bot-route-graph"' in routing
    assert 'id="dispatch-trace-panel"' in routing
    assert '<details id="bot-events"' in routing
    assert '<details id="bot-component-jobs"' in routing
    routing_controller = (STATIC / "routing-graph.js").read_text(encoding="utf-8")
    assert "eventsPanel" in routing_controller
    assert "componentJobsPanel" in routing_controller
    assert 'id="bot-jobs"' in jobs and 'id="job-config"' in jobs
    assert 'id="bot-bindings"' in onebot and 'id="binding-config"' in onebot
    assert '<details id="bot-command-policies"' in command
    assert '<details id="bot-command-diagnostics"' in command
    command_controller = (STATIC / "command-router.js").read_text(encoding="utf-8")
    assert "commandPoliciesPanel" in command_controller
    assert "businessCommandDiagnosticsPanel" in command_controller
    assert '<details id="bot-handler-policies"' in message
    assert '<details id="bot-handler-diagnostics"' in message
    message_controller = (STATIC / "message-router.js").read_text(encoding="utf-8")
    assert "messageHandlerPoliciesPanel" in message_controller
    assert "messageHandlerDiagnosticsPanel" in message_controller
    assert 'id="bot-audit"' in audit
    audit_controller = (STATIC / "kernel-audit.js").read_text(encoding="utf-8")
    assert "createModulePanel" in audit_controller
    assert "/modules/kernel.audit" in audit_controller
    assert "/queries/records" in audit_controller
    assert 'id="deployment-audit"' in deployment_audit
    deployment_audit_controller = (STATIC / "deployment-audit.js").read_text(encoding="utf-8")
    assert "createModulePanel" in deployment_audit_controller
    assert "/modules/deployment.audit" in deployment_audit_controller
    assert 'id="deployment-accounts"' in deployment_accounts
    accounts_controller = (STATIC / "accounts-panel.js").read_text(encoding="utf-8")
    assert "createModulePanel" in accounts_controller
    assert "/modules/deployment.accounts" in accounts_controller
    assert 'id="deployment-configuration"' in deployment_configuration
    configuration_controller = (
        STATIC / "deployment-configuration.js"
    ).read_text(encoding="utf-8")
    assert "createModulePanel" in configuration_controller
    assert "/modules/deployment.configuration" in configuration_controller
    assert 'id="bot-list"' in deployment_catalog
    assert 'id="create-bot-form"' in deployment_catalog
    assert 'id="rename-bot-form"' in deployment_catalog
    assert 'id="retry-bot"' in deployment_catalog
    assert 'id="bot-job-events"' in deployment_catalog
    catalog_controller = (STATIC / "bot-catalog.js").read_text(encoding="utf-8")
    assert "createModulePanel" in catalog_controller
    assert "/modules/deployment.bot-catalog" in catalog_controller
    assert "openScope({kind:'bot'" in catalog_controller
    assert "openScope" in host
    assert "/scopes/${encodeURIComponent(target.kind)}" in app
    assert 'id="deployment-package-import-form"' in deployment_packages
    assert 'id="deployment-package-list"' in deployment_packages
    package_controller = (STATIC / "package-registry.js").read_text(encoding="utf-8")
    assert "createModulePanel" in package_controller
    assert "/modules/deployment.package-registry" in package_controller
    assert "/uploads/package.import" in package_controller
    assert "/downloads/package.archive" in package_controller
    assert "createModulePanel" in host
    assert "descriptor.management_contributions" in host
    assert "forest.management-contribution/1" in host
    assert "#bot-agent" in agent_style
    assert ".memory-association" in memory_style
    assert ".route-graph-nodes" in routing_style
    assert ".dispatch-traces" in routing_style
    assert "#job-config" in jobs_style
    assert "#bot-bindings" in onebot_style
    assert 'id="dispatch-trace-more"' in routing
