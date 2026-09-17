from pathlib import Path

STATIC = Path(__file__).resolve().parents[1] / "src/forest/management/static"


def test_control_shell_contains_no_agent_or_memory_product_pages():
    index = (STATIC / "index.html").read_text(encoding="utf-8")
    app = (STATIC / "app.js").read_text(encoding="utf-8")
    style = (STATIC / "style.css").read_text(encoding="utf-8")
    assert 'id="bot-agent"' not in index
    assert 'id="bot-memory"' not in index
    assert 'id="bot-jobs"' not in index
    assert 'id="bot-bindings"' not in index
    assert 'id="bot-events"' not in index
    assert 'id="bot-component-jobs"' not in index
    assert "jobsPanel" not in app
    assert "eventsPanel" not in app
    assert "componentJobsPanel" not in app
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
    agent = (STATIC / "modules/agent.runtime.html").read_text(encoding="utf-8")
    memory = (STATIC / "modules/memory.graph.html").read_text(encoding="utf-8")
    routing = (STATIC / "modules/routing.graph.html").read_text(encoding="utf-8")
    jobs = (STATIC / "modules/scheduler.jobs.html").read_text(encoding="utf-8")
    onebot = (STATIC / "modules/adapter.onebot.html").read_text(encoding="utf-8")
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
