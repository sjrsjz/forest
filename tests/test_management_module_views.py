from pathlib import Path

STATIC = Path(__file__).resolve().parents[1] / "src/forest/management/static"


def test_control_shell_contains_no_agent_or_memory_product_pages():
    index = (STATIC / "index.html").read_text(encoding="utf-8")
    app = (STATIC / "app.js").read_text(encoding="utf-8")
    style = (STATIC / "style.css").read_text(encoding="utf-8")
    assert 'id="bot-agent"' not in index
    assert 'id="bot-memory"' not in index
    assert "moduleSurfaces" not in app
    assert "'agent.runtime'" not in app
    assert "'memory.graph'" not in app
    assert "runtimeModuleViews" in app
    assert "bot-agent" not in style and "memory-association" not in style


def test_official_module_templates_and_controllers_own_their_surfaces():
    agent = (STATIC / "modules/agent.runtime.html").read_text(encoding="utf-8")
    memory = (STATIC / "modules/memory.graph.html").read_text(encoding="utf-8")
    routing = (STATIC / "modules/routing.graph.html").read_text(encoding="utf-8")
    host = (STATIC / "runtime-module-views.js").read_text(encoding="utf-8")
    agent_style = (STATIC / "modules/agent.runtime.css").read_text(encoding="utf-8")
    memory_style = (STATIC / "modules/memory.graph.css").read_text(encoding="utf-8")
    routing_style = (STATIC / "modules/routing.graph.css").read_text(encoding="utf-8")
    assert 'id="bot-agent"' in agent and 'id="bot-memory"' not in agent
    assert 'id="bot-memory"' in memory and 'id="bot-agent"' not in memory
    assert 'id="bot-route-graph"' in routing
    assert "createModulePanel" in host
    assert "descriptor.management_view" in host
    assert "#bot-agent" in agent_style
    assert ".memory-association" in memory_style
    assert ".route-graph-nodes" in routing_style
