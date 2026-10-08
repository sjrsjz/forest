from pathlib import Path
from runpy import run_path

_violations = run_path(
    str(Path(__file__).resolve().parents[1] / "scripts" / "observe_live_bot.py")
)["_violations"]


def _state(*, members: int, ready: int, recorded: int, active: int, unmatched: int = 0):
    deployment = {
        "supply_state": "READY", "desired_state": "ONLINE",
        "observed_state": "ONLINE", "error_code": None,
    }
    bot = {
        "schema_bundles": ready,
        "ready_schema_bundles": ready,
        "unmatched_ready_schema_bundles": unmatched,
        "members": members,
        "lifecycle_recorded": recorded,
        "active_members": active,
        "memory": {"stale_attempts": 0, "stale_reservations": 0, "facet_states": {}},
        "concurrency": {"overlapping_sessions": 0},
        "conversation_claims": {"misaligned_conversations": 0},
    }
    return deployment, bot


def test_observer_accepts_optional_component_without_storage_bundle():
    deployment, bot = _state(members=23, ready=22, recorded=23, active=23)
    assert _violations(deployment, bot, 200) == []


def test_observer_rejects_missing_lifecycle_or_unmatched_ready_bundle():
    deployment, bot = _state(members=23, ready=22, recorded=22, active=23, unmatched=1)
    assert _violations(deployment, bot, 200) == [
        "RUNTIME_GRAPH_CARDINALITY_INVALID", "RUNTIME_LIFECYCLE_INVALID",
    ]
