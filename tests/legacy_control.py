"""Checksum-frozen pre-039 writers; used only before the tested migration."""

import gzip
import hashlib
import sys
from pathlib import Path
from types import ModuleType
from uuid import uuid4

from forest.packages import digest_json


def _module(name, expected):
    path = Path(__file__).parent / "fixtures" / "legacy_038_control" / (name + ".py.gz")
    source = gzip.decompress(path.read_bytes())
    assert hashlib.sha256(source).hexdigest() == expected
    module = ModuleType("frozen_pre_039_" + name)
    sys.modules[module.__name__] = module
    exec(compile(source, str(path), "exec"), module.__dict__)
    return module


FrozenBotControl = _module("control", "6423724f2e97330b339886e9474d9f67e8146b04cac7f35f32a96eca6d16e2ae").BotControl
_agent = _module("agent_config", "338a63a503d00bf594ba93418948835e79e2d80eda027b1c3d9e440df9e2fc27")
_agent.BotControl = FrozenBotControl
FrozenAgentConfig = _agent.AgentConfig


async def configure_agent_before_cutover(env, *, values, secret):
    """Actual signed old plan/apply, also compatible with the 022/028 schema."""
    control = FrozenAgentConfig(env.store, env.vault)
    operation = "old-agent-plan-" + uuid4().hex
    payload = {"expected_revision": 0, "values": values, "credentials_changed": True,
               "secret_digest": digest_json({"api_key": secret})}
    planned = await control.plan(await env.ticket("agent.config.plan", operation, payload),
        operation, payload, secret_ciphertext=env.vault.encrypt(secret))
    operation = "old-agent-apply-" + uuid4().hex
    payload = {key: planned[key] for key in ("expected_revision", "plan_id", "plan_digest")}
    return await control.apply(await env.ticket("agent.config.apply", operation, payload,
        plan_digest=payload["plan_digest"]), operation, payload)
