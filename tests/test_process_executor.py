import asyncio
import hashlib
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from forest.errors import ForestError
from forest.native_exec_helper import seccomp_available
from forest.package_capabilities import PackageJobCallScope
from forest.process_executor_builtin import parse_process_executor_configuration
from forest.process_executor_effect_driver import NativeProcessEffectDriver
from forest.runtime_contracts import RuntimeServiceInvocation


class FakePayloads:
    def __init__(self):
        self.saved = []

    async def epoch(self):
        return 1

    async def put_bytes(self, **values):
        self.saved.append(values)
        body = values["content"]

        class Saved:
            payload_id = "payload/" + values["owner_id"]
            content_digest = "sha256:" + hashlib.sha256(body).hexdigest()
            byte_length = len(body)
            media_type = values["media_type"]

        return Saved()


def profile(executable: Path, *, network_policy="inherit", wall_seconds=5):
    return {
        "profile_id": "fixture.echo",
        "executable": str(executable.resolve()),
        "executable_sha256": "sha256:" + hashlib.sha256(executable.read_bytes()).hexdigest(),
        "arguments_schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["value"],
            "properties": {"value": {"type": "string", "maxLength": 32}},
            "additionalProperties": False,
        },
        "argv": [{"literal": "-c"}, {"literal": "import sys;print(sys.argv[1])"}, {"argument": "value"}],
        "working_directory": str(executable.parent.resolve()),
        "environment": {"FOREST_FIXTURE": "1"},
        "run_as": {
            "uid": os.getuid() if hasattr(os, "getuid") else 0,
            "gid": os.getgid() if hasattr(os, "getgid") else 0,
        },
        "network_policy": network_policy,
        "require_posix_limits": False,
        "limits": {
            "cpu_seconds": 5,
            "memory_bytes": 256 * 1024 * 1024,
            "wall_seconds": wall_seconds,
            "output_bytes": 4096,
            "open_files": 64,
            "processes": 32,
            "terminate_grace_seconds": 1,
        },
    }


def runtime_with(profile_value, *, cancelled=None):
    payloads = FakePayloads()
    identity = SimpleNamespace(
        component_id="process.executor", generation_id="generation:process:test"
    )

    class Runtime:
        async def verify_generation_member(self, **values):
            assert values == {
                "component_id": identity.component_id,
                "generation_id": identity.generation_id,
            }

    runtime = NativeProcessEffectDriver(
        {"protocol": "forest.process.executor/1", "profiles": [profile_value]},
        1,
        identity,
        {
            "kind": "process.native",
            "name": "process.native.spawn",
        },
        runtime=Runtime(),
        payloads=payloads,
    )
    runtime.accepting = True
    scope = PackageJobCallScope(
        "bot-test",
        identity.component_id,
        identity.generation_id,
        "instance:test",
        "worker:test",
        "call:test",
        "job:test",
        "job.scheduler",
        "job-test",
        cancelled or (lambda: asyncio.sleep(0, result=False)),
    )
    return runtime, payloads, scope


def effect_plan(profile_value, arguments):
    parsed = parse_process_executor_configuration(
        {"protocol": "forest.process.executor/1", "profiles": [profile_value]}
    )[profile_value["profile_id"]]
    argv = NativeProcessEffectDriver._argv(parsed, arguments)
    return {
        "protocol": "forest.process-spawn/1",
        "profile_id": profile_value["profile_id"],
        "arguments": arguments,
        "executable": profile_value["executable"],
        "executable_sha256": profile_value["executable_sha256"],
        "argv": argv,
        "working_directory": profile_value["working_directory"],
        "environment": {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", **profile_value["environment"]},
        "run_as": profile_value["run_as"],
        "network_policy": profile_value["network_policy"],
        "require_posix_limits": profile_value["require_posix_limits"],
        "limits": profile_value["limits"],
    }


def invocation(operation_id="process-effect:test"):
    return RuntimeServiceInvocation(operation_id, 30, "job.scheduler", "job-test")


def test_profile_parser_rejects_dynamic_path_and_invalid_argument_schema(tmp_path):
    value = profile(Path(sys.executable))
    value["executable"] = "python"
    with pytest.raises(ForestError, match="Execution profile is invalid"):
        parse_process_executor_configuration(
            {"protocol": "forest.process.executor/1", "profiles": [value]}
        )

    value = profile(Path(sys.executable))
    value["arguments_schema"] = {"type": "definitely-not-a-json-schema-type"}
    with pytest.raises(ForestError, match="argument schema is invalid"):
        parse_process_executor_configuration(
            {"protocol": "forest.process.executor/1", "profiles": [value]}
        )


@pytest.mark.skipif(os.name != "posix", reason="native resource guard is exercised in Linux")
async def test_profile_executes_without_shell_and_saves_bounded_artifacts():
    executable = Path(sys.executable)
    value = profile(executable)
    runtime, payloads, scope = runtime_with(value)
    result = await runtime.invoke(
        effect_plan(value, {"value": "$(touch should-not-run)"}),
        scope=scope,
        invocation=invocation(),
    )
    assert result["exit_code"] == 0
    assert result["stdout_total_bytes"] == len(b"$(touch should-not-run)\n")
    assert payloads.saved[0]["content"] == b"$(touch should-not-run)\n"
    assert result["stdout"]["payload_id"].endswith(":stdout")


def test_profile_argument_schema_is_enforced_before_spawn():
    runtime, _payloads, _scope = runtime_with(profile(Path(sys.executable)))
    parsed = runtime.profiles["fixture.echo"]
    with pytest.raises(ForestError, match="Arguments do not match"):
        runtime._argv(parsed, {"value": 42})


@pytest.mark.skipif(os.name != "posix", reason="executable ownership is enforced on POSIX")
async def test_worker_process_plan_cannot_exceed_frozen_profile():
    value = profile(Path(sys.executable))
    runtime, _payloads, scope = runtime_with(value)
    plan = effect_plan(value, {"value": "allowed"})
    plan["argv"] = [value["executable"], "-c", "raise SystemExit(99)"]
    with pytest.raises(ForestError) as captured:
        await runtime.invoke(
            plan,
            scope=scope,
            invocation=invocation("process-effect:tampered-plan"),
        )
    assert captured.value.code == "PROCESS_EFFECT_PLAN_MISMATCH"


@pytest.mark.skipif(not seccomp_available(), reason="libseccomp is unavailable")
async def test_deny_profile_blocks_network_syscalls():
    value = profile(Path(sys.executable), network_policy="deny")
    value["argv"] = [
        {"literal": "-c"},
        {"literal": "import socket; socket.socket()"},
        {"argument": "value"},
    ]
    runtime, payloads, scope = runtime_with(value)
    result = await runtime.invoke(
        effect_plan(value, {"value": "ignored"}),
        scope=scope,
        invocation=invocation("process-effect:network"),
    )
    assert result["exit_code"] != 0
    assert result["network_policy"] == "deny"
    assert b"PermissionError" in next(
        item["content"] for item in payloads.saved if item["owner_id"].endswith(":stderr")
    )


@pytest.mark.skipif(os.name != "posix", reason="process-group cancellation requires POSIX")
async def test_cancellation_terminates_the_process_group():
    value = profile(Path(sys.executable))
    value["argv"] = [
        {"literal": "-c"},
        {"literal": "import time; time.sleep(30)"},
        {"argument": "value"},
    ]
    polls = 0

    async def cancelled():
        nonlocal polls
        polls += 1
        return polls >= 3

    runtime, _payloads, scope = runtime_with(value, cancelled=cancelled)
    with pytest.raises(ForestError) as captured:
        await runtime.invoke(
            effect_plan(value, {"value": "ignored"}),
            scope=scope,
            invocation=invocation("process-effect:cancel"),
        )
    assert captured.value.code == "JOB_CANCELLED"
