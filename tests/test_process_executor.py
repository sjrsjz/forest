import asyncio
import hashlib
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from forest.errors import ForestError
from forest.job_protocol import DurableJobExecutionRequest
from forest.native_exec_helper import seccomp_available
from forest.process_executor_builtin import parse_process_executor_configuration
from forest.process_executor_effect_driver import NativeProcessEffectDriver


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


def runtime_with(profile_value):
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
            "name": "job.process.execute",
            "job_type": "native.execute",
        },
        runtime=Runtime(),
        payloads=payloads,
    )
    runtime.accepting = True
    scope = SimpleNamespace(
        component_id=identity.component_id, generation_id=identity.generation_id
    )
    return runtime, payloads, scope


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
    runtime, payloads, scope = runtime_with(profile(executable))
    request = DurableJobExecutionRequest(
        "job-fixture",
        "native.execute",
        1,
        1,
        "test:operator",
        {"profile_id": "fixture.echo", "arguments": {"value": "$(touch should-not-run)"}},
        lambda: asyncio.sleep(0, result=False),
    )
    result = await runtime.invoke_job(request, scope=scope, invocation=None)
    assert result["exit_code"] == 0
    assert result["stdout_total_bytes"] == len(b"$(touch should-not-run)\n")
    assert payloads.saved[0]["content"] == b"$(touch should-not-run)\n"
    assert result["stdout"]["payload_id"].endswith(":stdout")


def test_profile_argument_schema_is_enforced_before_spawn():
    runtime, _payloads, _scope = runtime_with(profile(Path(sys.executable)))
    parsed = runtime.profiles["fixture.echo"]
    with pytest.raises(ForestError, match="Arguments do not match"):
        runtime._argv(parsed, {"value": 42})


@pytest.mark.skipif(not seccomp_available(), reason="libseccomp is unavailable")
async def test_deny_profile_blocks_network_syscalls():
    value = profile(Path(sys.executable), network_policy="deny")
    value["argv"] = [
        {"literal": "-c"},
        {"literal": "import socket; socket.socket()"},
        {"argument": "value"},
    ]
    runtime, payloads, scope = runtime_with(value)
    request = DurableJobExecutionRequest(
        "job-network",
        "native.execute",
        1,
        1,
        "test:operator",
        {"profile_id": "fixture.echo", "arguments": {"value": "ignored"}},
        lambda: asyncio.sleep(0, result=False),
    )
    result = await runtime.invoke_job(request, scope=scope, invocation=None)
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
    runtime, _payloads, scope = runtime_with(value)
    polls = 0

    async def cancelled():
        nonlocal polls
        polls += 1
        return polls >= 3

    request = DurableJobExecutionRequest(
        "job-cancel",
        "native.execute",
        1,
        1,
        "test:operator",
        {"profile_id": "fixture.echo", "arguments": {"value": "ignored"}},
        cancelled,
    )
    with pytest.raises(ForestError) as captured:
        await runtime.invoke_job(request, scope=scope, invocation=None)
    assert captured.value.code == "JOB_CANCELLED"
