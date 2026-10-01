"""Generation-owned native process effect driver for the builtin executor package."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import signal
import stat
import sys
from dataclasses import dataclass
from pathlib import Path

from jsonschema import Draft202012Validator, SchemaError

from forest.errors import ForestError
from forest.native_exec_helper import seccomp_available
from forest.package_capabilities import PackageJobCallScope
from forest.storage.payload_authority import PayloadSource

_PROFILE_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}\Z")


@dataclass(frozen=True)
class ExecutionProfile:
    profile_id: str
    executable: str
    executable_sha256: str
    arguments_schema: dict
    argv: tuple[dict, ...]
    working_directory: str
    environment: dict[str, str]
    run_as_uid: int
    run_as_gid: int
    network_policy: str
    require_posix_limits: bool
    limits: dict[str, int]


def parse_process_executor_configuration(value: object) -> dict[str, ExecutionProfile]:
    """Parse the complete immutable configuration used to build one Generation."""

    if not isinstance(value, dict) or set(value) != {"protocol", "profiles"}:
        raise ForestError(
            "PROCESS_EXECUTOR_CONFIGURATION_INVALID",
            "Process executor configuration fields differ",
            422,
        )
    profiles = value.get("profiles")
    if (
        value.get("protocol") != "forest.process.executor/1"
        or not isinstance(profiles, list)
        or len(profiles) > 128
    ):
        raise ForestError(
            "PROCESS_EXECUTOR_CONFIGURATION_INVALID",
            "Process executor configuration is invalid",
            422,
        )
    parsed: dict[str, ExecutionProfile] = {}
    required = {
        "profile_id",
        "executable",
        "executable_sha256",
        "arguments_schema",
        "argv",
        "working_directory",
        "environment",
        "run_as",
        "network_policy",
        "require_posix_limits",
        "limits",
    }
    for item in profiles:
        if not isinstance(item, dict) or set(item) != required:
            raise ForestError(
                "PROCESS_EXECUTOR_CONFIGURATION_INVALID",
                "Execution profile fields differ",
                422,
            )
        profile_id = item["profile_id"]
        executable = item["executable"]
        working_directory = item["working_directory"]
        digest = item["executable_sha256"]
        arguments_schema = item["arguments_schema"]
        argv = item["argv"]
        environment = item["environment"]
        run_as = item["run_as"]
        limits = item["limits"]
        try:
            Draft202012Validator.check_schema(arguments_schema)
        except SchemaError as exc:
            raise ForestError(
                "PROCESS_EXECUTOR_CONFIGURATION_INVALID",
                "Execution profile argument schema is invalid",
                422,
            ) from exc
        if (
            not isinstance(profile_id, str)
            or _PROFILE_ID.fullmatch(profile_id) is None
            or profile_id in parsed
            or not isinstance(executable, str)
            or not Path(executable).is_absolute()
            or "\x00" in executable
            or not isinstance(working_directory, str)
            or not Path(working_directory).is_absolute()
            or "\x00" in working_directory
            or not isinstance(digest, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None
            or not isinstance(arguments_schema, dict)
            or arguments_schema.get("type") != "object"
            or not isinstance(arguments_schema.get("properties"), dict)
            or not isinstance(arguments_schema.get("required"), list)
            or not isinstance(argv, list)
            or not argv
            or len(argv) > 256
            or not isinstance(environment, dict)
            or len(environment) > 128
            or any(
                not isinstance(key, str)
                or re.fullmatch(r"[A-Z_][A-Z0-9_]{0,63}", key) is None
                or not isinstance(val, str)
                or "\x00" in val
                or len(val.encode()) > 8192
                for key, val in environment.items()
            )
            or not isinstance(run_as, dict)
            or set(run_as) != {"uid", "gid"}
            or type(run_as["uid"]) is not int
            or type(run_as["gid"]) is not int
            or run_as["uid"] < 0
            or run_as["gid"] < 0
            or item["network_policy"] not in {"deny", "inherit"}
            or type(item["require_posix_limits"]) is not bool
            or not isinstance(limits, dict)
            or set(limits)
            != {
                "cpu_seconds",
                "memory_bytes",
                "wall_seconds",
                "output_bytes",
                "open_files",
                "processes",
                "terminate_grace_seconds",
            }
        ):
            raise ForestError(
                "PROCESS_EXECUTOR_CONFIGURATION_INVALID",
                "Execution profile is invalid",
                422,
            )
        ranges = {
            "cpu_seconds": (1, 3600),
            "memory_bytes": (16 * 1024 * 1024, 64 * 1024**3),
            "wall_seconds": (1, 3600),
            "output_bytes": (1024, 32 * 1024 * 1024),
            "open_files": (8, 4096),
            "processes": (1, 1024),
            "terminate_grace_seconds": (1, 60),
        }
        if any(
            type(limits[name]) is not int or not low <= limits[name] <= high
            for name, (low, high) in ranges.items()
        ):
            raise ForestError(
                "PROCESS_EXECUTOR_CONFIGURATION_INVALID",
                "Execution profile limits are invalid",
                422,
            )
        normalized_argv = []
        for token in argv:
            if not isinstance(token, dict) or len(token) != 1:
                raise ForestError(
                    "PROCESS_EXECUTOR_CONFIGURATION_INVALID",
                    "Execution profile argv template is invalid",
                    422,
                )
            kind, token_value = next(iter(token.items()))
            if (
                kind not in {"literal", "argument"}
                or not isinstance(token_value, str)
                or not token_value
                or "\x00" in token_value
                or len(token_value.encode()) > 8192
            ):
                raise ForestError(
                    "PROCESS_EXECUTOR_CONFIGURATION_INVALID",
                    "Execution profile argv token is invalid",
                    422,
                )
            normalized_argv.append(dict(token))
        argument_names = {
            token["argument"] for token in normalized_argv if "argument" in token
        }
        if not argument_names.issubset(set(arguments_schema["properties"])) or not (
            argument_names.issubset(set(arguments_schema["required"]))
        ):
            raise ForestError(
                "PROCESS_EXECUTOR_CONFIGURATION_INVALID",
                "Every argv argument must name a required schema property",
                422,
            )
        if item["network_policy"] == "deny" and not seccomp_available():
            raise ForestError(
                "PROCESS_EXECUTOR_ISOLATION_UNAVAILABLE",
                "The requested network isolation is unavailable",
                503,
            )
        if item["require_posix_limits"] and (
            os.name != "posix"
            or not hasattr(os, "getuid")
            or os.getuid() != run_as["uid"]
            or os.getgid() != run_as["gid"]
        ):
            raise ForestError(
                "PROCESS_EXECUTOR_IDENTITY_UNAVAILABLE",
                "The configured execution identity or POSIX limits are unavailable",
                503,
            )
        parsed[profile_id] = ExecutionProfile(
            profile_id,
            executable,
            digest,
            arguments_schema,
            tuple(normalized_argv),
            working_directory,
            dict(environment),
            run_as["uid"],
            run_as["gid"],
            item["network_policy"],
            item["require_posix_limits"],
            dict(limits),
        )
    return parsed


def process_executor_configuration_validator_factory():
    def create(_namespace: str):
        async def validate(value: object) -> None:
            parse_process_executor_configuration(value)

        return validate

    return create


class NativeProcessEffectDriver:
    def __init__(
        self,
        configuration,
        configuration_revision,
        identity,
        declaration,
        *,
        runtime,
        payloads,
    ) -> None:
        if (
            declaration.get("kind") != "process.native"
            or declaration.get("name") != "process.native.spawn"
        ):
            raise ForestError(
                "PACKAGE_EFFECT_DRIVER_BINDING_INVALID",
                "Native process driver received another Job declaration",
                500,
            )
        self.configuration_revision = configuration_revision
        self.identity = identity
        self.runtime = runtime
        self.payloads = payloads
        self.profiles = parse_process_executor_configuration(configuration)
        self.accepting = False

    async def start(self) -> None:
        self.accepting = True

    async def quiesce(self) -> None:
        self.accepting = False

    async def resume(self) -> None:
        self.accepting = True

    async def close(self) -> None:
        self.accepting = False

    async def _admit(self, scope) -> None:
        if (
            not self.accepting
            or scope.component_id != self.identity.component_id
            or scope.generation_id != self.identity.generation_id
        ):
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Native process Generation is not accepting calls",
                409,
            )
        await self.runtime.verify_generation_member(
            component_id=self.identity.component_id,
            generation_id=self.identity.generation_id,
        )

    @staticmethod
    def _verified_executable(profile: ExecutionProfile) -> None:
        path = Path(profile.executable)
        try:
            info = path.lstat()
            if (
                not stat.S_ISREG(info.st_mode)
                or path.is_symlink()
                or info.st_mode & 0o022
                or (os.name == "posix" and info.st_uid != 0)
            ):
                raise OSError("executable must be a non-writable regular file")
            hasher = hashlib.sha256()
            with path.open("rb") as executable:
                for chunk in iter(lambda: executable.read(1024 * 1024), b""):
                    hasher.update(chunk)
            digest = "sha256:" + hasher.hexdigest()
        except OSError as exc:
            raise ForestError(
                "PROCESS_EXECUTABLE_UNAVAILABLE", "Profile executable is unavailable", 503
            ) from exc
        if digest != profile.executable_sha256:
            raise ForestError(
                "PROCESS_EXECUTABLE_DIGEST_MISMATCH", "Profile executable digest differs", 409
            )
        directory = Path(profile.working_directory)
        try:
            directory_info = directory.lstat()
        except OSError as exc:
            raise ForestError(
                "PROCESS_WORKING_DIRECTORY_UNAVAILABLE",
                "Profile working directory is unavailable",
                503,
            ) from exc
        if (
            not stat.S_ISDIR(directory_info.st_mode)
            or directory.is_symlink()
            or directory_info.st_mode & 0o022
        ):
            raise ForestError(
                "PROCESS_WORKING_DIRECTORY_UNAVAILABLE",
                "Profile working directory is unavailable",
                503,
            )

    @staticmethod
    def _argv(profile: ExecutionProfile, arguments: dict) -> list[str]:
        if not Draft202012Validator(profile.arguments_schema).is_valid(arguments):
            raise ForestError(
                "PROCESS_ARGUMENTS_INVALID", "Arguments do not match the execution profile", 422
            )
        result = [profile.executable]
        for token in profile.argv:
            if "literal" in token:
                result.append(token["literal"])
                continue
            value = arguments.get(token["argument"])
            if isinstance(value, (str, int, float)) and not isinstance(value, bool):
                rendered = str(value)
            elif isinstance(value, bool):
                rendered = "true" if value else "false"
            else:
                raise ForestError(
                    "PROCESS_ARGUMENTS_INVALID", "An argv argument is absent or not scalar", 422
                )
            if "\x00" in rendered or len(rendered.encode()) > 8192:
                raise ForestError("PROCESS_ARGUMENTS_INVALID", "An argv argument is invalid", 422)
            result.append(rendered)
        return result

    async def _terminate(self, process, grace: int) -> None:
        if process.returncode is not None:
            return
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGTERM)
            else:
                process.terminate()
        except ProcessLookupError:
            await process.wait()
            return
        try:
            await asyncio.wait_for(process.wait(), timeout=grace)
        except TimeoutError:
            try:
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
            except ProcessLookupError:
                pass
            await process.wait()

    @staticmethod
    async def _read_bounded(stream, limit: int) -> tuple[bytes, int, bool]:
        kept = bytearray()
        total = 0
        while True:
            chunk = await stream.read(65536)
            if not chunk:
                break
            total += len(chunk)
            if len(kept) < limit:
                kept.extend(chunk[: limit - len(kept)])
        return bytes(kept), total, total > len(kept)

    async def _save_output(self, operation_id: str, stream_name: str, body: bytes):
        if not body:
            return None
        epoch = await self.payloads.epoch()
        saved = await self.payloads.put_bytes(
            owner_component_id="process.executor",
            owner_kind="job.output",
            owner_id=f"{operation_id}:{stream_name}",
            media_type="application/octet-stream",
            content=body,
            sources=(
                PayloadSource(
                    "process.executor",
                    "native.process.output",
                    operation_id,
                    self.configuration_revision,
                    part_id=stream_name,
                ),
            ),
            expected_deletion_epoch=epoch,
        )
        return {
            "payload_id": saved.payload_id,
            "content_digest": saved.content_digest,
            "byte_length": saved.byte_length,
            "media_type": saved.media_type,
        }

    async def invoke(self, payload: dict, *, scope, invocation) -> dict:
        await self._admit(scope)
        if not isinstance(scope, PackageJobCallScope):
            raise ForestError(
                "PROCESS_CALL_SCOPE_INVALID",
                "Native process effects require a durable Job call scope",
                403,
            )
        profile_id = payload.get("profile_id")
        arguments = payload.get("arguments")
        profile = self.profiles.get(profile_id)
        if profile is None:
            raise ForestError("PROCESS_PROFILE_NOT_FOUND", "Execution profile is unavailable", 404)
        if await scope.cancellation_requested():
            raise ForestError("JOB_CANCELLED", "Job was cancelled before execution", 409)
        self._verified_executable(profile)
        argv = self._argv(profile, arguments)
        environment = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", **profile.environment}
        manifest = {
            "executable": profile.executable,
            "argv": argv,
            "working_directory": profile.working_directory,
            "environment": environment,
            "run_as": {"uid": profile.run_as_uid, "gid": profile.run_as_gid},
            "network_policy": profile.network_policy,
            "require_posix_limits": profile.require_posix_limits,
            "limits": profile.limits,
        }
        submitted = {
            "executable": payload.get("executable"),
            "argv": payload.get("argv"),
            "working_directory": payload.get("working_directory"),
            "environment": payload.get("environment"),
            "run_as": payload.get("run_as"),
            "network_policy": payload.get("network_policy"),
            "require_posix_limits": payload.get("require_posix_limits"),
            "limits": payload.get("limits"),
        }
        if (
            payload.get("protocol") != "forest.process-spawn/1"
            or payload.get("executable_sha256") != profile.executable_sha256
            or submitted != manifest
        ):
            raise ForestError(
                "PROCESS_EFFECT_PLAN_MISMATCH",
                "Native process plan exceeds the frozen profile",
                403,
            )
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "forest.native_exec_helper",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=(os.name == "posix"),
            env={"PATH": os.environ.get("PATH", ""), "PYTHONPATH": os.environ.get("PYTHONPATH", "")},
        )
        stdout_task = asyncio.create_task(
            self._read_bounded(process.stdout, profile.limits["output_bytes"])
        )
        stderr_task = asyncio.create_task(
            self._read_bounded(process.stderr, profile.limits["output_bytes"])
        )
        try:
            process.stdin.write(json.dumps(manifest, separators=(",", ":")).encode())
            await process.stdin.drain()
            process.stdin.close()
            started = asyncio.get_running_loop().time()
            cancelled = False
            timed_out = False
            while process.returncode is None:
                if await scope.cancellation_requested():
                    cancelled = True
                    await self._terminate(process, profile.limits["terminate_grace_seconds"])
                    break
                if asyncio.get_running_loop().time() - started >= profile.limits["wall_seconds"]:
                    timed_out = True
                    await self._terminate(process, profile.limits["terminate_grace_seconds"])
                    break
                try:
                    await asyncio.wait_for(process.wait(), timeout=0.25)
                except TimeoutError:
                    pass
            stdout, stderr = await asyncio.gather(stdout_task, stderr_task)
        except BaseException:
            await self._terminate(process, profile.limits["terminate_grace_seconds"])
            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
            raise
        if cancelled:
            raise ForestError("JOB_CANCELLED", "Job was cancelled during execution", 409)
        if timed_out:
            raise ForestError("PROCESS_WALL_TIMEOUT", "Process exceeded its wall limit", 504)
        stdout_body, stdout_total, stdout_truncated = stdout
        stderr_body, stderr_total, stderr_truncated = stderr
        stdout_ref, stderr_ref = await asyncio.gather(
            self._save_output(invocation.operation_id, "stdout", stdout_body),
            self._save_output(invocation.operation_id, "stderr", stderr_body),
        )
        return {
            "protocol": "forest.process-spawn-result/1",
            "exit_code": process.returncode,
            "stdout": stdout_ref,
            "stderr": stderr_ref,
            "stdout_total_bytes": stdout_total,
            "stderr_total_bytes": stderr_total,
            "stdout_truncated": stdout_truncated,
            "stderr_truncated": stderr_truncated,
            "network_policy": profile.network_policy,
        }


def native_process_effect_driver_factory(*, runtime, payloads):
    def create(configuration, configuration_revision, identity, declaration):
        return NativeProcessEffectDriver(
            configuration,
            configuration_revision,
            identity,
            declaration,
            runtime=runtime,
            payloads=payloads,
        )

    return create
