"""Generation-owned native process effect driver for the builtin executor package."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import stat
import sys
from pathlib import Path

from jsonschema import Draft202012Validator

from forest.errors import ForestError
from forest.package_capabilities import PackageJobCallScope
from forest.process_executor_builtin import (
    ExecutionProfile,
    parse_process_executor_configuration,
)
from forest.storage.payload_authority import PayloadSource


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
