"""Configuration domain for the builtin native process executor package."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path

from jsonschema import Draft202012Validator, SchemaError

from forest.errors import ForestError
from forest.native_exec_helper import seccomp_available

_PROFILE_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}\Z")

PROCESS_EXECUTOR_CONFIGURATION_SCHEMA = json.loads(
    files("forest")
    .joinpath("builtin/process_executor/schemas/config.json")
    .read_text(encoding="utf-8")
)


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


def default_process_executor_configuration() -> dict:
    return {"protocol": "forest.process.executor/1", "profiles": []}


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


def validate_process_executor_configuration(value: object) -> dict:
    if not Draft202012Validator(PROCESS_EXECUTOR_CONFIGURATION_SCHEMA).is_valid(value):
        raise ForestError(
            "PROCESS_EXECUTOR_CONFIGURATION_INVALID",
            "Process executor configuration is invalid",
            422,
        )
    parse_process_executor_configuration(value)
    return value


def process_executor_configuration_validator_factory(_store):
    def create(_namespace: str):
        async def validate(value: object) -> None:
            validate_process_executor_configuration(value)

        return validate

    return create
