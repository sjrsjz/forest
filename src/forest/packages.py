"""Static package validation and immutable, content-addressed Python snapshots.

The catalog is an artifact cache, never a routing or Bot state authority. Package
validation does not import user code. Runtime identity is allocated by the kernel.
"""

from __future__ import annotations

import hashlib
import io
import json
import keyword
import os
import re
import shutil
import sqlite3
import stat
import sys
import tomllib
import zipfile
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from uuid import uuid4

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

from forest.bootstrap_materialization import validate_materialization_spec
from forest.command_contract import (
    IDENTIFIER as COMMAND_IDENTIFIER,
)
from forest.command_contract import (
    normalize_command_name,
    validate_argument_mode,
    validate_command_permission,
    validate_command_priority,
)
from forest.errors import ForestError
from forest.plugin_declarations import synthesize_manifest, verify_checksums, with_checksums
from forest.schema_bundles import (
    MAX_STORAGE_BASELINE_BYTES,
    StorageBundleSpec,
    validate_official_storage_baseline,
    validate_storage_baseline,
)
from forest.security import private_directory

SKIPPED = {".git", ".venv", "__pycache__", ".pytest_cache", ".ruff_cache"}
MAX_FILE_BYTES = 4 * 1024 * 1024
MAX_WHEEL_BYTES = 32 * 1024 * 1024
MAX_ARCHIVE_BYTES = 32 * 1024 * 1024
MAX_PACKAGE_BYTES = 64 * 1024 * 1024
MAX_PACKAGE_FILES = 512
MAX_PACKAGE_ENTRIES = 4096
MAX_PACKAGE_DEPTH = 32
MAX_MANAGEMENT_UI_BYTES = 256 * 1024
IDENTIFIER = re.compile(r"[a-zA-Z][a-zA-Z0-9_.-]{0,127}\Z")
SQL_FUNCTION = re.compile(r"[a-z_][a-z0-9_]{0,62}\Z")
ENTRYPOINT = re.compile(r"[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*:[a-zA-Z_]\w*\Z")
VERSION = re.compile(r"\d+\.\d+\.\d+(?:[-+][a-zA-Z0-9.-]+)?\Z")


def _valid_json_pointer(value: object, *, wildcards: bool = False) -> bool:
    if not isinstance(value, str) or not 2 <= len(value.encode("utf-8")) <= 1024:
        return False
    if not value.startswith("/"):
        return False
    for token in value[1:].split("/"):
        if not token or re.search(r"~(?![01])", token):
            return False
        if token == "*" and not wildcards:
            return False
    return True


MANIFEST_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "manifest_version",
        "runtime",
        "kernel_api",
        "package_id",
        "version",
        "entrypoint",
        "components",
    ],
    "properties": {
        "manifest_version": {"type": "integer", "const": 1},
        **{
            key: {"type": "string"}
            for key in ("runtime", "kernel_api", "package_id", "version", "entrypoint")
        },
        "dependency_lock": {"type": "string"},
        "module_root": {"type": "string"},
        "components": {
            "type": "array",
            "minItems": 1,
            "maxItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "id",
                    "scope",
                    "state_schema",
                    "reload_mode",
                    "config_schema",
                    "provides",
                ],
                "properties": {
                    **{
                        key: {"type": "string"}
                        for key in ("id", "reload_mode", "config_schema")
                    },
                    "scope": {"enum": ["bot", "deployment", "host"]},
                    "configuration_apply": {"const": "worker-host/1"},
                    "bootstrap_config": {"type": "string"},
                    "bootstrap_materialization": {"type": "string"},
                    "bootstrap_contribution": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["label", "description", "order", "schema", "initial_value"],
                        "properties": {
                            "label": {"type": "string", "minLength": 1, "maxLength": 128},
                            "description": {"type": "string", "minLength": 1, "maxLength": 512},
                            "order": {"type": "integer", "minimum": -10000, "maximum": 10000},
                            "schema": {"type": "string"},
                            "initial_value": {"type": "string"},
                            "ui_entrypoint": {"type": "string"},
                            "secret_refs": {
                                "type": "array", "maxItems": 16,
                                "items": {
                                    "type": "object", "additionalProperties": False,
                                    "required": ["field", "purpose"],
                                    "properties": {
                                        "field": {"type": "string"},
                                        "purpose": {"type": "string"},
                                        "allow_empty": {"type": "boolean"},
                                    },
                                },
                            },
                            "origins": {
                                "type": "array", "maxItems": 16,
                                "items": {
                                    "type": "object", "additionalProperties": False,
                                    "required": ["field", "schemes"],
                                    "properties": {
                                        "field": {"type": "string"},
                                        "schemes": {
                                            "type": "array", "minItems": 1, "maxItems": 4,
                                            "uniqueItems": True,
                                            "items": {"enum": ["http", "https", "ws", "wss"]},
                                        },
                                    },
                                },
                            },
                        },
                    },
                    "auto_install": {"type": "boolean"},
                    "state_schema": {"type": "integer", "minimum": 1, "maximum": 2147483647},
                    "config_immutable_paths": {
                        "type": "array",
                        "maxItems": 32,
                        "uniqueItems": True,
                        "items": {"type": "string"},
                    },
                    "state_value_schema": {"type": "string"},
                    "state_migrations": {
                        "type": "array",
                        "maxItems": 64,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["from_schema", "to_schema"],
                            "properties": {
                                key: {"type": "integer", "minimum": 1, "maximum": 2147483647}
                                for key in ("from_schema", "to_schema")
                            },
                        },
                    },
                    "state_mode": {"const": "durable"},
                    "checkpoint_schema": {"type": "integer", "const": 1},
                    "message_handler_takeover": {
                        "type": "string",
                        "enum": ["none", "compatible"],
                    },
                    "requires": {
                        "type": "array",
                        "maxItems": 32,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["name", "contract"],
                            "properties": {
                                "name": {"type": "string"},
                                "contract": {"type": "string"},
                                "required": {"type": "boolean"},
                                "startup_dependency": {"type": "boolean"},
                            },
                        },
                    },
                    "resources": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "max_frame_bytes": {
                                "type": "integer",
                                "minimum": 1024,
                                "maximum": 262144,
                            },
                            "max_queue_items": {"type": "integer", "minimum": 0, "maximum": 256},
                            **{
                                key: {"type": "integer", "minimum": 1, "maximum": 256}
                                for key in (
                                    "max_inflight",
                                    "max_state_inflight",
                                    "max_owned_tasks",
                                    "max_owned_resources",
                                )
                            },
                        },
                    },
                    "lifecycle": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "prepare_timeout_s": {
                                "type": "number",
                                "exclusiveMinimum": 0,
                                "maximum": 120,
                            },
                            "start_timeout_s": {
                                "type": "number",
                                "exclusiveMinimum": 0,
                                "maximum": 120,
                            },
                            "drain_timeout_s": {
                                "type": "number",
                                "exclusiveMinimum": 0,
                                "maximum": 86400,
                            },
                            "stop_timeout_s": {
                                "type": "number",
                                "exclusiveMinimum": 0,
                                "maximum": 120,
                            },
                            "replacement_guard_query": {"type": "string"},
                            "replacement_facts_schema": {"type": "string"},
                        },
                    },
                    "capabilities": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "requests": {"type": "array", "items": {"type": "string"}},
                            "official_bot_grants": {
                                "type": "array",
                                "maxItems": 16,
                                "items": {
                                    "type": "object",
                                    "additionalProperties": False,
                                    "required": ["capability", "delegable"],
                                    "properties": {
                                        "capability": {"type": "string"},
                                        "delegable": {"type": "boolean"},
                                    },
                                },
                            },
                        },
                    },
                    "contribution_directories": {
                        "type": "array",
                        "maxItems": 8,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["kind", "max_execution_timeout_s"],
                            "properties": {
                                "kind": {
                                    "enum": [
                                        "job-types",
                                        "shell-commands",
                                        "routing-routes",
                                        "agent-tools",
                                    ]
                                },
                                "cancellation_query": {"type": "string"},
                                "synchronization_command": {"type": "string"},
                                "cleanup_command": {"type": "string"},
                                "synchronize_configuration": {"type": "boolean"},
                                "max_execution_timeout_s": {
                                    "type": "integer",
                                    "minimum": 1,
                                    "maximum": 86400,
                                },
                            },
                        },
                    },
                    "provides": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "name",
                                "contract",
                                "semantics",
                                "input_schema",
                                "output_schema",
                            ],
                            "properties": {
                                key: {"type": "string"}
                                for key in ("name", "contract", "input_schema", "output_schema")
                            }
                            | {
                                "semantics": {
                                    "enum": ["at-most-once", "idempotent", "read"]
                                },
                                "visibility": {"enum": ["public", "management"]},
                            },
                        },
                    },
                    "effect_services": {
                        "type": "array",
                        "maxItems": 16,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "name",
                                "contract",
                                "kind",
                                "semantics",
                                "input_schema",
                                "output_schema",
                            ],
                            "properties": {
                                **{
                                    key: {"type": "string"}
                                    for key in (
                                        "name",
                                        "contract",
                                        "kind",
                                        "input_schema",
                                        "output_schema",
                                    )
                                },
                                "semantics": {"enum": ["at-most-once", "idempotent", "read"]},
                                "ingress": {
                                    "type": "object",
                                    "additionalProperties": False,
                                    "required": [
                                        "name",
                                        "semantics",
                                        "input_schema",
                                        "output_schema",
                                        "timeout_s",
                                    ],
                                    "properties": {
                                        "name": {"type": "string"},
                                        "semantics": {
                                            "enum": ["at-most-once", "idempotent", "read"]
                                        },
                                        "input_schema": {"type": "string"},
                                        "output_schema": {"type": "string"},
                                        "timeout_s": {
                                            "type": "number",
                                            "exclusiveMinimum": 0,
                                            "maximum": 86400,
                                        },
                                    },
                                },
                            },
                        },
                    },
                    **{
                        kind: {
                            "type": "array",
                            "maxItems": 64,
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": [
                                    "name",
                                    "contract",
                                    "function",
                                    "input_schema",
                                    "output_schema",
                                ],
                                "properties": {
                                    **{
                                        key: {"type": "string"}
                                        for key in (
                                            "name",
                                            "contract",
                                            "function",
                                            "input_schema",
                                            "output_schema",
                                        )
                                    },
                                    "payload_refs": {
                                        "type": "array",
                                        "maxItems": 32,
                                        "items": {
                                            "type": "object",
                                            "additionalProperties": False,
                                            "required": ["path", "owner_id_path"],
                                            "properties": {
                                                "path": {"type": "string"},
                                                "owner_component": {"type": "string"},
                                                "owner_component_path": {"type": "string"},
                                                "owner_kind": {"type": "string"},
                                                "owner_kind_path": {"type": "string"},
                                                "owner_id_path": {"type": "string"},
                                                "media_type_path": {"type": "string"},
                                                "byte_length_path": {"type": "string"},
                                                "management_asset_id": {"type": "string"},
                                                "target_ref_path": {"type": "string"},
                                                "part_id_path": {"type": "string"},
                                                "width_path": {"type": "string"},
                                                "height_path": {"type": "string"},
                                                "min_remaining_s": {
                                                    "type": "integer", "minimum": 0, "maximum": 1800,
                                                },
                                            },
                                            "allOf": [
                                                {
                                                    "oneOf": [
                                                        {"required": ["owner_component"]},
                                                        {"required": ["owner_component_path"]},
                                                    ]
                                                },
                                                {
                                                    "oneOf": [
                                                        {"required": ["owner_kind"]},
                                                        {"required": ["owner_kind_path"]},
                                                    ]
                                                },
                                            ],
                                        },
                                    },
                                },
                            },
                        }
                        for kind in (
                            "domain_queries",
                            "domain_commands",
                            "unit_of_work_participants",
                        )
                    },
                    "kernel_queries": {
                        "type": "array",
                        "maxItems": 16,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "name", "contract", "kind", "input_schema", "output_schema",
                            ],
                            "properties": {
                                key: {"type": "string"}
                                for key in (
                                    "name", "contract", "kind", "input_schema", "output_schema",
                                )
                            },
                        },
                    },
                    "kernel_commands": {
                        "type": "array",
                        "maxItems": 16,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "name", "contract", "kind", "input_schema", "output_schema",
                            ],
                            "properties": {
                                key: {"type": "string"}
                                for key in (
                                    "name", "contract", "kind", "input_schema", "output_schema",
                                )
                            },
                        },
                    },
                    "job_types": {
                        "type": "array",
                        "maxItems": 32,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "job_type",
                                "job_version",
                                "name",
                                "contract",
                                "handler",
                                "input_schema",
                                "output_schema",
                                "max_attempts",
                                "timeout_s",
                                "max_concurrency",
                            ],
                            "properties": {
                                **{
                                    key: {"type": "string"}
                                    for key in (
                                        "job_type",
                                        "name",
                                        "contract",
                                        "handler",
                                        "input_schema",
                                        "output_schema",
                                    )
                                },
                                "job_version": {
                                    "type": "integer",
                                    "minimum": 1,
                                    "maximum": 2147483647,
                                },
                                "max_attempts": {"type": "integer", "minimum": 1, "maximum": 32},
                                "timeout_s": {"type": "integer", "minimum": 1, "maximum": 86400},
                                "max_concurrency": {
                                    "type": "integer",
                                    "minimum": 1,
                                    "maximum": 128,
                                },
                            },
                        },
                    },
                    "shell_commands": {
                        "type": "array",
                        "maxItems": 64,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "command_id",
                                "command_version",
                                "service_id",
                                "service_version",
                                "argument_schema",
                                "management_operation_id",
                                "target_scope",
                                "service_class",
                                "required_capabilities",
                                "output_policy",
                                "timeout_s",
                            ],
                            "properties": {
                                "command_id": {"type": "string"},
                                "command_version": {
                                    "type": "integer",
                                    "minimum": 1,
                                    "maximum": 2147483647,
                                },
                                "service_id": {"type": "string"},
                                "service_version": {
                                    "type": "integer",
                                    "minimum": 1,
                                    "maximum": 2147483647,
                                },
                                "argument_schema": {"type": "string"},
                                "management_operation_id": {"type": "string"},
                                "management_action": {"type": "boolean"},
                                "target_scope": {
                                    "enum": ["CONVERSATION", "BINDING", "BOT", "DEPLOYMENT"]
                                },
                                "service_class": {"enum": ["P0", "P1"]},
                                "required_capabilities": {
                                    "type": "array",
                                    "minItems": 1,
                                    "maxItems": 32,
                                    "uniqueItems": True,
                                    "items": {"type": "string"},
                                },
                                "option_aliases": {
                                    "type": "array",
                                    "maxItems": 32,
                                    "uniqueItems": True,
                                    "items": {
                                        "type": "array",
                                        "minItems": 2,
                                        "maxItems": 2,
                                        "items": {"type": "string"},
                                    },
                                },
                                "output_policy": {
                                    "enum": ["SHORT", "ARTIFACT", "SILENT"]
                                },
                                "timeout_s": {
                                    "type": "integer",
                                    "minimum": 1,
                                    "maximum": 900,
                                },
                            },
                        },
                    },
                    "agent_tools": {
                        "type": "array",
                        "maxItems": 64,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "name",
                                "service_id",
                                "service_version",
                                "declaration_schema",
                                "approval_required",
                                "supports_query",
                            ],
                            "properties": {
                                "name": {"type": "string"},
                                "service_id": {"type": "string"},
                                "service_version": {
                                    "type": "integer",
                                    "minimum": 1,
                                    "maximum": 2147483647,
                                },
                                "declaration_schema": {"type": "string"},
                                "approval_required": {"type": "boolean"},
                                "supports_query": {"type": "boolean"},
                            },
                        },
                    },
                    "owned_work_handlers": {
                        "type": "array",
                        "maxItems": 32,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "name",
                                "contract",
                                "handler",
                                "input_schema",
                                "output_schema",
                                "max_concurrency",
                                "poll_interval_ms",
                                "claim_timeout_s",
                                "handler_timeout_s",
                                "settle_timeout_s",
                                "lease_seconds",
                            ],
                            "properties": {
                                "authority": {"enum": ["package", "kernel"]},
                                **{
                                    key: {"type": "string"}
                                    for key in (
                                        "name",
                                        "contract",
                                        "handler",
                                        "claim_function",
                                        "settle_function",
                                        "renew_function",
                                        "input_schema",
                                        "output_schema",
                                    )
                                },
                                "max_concurrency": {"type": "integer", "minimum": 1, "maximum": 32},
                                "poll_interval_ms": {
                                    "type": "integer",
                                    "minimum": 50,
                                    "maximum": 60000,
                                },
                                "claim_timeout_s": {"type": "integer", "minimum": 1, "maximum": 60},
                                "handler_timeout_s": {
                                    "type": "integer",
                                    "minimum": 1,
                                    "maximum": 86400,
                                },
                                "settle_timeout_s": {
                                    "type": "integer",
                                    "minimum": 1,
                                    "maximum": 60,
                                },
                                "renew_timeout_s": {
                                    "type": "integer",
                                    "minimum": 1,
                                    "maximum": 60,
                                },
                                "renew_interval_s": {
                                    "type": "integer",
                                    "minimum": 1,
                                    "maximum": 3600,
                                },
                                "lease_seconds": {
                                    "type": "integer",
                                    "minimum": 3,
                                    "maximum": 86400,
                                },
                                "drain_policy": {
                                    "enum": ["inflight", "backlog"]
                                },
                            },
                        },
                    },
                    "management_ui": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["entrypoint", "protocol"],
                        "properties": {
                            "entrypoint": {"type": "string"},
                            "protocol": {"const": "forest.management-ui/1"},
                            "contribution_id": {
                                "type": "string",
                                "pattern": "^[a-z0-9][a-z0-9._-]{0,127}$",
                            },
                            "label": {"type": "string", "minLength": 1, "maxLength": 128},
                            "group": {
                                "type": "string",
                                "pattern": "^[a-z0-9][a-z0-9._-]{0,63}$",
                            },
                            "order": {"type": "integer", "minimum": -10000, "maximum": 10000},
                            "bootstrap_catalog_query": {"type": "string"},
                        },
                    },
                    "management_queries": {
                        "type": "array",
                        "maxItems": 64,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["id", "port"],
                            "properties": {
                                "id": {"type": "string"},
                                "port": {"type": "string"},
                                "default_limit": {
                                    "type": "integer",
                                    "minimum": 1,
                                    "maximum": 10000,
                                },
                                "max_limit": {"type": "integer", "minimum": 1, "maximum": 10000},
                                "required_role": {"enum": ["ADMIN", "AUDITOR"]},
                                "actor_field": {"type": "string"},
                                "timeout_s": {
                                    "type": "number",
                                    "exclusiveMinimum": 0,
                                    "maximum": 120,
                                },
                            },
                        },
                    },
                    "management_actions": {
                        "type": "array",
                        "maxItems": 64,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["id", "port", "priority"],
                            "properties": {
                                "id": {"type": "string"},
                                "port": {"type": "string"},
                                "priority": {"enum": ["P0", "P1", "P2"]},
                                "authorization_path": {"type": "string"},
                                "transaction_preflight": {"type": "string"},
                                "actor_field": {"type": "string"},
                                "secret_field": {"type": "string"},
                                "timeout_s": {
                                    "type": "number",
                                    "exclusiveMinimum": 0,
                                    "maximum": 120,
                                },
                            },
                        },
                    },
                    "management_assets": {
                        "type": "array",
                        "maxItems": 16,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["id", "owner_kind", "owner_prefix", "media_types", "max_bytes"],
                            "properties": {
                                "id": {"type": "string"},
                                "owner_kind": {"type": "string"},
                                "owner_prefix": {"type": "string"},
                                "media_types": {
                                    "type": "array", "minItems": 1, "maxItems": 4,
                                    "uniqueItems": True,
                                    "items": {"enum": ["text/plain", "image/png", "image/jpeg", "image/webp"]},
                                },
                                "max_bytes": {"type": "integer", "minimum": 1, "maximum": 8388608},
                            },
                        },
                    },
                    "management_artifacts": {
                        "type": "array",
                        "maxItems": 16,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["id", "port", "direction", "media_type", "max_bytes"],
                            "properties": {
                                "id": {"type": "string"},
                                "port": {"type": "string"},
                                "direction": {"enum": ["download", "upload"]},
                                "media_type": {"const": "application/zip"},
                                "max_bytes": {
                                    "type": "integer", "minimum": 1, "maximum": MAX_ARCHIVE_BYTES,
                                },
                                "required_role": {"enum": ["ADMIN", "AUDITOR"]},
                                "reserve_port": {"type": "string"},
                                "settle_port": {"type": "string"},
                                "close_port": {"type": "string"},
                            },
                        },
                    },
                    "unit_of_work_plans": {
                        "type": "array",
                        "maxItems": 32,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "name",
                                "contract",
                                "input_schema",
                                "output_schema",
                                "timeout_s",
                                "steps",
                            ],
                            "properties": {
                                "name": {"type": "string"},
                                "contract": {"type": "string"},
                                "input_schema": {"type": "string"},
                                "output_schema": {"type": "string"},
                                "timeout_s": {
                                    "type": "number",
                                    "exclusiveMinimum": 0,
                                    "maximum": 120,
                                },
                                "steps": {
                                    "type": "array",
                                    "minItems": 1,
                                    "maxItems": 16,
                                    "items": {
                                        "type": "object",
                                        "additionalProperties": False,
                                        "required": ["id", "component_id", "participant"],
                                        "properties": {
                                            "id": {"type": "string"},
                                            "component_id": {"type": "string"},
                                            "participant": {"type": "string"},
                                            "input_path": {"type": "string"},
                                            "participant_generation_field": {
                                                "type": "string"
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                    "storage": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": [
                            "schema_id",
                            "schema_version",
                            "baseline",
                            "baseline_sha256",
                            "scope",
                            "repository_contract",
                        ],
                        "properties": {
                            "schema_id": {"type": "string"},
                            "schema_version": {"type": "string"},
                            "baseline": {"type": "string"},
                            "baseline_sha256": {"type": "string"},
                            "scope": {"enum": ["bot", "deployment", "host"]},
                            "repository_contract": {"type": "string"},
                            "execution_profile": {"enum": ["declarative", "official"]},
                        },
                    },
                    "publishes": {
                        "type": "array",
                        "maxItems": 32,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["topic", "contract", "schema"],
                            "properties": {
                                key: {"type": "string"} for key in ("topic", "contract", "schema")
                            },
                        },
                    },
                    "subscribes": {
                        "type": "array",
                        "maxItems": 32,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["subscriber_id", "topic", "contract", "schema"],
                            "properties": {
                                **{
                                    key: {"type": "string"}
                                    for key in ("subscriber_id", "topic", "contract", "schema")
                                },
                                "max_attempts": {"type": "integer", "minimum": 1, "maximum": 32},
                            },
                        },
                    },
                    "jobs": {
                        "type": "array",
                        "maxItems": 32,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["handler_id", "input_schema"],
                            "properties": {
                                "handler_id": {"type": "string"},
                                "input_schema": {"type": "string"},
                                "max_attempts": {"type": "integer", "minimum": 1, "maximum": 32},
                                "timeout_s": {
                                    "type": "number",
                                    "exclusiveMinimum": 0,
                                    "maximum": 120,
                                },
                                "max_concurrency": {"type": "integer", "minimum": 1, "maximum": 16},
                            },
                        },
                    },
                    "commands": {
                        "type": "array",
                        "maxItems": 64,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "command_id",
                                "name",
                                "handler",
                                "permission",
                                "argument_mode",
                                "input_schema",
                                "priority",
                            ],
                            "properties": {
                                **{
                                    key: {"type": "string"}
                                    for key in (
                                        "command_id",
                                        "name",
                                        "handler",
                                        "permission",
                                        "argument_mode",
                                        "input_schema",
                                    )
                                },
                                "priority": {"type": "integer", "minimum": -100, "maximum": 100},
                            },
                        },
                    },
                    "message_handlers": {
                        "type": "array",
                        "maxItems": 64,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["handler_id", "handler", "priority"],
                            "properties": {
                                "handler_id": {"type": "string"},
                                "handler": {"type": "string"},
                                "priority": {"type": "integer", "minimum": -100, "maximum": 100},
                            },
                        },
                    },
                },
            },
        },
    },
}


def digest_json(value: object) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _fail(code: str, message: str) -> None:
    raise ForestError(code, message, 422)


def contract_range(contract: str) -> tuple[tuple[int, int], tuple[int, int] | None]:
    """Explicit major.minor or >=major.minor,<major.minor, without implicit coercion."""
    if not isinstance(contract, str) or len(contract) > 64:
        _fail("INVALID_DEPENDENCY", "Dependency contract must be a version or bounded range")
    exact = re.fullmatch(r"([1-9]\d*)\.(\d+)", contract)
    if exact:
        return (int(exact[1]), int(exact[2])), None
    interval = re.fullmatch(r">=([1-9]\d*)\.(\d+),<([1-9]\d*)\.(\d+)", contract)
    if interval:
        lower, upper = (int(interval[1]), int(interval[2])), (int(interval[3]), int(interval[4]))
        if lower < upper:
            return lower, upper
    _fail("INVALID_DEPENDENCY", "Supported contracts are major.minor or >=major.minor,<major.minor")


def contract_matches(contract: str, provided: str) -> bool:
    lower, upper = contract_range(contract)
    version = tuple(map(int, provided.split(".")))
    return version == lower if upper is None else lower <= version < upper


def _signature(item: os.stat_result) -> tuple:
    return (
        item.st_dev,
        item.st_ino,
        item.st_mode,
        item.st_size,
        item.st_mtime_ns,
        item.st_ctime_ns,
        item.st_nlink,
        getattr(item, "st_reparse_tag", 0),
    )


def _reject_link(info: os.stat_result) -> None:
    if stat.S_ISLNK(info.st_mode) or getattr(info, "st_reparse_tag", 0):
        _fail("PACKAGE_LINK_REJECTED", "Package links and reparse points are not supported")


def _capture(root: Path) -> dict[str, bytes]:
    try:
        return _capture_files(root)
    except OSError as exc:
        raise ForestError(
            "PACKAGE_CHANGED", "Package became unavailable during capture", 422
        ) from exc


def source_fingerprint(source: Path) -> str:
    """Watch/build share one complete, portable capture and exclusion policy."""
    return digest_json(
        {name: hashlib.sha256(value).hexdigest() for name, value in _capture(source).items()}
    )


def _prepared_source(source_files: dict[str, bytes]) -> dict[str, bytes]:
    files = synthesize_manifest(source_files)
    if len(files) > MAX_PACKAGE_FILES or sum(map(len, files.values())) > MAX_PACKAGE_BYTES:
        _fail("PACKAGE_TOO_LARGE", "Generated package exceeds file or byte limits")
    if any(
        len(content) > (MAX_WHEEL_BYTES if name.endswith(".whl") else MAX_FILE_BYTES)
        for name, content in files.items()
    ):
        _fail("PACKAGE_TOO_LARGE", "Generated package file exceeds its byte limit")
    return files


def _validated_source(source: Path) -> tuple[Path, dict[str, bytes], dict[str, bytes], dict]:
    try:
        _reject_link(source.lstat())
        source = source.resolve(strict=True)
    except OSError as exc:
        raise ForestError("INVALID_PACKAGE_PATH", "Package source is unavailable", 422) from exc
    source_files = _capture(source)
    captured = _prepared_source(source_files)
    manifest = _manifest(captured)
    if captured != _prepared_source(_capture(source)):
        _fail("PACKAGE_CHANGED", "Package must remain stable throughout capture")
    return source, source_files, captured, manifest


def validate_source_package(source: Path) -> dict:
    """Validate a stable source capture without publishing an artifact catalog."""

    _, _, captured, manifest = _validated_source(source)
    return {
        "manifest": manifest,
        "digest": digest_json(
            {name: hashlib.sha256(value).hexdigest() for name, value in captured.items()}
        ),
    }


def _capture_files(root: Path) -> dict[str, bytes]:
    captured: dict[str, bytes] = {}
    names: set[str] = set()
    total = 0
    root_info = root.lstat()
    _reject_link(root_info)
    if not stat.S_ISDIR(root_info.st_mode):
        _fail("INVALID_PACKAGE_PATH", "Package source must be a regular directory")
    pending = [(root, root_info)]
    directories = []
    entry_count = 0
    while pending:
        current, current_info = pending.pop()
        if _signature(current.lstat()) != _signature(current_info):
            _fail("PACKAGE_CHANGED", "Package directory changed during capture")
        directories.append((current, current_info))
        # scandir errors must propagate; os.walk's default would silently omit unreadable trees.
        with os.scandir(current) as entries:
            children = []
            for entry in entries:
                entry_count += 1
                if entry_count > MAX_PACKAGE_ENTRIES:
                    _fail("PACKAGE_TOO_LARGE", "Package exceeds directory entry limits")
                children.append(entry.name)
        for name in sorted(children):
            path = current / name
            info = path.lstat()
            _reject_link(info)
            if stat.S_ISDIR(info.st_mode) and name in SKIPPED:
                continue
            rel = path.relative_to(root).as_posix()
            stem = name.split(".", 1)[0].upper()
            if (
                name.startswith(".env")
                or re.search(r'[<>:"\\|?*\x00-\x1f]', name)
                or name.endswith((".", " "))
                or stem in {"CON", "PRN", "AUX", "NUL"}
                or re.fullmatch(r"(?:COM|LPT)[1-9]", stem)
            ):
                _fail("PACKAGE_FILE_REJECTED", "Package contains a private or nonportable path")
            if rel.casefold() in names:
                _fail("PACKAGE_PATH_COLLISION", "Package has case-insensitive path collisions")
            names.add(rel.casefold())
            if len(path.relative_to(root).parts) > MAX_PACKAGE_DEPTH:
                _fail("PACKAGE_TOO_LARGE", "Package exceeds directory depth limits")
            if stat.S_ISDIR(info.st_mode):
                pending.append((path, info))
                continue
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                _fail("PACKAGE_FILE_REJECTED", "Package entries must be ordinary single-link files")
            file_limit = MAX_WHEEL_BYTES if rel.endswith(".whl") else MAX_FILE_BYTES
            if info.st_size > file_limit or len(captured) >= MAX_PACKAGE_FILES:
                _fail("PACKAGE_TOO_LARGE", "Package exceeds file limits")
            flags = (
                os.O_RDONLY
                | getattr(os, "O_BINARY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0)
            )
            with os.fdopen(os.open(path, flags), "rb") as stream:
                before = os.fstat(stream.fileno())
                if _signature(info) != _signature(before):
                    _fail("PACKAGE_CHANGED", "Package entry changed before being read")
                content = stream.read(file_limit + 1)
                after = os.fstat(stream.fileno())
            if (
                _signature(before) != _signature(after)
                or _signature(after) != _signature(path.lstat())
                or len(content) != after.st_size
            ):
                _fail("PACKAGE_CHANGED", "Package changed while being captured")
            total += len(content)
            if len(content) > file_limit or total > MAX_PACKAGE_BYTES:
                _fail("PACKAGE_TOO_LARGE", "Package exceeds byte limits")
            captured[rel] = content
    for directory, initial in directories:
        if _signature(directory.lstat()) != _signature(initial):
            _fail("PACKAGE_CHANGED", "Package directory changed during capture")
    return captured


def _schema(files: dict[str, bytes], reference: str) -> dict:
    if not isinstance(reference, str):
        _fail("INVALID_SCHEMA", "Schema reference must name a package JSON file")
    parts = PurePosixPath(reference)
    if parts.is_absolute() or ".." in parts.parts or reference not in files:
        _fail("INVALID_SCHEMA", "Schema must be contained in the immutable package")
    try:

        def reject_constant(value):
            raise ValueError(f"Non-finite JSON number: {value}")

        value = json.loads(files[reference], parse_constant=reject_constant)
        Draft202012Validator.check_schema(value)

        # Resolve no network or external filesystem references during admission.
        def references(node):
            if isinstance(node, dict):
                for key, item in node.items():
                    if key in {"$id"} and item:
                        _fail("INVALID_SCHEMA", "Schema base URI changes are not supported")
                    if key in {"$ref", "$dynamicRef"} and (
                        not isinstance(item, str) or not item.startswith("#")
                    ):
                        _fail("INVALID_SCHEMA", "Schema references must be local fragments")
                    references(item)
            elif isinstance(node, list):
                for item in node:
                    references(item)

        references(value)
        if not isinstance(value, dict):
            _fail("INVALID_SCHEMA", "Schema must be a JSON object")
        return value
    except (ValueError, UnicodeError, SchemaError, RecursionError) as exc:
        raise ForestError("INVALID_SCHEMA", "Invalid package JSON schema", 422) from exc


def _management_ui(files: dict[str, bytes], component: dict) -> dict | None:
    declaration = component.get("management_ui")
    if declaration is None:
        return None
    reference = declaration["entrypoint"]
    path = PurePosixPath(reference)
    if (
        not isinstance(reference, str)
        or not reference.startswith("ui/")
        or path.is_absolute()
        or ".." in path.parts
        or path.suffix.lower() != ".html"
        or reference not in files
    ):
        _fail("INVALID_MANAGEMENT_UI", "Management UI must be a packaged ui/*.html file")
    content = files[reference]
    if not content or len(content) > MAX_MANAGEMENT_UI_BYTES:
        _fail("INVALID_MANAGEMENT_UI", "Management UI exceeds its byte limit")
    try:
        html = content.decode("utf-8")
    except UnicodeError as exc:
        raise ForestError("INVALID_MANAGEMENT_UI", "Management UI must be UTF-8 HTML", 422) from exc
    if "\x00" in html or not re.search(r"<!doctype\s+html|<html(?:\s|>)", html, re.IGNORECASE):
        _fail("INVALID_MANAGEMENT_UI", "Management UI must be a complete HTML document")
    return {
        "entrypoint": reference,
        "protocol": declaration["protocol"],
        "sha256": hashlib.sha256(content).hexdigest(),
        "bytes": len(content),
    }


def _bootstrap_ui(files: dict[str, bytes], component: dict) -> tuple[str, bytes] | None:
    declaration = component.get("bootstrap_contribution")
    reference = None if declaration is None else declaration.get("ui_entrypoint")
    if reference is None:
        return None
    path = PurePosixPath(reference)
    if (
        not reference.startswith("ui/")
        or path.is_absolute()
        or ".." in path.parts
        or path.suffix.lower() != ".html"
        or reference not in files
    ):
        _fail("INVALID_BOOTSTRAP_UI", "Bootstrap UI must be a packaged ui/*.html file")
    content = files[reference]
    if not content or len(content) > MAX_MANAGEMENT_UI_BYTES:
        _fail("INVALID_BOOTSTRAP_UI", "Bootstrap UI exceeds its byte limit")
    try:
        html = content.decode("utf-8")
    except UnicodeError as exc:
        raise ForestError("INVALID_BOOTSTRAP_UI", "Bootstrap UI must be UTF-8 HTML", 422) from exc
    if "\x00" in html or not re.search(r"<!doctype\s+html|<html(?:\s|>)", html, re.IGNORECASE):
        _fail("INVALID_BOOTSTRAP_UI", "Bootstrap UI must be a complete HTML document")
    return reference, content


def _bootstrap_materialization(files: dict[str, bytes], component: dict) -> dict | None:
    reference = component.get("bootstrap_materialization")
    if reference is None:
        return None
    path = PurePosixPath(reference)
    if (
        component["scope"] != "bot"
        or component.get("auto_install") is not True
        or path.is_absolute()
        or ".." in path.parts
        or path.suffix != ".json"
        or reference not in files
        or len(files[reference]) > 262_144
    ):
        _fail(
            "INVALID_BOOTSTRAP_MATERIALIZATION",
            "Materialization must name a bounded JSON file in an auto-installed Bot package",
        )
    try:
        spec = json.loads(files[reference])
        json.dumps(spec, allow_nan=False)
    except (UnicodeError, ValueError, TypeError) as exc:
        raise ForestError(
            "INVALID_BOOTSTRAP_MATERIALIZATION",
            "Bootstrap materialization is not JSON", 422,
        ) from exc
    return validate_materialization_spec(
        spec, has_contribution=component.get("bootstrap_contribution") is not None
    )


def storage_bundle_digest(spec: StorageBundleSpec, execution_profile: str) -> str:
    """Bind installed storage to its schema, independently of runtime code."""

    return digest_json(
        {
            "protocol": "forest.storage-bundle/1",
            **spec.as_fact(),
            "component_id": spec.component_id,
            "execution_profile": execution_profile,
        }
    )


def _storage_bundle(files: dict[str, bytes], component: dict) -> dict | None:
    declaration = component.get("storage")
    if declaration is None:
        return None
    if declaration["scope"] != component["scope"]:
        _fail(
            "INVALID_STORAGE_BUNDLE",
            "Storage scope must match its owning component scope",
        )
    schema_id = declaration["schema_id"]
    reference = declaration["baseline"]
    path = PurePosixPath(reference)
    if (
        not isinstance(reference, str)
        or not reference.startswith("schema/")
        or path.is_absolute()
        or ".." in path.parts
        or path.suffix.lower() != ".sql"
        or reference not in files
    ):
        _fail(
            "INVALID_STORAGE_BUNDLE",
            "Storage baseline must be a packaged schema/*.sql file",
        )
    content = files[reference]
    if not content or len(content) > MAX_STORAGE_BASELINE_BYTES:
        _fail("INVALID_STORAGE_BUNDLE", "Storage baseline exceeds its byte limit")
    spec = StorageBundleSpec(
        component["id"],
        schema_id,
        declaration["schema_version"],
        declaration["baseline_sha256"],
        declaration["repository_contract"],
        declaration["scope"],
    )
    execution_profile = declaration.get("execution_profile", "declarative")
    if execution_profile == "official":
        validate_official_storage_baseline(content, spec.baseline_sha256)
    else:
        validate_storage_baseline(content, spec.baseline_sha256)
    return {
        "schema_id": spec.schema_id,
        "schema_version": spec.schema_version,
        "namespace": spec.namespace,
        "baseline": reference,
        "baseline_sha256": spec.baseline_sha256,
        "scope": declaration["scope"],
        "repository_contract": spec.repository_contract,
        "execution_profile": execution_profile,
        "bytes": len(content),
        "package_digest": storage_bundle_digest(spec, execution_profile),
    }


def _manifest(files: dict[str, bytes]) -> dict:
    verify_checksums(files)
    try:
        manifest = tomllib.loads(files["module.toml"].decode("utf-8"))
    except (KeyError, ValueError, UnicodeError, RecursionError) as exc:
        raise ForestError("INVALID_MANIFEST", "A valid module.toml is required", 422) from exc
    if not Draft202012Validator(MANIFEST_SCHEMA).is_valid(manifest):
        _fail("INVALID_MANIFEST", "Manifest fields do not match the supported schema")
    try:
        digest_json(manifest)
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise ForestError(
            "INVALID_MANIFEST", "Manifest must contain finite JSON values", 422
        ) from exc
    if manifest.get("manifest_version") != 1 or manifest.get("runtime") != "python-worker":
        _fail("UNSUPPORTED_MANIFEST", "Only manifest v1 and python-worker are implemented")
    if manifest.get("kernel_api") != ">=1.0,<2.0":
        _fail("KERNEL_API_INCOMPATIBLE", "Package must target kernel API >=1.0,<2.0")
    for key in ("package_id",):
        if not isinstance(manifest.get(key), str) or not IDENTIFIER.fullmatch(manifest[key]):
            _fail("INVALID_MANIFEST", f"Invalid {key}")
    if not isinstance(manifest.get("version"), str) or not VERSION.fullmatch(manifest["version"]):
        _fail("INVALID_MANIFEST", "Invalid package version")
    entrypoint = manifest.get("entrypoint", "")
    if not isinstance(entrypoint, str) or not ENTRYPOINT.fullmatch(entrypoint):
        _fail("INVALID_MANIFEST", "Entrypoint must be a Python module:Class")
    module_root = manifest.get("module_root", "python")
    root = PurePosixPath(module_root)
    if (
        not isinstance(module_root, str)
        or not module_root
        or len(module_root) > 128
        or root.is_absolute()
        or module_root != root.as_posix()
        or any(part in {"", ".", ".."} for part in root.parts)
    ):
        _fail("INVALID_MANIFEST", "module_root must be a portable relative directory")
    module = module_root + "/" + entrypoint.split(":")[0].replace(".", "/")
    if module + ".py" not in files and module + "/__init__.py" not in files:
        _fail("ENTRYPOINT_MISSING", "Entrypoint is absent from the declared module_root")
    components = manifest.get("components")
    if not isinstance(components, list) or len(components) != 1:
        _fail(
            "UNSUPPORTED_COMPONENT_SET", "This runtime currently supports one component per package"
        )
    component = components[0]
    if not IDENTIFIER.fullmatch(component.get("id", "")):
        _fail("INVALID_COMPONENT", "A valid component identity is required")
    if component.get("reload_mode") != "drain":
        _fail("UNSUPPORTED_STATE_SCHEMA", "Only drain reload is supported")
    if any(
        not _valid_json_pointer(path)
        for path in component.get("config_immutable_paths", ())
    ):
        _fail("INVALID_MANIFEST", "Immutable configuration paths must be JSON pointers")
    if "state_value_schema" in component:
        _schema(files, component["state_value_schema"])
    migration_sources = set()
    for migration in component.get("state_migrations", []):
        if (
            migration["to_schema"] != component["state_schema"]
            or migration["from_schema"] == migration["to_schema"]
            or migration["from_schema"] in migration_sources
        ):
            _fail(
                "INVALID_STATE_MIGRATION", "Migration sources must uniquely target candidate state"
            )
        migration_sources.add(migration["from_schema"])
    dependencies = component.get("requires", [])
    dependency_names = set()
    for dependency in dependencies:
        name = dependency["name"]
        if not IDENTIFIER.fullmatch(name) or name in dependency_names:
            _fail("INVALID_DEPENDENCY", "Dependency service names must be valid and unique")
        contract_range(dependency["contract"])
        dependency_names.add(name)
    service_call_grants = {
        item.removeprefix("service.call:")
        for item in component.get("capabilities", {}).get("requests", ())
        if item.startswith("service.call:")
    }
    local_effects = {item["name"] for item in component.get("effect_services", ())}
    if dependency_names - service_call_grants or service_call_grants - dependency_names - local_effects:
        _fail(
            "INVALID_DEPENDENCY",
            "Service call grants must name a declared dependency or local effect",
        )
    _schema(files, component.get("config_schema"))
    bootstrap_config = component.get("bootstrap_config")
    if component.get("auto_install") and (
        bootstrap_config is None or component["scope"] != "bot"
    ):
        _fail(
            "INVALID_BOOTSTRAP_CONFIGURATION",
            "Automatically installed builtins need a seed and Bot scope",
        )
    if bootstrap_config is not None:
        parts = PurePosixPath(bootstrap_config)
        if (parts.is_absolute() or ".." in parts.parts or not bootstrap_config.endswith(".json")
                or bootstrap_config not in files):
            _fail(
                "INVALID_BOOTSTRAP_CONFIGURATION",
                "Bootstrap configuration must name a package JSON file",
            )
        try:
            seed = json.loads(files[bootstrap_config])
            json.dumps(seed, allow_nan=False)
        except (ValueError, UnicodeError, TypeError):
            _fail(
                "INVALID_BOOTSTRAP_CONFIGURATION",
                "Bootstrap configuration must contain JSON",
            )
        if not isinstance(seed, dict):
            _fail(
                "INVALID_BOOTSTRAP_CONFIGURATION",
                "Bootstrap configuration must be a JSON object",
            )
    contribution = component.get("bootstrap_contribution")
    if contribution is not None:
        if component["scope"] != "bot" or not component.get("auto_install"):
            _fail(
                "INVALID_BOOTSTRAP_CONTRIBUTION",
                "Creation-time contributions require an auto-installed Bot component",
            )
        setup_schema = _schema(files, contribution["schema"])
        reference = contribution["initial_value"]
        parts = PurePosixPath(reference)
        if (
            parts.is_absolute()
            or ".." in parts.parts
            or not reference.endswith(".json")
            or reference not in files
        ):
            _fail("INVALID_BOOTSTRAP_CONTRIBUTION", "Initial value path is invalid")
        try:
            initial_value = json.loads(files[reference])
            json.dumps(initial_value, allow_nan=False)
        except (ValueError, UnicodeError, TypeError) as exc:
            raise ForestError(
                "INVALID_BOOTSTRAP_CONTRIBUTION", "Initial value is not JSON", 422
            ) from exc
        if (
            setup_schema.get("type") != "object"
            or not isinstance(initial_value, dict)
            or not Draft202012Validator(setup_schema).is_valid(initial_value)
        ):
            _fail(
                "INVALID_BOOTSTRAP_CONTRIBUTION",
                "Creation-time schema and initial value must describe an object",
            )
        for kind in ("secret_refs", "origins"):
            fields = [item["field"] for item in contribution.get(kind, ())]
            if len(fields) != len(set(fields)):
                _fail("INVALID_BOOTSTRAP_CONTRIBUTION", "Creation input constraint repeats a field")
            for field in fields:
                property_schema = setup_schema.get("properties", {}).get(field)
                if (
                    not isinstance(field, str)
                    or re.fullmatch(r"[a-z][a-z0-9_]{0,127}", field) is None
                    or not isinstance(property_schema, dict)
                    or property_schema.get("type") != "string"
                    or field not in setup_schema.get("required", ())
                ):
                    _fail(
                        "INVALID_BOOTSTRAP_CONTRIBUTION",
                        "Creation input constraint must target a required string field",
                    )
        _bootstrap_ui(files, component)
    _bootstrap_materialization(files, component)
    replacement_facts_schema = component.get("lifecycle", {}).get(
        "replacement_facts_schema"
    )
    if replacement_facts_schema is not None:
        _schema(files, replacement_facts_schema)
    _management_ui(files, component)
    _storage_bundle(files, component)
    services = component.get("provides", [])
    effect_services = component.get("effect_services", [])
    domain_queries = component.get("domain_queries", [])
    domain_commands = component.get("domain_commands", [])
    kernel_queries = component.get("kernel_queries", [])
    kernel_commands = component.get("kernel_commands", [])
    management_queries = component.get("management_queries", [])
    management_actions = component.get("management_actions", [])
    management_assets = component.get("management_assets", [])
    management_artifacts = component.get("management_artifacts", [])
    asset_by_id = {}
    for asset in management_assets:
        asset_id = asset["id"]
        if (
            not IDENTIFIER.fullmatch(asset_id)
            or not IDENTIFIER.fullmatch(asset["owner_kind"])
            or not re.fullmatch(r"[a-z][a-z0-9._-]{0,31}", asset["owner_prefix"])
            or asset_id in asset_by_id
        ):
            _fail("INVALID_MANAGEMENT_ASSETS", "Management asset declarations must have unique valid identities")
        asset_by_id[asset_id] = asset
    unit_of_work_participants = component.get("unit_of_work_participants", [])
    unit_of_work_plans = component.get("unit_of_work_plans", [])
    job_types = component.get("job_types", [])
    shell_commands = component.get("shell_commands", [])
    agent_tools = component.get("agent_tools", [])
    owned_work_handlers = component.get("owned_work_handlers", [])
    commands = component.get("commands", [])
    message_handlers = component.get("message_handlers", [])
    if component.get("message_handler_takeover", "none") == "compatible" and not message_handlers:
        _fail(
            "INVALID_MESSAGE_HANDLERS",
            "Compatible Handler takeover requires at least one message handler",
        )
    subscriptions = component.get("subscribes", [])
    component_jobs = component.get("jobs", [])
    if len(services) > 32 or (
        not services
        and not effect_services
        and not domain_queries
        and not domain_commands
        and not kernel_queries
        and not kernel_commands
        and not unit_of_work_participants
        and not unit_of_work_plans
        and not job_types
        and not shell_commands
        and not agent_tools
        and not owned_work_handlers
        and not commands
        and not message_handlers
        and not subscriptions
        and not component_jobs
    ):
        _fail(
            "INVALID_SERVICES",
            "Declare at least one service, subscription, job, command, or message handler",
        )
    names = set()
    for service in (
        *services,
        *effect_services,
        *domain_queries,
        *domain_commands,
        *kernel_queries,
        *kernel_commands,
        *unit_of_work_participants,
        *unit_of_work_plans,
        *job_types,
        *owned_work_handlers,
    ):
        name = service.get("name", "")
        if not IDENTIFIER.fullmatch(name) or name in names:
            _fail("INVALID_SERVICES", "Service names must be valid and unique")
        names.add(name)
        if len(service.get("contract", "")) > 64 or not re.fullmatch(
            r"[1-9]\d*\.\d+", service.get("contract", "")
        ):
            _fail("INVALID_SERVICES", "A service contract major.minor is required")
        _schema(files, service.get("input_schema"))
        _schema(files, service.get("output_schema"))
        if service in effect_services and not IDENTIFIER.fullmatch(service.get("kind", "")):
            _fail("INVALID_SERVICES", "Effect services require a valid driver kind")
        if service in (*kernel_queries, *kernel_commands) and (
            component["scope"] != "deployment"
            or not IDENTIFIER.fullmatch(service.get("kind", ""))
        ):
            _fail(
                "INVALID_MANAGEMENT_OPERATIONS",
                "Kernel management ports require a deployment scope and exact kind",
            )
        if service in effect_services and service.get("ingress") is not None:
            ingress = service["ingress"]
            ingress_name = ingress.get("name", "")
            if not IDENTIFIER.fullmatch(ingress_name) or ingress_name in names:
                _fail(
                    "INVALID_SERVICES",
                    "Effect ingress ports require a unique valid service name",
                )
            names.add(ingress_name)
            _schema(files, ingress.get("input_schema"))
            _schema(files, ingress.get("output_schema"))
        if service in (
            *domain_queries,
            *domain_commands,
            *unit_of_work_participants,
        ) and not SQL_FUNCTION.fullmatch(service.get("function", "")):
            _fail(
                "INVALID_DOMAIN_AUTHORITY",
                "Domain authority functions must be portable unqualified SQL identifiers",
            )
        payload_refs = service.get("payload_refs", [])
        if payload_refs and service not in (*domain_commands, *unit_of_work_participants):
            _fail(
                "INVALID_DOMAIN_AUTHORITY",
                "Managed payload references are only valid on write ports",
            )
        for reference in payload_refs:
            path = reference.get("path")
            bound_paths = [reference.get("owner_id_path")]
            owner_component = reference.get("owner_component")
            component_path = reference.get("owner_component_path")
            owner_kind = reference.get("owner_kind")
            owner_kind_path = reference.get("owner_kind_path")
            bound_paths.extend(
                value for value in (component_path, owner_kind_path) if value is not None
            )
            bound_paths.extend(
                value for value in (
                    reference.get("media_type_path"), reference.get("byte_length_path"),
                    reference.get("target_ref_path"), reference.get("part_id_path"),
                    reference.get("width_path"), reference.get("height_path"),
                ) if value is not None
            )
            if (
                not _valid_json_pointer(path, wildcards=True)
                or path.split("/").count("*") != reference["owner_id_path"].split("/").count("*")
                or any(
                    not _valid_json_pointer(owner_path, wildcards=True)
                    or owner_path.split("/").count("*")
                    not in (0, path.split("/").count("*"))
                    for owner_path in bound_paths
                )
                or (owner_component is None) == (component_path is None)
                or (owner_kind is None) == (owner_kind_path is None)
                or (
                    owner_component is not None
                    and not IDENTIFIER.fullmatch(owner_component)
                )
                or (owner_kind is not None and not IDENTIFIER.fullmatch(owner_kind))
            ):
                _fail(
                    "INVALID_DOMAIN_AUTHORITY",
                    "Managed payload reference declarations must use exact JSON pointers and owners",
                )
            asset_id = reference.get("management_asset_id")
            if asset_id is not None:
                asset = asset_by_id.get(asset_id)
                if (
                    asset is None
                    or service not in domain_commands
                    or owner_component != component["id"]
                    or owner_kind != asset["owner_kind"]
                    or any(reference.get(name) is None for name in (
                        "target_ref_path", "part_id_path", "media_type_path",
                        "byte_length_path", "width_path", "height_path",
                    ))
                    or not any(
                        action["port"] == service["name"] and action.get("actor_field")
                        for action in management_actions
                    )
                ):
                    _fail(
                        "INVALID_MANAGEMENT_ASSETS",
                        "Asset-backed payload refs require a matching management action and exact metadata paths",
                    )
        if service in owned_work_handlers:
            authority = service.get("authority", "package")
            renewable = "renew_function" in service or authority == "kernel"
            if (
                (authority == "package" and (
                    not SQL_FUNCTION.fullmatch(service.get("claim_function", ""))
                    or not SQL_FUNCTION.fullmatch(service.get("settle_function", ""))
                ))
                or (authority == "kernel" and (
                    component["scope"] != "deployment"
                    or any(key in service for key in (
                        "claim_function", "settle_function", "renew_function"
                    ))
                ))
                or not IDENTIFIER.fullmatch(service.get("handler", ""))
                or renewable != ("renew_timeout_s" in service)
                or renewable != ("renew_interval_s" in service)
                or (renewable and (
                    (authority == "package" and not SQL_FUNCTION.fullmatch(
                        service["renew_function"]
                    ))
                    or service["lease_seconds"] <= (
                        service["renew_interval_s"] + service["renew_timeout_s"]
                        + service["settle_timeout_s"]
                    )
                ))
                or (not renewable and service["lease_seconds"] <= (
                    service["handler_timeout_s"] + service["settle_timeout_s"]
                ))
            ):
                _fail(
                    "INVALID_OWNED_WORK",
                    "Owned work requires a fenced lease beyond execution or renewal",
                )
    if (
        domain_queries or domain_commands or unit_of_work_participants or
        any(item.get("authority", "package") == "package" for item in owned_work_handlers)
    ) and component.get("storage") is None:
        _fail("INVALID_DOMAIN_AUTHORITY", "Domain authority ports require package storage")
    functions = [
        item["function"] for item in (*domain_queries, *domain_commands, *unit_of_work_participants)
    ] + [
        function
        for item in owned_work_handlers
        if item.get("authority", "package") == "package"
        for function in (item["claim_function"], item["settle_function"],
                         *([item["renew_function"]] if "renew_function" in item else []))
    ]
    if len(functions) != len(set(functions)):
        _fail("INVALID_DOMAIN_AUTHORITY", "Domain authority functions must be unique")
    participant_names = {item["name"] for item in unit_of_work_participants}
    for plan in unit_of_work_plans:
        step_ids = set()
        for step in plan["steps"]:
            step_id = step.get("id", "")
            component_id = step.get("component_id", "")
            participant = step.get("participant", "")
            input_path = step.get("input_path")
            participant_generation_field = step.get(
                "participant_generation_field"
            )
            if (
                participant == "component.configuration-rebuild.record"
                or not IDENTIFIER.fullmatch(step_id)
                or step_id in step_ids
                or not IDENTIFIER.fullmatch(component_id)
                or not IDENTIFIER.fullmatch(participant)
                or (input_path is not None and not _valid_json_pointer(input_path))
                or (
                    participant_generation_field is not None
                    and not re.fullmatch(
                        r"[a-z][a-z0-9_]{0,63}",
                        participant_generation_field,
                    )
                )
                or (component_id == component["id"] and participant not in participant_names)
            ):
                _fail(
                    "INVALID_UNIT_OF_WORK_PLAN",
                    "UnitOfWork steps must name unique IDs and declared participants",
                )
            step_ids.add(step_id)
    query_ports = {item["name"] for item in (*domain_queries, *kernel_queries)}
    guard_query = component.get("lifecycle", {}).get("replacement_guard_query")
    if guard_query is not None and guard_query not in {
        item["name"] for item in domain_queries
    }:
        _fail(
            "INVALID_REPLACEMENT_GUARD",
            "Replacement guard must name a declared Domain Query",
        )
    query_ports.update(
        item["name"] for item in services if item["semantics"] == "read"
    )
    command_ports = {item["name"] for item in domain_commands}
    management_command_ports = command_ports | {
        item["name"] for item in kernel_commands
    }
    management_ids = set()
    for declaration in (*management_queries, *management_actions, *management_artifacts):
        operation_id = declaration.get("id", "")
        if not IDENTIFIER.fullmatch(operation_id) or operation_id in management_ids:
            _fail(
                "INVALID_MANAGEMENT_OPERATIONS",
                "Management operation identities must be valid and unique",
            )
        management_ids.add(operation_id)
    effect_port_semantics = {
        item["name"]: item["semantics"] for item in effect_services
    }
    for declaration in management_artifacts:
        expected = "read" if declaration["direction"] == "download" else "idempotent"
        if (
            effect_port_semantics.get(declaration["port"]) != expected
            or (declaration["direction"] == "upload"
                and declaration.get("required_role", "ADMIN") != "ADMIN")
            or (("reserve_port" in declaration) != ("settle_port" in declaration))
            or (("reserve_port" in declaration) != ("close_port" in declaration))
            or (declaration["direction"] != "upload" and "reserve_port" in declaration)
            or ("close_port" in declaration and (
                effect_port_semantics.get(declaration["close_port"]) != "idempotent"
                or declaration["close_port"] == declaration["port"]
            ))
            or ("reserve_port" in declaration and (
                declaration["reserve_port"] not in command_ports
                or declaration["settle_port"] not in command_ports
                or declaration["reserve_port"] == declaration["settle_port"]
            ))
        ):
            _fail(
                "INVALID_MANAGEMENT_OPERATIONS",
                "Artifact direction and role must match its declared effect port",
            )
    for declaration in management_queries:
        if declaration.get("port") not in query_ports:
            _fail(
                "INVALID_MANAGEMENT_OPERATIONS",
                "Management queries must bind a Domain or kernel Query or read-only service port",
            )
        if declaration.get("default_limit", 1) > declaration.get("max_limit", 10000):
            _fail(
                "INVALID_MANAGEMENT_OPERATIONS",
                "Management query default_limit cannot exceed max_limit",
            )
        actor_field = declaration.get("actor_field")
        if actor_field is not None:
            port = next(
                item for item in (*domain_queries, *kernel_queries, *services)
                if item["name"] == declaration["port"]
            )
            input_schema = _schema(files, port["input_schema"])
            properties = input_schema.get("properties", {})
            if (
                declaration.get("required_role") != "ADMIN"
                or not IDENTIFIER.fullmatch(actor_field)
                or actor_field not in input_schema.get("required", ())
                or not isinstance(properties, dict)
                or not isinstance(properties.get(actor_field), dict)
                or properties[actor_field].get("type") != "string"
            ):
                _fail(
                    "INVALID_MANAGEMENT_OPERATIONS",
                    "Actor-bound management queries require ADMIN and a required string field",
                )
    management_query_ports = {item["port"] for item in management_queries}
    for service in services:
        if service.get("visibility", "public") == "management" and (
            service["semantics"] != "read"
            or service["name"] not in management_query_ports
        ):
            _fail(
                "INVALID_MANAGEMENT_OPERATIONS",
                "Management-only services must be read-only management query ports",
            )
    kernel_by_name = {item["name"]: item for item in kernel_commands}
    for declaration in management_actions:
        if declaration.get("port") not in management_command_ports:
            _fail(
                "INVALID_MANAGEMENT_OPERATIONS",
                "Management actions must bind an exact Domain or kernel Command port",
            )
        preflight = declaration.get("transaction_preflight")
        if preflight is not None and not IDENTIFIER.fullmatch(preflight):
            _fail(
                "INVALID_MANAGEMENT_OPERATIONS",
                "Management transaction_preflight must name one host capability",
            )
        pointer = declaration.get("authorization_path")
        if pointer is not None and not _valid_json_pointer(pointer):
            _fail(
                "INVALID_MANAGEMENT_OPERATIONS",
                "Management authorization_path must be an exact JSON pointer",
            )
        actor_field = declaration.get("actor_field")
        if actor_field is not None:
            port = next(
                item for item in (*domain_commands, *kernel_commands)
                if item["name"] == declaration["port"]
            )
            input_schema = _schema(files, port["input_schema"])
            properties = input_schema.get("properties", {})
            actor_schema = properties.get(actor_field)
            if (
                not IDENTIFIER.fullmatch(actor_field)
                or actor_field not in input_schema.get("required", ())
                or not isinstance(actor_schema, dict)
                or actor_schema.get("type") != "string"
            ):
                _fail(
                    "INVALID_MANAGEMENT_OPERATIONS",
                    "Management actor_field must name one required string command field",
                )
            if pointer == "/" + actor_field.replace("~", "~0").replace("/", "~1"):
                _fail(
                    "INVALID_MANAGEMENT_OPERATIONS",
                    "Management authorization_path cannot reference an injected actor field",
                )
        secret_field = declaration.get("secret_field")
        if secret_field is not None:
            port = kernel_by_name.get(declaration["port"])
            schema = _schema(files, port["input_schema"]) if port is not None else {}
            properties = schema.get("properties", {})
            if (
                port is None
                or not IDENTIFIER.fullmatch(secret_field)
                or secret_field == actor_field
                or secret_field not in schema.get("required", ())
                or not isinstance(properties, dict)
                or properties.get(secret_field, {}).get("type") != "string"
            ):
                _fail(
                    "INVALID_MANAGEMENT_OPERATIONS",
                    "Host secret input requires a required string on a kernel Command",
                )
    if sorted(item["name"] for item in kernel_commands) != sorted(
        action["port"] for action in management_actions
        if action["port"] in kernel_by_name
    ):
        _fail(
            "INVALID_MANAGEMENT_OPERATIONS",
            "Kernel Commands must bind exactly one declared management Action",
        )
    if sorted(item["name"] for item in kernel_queries) != sorted(
        query["port"] for query in management_queries
        if query["port"] in {item["name"] for item in kernel_queries}
    ):
        _fail(
            "INVALID_MANAGEMENT_OPERATIONS",
            "Kernel Queries must bind exactly one declared management Query",
        )
    bootstrap_catalog_query = component.get("management_ui", {}).get(
        "bootstrap_catalog_query"
    )
    if bootstrap_catalog_query is not None and not any(
        query["id"] == bootstrap_catalog_query
        and any(
            port["name"] == query["port"]
            and port["kind"] == "deployment.bootstrap-catalog"
            for port in kernel_queries
        )
        for query in management_queries
    ):
        _fail(
            "INVALID_MANAGEMENT_UI",
            "Bootstrap editor composition requires an exact kernel catalog Query",
        )
    directory_kinds = set()
    for declaration in component.get("contribution_directories", []):
        kind = declaration.get("kind")
        if (
            kind in directory_kinds
            or kind
            not in {"job-types", "shell-commands", "routing-routes", "agent-tools"}
            or (
                kind != "agent-tools"
                and (
                    declaration.get("synchronization_command") not in command_ports
                    or declaration.get("cleanup_command") not in command_ports
                    or declaration.get("cleanup_command")
                    == declaration.get("synchronization_command")
                )
            )
            or (
                kind == "agent-tools"
                and any(
                    field in declaration
                    for field in (
                        "synchronization_command",
                        "cleanup_command",
                        "cancellation_query",
                        "synchronize_configuration",
                    )
                )
            )
            or (
                kind == "job-types"
                and declaration.get("cancellation_query") not in query_ports
            )
            or (
                kind in {"shell-commands", "routing-routes"}
                and "cancellation_query" in declaration
            )
            or (
                "synchronize_configuration" in declaration
                and type(declaration["synchronize_configuration"]) is not bool
            )
        ):
            _fail(
                "INVALID_CONTRIBUTION_DIRECTORY",
                "Contribution directory lifecycle and query ports are invalid",
            )
        directory_kinds.add(kind)
    contributed_job_types = set()
    contributed_job_handlers = set()
    for definition in job_types:
        job_type = definition.get("job_type", "")
        handler = definition.get("handler", "")
        if (
            not IDENTIFIER.fullmatch(job_type)
            or job_type in contributed_job_types
            or not IDENTIFIER.fullmatch(handler)
            or handler in contributed_job_handlers
        ):
            _fail("INVALID_JOB_TYPES", "Job type identities must be valid and unique")
        contributed_job_types.add(job_type)
        contributed_job_handlers.add(handler)
    service_contracts = {
        (item["name"], contract_range(item["contract"])[0][0])
        for item in services
    }
    shell_command_keys = set()
    management_action_by_id = {item["id"]: item for item in management_actions}
    for declaration in shell_commands:
        key = (declaration.get("command_id"), declaration.get("command_version"))
        target = (
            declaration.get("service_id"),
            declaration.get("service_version"),
        )
        argument_schema = _schema(files, declaration.get("argument_schema"))
        properties = argument_schema.get("properties", {})
        argument_properties = properties.get("arguments", {}).get("properties", {})
        aliases = declaration.get("option_aliases", [])
        capabilities = declaration.get("required_capabilities", [])
        if (
            key in shell_command_keys
            or not IDENTIFIER.fullmatch(declaration.get("command_id", ""))
            or target not in service_contracts
            or not re.fullmatch(
                r"[a-z0-9][a-z0-9._-]{0,255}",
                declaration.get("management_operation_id", ""),
            )
            or (
                declaration.get("management_action", False)
                and declaration["management_operation_id"] not in management_action_by_id
            )
            or properties.get("protocol", {}).get("const")
            != "forest.shell.arguments/1"
            or properties.get("command_id", {}).get("const")
            != declaration.get("command_id")
            or not isinstance(properties.get("arguments"), dict)
            or any(
                not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,199}", capability)
                for capability in capabilities
            )
            or any(
                not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,127}", alias[0])
                for alias in aliases
            )
            or any(alias[1] not in argument_properties for alias in aliases)
        ):
            _fail(
                "INVALID_SHELL_COMMAND",
                "Shell command must bind one declared service and matching argument schema",
            )
        if declaration.get("management_action", False):
            action = management_action_by_id[declaration["management_operation_id"]]
            port = next(
                item for item in domain_commands if item["name"] == action["port"]
            )
            receipt_schema = _schema(files, port["output_schema"])
            if (
                "request_id" not in receipt_schema.get("required", ())
                or receipt_schema.get("properties", {}).get("request_id", {}).get("type")
                != "string"
            ):
                _fail(
                    "INVALID_SHELL_COMMAND",
                    "Shell management actions require a durable request_id receipt",
                )
        shell_command_keys.add(key)
    agent_tool_names = set()
    agent_tool_services = set()
    for declaration in agent_tools:
        name = declaration.get("name", "")
        target = (
            declaration.get("service_id"),
            declaration.get("service_version"),
        )
        tool_declaration = _schema(files, declaration.get("declaration_schema"))
        parameters = tool_declaration.get("parameters")
        try:
            Draft202012Validator.check_schema(parameters)
        except (SchemaError, TypeError) as exc:
            _fail("INVALID_AGENT_TOOL", f"Agent tool parameters are invalid: {exc.message}")
        if (
            name in agent_tool_names
            or target in agent_tool_services
            or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]{0,127}", name)
            or target not in service_contracts
            or set(tool_declaration) - {"name", "description", "parameters", "strict"}
            or tool_declaration.get("name") != name
            or not isinstance(tool_declaration.get("description"), str)
            or not tool_declaration["description"].strip()
            or not isinstance(parameters, dict)
            or tool_declaration.get("strict", True) is not True
        ):
            _fail(
                "INVALID_AGENT_TOOL",
                "Agent tool must bind one declared service and a matching strict declaration",
            )
        agent_tool_names.add(name)
        agent_tool_services.add(target)
    for definition in owned_work_handlers:
        handler = definition["handler"]
        if handler in contributed_job_handlers:
            _fail("INVALID_OWNED_WORK", "Owned work handlers must be package-unique")
        contributed_job_handlers.add(handler)
    if len(
        {
            definition.get("drain_policy", "inflight")
            for definition in owned_work_handlers
        }
    ) > 1:
        _fail(
            "INVALID_OWNED_WORK",
            "One package Generation must use one owned-work drain policy",
        )
    published_topics = set()
    for publication in component.get("publishes", []):
        topic = publication.get("topic", "")
        if not IDENTIFIER.fullmatch(topic) or topic in published_topics:
            _fail("INVALID_EVENTS", "Published event topics must be valid and unique")
        published_topics.add(topic)
        if not re.fullmatch(r"[1-9]\d*\.\d+", publication.get("contract", "")):
            _fail("INVALID_EVENTS", "Published events require an exact major.minor contract")
        _schema(files, publication.get("schema"))
    subscriber_ids = set()
    for subscription in component.get("subscribes", []):
        subscriber_id = subscription.get("subscriber_id", "")
        if not IDENTIFIER.fullmatch(subscriber_id) or subscriber_id in subscriber_ids:
            _fail("INVALID_EVENTS", "Durable subscriber identities must be valid and unique")
        subscriber_ids.add(subscriber_id)
        if not IDENTIFIER.fullmatch(subscription.get("topic", "")) or not re.fullmatch(
            r"[1-9]\d*\.\d+", subscription.get("contract", "")
        ):
            _fail("INVALID_EVENTS", "Subscriptions require a valid topic and exact contract")
        _schema(files, subscription.get("schema"))
    job_ids = set()
    for definition in component.get("jobs", []):
        handler_id = definition.get("handler_id", "")
        if (
            not IDENTIFIER.fullmatch(handler_id)
            or handler_id in job_ids
            or handler_id in contributed_job_handlers
        ):
            _fail("INVALID_JOBS", "Durable job handler identities must be valid and unique")
        job_ids.add(handler_id)
        _schema(files, definition.get("input_schema"))
    command_ids = set()
    command_names = set()
    command_handlers = set()
    for definition in commands:
        command_id = definition.get("command_id", "")
        name = definition.get("name", "")
        handler = definition.get("handler", "")
        try:
            normalized = normalize_command_name(name, require_normalized=True)
            validate_command_permission(definition.get("permission"))
            validate_argument_mode(definition.get("argument_mode"))
            validate_command_priority(definition.get("priority"))
        except ValueError as exc:
            _fail("INVALID_COMMANDS", str(exc))
        if (
            not COMMAND_IDENTIFIER.fullmatch(command_id)
            or command_id in command_ids
            or normalized in command_names
            or not isinstance(handler, str)
            or not handler.isidentifier()
            or keyword.iskeyword(handler)
            or handler in command_handlers
        ):
            _fail("INVALID_COMMANDS", "Command identities, names, and handlers must be unique")
        command_ids.add(command_id)
        command_names.add(normalized)
        command_handlers.add(handler)
        _schema(files, definition.get("input_schema"))
    message_handler_ids = set()
    message_handler_methods = set()
    for definition in message_handlers:
        handler_id = definition.get("handler_id", "")
        handler = definition.get("handler", "")
        priority = definition.get("priority")
        if (
            not IDENTIFIER.fullmatch(handler_id)
            or handler_id in message_handler_ids
            or not isinstance(handler, str)
            or not handler.isidentifier()
            or keyword.iskeyword(handler)
            or handler in message_handler_methods
            or type(priority) is not int
            or not -100 <= priority <= 100
        ):
            _fail(
                "INVALID_MESSAGE_HANDLERS",
                "Message handler identities, methods, and priorities must be valid and unique",
            )
        message_handler_ids.add(handler_id)
        message_handler_methods.add(handler)
    capabilities = component.get("capabilities", {}).get("requests", [])
    allowed_capabilities = (
        {"state.read", "state.write", "payload.read", "payload.write"}
        | {
            "service.call:" + name
            for name in dependency_names | {item["name"] for item in effect_services}
        }
        | {"domain.query:" + item["name"] for item in domain_queries}
        | {"domain.command:" + item["name"] for item in domain_commands}
        | {"uow.execute:" + item["name"] for item in unit_of_work_plans}
        | {"event.publish:" + topic for topic in published_topics}
        | {"job.schedule:" + handler_id for handler_id in job_ids}
    )
    if any(item not in allowed_capabilities for item in capabilities) or len(capabilities) != len(
        set(capabilities)
    ):
        _fail(
            "UNSUPPORTED_CAPABILITY",
            "Capabilities must name an explicitly declared payload, state, service, event, or job operation",
        )
    official_bot_grants = component.get("capabilities", {}).get("official_bot_grants", ())
    granted_names = [item["capability"] for item in official_bot_grants]
    if (
        (
            "official_bot_grants" in component.get("capabilities", {})
            and component["scope"] != "bot"
        )
        or any(IDENTIFIER.fullmatch(name) is None for name in granted_names)
        or len(granted_names) != len(set(granted_names))
    ):
        _fail(
            "INVALID_OFFICIAL_BOT_GRANTS",
            "Official Bot grants require Bot scope and unique capability names",
        )
    for relative, source in files.items():
        if relative.endswith(".py"):
            try:
                compile(source, relative, "exec", dont_inherit=True)
            except (SyntaxError, ValueError) as exc:
                raise ForestError(
                    "PACKAGE_SYNTAX_ERROR",
                    f"Invalid Python in {relative}",
                    422,
                    details={
                        "file": relative,
                        "line": getattr(exc, "lineno", None),
                        "column": getattr(exc, "offset", None),
                        "phase": "syntax",
                    },
                ) from exc
    if "dependency_lock" in manifest:
        from forest.environments import validate_lock

        validate_lock(files, manifest["dependency_lock"])
    elif any(path.endswith(".whl") for path in files):
        _fail("INVALID_DEPENDENCY_LOCK", "Vendored wheels require an explicit dependency lock")
    return manifest


@dataclass(frozen=True)
class PackageSnapshot:
    root: Path
    digest: str
    manifest: dict

    @property
    def module_root(self) -> Path:
        return self.root / self.manifest.get("module_root", "python")

    @property
    def entrypoint(self) -> str:
        return self.manifest["entrypoint"]

    @property
    def component(self) -> dict:
        return self.manifest["components"][0]

    def schema(self, reference: str) -> dict:
        return _schema(self._verified_files(), reference)

    def schemas(self, references) -> dict[str, dict]:
        """Load a set of schemas from one verified immutable package capture."""

        files = self._verified_files()
        return {
            reference: _schema(files, reference)
            for reference in dict.fromkeys(references)
        }

    def bootstrap_configuration(self) -> object:
        """Read the bootstrap seed from this exact captured artifact."""

        reference = self.component.get("bootstrap_config")
        if (
            not isinstance(reference, str)
            or not re.fullmatch(r"[a-zA-Z0-9_./-]+\.json", reference)
            or PurePosixPath(reference).is_absolute()
            or ".." in PurePosixPath(reference).parts
        ):
            _fail("PACKAGE_CONFIGURATION_INVALID", "Bootstrap configuration path is invalid")
        source = self._verified_files().get(reference)
        if source is None:
            _fail("PACKAGE_CONFIGURATION_INVALID", "Bootstrap configuration is absent")
        try:
            return json.loads(source)
        except (UnicodeError, ValueError) as exc:
            raise ForestError(
                "PACKAGE_CONFIGURATION_INVALID", "Bootstrap configuration is not JSON", 422
            ) from exc

    def bootstrap_contribution(self) -> tuple[dict, dict, dict] | None:
        """Read a creation-time contribution from this captured artifact."""

        declaration = self.component.get("bootstrap_contribution")
        if declaration is None:
            return None
        files = self._verified_files()
        schema = _schema(files, declaration["schema"])
        initial_value = json.loads(files[declaration["initial_value"]])
        return declaration.copy(), schema, initial_value

    def bootstrap_materialization(self) -> dict | None:
        """Read a validated creation-to-runtime template from the exact artifact."""

        return _bootstrap_materialization(self._verified_files(), self.component)

    def bootstrap_ui(self) -> tuple[bytes, str] | None:
        """Return exact packaged setup HTML and its content hash."""

        result = _bootstrap_ui(self._verified_files(), self.component)
        if result is None:
            return None
        _reference, content = result
        return content, hashlib.sha256(content).hexdigest()

    def management_ui(self) -> tuple[dict, bytes] | None:
        files = self._verified_files()
        metadata = _management_ui(files, self.component)
        if metadata is None:
            return None
        return metadata, files[metadata["entrypoint"]]

    def storage_bundle(self) -> tuple[dict, bytes] | None:
        files = self._verified_files()
        metadata = _storage_bundle(files, self.component)
        if metadata is None:
            return None
        return metadata, files[metadata["baseline"]]

    def _verified_files(self) -> dict[str, bytes]:
        files = _capture(self.root)
        actual = digest_json(
            {name: hashlib.sha256(value).hexdigest() for name, value in files.items()}
        )
        if actual != self.digest or _manifest(files) != self.manifest:
            _fail("PACKAGE_TAMPERED", "The immutable package no longer matches its captured digest")
        return files

    def verify(self) -> None:
        self._verified_files()


class PackageCatalog:
    def __init__(self, root: Path) -> None:
        self.root = private_directory(root)

    def _source_files(self, source: Path) -> tuple[dict[str, bytes], dict[str, bytes]]:
        source, source_files, captured, _ = _validated_source(source)
        catalog_root = self.root.resolve()
        if catalog_root == source or catalog_root.is_relative_to(source):
            _fail("INVALID_PACKAGE_PATH", "Artifact catalog cannot be inside the source package")
        return source_files, captured

    def build(self, source: Path, *, development: bool = False) -> PackageSnapshot:
        source_files, captured = self._source_files(source)
        if development:
            source_digest = digest_json(
                {name: hashlib.sha256(value).hexdigest() for name, value in source_files.items()}
            ).removeprefix("sha256:")
            # The manifest has already passed TOML/schema validation. Replace the
            # top-level version only; no source file is changed by a dev build.
            lines = captured["module.toml"].decode("utf-8").splitlines(keepends=True)
            replaced = False
            for index, line in enumerate(lines):
                if line.lstrip().startswith("["):
                    break
                if re.match(r"\s*version\s*=", line):
                    base = _manifest(captured)["version"].split("+", 1)[0]
                    lines[index] = f'version = "{base}+dev.{source_digest}"\n'
                    replaced = True
                    break
            if not replaced:
                _fail("INVALID_MANIFEST", "Development builds require a top-level version")
            captured["module.toml"] = "".join(lines).encode("utf-8")
            if "checksums.json" in captured:
                captured = with_checksums(captured)
        return self._publish(captured)

    def build_official(self, source: Path) -> PackageSnapshot:
        """Register a host-supplied official deployment source as a trusted artifact.

        This authority is deliberately separate from ZIP import and ordinary build:
        an artifact's own manifest cannot grant itself official SQL execution.
        """

        _source_files, captured = self._source_files(source)
        component = _manifest(captured)["components"][0]
        storage = component.get("storage")
        if (
            component["scope"] != "deployment"
            or (storage is not None and storage.get("execution_profile") != "official")
        ):
            _fail(
                "OFFICIAL_PACKAGE_SOURCE_INVALID",
                "Official deployment source must declare deployment scope and trusted storage",
            )
        snapshot = self._publish(captured)
        with closing(sqlite3.connect(self.root / "catalog.sqlite3", timeout=10)) as db, db:
            db.execute("PRAGMA synchronous=FULL")
            db.execute(
                "CREATE TABLE IF NOT EXISTS official_deployment_artifacts "
                "(digest TEXT PRIMARY KEY, component_id TEXT NOT NULL)"
            )
            db.execute(
                "INSERT OR IGNORE INTO official_deployment_artifacts VALUES (?,?)",
                (snapshot.digest, component["id"]),
            )
        return snapshot

    def get_official_deployment(self, digest: str) -> PackageSnapshot:
        """Open only artifacts admitted by the trusted host source path."""

        snapshot = self.get(digest)
        rows = self._rows(
            "SELECT component_id FROM official_deployment_artifacts WHERE digest=?",
            (digest,),
            table="official_deployment_artifacts",
        )
        if len(rows) != 1 or rows[0][0] != snapshot.component["id"]:
            _fail(
                "OFFICIAL_PACKAGE_NOT_REGISTERED",
                "Deployment artifact has no host-issued official provenance",
            )
        return snapshot

    def is_official_deployment(self, digest: str) -> bool:
        snapshot = self.get(digest)
        rows = self._rows(
            "SELECT component_id FROM official_deployment_artifacts WHERE digest=?",
            (digest,), table="official_deployment_artifacts",
        )
        return len(rows) == 1 and rows[0][0] == snapshot.component["id"]

    def import_archive(
        self,
        data: bytes,
        *,
        check_manifest: Callable[[dict], None] | None = None,
        bind_version: bool = True,
    ) -> PackageSnapshot:
        """Validate a complete ZIP before publishing any package file."""
        if not isinstance(data, bytes) or len(data) > MAX_ARCHIVE_BYTES:
            _fail("PACKAGE_TOO_LARGE", "Package ZIP exceeds the upload limit")
        captured = archive_files(data)
        manifest = _manifest(captured)
        if check_manifest is not None:
            # Deployment namespace policy must reject before the local artifact
            # registry reserves an immutable package_id/version pair.
            check_manifest(manifest)
        return self._publish(captured, bind_version=bind_version)

    def import_archive_idempotent(
        self,
        data: bytes,
        *,
        operation_id: str,
        actor_ref: str,
        archive_sha256: str,
        check_manifest: Callable[[dict], None] | None = None,
    ) -> PackageSnapshot:
        """Publish an archive and its replay receipt in one catalog transaction."""
        self._check_import_identity(
            operation_id=operation_id, actor_ref=actor_ref,
            archive_sha256=archive_sha256,
        )
        with closing(sqlite3.connect(self.root / "catalog.sqlite3", timeout=30)) as db, db:
            self._prepare_catalog_tables(db, imports=True)
            db.execute("BEGIN IMMEDIATE")
            prior = db.execute(
                "SELECT actor_ref,archive_sha256,digest FROM package_imports WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if prior is not None:
                if prior[:2] != (actor_ref, archive_sha256):
                    _fail("PACKAGE_IMPORT_OPERATION_CONFLICT", "Import key was used for another request")
                if not isinstance(data, bytes) or not 1 <= len(data) <= MAX_ARCHIVE_BYTES:
                    _fail("PACKAGE_TOO_LARGE", "Package ZIP exceeds the upload limit")
                if (
                    "sha256:" + hashlib.sha256(data).hexdigest() != archive_sha256
                ):
                    _fail("PACKAGE_ARCHIVE_DIGEST_MISMATCH", "Package ZIP differs from its declared digest")
                return self.get(prior[2])
            closed = db.execute(
                "SELECT actor_ref,archive_sha256 FROM package_import_closures WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if closed is not None:
                if closed != (actor_ref, archive_sha256):
                    _fail("PACKAGE_IMPORT_OPERATION_CONFLICT", "Import key was used for another request")
                _fail("PACKAGE_IMPORT_CLOSED", "Import operation was closed before publication")
            if not isinstance(data, bytes) or not 1 <= len(data) <= MAX_ARCHIVE_BYTES:
                _fail("PACKAGE_TOO_LARGE", "Package ZIP exceeds the upload limit")
            if "sha256:" + hashlib.sha256(data).hexdigest() != archive_sha256:
                _fail("PACKAGE_ARCHIVE_DIGEST_MISMATCH", "Package ZIP differs from its declared digest")
            captured = archive_files(data)
            manifest = _manifest(captured)
            if check_manifest is not None:
                check_manifest(manifest)
            return self._publish(
                captured, import_operation=(operation_id, actor_ref, archive_sha256),
                locked_db=db,
            )

    def close_import_if_absent(
        self, *, operation_id: str, actor_ref: str, archive_sha256: str
    ) -> dict:
        """Linearize terminal absence against a concurrent or late ZIP publication."""
        self._check_import_identity(
            operation_id=operation_id, actor_ref=actor_ref,
            archive_sha256=archive_sha256,
        )
        with closing(sqlite3.connect(self.root / "catalog.sqlite3", timeout=30)) as db, db:
            self._prepare_catalog_tables(db, imports=True)
            db.execute("BEGIN IMMEDIATE")
            prior = db.execute(
                "SELECT actor_ref,archive_sha256,digest FROM package_imports WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if prior is not None:
                if prior[:2] != (actor_ref, archive_sha256):
                    _fail("PACKAGE_IMPORT_OPERATION_CONFLICT", "Import key was used for another request")
                self.get(prior[2]).verify()
                return {"found": True, "package_digest": prior[2], "closed": False}
            closed = db.execute(
                "SELECT actor_ref,archive_sha256 FROM package_import_closures WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if closed is not None:
                if closed != (actor_ref, archive_sha256):
                    _fail("PACKAGE_IMPORT_OPERATION_CONFLICT", "Import key was used for another request")
            else:
                db.execute(
                    "INSERT INTO package_import_closures VALUES (?,?,?)",
                    (operation_id, actor_ref, archive_sha256),
                )
            return {"found": False, "package_digest": None, "closed": True}

    def import_receipt(
        self, *, operation_id: str, actor_ref: str, archive_sha256: str
    ) -> dict | None:
        """Recover a committed import without trusting an HTTP response or ZIP staging."""
        digest = self._import_digest(
            operation_id=operation_id, actor_ref=actor_ref,
            archive_sha256=archive_sha256,
        )
        return self.artifact(digest) if digest is not None else None

    def _import_digest(
        self, *, operation_id: str, actor_ref: str, archive_sha256: str
    ) -> str | None:
        self._check_import_identity(
            operation_id=operation_id, actor_ref=actor_ref,
            archive_sha256=archive_sha256,
        )
        prior = self._rows(
            "SELECT actor_ref,archive_sha256,digest FROM package_imports WHERE operation_id=?",
            (operation_id,), table="package_imports",
        )
        if prior:
            if prior[0][:2] != (actor_ref, archive_sha256):
                _fail("PACKAGE_IMPORT_OPERATION_CONFLICT", "Import key was used for another request")
            return prior[0][2]
        return None

    @staticmethod
    def _check_import_identity(
        *, operation_id: str, actor_ref: str, archive_sha256: str
    ) -> None:
        if (
            not isinstance(operation_id, str)
            or re.fullmatch(r"[A-Za-z0-9:._/-]{1,256}", operation_id) is None
            or not isinstance(actor_ref, str)
            or not 1 <= len(actor_ref) <= 128
            or not isinstance(archive_sha256, str)
            or re.fullmatch(r"sha256:[a-f0-9]{64}", archive_sha256) is None
        ):
            _fail("PACKAGE_IMPORT_OPERATION_INVALID", "Import operation identity is invalid")

    def export_archive(self, digest: str) -> bytes:
        files = self.get(digest)._verified_files()
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, content in sorted(files.items()):
                entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                entry.compress_type = zipfile.ZIP_DEFLATED
                entry.create_system = 3
                entry.external_attr = (stat.S_IFREG | 0o400) << 16
                archive.writestr(entry, content)
        result = output.getvalue()
        if len(result) > MAX_ARCHIVE_BYTES:
            _fail("PACKAGE_TOO_LARGE", "Exported ZIP exceeds the archive limit")
        return result

    def get(self, digest: str) -> PackageSnapshot:
        if not isinstance(digest, str) or not re.fullmatch(r"sha256:[a-f0-9]{64}", digest):
            _fail("INVALID_PACKAGE_DIGEST", "Package digest must be SHA-256")
        rows = self._rows("SELECT digest FROM packages WHERE digest=?", (digest,))
        if not rows:
            rows = self._rows(
                "SELECT digest FROM artifacts WHERE digest=?", (digest,), table="artifacts"
            )
        if not rows:
            raise ForestError("PACKAGE_NOT_FOUND", "Package artifact is not registered", 404)
        root = self.root / digest.removeprefix("sha256:")
        files = _capture(root)
        snapshot = PackageSnapshot(root, digest, _manifest(files))
        snapshot.verify()
        return snapshot

    def find(self, package_id: str, version: str) -> PackageSnapshot:
        rows = self._rows(
            "SELECT digest FROM packages WHERE package_id=? AND version=?", (package_id, version)
        )
        if not rows:
            raise ForestError("PACKAGE_NOT_FOUND", "Package version is not registered", 404)
        return self.get(rows[0][0])

    def list(self) -> list[PackageSnapshot]:
        return [
            self.get(row[0])
            for row in self._rows(
                "SELECT DISTINCT digest FROM packages ORDER BY package_id,version", ()
            )
        ]

    @staticmethod
    def _item(snapshot: PackageSnapshot) -> dict:
        component = snapshot.component
        return {
            "package_id": snapshot.manifest["package_id"],
            "package_version": snapshot.manifest["version"],
            "package_digest": snapshot.digest,
            "manifest_digest": digest_json(snapshot.manifest),
            "component_id": component["id"],
            "scope": component["scope"],
            "runtime": snapshot.manifest["runtime"],
            "kernel_api": snapshot.manifest["kernel_api"],
            "requested_capabilities": list(
                component.get("capabilities", {}).get("requests", ())
            ),
        }

    def artifact(self, package_digest: str) -> dict:
        """Project one verified immutable artifact for component selection."""
        snapshot = self.get(package_digest)
        component = snapshot.component
        storage = component.get("storage")
        return {
            "protocol": "forest.package-catalog-artifact/1",
            **self._item(snapshot),
            "configuration_schema_digest": digest_json(
                snapshot.schema(component["config_schema"])
            ),
            "requires": [
                {
                    "name": item["name"],
                    "contract": item["contract"],
                    "required": item.get("required", True),
                    "startup_dependency": item.get("startup_dependency", False),
                }
                for item in component.get("requires", ())
            ],
            "provides": [
                {"name": item["name"], "contract": item["contract"]}
                for item in component.get("provides", ())
            ],
            "storage_execution_profile": (
                storage.get("execution_profile", "declarative")
                if storage is not None else None
            ),
        }

    def page(
        self,
        *,
        after_package_id: str = "",
        after_version: str = "",
        limit: int = 32,
        catalog_revision: str | None = None,
        scope: str | None = None,
    ) -> dict:
        """Project verified installable artifacts without exposing files or paths."""
        if (
            not isinstance(after_package_id, str)
            or not isinstance(after_version, str)
            or (not after_package_id and after_version)
            or (after_package_id and (not after_version or catalog_revision is None))
            or (after_package_id and not re.fullmatch(
                r"[a-z0-9][a-z0-9._-]{0,127}", after_package_id
            ))
            or (after_version and not re.fullmatch(
                r"[0-9A-Za-z][0-9A-Za-z.+-]{0,127}", after_version
            ))
            or type(limit) is not int
            or not 1 <= limit <= 64
            or (catalog_revision is not None and not isinstance(catalog_revision, str))
            or (scope is not None and (
                not isinstance(scope, str)
                or scope not in {"bot", "deployment", "host"}
            ))
        ):
            _fail("PACKAGE_CATALOG_PAGE_INVALID", "Package catalog page input is invalid")
        rows = self._rows(
            "SELECT package_id,version,digest FROM packages ORDER BY package_id,version", ()
        )
        if scope is not None:
            scoped_rows = []
            for package_id, package_version, digest in rows:
                snapshot = self.get(digest)
                if (snapshot.manifest["package_id"], snapshot.manifest["version"]) != (
                    package_id, package_version
                ):
                    _fail("PACKAGE_TAMPERED", "Catalog index differs from its immutable package")
                if snapshot.component["scope"] == scope:
                    scoped_rows.append((package_id, package_version, digest))
            rows = scoped_rows
        revision = digest_json({
            "protocol": "forest.package-catalog-revision/1",
            "scope": scope,
            "entries": rows,
        })
        if catalog_revision is not None and catalog_revision != revision:
            raise ForestError(
                "PACKAGE_CATALOG_CHANGED", "Package catalog changed during pagination", 409
            )
        following = [
            row for row in rows if (row[0], row[1]) > (after_package_id, after_version)
        ]
        page = following[:limit]
        items = []
        for package_id, package_version, digest in page:
            snapshot = self.get(digest)
            if (snapshot.manifest["package_id"], snapshot.manifest["version"]) != (
                package_id, package_version
            ):
                _fail("PACKAGE_TAMPERED", "Catalog index differs from its immutable package")
            items.append(self._item(snapshot))
        return {
            "protocol": "forest.package-catalog-page/1",
            "catalog_revision": revision,
            "items": items,
            "next_after": (
                {"package_id": page[-1][0], "version": page[-1][1]}
                if len(following) > limit else None
            ),
        }

    def _rows(self, query: str, parameters: tuple, *, table: str = "packages") -> list[tuple]:
        path = self.root / "catalog.sqlite3"
        if not path.exists():
            return []
        with closing(sqlite3.connect(path, timeout=10)) as db:
            if not db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone():
                return []
            return db.execute(query, parameters).fetchall()

    def _publish(
        self, captured: dict[str, bytes], *, bind_version: bool = True,
        import_operation: tuple[str, str, str] | None = None,
        locked_db: sqlite3.Connection | None = None,
    ) -> PackageSnapshot:
        if locked_db is None:
            with closing(sqlite3.connect(self.root / "catalog.sqlite3", timeout=30)) as db, db:
                self._prepare_catalog_tables(db, imports=import_operation is not None)
                db.execute("BEGIN IMMEDIATE")
                return self._publish(
                    captured, bind_version=bind_version,
                    import_operation=import_operation, locked_db=db,
                )
        manifest = _manifest(captured)
        digest = digest_json(
            {name: hashlib.sha256(value).hexdigest() for name, value in captured.items()}
        )
        destination = self.root / digest.removeprefix("sha256:")
        if import_operation is not None:
            prior = locked_db.execute(
                "SELECT actor_ref,archive_sha256,digest FROM package_imports "
                "WHERE operation_id=?", (import_operation[0],),
            ).fetchone()
            if prior is not None:
                if prior[:2] != import_operation[1:]:
                    _fail(
                        "PACKAGE_IMPORT_OPERATION_CONFLICT",
                        "Import key was used for another request",
                    )
                return self.get(prior[2])
            closed = locked_db.execute(
                "SELECT actor_ref,archive_sha256 FROM package_import_closures "
                "WHERE operation_id=?", (import_operation[0],),
            ).fetchone()
            if closed is not None:
                if closed != import_operation[1:]:
                    _fail(
                        "PACKAGE_IMPORT_OPERATION_CONFLICT",
                        "Import key was used for another request",
                    )
                _fail("PACKAGE_IMPORT_CLOSED", "Import operation was closed before publication")
        row = locked_db.execute(
            "SELECT digest FROM packages WHERE package_id=? AND version=?",
            (manifest["package_id"], manifest["version"]),
        ).fetchone()
        if bind_version and row is not None and row[0] != digest:
            _fail(
                "PACKAGE_VERSION_CONFLICT", "A package version cannot silently change content"
            )
        if not destination.exists():
            staging = self.root / (".capture-" + uuid4().hex)
            staging.mkdir(mode=0o700)
            try:
                for name, data in captured.items():
                    target = staging / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with target.open("xb") as stream:
                        stream.write(data)
                        stream.flush()
                        os.fsync(stream.fileno())
                    if sys.platform != "win32":
                        target.chmod(0o400)
                staging.rename(destination)
            except OSError as exc:
                raise ForestError(
                    "PACKAGE_STORAGE_ERROR", "Unable to publish package artifact", 500
                ) from exc
            finally:
                # Only this build's still-unpublished directory may be removed. Check the
                # resolved parent and reparse status before any recursive Windows cleanup.
                if staging.exists():
                    info = staging.lstat()
                    if (
                        not stat.S_ISLNK(info.st_mode)
                        and not getattr(info, "st_reparse_tag", 0)
                        and staging.resolve().parent == self.root.resolve()
                    ):
                        shutil.rmtree(staging)
        snapshot = PackageSnapshot(destination, digest, manifest)
        snapshot.verify()
        locked_db.execute("INSERT OR IGNORE INTO artifacts VALUES (?)", (digest,))
        if bind_version:
            locked_db.execute(
                "INSERT OR IGNORE INTO packages VALUES (?,?,?)",
                (manifest["package_id"], manifest["version"], digest),
            )
        if import_operation is not None:
            locked_db.execute(
                "INSERT INTO package_imports VALUES (?,?,?,?)",
                (*import_operation, digest),
            )
        return snapshot

    @staticmethod
    def _prepare_catalog_tables(db: sqlite3.Connection, *, imports: bool) -> None:
        db.execute("PRAGMA synchronous=FULL")
        db.execute(
            "CREATE TABLE IF NOT EXISTS packages (package_id TEXT, version TEXT, "
            "digest TEXT NOT NULL, PRIMARY KEY(package_id,version))"
        )
        db.execute("CREATE TABLE IF NOT EXISTS artifacts (digest TEXT PRIMARY KEY)")
        if imports:
            db.execute(
                "CREATE TABLE IF NOT EXISTS package_imports ("
                "operation_id TEXT PRIMARY KEY, actor_ref TEXT NOT NULL, "
                "archive_sha256 TEXT NOT NULL, digest TEXT NOT NULL)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS package_import_closures ("
                "operation_id TEXT PRIMARY KEY, actor_ref TEXT NOT NULL, "
                "archive_sha256 TEXT NOT NULL)"
            )


def archive_files(
    data: bytes,
    *,
    max_files: int = MAX_PACKAGE_FILES,
    max_bytes: int = MAX_PACKAGE_BYTES,
    max_file_bytes: int = MAX_FILE_BYTES,
    allow_wheels: bool = True,
) -> dict[str, bytes]:
    """Bounded portable ZIP decoder, shared by package and wheel admission.

    Never uses extract()/extractall(). Paths are validated before inflation;
    links, ambiguous paths, duplicate members and unsupported compression fail.
    """
    files: dict[str, bytes] = {}
    seen: set[str] = set()
    kinds: dict[str, bool] = {}
    spellings: dict[str, str] = {}
    total = 0
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            entries = archive.infolist()
            if len(entries) > MAX_PACKAGE_ENTRIES:
                _fail("PACKAGE_TOO_LARGE", "ZIP exceeds entry limits")
            for entry in entries:
                name = entry.filename
                directory = entry.is_dir()
                relative = name[:-1] if directory else name
                parts = relative.split("/")
                if (
                    not relative
                    or len(name) > 1024
                    or entry.orig_filename != name
                    or len(parts) > MAX_PACKAGE_DEPTH
                ):
                    _fail("PACKAGE_FILE_REJECTED", "ZIP contains an invalid path")
                for part in parts:
                    stem = part.split(".", 1)[0].upper()
                    if (
                        not part
                        or part in {".", ".."}
                        or part.startswith(".env")
                        or re.search(r'[<>:"\\|?*\x00-\x1f]', part)
                        or part.endswith((".", " "))
                        or stem in {"CON", "PRN", "AUX", "NUL"}
                        or re.fullmatch(r"(?:COM|LPT)[1-9]", stem)
                    ):
                        _fail("PACKAGE_FILE_REJECTED", "ZIP contains a nonportable/private path")
                if any(part in SKIPPED for part in (parts if directory else parts[:-1])):
                    _fail("PACKAGE_FILE_REJECTED", "ZIP contains excluded source/cache directories")
                key = relative.casefold()
                if key in seen:
                    _fail("PACKAGE_PATH_COLLISION", "ZIP contains duplicate or colliding paths")
                seen.add(key)
                # Implicit parent directories also participate in collision checks.
                for length in range(1, len(parts)):
                    original_parent = "/".join(parts[:length])
                    parent = original_parent.casefold()
                    if parent in spellings and spellings[parent] != original_parent:
                        _fail("PACKAGE_PATH_COLLISION", "ZIP has case-colliding directory names")
                    spellings[parent] = original_parent
                    if parent in kinds and not kinds[parent]:
                        _fail("PACKAGE_PATH_COLLISION", "ZIP file shadows a directory")
                    kinds[parent] = True
                if key in kinds and kinds[key] != directory:
                    _fail("PACKAGE_PATH_COLLISION", "ZIP file shadows a directory")
                kinds[key] = directory
                if key in spellings and spellings[key] != relative:
                    _fail("PACKAGE_PATH_COLLISION", "ZIP has case-colliding directory names")
                spellings[key] = relative
                mode = (entry.external_attr >> 16) & 0xFFFF
                file_type = stat.S_IFMT(mode)
                if file_type not in ({0, stat.S_IFDIR} if directory else {0, stat.S_IFREG}):
                    _fail("PACKAGE_LINK_REJECTED", "ZIP links and special files are rejected")
                if entry.flag_bits & 1 or entry.compress_type not in {
                    zipfile.ZIP_STORED,
                    zipfile.ZIP_DEFLATED,
                }:
                    _fail("INVALID_PACKAGE_ARCHIVE", "Unsupported ZIP encryption/compression")
                if directory:
                    if entry.file_size:
                        _fail("INVALID_PACKAGE_ARCHIVE", "ZIP directory contains file content")
                    continue
                limit = (
                    MAX_WHEEL_BYTES if allow_wheels and name.endswith(".whl") else max_file_bytes
                )
                if len(files) >= max_files or entry.file_size > limit:
                    _fail("PACKAGE_TOO_LARGE", "ZIP exceeds file limits")
                total += entry.file_size
                if total > max_bytes:
                    _fail("PACKAGE_TOO_LARGE", "ZIP exceeds expanded byte limit")
                with archive.open(entry) as stream:
                    value = stream.read(limit + 1)
                if len(value) != entry.file_size or len(value) > limit:
                    _fail("INVALID_PACKAGE_ARCHIVE", "ZIP entry size does not match content")
                files[name] = value
        return files
    except (zipfile.BadZipFile, NotImplementedError, OSError, ValueError, EOFError) as exc:
        raise ForestError("INVALID_PACKAGE_ARCHIVE", "Invalid complete package ZIP", 422) from exc
