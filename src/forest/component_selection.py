"""Host-side read fence for the Worker Host's Bot-scoped selection snapshot."""

from __future__ import annotations

import re
from contextlib import asynccontextmanager
from dataclasses import dataclass

from psycopg import sql
from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.packages import PackageCatalog, PackageSnapshot, digest_json
from forest.runtime_modules import RuntimeModuleDescriptor
from forest.schema_bundles import module_schema_namespace


@dataclass(frozen=True)
class ComponentSelectionEntry:
    component_id: str
    source_kind: str
    package_digest: str | None
    manifest_digest: str | None
    approved_capabilities: tuple[str, ...]
    desired_enabled: bool
    desired_revision: int

    @classmethod
    def from_dict(cls, value: object) -> ComponentSelectionEntry:
        if (
            not isinstance(value, dict)
            or set(value) != {
                "component_id", "source_kind", "package_digest", "manifest_digest",
                "approved_capabilities", "desired_enabled", "desired_revision",
            }
            or not isinstance(value["component_id"], str)
            or not isinstance(value["source_kind"], str)
            or value["source_kind"] not in {"builtin", "installed"}
            or not isinstance(value["approved_capabilities"], list)
            or any(not isinstance(item, str) for item in value["approved_capabilities"])
            or type(value["desired_enabled"]) is not bool
            or type(value["desired_revision"]) is not int
            or value["desired_revision"] < 1
        ):
            raise ForestError(
                "COMPONENT_SELECTION_SNAPSHOT_INVALID",
                "Worker Host selection entry is invalid",
                503,
            )
        if value["source_kind"] == "builtin":
            valid_source = (
                value["package_digest"] is None
                and value["manifest_digest"] is None
                and not value["approved_capabilities"]
            )
        else:
            valid_source = (
                isinstance(value["package_digest"], str)
                and isinstance(value["manifest_digest"], str)
            )
        if not valid_source:
            raise ForestError(
                "COMPONENT_SELECTION_SNAPSHOT_INVALID",
                "Worker Host selection source is invalid",
                503,
            )
        return cls(
            value["component_id"], value["source_kind"], value["package_digest"],
            value["manifest_digest"], tuple(value["approved_capabilities"]),
            value["desired_enabled"], value["desired_revision"],
        )

    def as_dict(self) -> dict:
        return {
            "component_id": self.component_id,
            "source_kind": self.source_kind,
            "package_digest": self.package_digest,
            "manifest_digest": self.manifest_digest,
            "approved_capabilities": list(self.approved_capabilities),
            "desired_enabled": self.desired_enabled,
            "desired_revision": self.desired_revision,
        }


@dataclass(frozen=True)
class ComponentSelectionSnapshot:
    revision: int
    entries: tuple[ComponentSelectionEntry, ...]
    digest: str

    def __post_init__(self) -> None:
        if (
            type(self.revision) is not int
            or self.revision < 0
            or not isinstance(self.entries, tuple)
            or len(self.entries) > 128
            or any(not isinstance(item, ComponentSelectionEntry) for item in self.entries)
            or tuple(item.component_id for item in self.entries) != tuple(
                sorted({item.component_id for item in self.entries})
            )
            or any(item.desired_revision > self.revision for item in self.entries)
        ):
            raise ForestError(
                "COMPONENT_SELECTION_SNAPSHOT_INVALID",
                "Worker Host selection snapshot is invalid",
                503,
            )
        for item in self.entries:
            if ComponentSelectionEntry.from_dict(item.as_dict()) != item:
                raise ForestError(
                    "COMPONENT_SELECTION_SNAPSHOT_INVALID",
                    "Worker Host selection snapshot is invalid",
                    503,
                )
        frozen = {
            "protocol": "forest.component-selection-snapshot/1",
            "revision": self.revision,
            "entries": [item.as_dict() for item in self.entries],
        }
        if digest_json(frozen) != self.digest:
            raise ForestError(
                "COMPONENT_SELECTION_SNAPSHOT_INVALID",
                "Worker Host selection digest is invalid",
                503,
            )


@dataclass(frozen=True)
class SelectedPackageMembers:
    official: tuple[RuntimeModuleDescriptor, ...]
    installed: tuple[ComponentSelectionEntry, ...]


def select_package_members(
    selection: ComponentSelectionSnapshot,
    official_descriptors: tuple[RuntimeModuleDescriptor, ...],
    catalog: PackageCatalog,
) -> SelectedPackageMembers:
    """Read one frozen membership before either preview or provisioning."""

    official_ids = {item.component_id for item in official_descriptors}
    disabled: set[str] = set()
    selected_installed = []
    for entry in selection.entries:
        if entry.component_id.startswith("kernel."):
            raise ForestError(
                "COMPONENT_SELECTION_INVALID",
                "Kernel authorities cannot be selected as packages",
                409,
            )
        if entry.source_kind == "builtin":
            if entry.component_id not in official_ids:
                raise ForestError(
                    "COMPONENT_SELECTION_INVALID",
                    "Builtin component selection is invalid",
                    409,
                )
            if not entry.desired_enabled:
                disabled.add(entry.component_id)
            continue
        if entry.component_id in official_ids:
            raise ForestError(
                "COMPONENT_SELECTION_INVALID",
                "Installed package cannot replace an official component identity",
                409,
            )
        snapshot = catalog.get(entry.package_digest)
        if (
            snapshot.component["id"] != entry.component_id
            or digest_json(snapshot.manifest) != entry.manifest_digest
        ):
            raise ForestError(
                "COMPONENT_SELECTION_ARTIFACT_CHANGED",
                "Selected package declaration differs from its pinned artifact",
                409,
            )
        if entry.desired_enabled:
            selected_installed.append(entry)
    return SelectedPackageMembers(
        tuple(item for item in official_descriptors if item.component_id not in disabled),
        tuple(selected_installed),
    )


class ComponentSelectionReader:
    """Read the package-owned projection without a Worker process at cold start."""

    def __init__(self, store, snapshot: PackageSnapshot) -> None:
        snapshot.verify()
        self.store = store
        component = snapshot.component
        storage = component.get("storage")
        queries = {
            item["name"]: item["function"]
            for item in component.get("domain_queries", ())
        }
        required = (
            "component.selection.snapshot",
            "component.selection.project",
            "component.selection.operation-probe",
        )
        if (
            component.get("scope") != "bot"
            or not isinstance(storage, dict)
            or storage.get("scope") != "bot"
            or not all(name in queries for name in required)
        ):
            raise ForestError(
                "COMPONENT_SELECTION_BINDING_INVALID",
                "Selection authority package has no complete Bot-scoped read binding",
                503,
            )
        self.namespace = module_schema_namespace(storage["schema_id"])
        self.functions = queries

    async def exclusive_fence_in(self, conn) -> None:
        """Serialize a caller-owned selection write transaction with publication."""

        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            ("forest.component.selection/" + self.store.bot_id,),
        )

    @asynccontextmanager
    async def publication_fence(
        self, *, expected_revision: int | None = None, expected_digest: str | None = None
    ):
        """Hold a shared Bot selection lock through Graph publication."""
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock_shared(hashtextextended(%s,0))",
                ("forest.component.selection/" + self.store.bot_id,),
            )
            snapshot = await self.snapshot_in(conn)
            if (
                expected_revision is not None and snapshot.revision != expected_revision
            ) or (expected_digest is not None and snapshot.digest != expected_digest):
                raise ForestError(
                    "COMPONENT_SELECTION_CHANGED",
                    "Worker Host selection changed before Graph publication",
                    409,
                )
            yield snapshot

    async def snapshot_in(self, conn) -> ComponentSelectionSnapshot:
        """Read one selection snapshot on a caller-owned advisory-fenced transaction."""

        result = await conn.execute(
            sql.SQL("SELECT {}(%s::jsonb,%s::jsonb) AS result").format(
                sql.Identifier(self.namespace, self.functions["component.selection.snapshot"])
            ),
            (Jsonb({"bot_id": self.store.bot_id}), Jsonb({})),
        )
        envelope = (await result.fetchone())["result"]
        return self._decode_snapshot(envelope)

    async def project_in(
        self,
        conn,
        *,
        component_id: str,
        source_kind: str,
        package_digest: str | None,
        manifest_digest: str | None,
        approved_capabilities: tuple[str, ...],
        desired_enabled: bool,
        expected_selection_revision: int,
    ) -> ComponentSelectionSnapshot:
        """Ask Worker Host SQL for a hypothetical selection on a fenced transaction."""

        result = await conn.execute(
            sql.SQL("SELECT {}(%s::jsonb,%s::jsonb) AS result").format(
                sql.Identifier(self.namespace, self.functions["component.selection.project"])
            ),
            (
                Jsonb({
                    "bot_id": self.store.bot_id,
                    "port_kind": "query",
                    "port_name": "component.selection.project",
                }),
                Jsonb({
                    "component_id": component_id,
                    "source_kind": source_kind,
                    "package_digest": package_digest,
                    "manifest_digest": manifest_digest,
                    "approved_capabilities": list(approved_capabilities),
                    "desired_enabled": desired_enabled,
                    "expected_selection_revision": expected_selection_revision,
                }),
            ),
        )
        return self._decode_snapshot((await result.fetchone())["result"], allow_error=True)

    async def operation_replay_in(
        self,
        conn,
        *,
        operation_id: str,
        input_digest: str,
        desired_enabled: bool,
    ) -> dict | None:
        """Probe package-owned replay facts under the caller's exclusive selection fence."""

        result = await conn.execute(
            sql.SQL("SELECT {}(%s::jsonb,%s::jsonb) AS result").format(
                sql.Identifier(
                    self.namespace, self.functions["component.selection.operation-probe"]
                )
            ),
            (
                Jsonb({
                    "bot_id": self.store.bot_id,
                    "port_kind": "query",
                    "port_name": "component.selection.operation-probe",
                }),
                Jsonb({
                    "operation_id": operation_id,
                    "input_digest": input_digest,
                    "desired_enabled": desired_enabled,
                }),
            ),
        )
        envelope = (await result.fetchone())["result"]
        if not isinstance(envelope, dict) or envelope.get("protocol") != "forest.domain-result/1":
            raise ForestError(
                "COMPONENT_SELECTION_OPERATION_INVALID",
                "Worker Host selection operation probe is invalid",
                503,
            )
        if envelope.get("ok") is False:
            error = envelope.get("error")
            if (
                isinstance(error, dict)
                and error.get("code") in {
                    "COMPONENT_SELECTION_INVALID", "COMPONENT_SELECTION_OPERATION_CONFLICT"
                }
                and isinstance(error.get("message"), str)
                and type(error.get("status")) is int
            ):
                raise ForestError(error["code"], error["message"], error["status"])
        value = envelope.get("value")
        if envelope.get("ok") is not True or not isinstance(value, dict) or set(value) != {
            "protocol", "disposition", "receipt"
        } or value.get("protocol") != "forest.component-selection-operation-probe/1":
            raise ForestError(
                "COMPONENT_SELECTION_OPERATION_INVALID",
                "Worker Host selection operation probe is invalid",
                503,
            )
        if value["disposition"] == "ABSENT" and value["receipt"] is None:
            return None
        receipt = value["receipt"]
        if (
            value["disposition"] != "REPLAY"
            or not isinstance(receipt, dict)
            or set(receipt) != {
                "protocol", "operation_id", "request_id", "component_id", "desired_enabled",
                "selection_revision", "expected_graph_revision",
            }
            or receipt["protocol"] != "forest.component-selection-operation/2"
            or receipt["operation_id"] != operation_id
            or not isinstance(receipt["request_id"], str)
            or not re.fullmatch(r"selection:[0-9a-f]{32}", receipt["request_id"])
            or not isinstance(receipt["component_id"], str)
            or receipt["desired_enabled"] is not desired_enabled
            or type(receipt["selection_revision"]) is not int
            or receipt["selection_revision"] < 1
            or type(receipt["expected_graph_revision"]) is not int
            or receipt["expected_graph_revision"] < 1
        ):
            raise ForestError(
                "COMPONENT_SELECTION_OPERATION_INVALID",
                "Worker Host selection operation probe is invalid",
                503,
            )
        return receipt

    @staticmethod
    def _decode_snapshot(envelope, *, allow_error: bool = False) -> ComponentSelectionSnapshot:
        if allow_error and isinstance(envelope, dict) and envelope.get("ok") is False:
            error = envelope.get("error")
            if (
                envelope.get("protocol") == "forest.domain-result/1"
                and isinstance(error, dict)
                and error.get("code") in {
                    "COMPONENT_SELECTION_INVALID",
                    "COMPONENT_SELECTION_REVISION_CONFLICT",
                    "COMPONENT_SELECTION_LIMIT",
                }
                and isinstance(error.get("message"), str)
                and type(error.get("status")) is int
            ):
                raise ForestError(error["code"], error["message"], error["status"])
        if (
            not isinstance(envelope, dict)
            or envelope.get("protocol") != "forest.domain-result/1"
            or envelope.get("ok") is not True
        ):
            raise ForestError(
                "COMPONENT_SELECTION_SNAPSHOT_INVALID",
                "Worker Host selection snapshot is invalid",
                503,
            )
        value = envelope.get("value")
        if (
            not isinstance(value, dict)
            or set(value) != {"protocol", "revision", "entries"}
            or value.get("protocol") != "forest.component-selection-snapshot/1"
            or type(value.get("revision")) is not int
            or value["revision"] < 0
            or not isinstance(value.get("entries"), list)
            or len(value["entries"]) > 128
        ):
            raise ForestError(
                "COMPONENT_SELECTION_SNAPSHOT_INVALID",
                "Worker Host selection snapshot is invalid",
                503,
            )
        entries = tuple(ComponentSelectionEntry.from_dict(item) for item in value["entries"])
        frozen = {
            "protocol": "forest.component-selection-snapshot/1",
            "revision": value["revision"],
            "entries": [item.as_dict() for item in entries],
        }
        return ComponentSelectionSnapshot(
            value["revision"], entries, digest_json(frozen)
        )

    async def snapshot(self) -> ComponentSelectionSnapshot:
        async with self.publication_fence() as snapshot:
            return snapshot
