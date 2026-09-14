"""Offline, evidence-bound activation of a restored deployment database."""

from __future__ import annotations

import hashlib
import hmac
import os
import re
import stat
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from uuid import UUID, uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

from forest.backup_artifact import FORMAT, decrypt_file, inspect_zip
from forest.bootstrap import BootstrapState, normalize_dsn
from forest.errors import ForestError
from forest.packages import digest_json
from forest.security import private_directory

_NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_PACKAGE_MEMBER = re.compile(r"^artifacts/[a-f0-9]{64}\.zip$")
_DATABASE_KEYS = {
    "scope", "bot_id", "identity", "identity_digest", "migration_digest", "migrations",
    "database_name_digest", "server_version", "encoding", "collation", "character_type",
    "transaction_snapshot", "wal_lsn", "deletion_epoch", "captured_at", "tables",
    "constraints", "sequences", "member_count", "byte_size", "content_digest",
}
_TABLE_KEYS = {"name", "member", "rows", "columns", "copied_columns"}
_COLUMN_KEYS = {"name", "type", "not_null", "identity", "generated"}
_DIGEST = re.compile(r"^sha256:[a-f0-9]{64}$")
_MARKER = re.compile(
    r"^forest:restore-stage:([0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}):(sha256:[a-f0-9]{64})$"
)


@dataclass(frozen=True)
class RecoveryArtifact:
    path: Path
    artifact_digest: str
    manifest_digest: str
    backup_id: str
    database: dict[str, Any]
    members: dict[str, dict[str, Any]]


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _regular_file(path: str | Path, code: str) -> Path:
    candidate = Path(path).absolute()
    try:
        info = candidate.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink != 1
            or candidate.is_symlink()
            or getattr(info, "st_reparse_tag", 0)
        ):
            raise OSError
        return candidate.resolve(strict=True)
    except OSError:
        raise ForestError(code, "Recovery input must be a regular protected file", 422) from None


def read_secret_file(path: str | Path, *, kind: str) -> str:
    code = "RECOVERY_PASSPHRASE_FILE_INVALID" if kind == "passphrase" else "RECOVERY_DSN_FILE_INVALID"
    source = _regular_file(path, code)
    try:
        if source.stat().st_size > 8193:
            raise ValueError
        raw = source.read_bytes()
        if raw.endswith(b"\n"):
            raw = raw[:-1]
            if raw.endswith(b"\r"):
                raw = raw[:-1]
        value = raw.decode("utf-8")
        if not value or "\x00" in value:
            raise ValueError
        return value
    except (OSError, UnicodeError, ValueError):
        raise ForestError(code, "Recovery secret file is invalid", 422) from None


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
    except OSError:
        raise ForestError("RECOVERY_ARTIFACT_UNAVAILABLE", "Recovery artifact is unavailable", 422) from None
    return "sha256:" + digest.hexdigest()


def _validate_database_cut(
    database: dict[str, Any], members: dict[str, dict[str, Any]]
) -> set[str]:
    if not isinstance(database, dict) or set(database) != _DATABASE_KEYS:
        raise ForestError("BACKUP_MANIFEST_INVALID", "Backup database cut is invalid", 409)
    scope, bot_id = database.get("scope"), database.get("bot_id")
    if (
        scope not in {"deployment", "bot"}
        or (scope == "deployment" and bot_id is not None)
        or (
            scope == "bot"
            and (
                not isinstance(bot_id, str)
                or not 1 <= len(bot_id) <= 128
                or "/" in bot_id
                or "\\" in bot_id
            )
        )
    ):
        raise ForestError("BACKUP_MANIFEST_INVALID", "Backup database scope is invalid", 409)
    identity = database.get("identity")
    migrations = database.get("migrations")
    tables = database.get("tables")
    if (
        not isinstance(identity, dict)
        or digest_json(identity) != database.get("identity_digest")
        or not isinstance(migrations, list)
        or not migrations
        or any(
            not isinstance(row, dict)
            or set(row) != {"version", "checksum"}
            or not isinstance(row["version"], str)
            or not row["version"]
            or not isinstance(row["checksum"], str)
            or not row["checksum"]
            for row in migrations
        )
        or digest_json([[row["version"], row["checksum"]] for row in migrations])
        != database.get("migration_digest")
        or not isinstance(tables, list)
        or not tables
        or not isinstance(database.get("constraints"), list)
        or not isinstance(database.get("sequences"), list)
    ):
        raise ForestError("BACKUP_MANIFEST_INVALID", "Backup database contract is invalid", 409)
    prefix = "deployment" if scope == "deployment" else "bots/" + bot_id
    exported: list[list[str]] = []
    table_members: set[str] = set()
    for index, table in enumerate(tables):
        columns = table.get("columns") if isinstance(table, dict) else None
        copied = table.get("copied_columns") if isinstance(table, dict) else None
        expected_member = (
            f"{prefix}/tables/{index:04d}-{table.get('name')}.bin"
            if isinstance(table, dict)
            else ""
        )
        if (
            not isinstance(table, dict)
            or set(table) != _TABLE_KEYS
            or not isinstance(table.get("name"), str)
            or not _NAME.fullmatch(table["name"])
            or table.get("member") != expected_member
            or type(table.get("rows")) is not int
            or table["rows"] < 0
            or not isinstance(columns, list)
            or not columns
            or any(
                not isinstance(column, dict)
                or set(column) != _COLUMN_KEYS
                or not isinstance(column.get("name"), str)
                or not _NAME.fullmatch(column["name"])
                or not isinstance(column.get("type"), str)
                or type(column.get("not_null")) is not bool
                or not isinstance(column.get("identity"), str)
                or not isinstance(column.get("generated"), str)
                for column in columns
            )
            or copied != [column["name"] for column in columns if not column["generated"]]
            or not copied
        ):
            raise ForestError("BACKUP_MANIFEST_INVALID", "Backup table contract is invalid", 409)
        member = members.get(expected_member)
        if member is None or expected_member in table_members:
            raise ForestError("BACKUP_MANIFEST_INVALID", "Backup table member is invalid", 409)
        table_members.add(expected_member)
        exported.append([expected_member, member["digest"]])
    if (
        digest_json(exported) != database.get("content_digest")
        or not _DIGEST.fullmatch(str(database.get("content_digest")))
        or database.get("member_count") != len(tables)
        or database.get("byte_size") != sum(members[name]["size"] for name in table_members)
    ):
        raise ForestError("BACKUP_MANIFEST_INVALID", "Backup database digest is invalid", 409)
    return table_members


def _manifest_database(manifest: dict[str, Any], members: dict[str, dict[str, Any]]) -> dict[str, Any]:
    required = {
        "format", "backup_id", "created_at", "databases", "members", "includes",
        "roster", "activation",
    }
    roster = manifest.get("roster")
    includes = manifest.get("includes")
    databases = manifest.get("databases")
    if (
        set(manifest) != required
        or manifest.get("format") != FORMAT
        or manifest.get("activation") != "ISOLATED_RESTORE_REQUIRED"
        or not isinstance(manifest.get("backup_id"), str)
        or not isinstance(roster, dict)
        or set(roster) != {"start", "end", "start_digest", "end_digest", "consistent"}
        or roster["consistent"] is not True
        or roster["start"] != roster["end"]
        or roster["start_digest"] != roster["end_digest"]
        or digest_json(roster["start"]) != roster["start_digest"]
        or not isinstance(includes, dict)
        or set(includes) != {"recovery_key", "packages"}
        or includes["recovery_key"] is not True
        or type(includes["packages"]) is not bool
        or not isinstance(databases, list)
    ):
        raise ForestError("BACKUP_MANIFEST_INVALID", "Backup manifest is incompatible", 409)
    deployments = []
    database_members: set[str] = set()
    scopes: set[tuple[str, str]] = set()
    for database in databases:
        if not isinstance(database, dict):
            raise ForestError("BACKUP_MANIFEST_INVALID", "Backup database cut is invalid", 409)
        key = (str(database.get("scope")), str(database.get("bot_id") or ""))
        if key in scopes:
            raise ForestError("BACKUP_MANIFEST_INVALID", "Backup database cut is duplicated", 409)
        scopes.add(key)
        current_members = _validate_database_cut(database, members)
        if database_members & current_members:
            raise ForestError("BACKUP_MANIFEST_INVALID", "Backup table member is duplicated", 409)
        database_members |= current_members
        if database["scope"] == "deployment":
            deployments.append(database)
    if len(deployments) != 1:
        raise ForestError("BACKUP_MANIFEST_INVALID", "Backup must contain one deployment cut", 409)
    database = deployments[0]
    identity = database.get("identity")
    if (
        not isinstance(identity, dict)
        or set(identity) != {"setup_id", "plan_digest"}
        or any(not isinstance(identity.get(key), str) for key in identity)
    ):
        raise ForestError("BACKUP_MANIFEST_INVALID", "Deployment cut is invalid", 409)
    package_members = {name for name in members if _PACKAGE_MEMBER.fullmatch(name)}
    allowed = database_members | {"recovery/master.key"} | package_members
    if (
        (bool(package_members) and includes["packages"] is not True)
        or set(members) != allowed
        or "recovery/master.key" not in members
    ):
        raise ForestError("BACKUP_MANIFEST_INVALID", "Backup member purpose is invalid", 409)
    return database


def open_recovery_artifact(
    data_dir: str | Path, artifact_path: str | Path, passphrase: str
) -> RecoveryArtifact:
    artifact = _regular_file(artifact_path, "RECOVERY_ARTIFACT_UNAVAILABLE")
    artifact_digest = _file_digest(artifact)
    work = private_directory(Path(data_dir) / "deployment-recovery")
    descriptor, temporary_name = tempfile.mkstemp(prefix=".manifest-", suffix=".zip", dir=work)
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        header = decrypt_file(artifact, temporary, passphrase)
        manifest = inspect_zip(temporary)
        with zipfile.ZipFile(temporary) as archive:
            manifest_bytes = archive.read("manifest.json")
        members_value = manifest.get("members")
        if not isinstance(members_value, list) or any(not isinstance(item, dict) for item in members_value):
            raise ForestError("BACKUP_MANIFEST_INVALID", "Backup member roster is invalid", 409)
        members = {item.get("name"): item for item in members_value}
        if len(members) != len(members_value) or any(not isinstance(name, str) for name in members):
            raise ForestError("BACKUP_MANIFEST_INVALID", "Backup member roster is invalid", 409)
        database = _manifest_database(manifest, members)
        if header.get("backup_id") != manifest.get("backup_id"):
            raise ForestError("BACKUP_MANIFEST_INVALID", "Backup identity differs", 409)
        master_key = _regular_file(
            Path(data_dir) / "master.key", "RECOVERY_MASTER_KEY_UNAVAILABLE"
        )
        if not hmac.compare_digest(
            _file_digest(master_key), members["recovery/master.key"]["digest"]
        ):
            raise ForestError(
                "RECOVERY_MASTER_KEY_MISMATCH",
                "Backup recovery key differs from this deployment",
                409,
            )
        return RecoveryArtifact(
            path=artifact,
            artifact_digest=artifact_digest,
            manifest_digest="sha256:" + hashlib.sha256(manifest_bytes).hexdigest(),
            backup_id=manifest["backup_id"],
            database=database,
            members=members,
        )
    finally:
        temporary.unlink(missing_ok=True)


async def validate_recovery_target(
    target_dsn: str,
    artifact: RecoveryArtifact,
    bootstrap_plan: dict[str, Any],
    *,
    on_validated: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if target_dsn.startswith(("postgresql://", "postgres://")):
        canonical_dsn, target_identity = normalize_dsn(target_dsn)
    else:
        try:
            values = conninfo_to_dict(target_dsn)
            canonical_dsn = make_conninfo(**dict(sorted(values.items())))
            target_identity = {
                key: values[key] for key in ("host", "port", "dbname", "user")
            }
            if "hostaddr" in values:
                target_identity["hostaddr"] = values["hostaddr"]
        except (KeyError, TypeError, ValueError):
            raise ForestError("RECOVERY_TARGET_UNAVAILABLE", "Recovery target is invalid", 409) from None
    database = artifact.database
    tables = database["tables"]
    table_names = [table["name"] for table in tables]
    try:
        async with await psycopg.AsyncConnection.connect(
            canonical_dsn, row_factory=dict_row, connect_timeout=10
        ) as conn:
            async with conn.transaction():
                await conn.execute("SET LOCAL lock_timeout='10s'")
                await conn.execute("SET LOCAL statement_timeout='30min'")
                await conn.execute("SET LOCAL idle_in_transaction_session_timeout='30min'")
                lock_names = ["forest_schema_migrations", *table_names]
                await conn.execute(sql.SQL("LOCK TABLE {} IN SHARE MODE").format(
                    sql.SQL(",").join(
                        sql.SQL("public.{}").format(sql.Identifier(name)) for name in lock_names
                    )
                ))
                base = await (await conn.execute(
                    "SELECT current_database() AS database_name,current_user AS user_name,"
                    "current_setting('server_version_num') AS server_version,"
                    "current_setting('server_encoding') AS encoding,"
                    "(SELECT datcollate FROM pg_database WHERE datname=current_database()) AS collation,"
                    "(SELECT datctype FROM pg_database WHERE datname=current_database()) AS character_type,"
                    "(SELECT shobj_description(oid,'pg_database') FROM pg_database "
                    "WHERE datname=current_database()) AS marker"
                )).fetchone()
                marker = base["marker"]
                match = _MARKER.fullmatch(marker or "")
                if (
                    base["database_name"] != target_identity["dbname"]
                    or base["user_name"] != target_identity["user"]
                    or match is None
                    or match[2] != database["content_digest"]
                    or any(base[key] != database[key] for key in (
                        "server_version", "encoding", "collation", "character_type"
                    ))
                ):
                    raise ForestError(
                        "RECOVERY_TARGET_IDENTITY_MISMATCH",
                        "Recovery target identity or environment differs",
                        409,
                    )
                UUID(match[1])
                migrations = _json_value(await (await conn.execute(
                    "SELECT version,checksum FROM public.forest_schema_migrations ORDER BY version"
                )).fetchall())
                if (
                    migrations != database["migrations"]
                    or digest_json([[row["version"], row["checksum"]] for row in migrations])
                    != database["migration_digest"]
                ):
                    raise ForestError(
                        "RECOVERY_SCHEMA_VERSION_MISMATCH", "Recovery migrations differ", 409
                    )
                rows = await (await conn.execute(
                    "SELECT c.relname AS table_name,a.attnum,a.attname AS column_name,"
                    "format_type(a.atttypid,a.atttypmod) AS data_type,a.attnotnull,a.attidentity,"
                    "a.attgenerated FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped "
                    "WHERE n.nspname='public' AND c.relkind='r' "
                    "AND c.relname<>'forest_schema_migrations' ORDER BY c.relname,a.attnum"
                )).fetchall()
                columns: dict[str, list[dict[str, Any]]] = {}
                for row in rows:
                    columns.setdefault(row["table_name"], []).append({
                        "name": row["column_name"], "type": row["data_type"],
                        "not_null": row["attnotnull"], "identity": row["attidentity"],
                        "generated": row["attgenerated"],
                    })
                if columns != {table["name"]: table["columns"] for table in tables}:
                    raise ForestError("RECOVERY_SCHEMA_MISMATCH", "Recovery table schema differs", 409)
                constraints = _json_value(await (await conn.execute(
                    "SELECT c.conname,c.contype,r.relname AS table_name,"
                    "COALESCE(fr.relname,'') AS referenced_table,"
                    "pg_get_constraintdef(c.oid,true) AS definition FROM pg_constraint c "
                    "JOIN pg_class r ON r.oid=c.conrelid JOIN pg_namespace n ON n.oid=r.relnamespace "
                    "LEFT JOIN pg_class fr ON fr.oid=c.confrelid "
                    "WHERE n.nspname='public' ORDER BY r.relname,c.conname"
                )).fetchall())
                sequences = _json_value(await (await conn.execute(
                    "SELECT sequencename,start_value,min_value,max_value,increment_by,cycle,last_value "
                    "FROM pg_sequences WHERE schemaname='public' ORDER BY sequencename"
                )).fetchall())
                if constraints != database["constraints"] or sequences != database["sequences"]:
                    raise ForestError(
                        "RECOVERY_SCHEMA_MISMATCH", "Recovery constraints or sequences differ", 409
                    )
                for table in tables:
                    count = await (await conn.execute(
                        sql.SQL("SELECT count(*) AS n FROM public.{}").format(
                            sql.Identifier(table["name"])
                        )
                    )).fetchone()
                    digest = hashlib.sha256()
                    query = sql.SQL("COPY public.{} ({}) TO STDOUT (FORMAT BINARY)").format(
                        sql.Identifier(table["name"]),
                        sql.SQL(",").join(
                            sql.Identifier(name) for name in table["copied_columns"]
                        ),
                    )
                    async with conn.cursor().copy(query) as copy:
                        async for chunk in copy:
                            digest.update(bytes(chunk))
                    if (
                        count["n"] != table["rows"]
                        or "sha256:" + digest.hexdigest()
                        != artifact.members[table["member"]]["digest"]
                    ):
                        raise ForestError(
                            "RECOVERY_CONTENT_MISMATCH", "Recovery table content differs", 409
                        )
                identity = _json_value(await (await conn.execute(
                    "SELECT setup_id,plan_digest FROM web_setup_state WHERE singleton"
                )).fetchone())
                if (
                    identity != database["identity"]
                    or digest_json(identity) != database["identity_digest"]
                    or identity.get("setup_id") != bootstrap_plan.get("setup_id")
                    or identity.get("plan_digest") != bootstrap_plan.get("plan_digest")
                ):
                    raise ForestError(
                        "RECOVERY_DEPLOYMENT_IDENTITY_MISMATCH",
                        "Recovery target belongs to another deployment",
                        409,
                    )
                evidence = {
                    "target_dsn": canonical_dsn,
                    "target_identity": target_identity,
                    "restore_id": match[1],
                    "target_marker": marker,
                }
                applied = on_validated(evidence) if on_validated is not None else None
                return evidence, applied
    except ForestError:
        raise
    except (psycopg.Error, OSError, ValueError, KeyError, TypeError):
        raise ForestError(
            "RECOVERY_TARGET_UNAVAILABLE", "Recovery target could not be verified", 409
        ) from None


class DeploymentRecoveryManager:
    def __init__(self, data_dir: str | Path) -> None:
        self.data_dir = private_directory(data_dir)
        self.state = BootstrapState(self.data_dir)

    async def plan(
        self, *, artifact_path: str | Path, passphrase_file: str | Path,
        target_dsn_file: str | Path | None = None, target_dsn: str | None = None,
    ) -> dict[str, Any]:
        if self.state.status()["stage"] != "COMPLETE":
            raise ForestError(
                "RECOVERY_BOOTSTRAP_REQUIRED",
                "A completed local bootstrap state is required for deployment recovery",
                409,
            )
        passphrase = read_secret_file(passphrase_file, kind="passphrase")
        if (target_dsn_file is None) == (target_dsn is None):
            raise ForestError("RECOVERY_TARGET_INVALID", "Choose exactly one recovery target", 422)
        if target_dsn_file is not None:
            target_dsn = read_secret_file(target_dsn_file, kind="dsn")
        assert target_dsn is not None
        artifact = open_recovery_artifact(self.data_dir, artifact_path, passphrase)
        bootstrap_plan = self.state.load_plan()
        assert bootstrap_plan is not None
        evidence, _ = await validate_recovery_target(target_dsn, artifact, bootstrap_plan)
        if evidence["target_identity"] == bootstrap_plan["target_identity"]:
            raise ForestError("RECOVERY_TARGET_UNCHANGED", "Recovery target is already active", 409)
        return self.state.plan_recovery(
            recovery_id=str(uuid4()),
            target_dsn=target_dsn,
            artifact_path=artifact.path,
            artifact_digest=artifact.artifact_digest,
            backup_id=artifact.backup_id,
            manifest_digest=artifact.manifest_digest,
            content_digest=artifact.database["content_digest"],
            restore_id=evidence["restore_id"],
            target_marker=evidence["target_marker"],
        )

    async def apply(
        self, *, recovery_id: str, plan_digest: str, passphrase_file: str | Path
    ) -> dict[str, Any]:
        if (
            not isinstance(recovery_id, str)
            or not 1 <= len(recovery_id) <= 128
            or not isinstance(plan_digest, str)
            or _DIGEST.fullmatch(plan_digest) is None
        ):
            raise ForestError("RECOVERY_PLAN_INVALID", "Recovery plan reference is invalid", 422)
        frozen = self.state.read_recovery_plan(recovery_id)
        if not hmac.compare_digest(frozen["recovery_digest"], plan_digest):
            raise ForestError("RECOVERY_PLAN_CHANGED", "Recovery plan digest differs", 409)
        passphrase = read_secret_file(passphrase_file, kind="passphrase")
        artifact = open_recovery_artifact(
            self.data_dir, frozen["artifact_path"], passphrase
        )
        for key, actual in (
            ("artifact_digest", artifact.artifact_digest),
            ("manifest_digest", artifact.manifest_digest),
            ("backup_id", artifact.backup_id),
            ("content_digest", artifact.database["content_digest"]),
        ):
            if not hmac.compare_digest(frozen[key], actual):
                raise ForestError("RECOVERY_ARTIFACT_CHANGED", "Recovery artifact changed", 409)
        bootstrap_plan = self.state.load_plan()
        assert bootstrap_plan is not None

        def commit(evidence: dict[str, Any]) -> dict[str, Any]:
            if (
                evidence["target_identity"] != frozen["target_identity"]
                or evidence["target_marker"] != frozen["target_marker"]
                or evidence["restore_id"] != frozen["restore_id"]
            ):
                raise ForestError("RECOVERY_TARGET_CHANGED", "Recovery target changed", 409)
            return self.state.apply_recovery(recovery_id, plan_digest)

        _, applied = await validate_recovery_target(
            frozen["target_dsn"], artifact, bootstrap_plan, on_validated=commit
        )
        assert applied is not None
        return applied
