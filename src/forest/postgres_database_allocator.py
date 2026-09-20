"""Bounded PostgreSQL database allocation for one approved deployment cluster."""

from __future__ import annotations

import hashlib
import re
import secrets
from dataclasses import dataclass, field
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from forest.errors import ForestError


@dataclass(frozen=True)
class LocalPostgresDatabaseAllocator:
    maintenance_dsn: str = field(repr=False)
    database_prefix: str = "forest_bot_"
    _parsed: object = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        parsed = self._parse(self.maintenance_dsn)
        if re.fullmatch(r"[a-z][a-z0-9_]{0,29}", self.database_prefix) is None:
            raise ValueError("database_prefix is invalid")
        object.__setattr__(self, "_parsed", parsed)

    def database_name(self, bot_id: str) -> str:
        if not isinstance(bot_id, str) or not bot_id:
            raise ForestError("STORAGE_ALLOCATION_INPUT_INVALID", "Bot identity is invalid", 422)
        try:
            digest = hashlib.sha256(bot_id.encode("utf-8")).hexdigest()
        except UnicodeError as exc:
            raise ForestError(
                "STORAGE_ALLOCATION_INPUT_INVALID", "Bot identity is invalid", 422
            ) from exc
        return self.database_prefix + digest[:24]

    def role_name(self, bot_id: str) -> str:
        return "forest_role_" + self.database_name(bot_id).removeprefix(self.database_prefix)

    def new_target_dsn(self, bot_id: str) -> str:
        return self.target_dsn(bot_id, secrets.token_urlsafe(48))

    def validate_target(self, bot_id: str, target_dsn: str) -> None:
        self._validate_target(bot_id, target_dsn)

    def target_dsn(self, bot_id: str, password: str) -> str:
        if not self._valid_password(password):
            raise ForestError(
                "STORAGE_ALLOCATION_INPUT_INVALID", "Database credential is invalid", 422
            )
        host = self._parsed.hostname or ""
        if ":" in host:
            host = f"[{host}]"
        port = "" if self._parsed.port is None else f":{self._parsed.port}"
        netloc = (
            f"{quote(self.role_name(bot_id), safe='')}:{quote(password, safe='')}@{host}{port}"
        )
        return urlunsplit(
            self._parsed._replace(netloc=netloc, path="/" + self.database_name(bot_id))
        )

    async def ensure_database(self, *, bot_id: str, target_dsn: str) -> str:
        target = self._validate_target(bot_id, target_dsn)
        name = self.database_name(bot_id)
        owner = self.role_name(bot_id)
        password = unquote(target.password or "")
        maintenance = urlunsplit(self._parsed._replace(path="/postgres"))
        try:
            connection = await psycopg.AsyncConnection.connect(
                maintenance,
                autocommit=True,
                row_factory=dict_row,
            )
            try:
                await connection.execute(
                    "SELECT pg_advisory_lock(hashtextextended(%s,0))",
                    (f"forest.storage-allocation/{name}",),
                )
                role = await (
                    await connection.execute(
                        "SELECT rolsuper,rolinherit,rolcreaterole,rolcreatedb,rolcanlogin,"
                        "rolreplication,rolbypassrls FROM pg_roles WHERE rolname=%s",
                        (owner,),
                    )
                ).fetchone()
                if role is None:
                    await connection.execute(
                        sql.SQL(
                            "CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER INHERIT "
                            "NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
                        ).format(sql.Identifier(owner), sql.Literal(password))
                    )
                elif role != {
                    "rolsuper": False,
                    "rolinherit": True,
                    "rolcreaterole": False,
                    "rolcreatedb": False,
                    "rolcanlogin": True,
                    "rolreplication": False,
                    "rolbypassrls": False,
                }:
                    raise ForestError(
                        "STORAGE_ALLOCATION_ROLE_MISMATCH",
                        "Allocated database role privileges differ",
                        409,
                    )
                else:
                    await connection.execute(
                        sql.SQL("ALTER ROLE {} PASSWORD {}").format(
                            sql.Identifier(owner), sql.Literal(password)
                        )
                    )
                existing = await (
                    await connection.execute(
                        "SELECT pg_get_userbyid(datdba) AS owner FROM pg_database WHERE datname=%s",
                        (name,),
                    )
                ).fetchone()
                if existing is None:
                    await connection.execute(
                        sql.SQL("CREATE DATABASE {} OWNER {}").format(
                            sql.Identifier(name), sql.Identifier(owner)
                        )
                    )
                    existing = await (
                        await connection.execute(
                            "SELECT pg_get_userbyid(datdba) AS owner "
                            "FROM pg_database WHERE datname=%s",
                            (name,),
                        )
                    ).fetchone()
                if existing is None or existing["owner"] != owner:
                    raise ForestError(
                        "STORAGE_ALLOCATION_OWNER_MISMATCH",
                        "Allocated database owner differs",
                        409,
                    )
            finally:
                await connection.close()
        except ForestError:
            raise
        except psycopg.Error as exc:
            raise ForestError(
                "STORAGE_ALLOCATION_FAILED",
                "Bot database could not be allocated",
                503,
            ) from exc
        return target_dsn

    def _validate_target(self, bot_id: str, target_dsn: str):
        try:
            target = urlsplit(target_dsn)
            password = unquote(target.password or "")
            valid = (
                target.scheme == self._parsed.scheme
                and target.hostname == self._parsed.hostname
                and target.port == self._parsed.port
                and unquote(target.username or "") == self.role_name(bot_id)
                and target.password is not None
                and self._valid_password(password)
                and target.path == "/" + self.database_name(bot_id)
                and target.query == self._parsed.query
                and target.fragment == ""
            )
        except (TypeError, ValueError, UnicodeError):
            valid = False
            target = None
        if not valid:
            raise ForestError(
                "STORAGE_ALLOCATION_SCOPE_MISMATCH",
                "Target DSN is outside the approved deployment cluster",
                409,
            )
        return target

    @staticmethod
    def _valid_password(value: object) -> bool:
        return (
            isinstance(value, str)
            and 32 <= len(value) <= 256
            and value.isascii()
            and all(33 <= ord(character) <= 126 for character in value)
        )

    @staticmethod
    def _parse(value: str):
        try:
            parsed = urlsplit(value)
            valid = (
                parsed.scheme in {"postgres", "postgresql"}
                and parsed.hostname is not None
                and parsed.username is not None
                and parsed.path not in {"", "/"}
                and parsed.fragment == ""
            )
        except (TypeError, ValueError):
            valid = False
            parsed = None
        if not valid:
            raise ValueError("maintenance_dsn is invalid")
        return parsed
