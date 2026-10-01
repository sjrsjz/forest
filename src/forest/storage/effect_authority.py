"""Clean external-effect authority with explicit handoff and UNKNOWN settlement."""

from __future__ import annotations

import hashlib
import inspect
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from psycopg import sql

from forest.errors import ForestError
from forest.schema_bundles import module_schema_namespace

_SHA256_RE = re.compile(r"sha256:[a-f0-9]{64}\Z")


def _bounded(value, field: str, limit: int, *, optional: bool = False):
    if value is None and optional:
        return None
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= limit
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ForestError("EFFECT_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _validated_digest(value, field: str, *, optional: bool = False):
    if value is None and optional:
        return None
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ForestError("EFFECT_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _digest(value) -> str:
    try:
        wire = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise ForestError("EFFECT_INPUT_INVALID", "Effect identity is not canonical", 422) from exc
    return "sha256:" + hashlib.sha256(wire).hexdigest()


async def _call(port, conn, **values):
    result = port(conn, **values)
    return await result if inspect.isawaitable(result) else result


@dataclass(frozen=True)
class EffectPermit:
    bot_id: str
    effect_operation_id: str
    attempt_id: str
    attempt_no: int
    wire_payload_id: str
    wire_payload_digest: str
    executor_component_id: str
    executor_generation_id: str
    fence_kind: str
    fence_id: str
    fence_epoch: int
    deadline_at: datetime


class KernelEffectRepository:
    """Linearize external handoff without importing business effect semantics."""

    def __init__(
        self,
        store,
        *,
        namespace: str | None = None,
        payload_verifier=None,
        authorization_consumer=None,
        fence_verifier=None,
        fence_kind: str | None = None,
    ) -> None:
        self.store = store
        self.namespace = namespace or module_schema_namespace("forest.kernel.effect")
        self.payload_verifier = payload_verifier
        self.authorization_consumer = authorization_consumer
        self.fence_verifiers = {}
        if (fence_verifier is None) != (fence_kind is None):
            raise ValueError("Effect fence verifier and kind must be supplied together")
        if fence_verifier is not None:
            self.register_fence_verifier(fence_kind, fence_verifier)

    def _table(self, name: str):
        return sql.SQL("{}.{}").format(sql.Identifier(self.namespace), sql.Identifier(name))

    def register_fence_verifier(self, fence_kind: str, verifier) -> None:
        _bounded(fence_kind, "fence_kind", 64)
        if verifier is None:
            raise ValueError("Effect fence verifier is required")
        existing = self.fence_verifiers.get(fence_kind)
        if existing is not None and existing is not verifier:
            raise ForestError(
                "EFFECT_FENCE_KIND_CONFLICT", "Effect fence kind already has an owner", 409
            )
        self.fence_verifiers[fence_kind] = verifier

    async def _verify_payload(self, conn, *, payload_id, digest, owner_kind, owner_id):
        if self.payload_verifier is None:
            raise ForestError("EFFECT_PAYLOAD_PORT_REQUIRED", "Payload authority is unavailable", 503)
        return await _call(
            self.payload_verifier,
            conn,
            payload_id=payload_id,
            digest=digest,
            owner_component_id="kernel.effect",
            owner_kind=owner_kind,
            owner_id=owner_id,
        )

    async def _verify_fence(self, conn, permit: EffectPermit) -> None:
        verifier = self.fence_verifiers.get(permit.fence_kind)
        if verifier is None:
            raise ForestError(
                "EFFECT_FENCE_PORT_REQUIRED", "Executor fence authority is unavailable", 503
            )
        await _call(
            verifier,
            conn,
            executor_component_id=permit.executor_component_id,
            executor_generation_id=permit.executor_generation_id,
            fence_kind=permit.fence_kind,
            fence_id=permit.fence_id,
            fence_epoch=permit.fence_epoch,
        )

    @staticmethod
    def _permit(row) -> EffectPermit:
        return EffectPermit(
            bot_id=row["bot_id"],
            effect_operation_id=row["effect_operation_id"],
            attempt_id=row["attempt_id"],
            attempt_no=row["attempt_no"],
            wire_payload_id=row["wire_payload_id"],
            wire_payload_digest=row["wire_payload_digest"],
            executor_component_id=row["executor_component_id"],
            executor_generation_id=row["executor_generation_id"],
            fence_kind=row["fence_kind"],
            fence_id=row["fence_id"],
            fence_epoch=row["fence_epoch"],
            deadline_at=row["deadline_at"],
        )

    @staticmethod
    def _matches_permit(row, permit: EffectPermit) -> bool:
        return all(
            row[field] == getattr(permit, field)
            for field in (
                "bot_id",
                "effect_operation_id",
                "attempt_id",
                "attempt_no",
                "wire_payload_id",
                "wire_payload_digest",
                "executor_component_id",
                "executor_generation_id",
                "fence_kind",
                "fence_id",
                "fence_epoch",
                "deadline_at",
            )
        )

    async def admit_in(
        self,
        conn,
        *,
        effect_operation_id: str,
        input_digest: str,
        effect_kind: str,
        target_kind: str,
        target_id: str,
        producer_component_id: str,
        producer_generation_id: str,
        root_operation_kind: str,
        root_operation_id: str,
        input_payload_id: str | None = None,
        input_payload_digest: str | None = None,
    ) -> dict:
        for field, value, limit in (
            ("effect_operation_id", effect_operation_id, 256),
            ("effect_kind", effect_kind, 128),
            ("target_kind", target_kind, 64),
            ("target_id", target_id, 512),
            ("producer_component_id", producer_component_id, 128),
            ("producer_generation_id", producer_generation_id, 256),
            ("root_operation_kind", root_operation_kind, 64),
            ("root_operation_id", root_operation_id, 256),
        ):
            _bounded(value, field, limit)
        _validated_digest(input_digest, "input_digest")
        if (input_payload_id is None) != (input_payload_digest is None):
            raise ForestError("EFFECT_INPUT_INVALID", "Input payload identity is incomplete", 422)
        if input_payload_id is not None:
            _bounded(input_payload_id, "input_payload_id", 256)
            _validated_digest(input_payload_digest, "input_payload_digest")
            await self._verify_payload(
                conn,
                payload_id=input_payload_id,
                digest=input_payload_digest,
                owner_kind="effect.input",
                owner_id=effect_operation_id,
            )
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}(bot_id,effect_operation_id,input_digest,effect_kind,target_kind,"
                "target_id,producer_component_id,producer_generation_id,root_operation_kind,"
                "root_operation_id,input_payload_id,input_payload_digest) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"
            ).format(self._table("effect_operations")),
            (
                self.store.bot_id,
                effect_operation_id,
                input_digest,
                effect_kind,
                target_kind,
                target_id,
                producer_component_id,
                producer_generation_id,
                root_operation_kind,
                root_operation_id,
                input_payload_id,
                input_payload_digest,
            ),
        )
        row = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND effect_operation_id=%s").format(
                    self._table("effect_operations")
                ),
                (self.store.bot_id, effect_operation_id),
            )
        ).fetchone()
        expected = {
            "input_digest": input_digest,
            "effect_kind": effect_kind,
            "target_kind": target_kind,
            "target_id": target_id,
            "producer_component_id": producer_component_id,
            "producer_generation_id": producer_generation_id,
            "root_operation_kind": root_operation_kind,
            "root_operation_id": root_operation_id,
            "input_payload_id": input_payload_id,
            "input_payload_digest": input_payload_digest,
        }
        if row is None or any(row[key] != value for key, value in expected.items()):
            raise ForestError("EFFECT_ID_CONFLICT", "Effect operation identity differs", 409)
        return dict(row)

    async def admit(self, **values) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.admit_in(conn, **values)

    async def load(self, effect_operation_id: str) -> dict:
        _bounded(effect_operation_id, "effect_operation_id", 256)
        async with self.store.pool.connection() as conn, conn.transaction():
            effect = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND effect_operation_id=%s"
                    ).format(self._table("effect_operations")),
                    (self.store.bot_id, effect_operation_id),
                )
            ).fetchone()
            if effect is None:
                raise ForestError("EFFECT_NOT_FOUND", "Effect operation is unavailable", 404)
            attempt = None
            if effect["current_attempt_id"] is not None:
                attempt = await (
                    await conn.execute(
                        sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND attempt_id=%s").format(
                            self._table("effect_attempts")
                        ),
                        (self.store.bot_id, effect["current_attempt_id"]),
                    )
                ).fetchone()
            observations = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND attempt_id=%s "
                        "ORDER BY observed_at,observation_operation_id"
                    ).format(self._table("effect_observations")),
                    (self.store.bot_id, effect["current_attempt_id"]),
                )
            ).fetchall() if effect["current_attempt_id"] is not None else []
        return {
            "effect": dict(effect),
            "attempt": dict(attempt) if attempt is not None else None,
            "observations": [dict(row) for row in observations],
        }

    async def prove_not_applied_in(self, conn, attempts: list[dict]) -> dict:
        """Lock settled Effect facts for a later participant in this transaction."""
        if not isinstance(attempts, list) or len(attempts) > 64:
            raise ForestError("EFFECT_INPUT_INVALID", "Effect proof set is invalid", 422)
        identities = []
        for item in attempts:
            if not isinstance(item, dict) or set(item) != {
                "effect_operation_id", "attempt_id"
            }:
                raise ForestError("EFFECT_INPUT_INVALID", "Effect proof identity is invalid", 422)
            identities.append((
                _bounded(item["effect_operation_id"], "effect_operation_id", 256),
                _bounded(item["attempt_id"], "attempt_id", 256),
            ))
        if len({operation for operation, _ in identities}) != len(identities):
            raise ForestError("EFFECT_INPUT_INVALID", "Effect proof repeats an operation", 422)
        receipts = []
        for operation_id, attempt_id in sorted(identities):
            effect = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT state,current_attempt_id,attempt_count FROM {} "
                        "WHERE bot_id=%s AND effect_operation_id=%s FOR UPDATE"
                    ).format(self._table("effect_operations")),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            attempt = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT state,attempt_no FROM {} WHERE bot_id=%s AND "
                        "effect_operation_id=%s AND attempt_id=%s FOR UPDATE"
                    ).format(self._table("effect_attempts")),
                    (self.store.bot_id, operation_id, attempt_id),
                )
            ).fetchone()
            if (
                effect is None or attempt is None
                or effect["state"] != "NOT_APPLIED"
                or effect["current_attempt_id"] != attempt_id
                or effect["attempt_count"] != attempt["attempt_no"]
            ):
                raise ForestError(
                    "EFFECT_UNSENT_PROOF_UNAVAILABLE",
                    "Effect is not conclusively unsent", 409,
                )
            observation = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT observation_operation_id,evidence_payload_digest "
                        "FROM {} WHERE bot_id=%s AND attempt_id=%s "
                        "AND accepted AND resulting_state='NOT_APPLIED' "
                        "ORDER BY observed_at DESC,observation_operation_id DESC LIMIT 1"
                    ).format(self._table("effect_observations")),
                    (self.store.bot_id, attempt_id),
                )
            ).fetchone()
            if observation is None:
                raise ForestError(
                    "EFFECT_UNSENT_PROOF_UNAVAILABLE",
                    "Effect has no accepted unsent observation", 409,
                )
            receipts.append({
                "effect_operation_id": operation_id,
                "attempt_id": attempt_id,
                "observation_operation_id": observation["observation_operation_id"],
                "evidence_payload_digest": observation["evidence_payload_digest"],
            })
        return {
            "protocol": "forest.effect-unsent-proof/1",
            "attempts": receipts,
            "evidence_set_digest": _digest(receipts),
        }

    async def permit_in(
        self,
        conn,
        *,
        effect_operation_id: str,
        attempt_id: str,
        permit_operation_id: str,
        wire_payload_id: str,
        wire_payload_digest: str,
        authorization_ticket_id: str,
        authorization_action: str,
        authorization_resource_kind: str,
        authorization_resource_id: str,
        authorization_plan_digest: str | None,
        executor_component_id: str,
        executor_generation_id: str,
        fence_kind: str,
        fence_id: str,
        fence_epoch: int,
        deadline_at: datetime,
    ) -> EffectPermit:
        for field, value, limit in (
            ("effect_operation_id", effect_operation_id, 256),
            ("attempt_id", attempt_id, 256),
            ("permit_operation_id", permit_operation_id, 256),
            ("wire_payload_id", wire_payload_id, 256),
            ("authorization_ticket_id", authorization_ticket_id, 256),
            ("authorization_action", authorization_action, 200),
            ("authorization_resource_kind", authorization_resource_kind, 64),
            ("authorization_resource_id", authorization_resource_id, 512),
            ("executor_component_id", executor_component_id, 128),
            ("executor_generation_id", executor_generation_id, 256),
            ("fence_kind", fence_kind, 64),
            ("fence_id", fence_id, 256),
        ):
            _bounded(value, field, limit)
        _validated_digest(wire_payload_digest, "wire_payload_digest")
        _validated_digest(authorization_plan_digest, "authorization_plan_digest", optional=True)
        if type(fence_epoch) is not int or fence_epoch < 1:
            raise ForestError("EFFECT_INPUT_INVALID", "Invalid fence_epoch", 422)
        if not isinstance(deadline_at, datetime) or deadline_at.tzinfo is None:
            raise ForestError("EFFECT_INPUT_INVALID", "Effect deadline needs a timezone", 422)

        replay = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND "
                    "(attempt_id=%s OR permit_operation_id=%s)"
                ).format(self._table("effect_attempts")),
                (self.store.bot_id, attempt_id, permit_operation_id),
            )
        ).fetchone()
        replay_expected = {
            "attempt_id": attempt_id,
            "permit_operation_id": permit_operation_id,
            "effect_operation_id": effect_operation_id,
            "wire_payload_id": wire_payload_id,
            "wire_payload_digest": wire_payload_digest,
            "authorization_ticket_id": authorization_ticket_id,
            "authorization_action": authorization_action,
            "authorization_resource_kind": authorization_resource_kind,
            "authorization_resource_id": authorization_resource_id,
            "authorization_plan_digest": authorization_plan_digest,
            "executor_component_id": executor_component_id,
            "executor_generation_id": executor_generation_id,
            "fence_kind": fence_kind,
            "fence_id": fence_id,
            "fence_epoch": fence_epoch,
            "deadline_at": deadline_at,
        }
        if replay is not None:
            if any(replay[key] != value for key, value in replay_expected.items()):
                raise ForestError("EFFECT_ATTEMPT_CONFLICT", "Effect attempt identity differs", 409)
            return self._permit(replay)

        provisional = EffectPermit(
            self.store.bot_id,
            effect_operation_id,
            attempt_id,
            0,
            wire_payload_id,
            wire_payload_digest,
            executor_component_id,
            executor_generation_id,
            fence_kind,
            fence_id,
            fence_epoch,
            deadline_at,
        )
        await self._verify_fence(conn, provisional)
        if self.authorization_consumer is None:
            raise ForestError(
                "EFFECT_AUTHORIZATION_PORT_REQUIRED", "Authorization authority is unavailable", 503
            )
        effect = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND effect_operation_id=%s").format(
                    self._table("effect_operations")
                ),
                (self.store.bot_id, effect_operation_id),
            )
        ).fetchone()
        if effect is None:
            raise ForestError("EFFECT_NOT_FOUND", "Effect operation is unavailable", 404)
        authorization = await _call(
            self.authorization_consumer,
            conn,
            ticket_id=authorization_ticket_id,
            operation_id=permit_operation_id,
            action=authorization_action,
            resource_kind=authorization_resource_kind,
            resource_id=authorization_resource_id,
            input_digest=effect["input_digest"],
            plan_digest=authorization_plan_digest,
            audience_component_id=executor_component_id,
            audience_generation_id=executor_generation_id,
        )
        policy_revision = authorization.get("policy_revision") if isinstance(authorization, dict) else None
        if type(policy_revision) is not int or policy_revision < 1:
            raise ForestError("EFFECT_AUTHORIZATION_INVALID", "Authorization receipt is invalid", 503)
        await self._verify_payload(
            conn,
            payload_id=wire_payload_id,
            digest=wire_payload_digest,
            owner_kind="effect.wire",
            owner_id=attempt_id,
        )

        effect = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND effect_operation_id=%s FOR UPDATE"
                ).format(self._table("effect_operations")),
                (self.store.bot_id, effect_operation_id),
            )
        ).fetchone()
        replay = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND "
                    "(attempt_id=%s OR permit_operation_id=%s)"
                ).format(self._table("effect_attempts")),
                (self.store.bot_id, attempt_id, permit_operation_id),
            )
        ).fetchone()
        if replay is not None:
            if any(replay[key] != value for key, value in replay_expected.items()):
                raise ForestError("EFFECT_ATTEMPT_CONFLICT", "Effect attempt identity differs", 409)
            return self._permit(replay)
        if effect is None or effect["state"] not in ("PENDING", "NOT_APPLIED"):
            raise ForestError("EFFECT_NOT_DISPATCHABLE", "Effect cannot create another attempt", 409)
        now = datetime.now(timezone.utc)
        if not now < deadline_at <= now + timedelta(seconds=30):
            raise ForestError("EFFECT_INPUT_INVALID", "Effect permit lifetime is invalid", 422)
        attempt_no = effect["attempt_count"] + 1
        row = await (
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,attempt_id,effect_operation_id,attempt_no,"
                    "permit_operation_id,wire_payload_id,wire_payload_digest,authorization_ticket_id,"
                    "authorization_action,authorization_resource_kind,authorization_resource_id,"
                    "authorization_plan_digest,authorization_policy_revision,executor_component_id,"
                    "executor_generation_id,fence_kind,fence_id,fence_epoch,deadline_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "RETURNING *"
                ).format(self._table("effect_attempts")),
                (
                    self.store.bot_id,
                    attempt_id,
                    effect_operation_id,
                    attempt_no,
                    permit_operation_id,
                    wire_payload_id,
                    wire_payload_digest,
                    authorization_ticket_id,
                    authorization_action,
                    authorization_resource_kind,
                    authorization_resource_id,
                    authorization_plan_digest,
                    policy_revision,
                    executor_component_id,
                    executor_generation_id,
                    fence_kind,
                    fence_id,
                    fence_epoch,
                    deadline_at,
                ),
            )
        ).fetchone()
        await conn.execute(
            sql.SQL(
                "UPDATE {} SET state='PREPARED',current_attempt_id=%s,attempt_count=%s,"
                "revision=revision+1,updated_at=clock_timestamp() "
                "WHERE bot_id=%s AND effect_operation_id=%s"
            ).format(self._table("effect_operations")),
            (attempt_id, attempt_no, self.store.bot_id, effect_operation_id),
        )
        return self._permit(row)

    async def permit(self, **values) -> EffectPermit:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.permit_in(conn, **values)

    async def mark_handoff_in(self, conn, *, permit: EffectPermit, handoff_operation_id: str) -> dict:
        if type(permit) is not EffectPermit or permit.bot_id != self.store.bot_id:
            raise ForestError("EFFECT_INPUT_INVALID", "Invalid effect permit", 422)
        _bounded(handoff_operation_id, "handoff_operation_id", 256)
        replay = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND attempt_id=%s").format(
                    self._table("effect_attempts")
                ),
                (self.store.bot_id, permit.attempt_id),
            )
        ).fetchone()
        if replay is not None and replay["handoff_operation_id"] == handoff_operation_id:
            if not self._matches_permit(replay, permit):
                raise ForestError("EFFECT_STALE_PERMIT", "Effect permit identity differs", 409)
            return dict(replay)
        await self._verify_fence(conn, permit)
        await self._verify_payload(
            conn,
            payload_id=permit.wire_payload_id,
            digest=permit.wire_payload_digest,
            owner_kind="effect.wire",
            owner_id=permit.attempt_id,
        )
        effect = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND effect_operation_id=%s FOR UPDATE"
                ).format(self._table("effect_operations")),
                (self.store.bot_id, permit.effect_operation_id),
            )
        ).fetchone()
        attempt = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND attempt_id=%s FOR UPDATE").format(
                    self._table("effect_attempts")
                ),
                (self.store.bot_id, permit.attempt_id),
            )
        ).fetchone()
        if effect is None or attempt is None or effect["current_attempt_id"] != permit.attempt_id:
            raise ForestError("EFFECT_STALE_PERMIT", "Effect permit is no longer current", 409)
        if attempt["state"] != "PREPARED":
            if attempt["handoff_operation_id"] == handoff_operation_id:
                if not self._matches_permit(attempt, permit):
                    raise ForestError("EFFECT_STALE_PERMIT", "Effect permit identity differs", 409)
                return dict(attempt)
            raise ForestError("EFFECT_HANDOFF_CONFLICT", "Effect attempt was already handed off", 409)
        expected = {
            "attempt_no": permit.attempt_no,
            "wire_payload_id": permit.wire_payload_id,
            "wire_payload_digest": permit.wire_payload_digest,
            "executor_component_id": permit.executor_component_id,
            "executor_generation_id": permit.executor_generation_id,
            "fence_kind": permit.fence_kind,
            "fence_id": permit.fence_id,
            "fence_epoch": permit.fence_epoch,
            "deadline_at": permit.deadline_at,
        }
        if any(attempt[key] != value for key, value in expected.items()):
            raise ForestError("EFFECT_STALE_PERMIT", "Effect permit identity differs", 409)
        now = await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone()
        if attempt["deadline_at"] <= now["now"]:
            raise ForestError("EFFECT_PERMIT_EXPIRED", "Effect permit expired before handoff", 409)
        attempt = await (
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state='DISPATCHING',handoff_operation_id=%s,"
                    "started_at=clock_timestamp() WHERE bot_id=%s AND attempt_id=%s RETURNING *"
                ).format(self._table("effect_attempts")),
                (handoff_operation_id, self.store.bot_id, permit.attempt_id),
            )
        ).fetchone()
        await conn.execute(
            sql.SQL(
                "UPDATE {} SET state='DISPATCHING',revision=revision+1,"
                "updated_at=clock_timestamp() WHERE bot_id=%s AND effect_operation_id=%s"
            ).format(self._table("effect_operations")),
            (self.store.bot_id, permit.effect_operation_id),
        )
        return dict(attempt)

    async def mark_handoff(self, **values) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.mark_handoff_in(conn, **values)

    async def observe_in(
        self,
        conn,
        *,
        effect_operation_id: str,
        attempt_id: str,
        observation_operation_id: str,
        status: str,
        evidence_kind: str,
        evidence_payload_id: str | None = None,
        evidence_payload_digest: str | None = None,
    ) -> dict:
        for field, value, limit in (
            ("effect_operation_id", effect_operation_id, 256),
            ("attempt_id", attempt_id, 256),
            ("observation_operation_id", observation_operation_id, 256),
            ("evidence_kind", evidence_kind, 64),
        ):
            _bounded(value, field, limit)
        if status not in ("SUCCEEDED", "NOT_APPLIED", "UNKNOWN"):
            raise ForestError("EFFECT_INPUT_INVALID", "Invalid effect observation", 422)
        if (evidence_payload_id is None) != (evidence_payload_digest is None):
            raise ForestError("EFFECT_INPUT_INVALID", "Evidence payload identity is incomplete", 422)
        if evidence_payload_id is not None:
            _bounded(evidence_payload_id, "evidence_payload_id", 256)
            _validated_digest(evidence_payload_digest, "evidence_payload_digest")
        input_digest = _digest(
            [attempt_id, status, evidence_kind, evidence_payload_id, evidence_payload_digest]
        )
        existing = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND observation_operation_id=%s"
                ).format(self._table("effect_observations")),
                (self.store.bot_id, observation_operation_id),
            )
        ).fetchone()
        if existing is not None:
            if existing["input_digest"] != input_digest:
                raise ForestError("EFFECT_OBSERVATION_CONFLICT", "Observation input differs", 409)
            return dict(existing)
        if evidence_payload_id is not None:
            await self._verify_payload(
                conn,
                payload_id=evidence_payload_id,
                digest=evidence_payload_digest,
                owner_kind="effect.evidence",
                owner_id=observation_operation_id,
            )
        effect = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND effect_operation_id=%s FOR UPDATE"
                ).format(self._table("effect_operations")),
                (self.store.bot_id, effect_operation_id),
            )
        ).fetchone()
        attempt = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND attempt_id=%s FOR UPDATE").format(
                    self._table("effect_attempts")
                ),
                (self.store.bot_id, attempt_id),
            )
        ).fetchone()
        if (
            effect is None
            or attempt is None
            or attempt["effect_operation_id"] != effect_operation_id
        ):
            raise ForestError("EFFECT_ATTEMPT_NOT_FOUND", "Effect attempt is unavailable", 404)
        is_current = effect["current_attempt_id"] == attempt_id
        if not is_current and attempt["state"] not in ("SUCCEEDED", "NOT_APPLIED"):
            raise ForestError("EFFECT_ATTEMPT_NOT_FOUND", "Effect attempt is unavailable", 404)
        existing = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND observation_operation_id=%s"
                ).format(self._table("effect_observations")),
                (self.store.bot_id, observation_operation_id),
            )
        ).fetchone()
        if existing is not None:
            if existing["input_digest"] != input_digest:
                raise ForestError("EFFECT_OBSERVATION_CONFLICT", "Observation input differs", 409)
            return dict(existing)

        previous = attempt["state"]
        accepted = False
        conflict_code = "EFFECT_EVIDENCE_CONFLICT"
        resulting = previous
        transition = False
        if is_current and previous == "DISPATCHING":
            accepted, conflict_code, resulting, transition = True, None, status, True
        elif is_current and previous == "UNKNOWN" and evidence_kind == "reconciliation" and status in (
            "SUCCEEDED",
            "NOT_APPLIED",
        ):
            accepted, conflict_code, resulting, transition = True, None, status, True
        elif previous == status:
            accepted, conflict_code = True, None
        row = await (
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,observation_operation_id,attempt_id,input_digest,"
                    "evidence_kind,observed_status,evidence_payload_id,evidence_payload_digest,"
                    "accepted,previous_state,resulting_state,conflict_code) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                ).format(self._table("effect_observations")),
                (
                    self.store.bot_id,
                    observation_operation_id,
                    attempt_id,
                    input_digest,
                    evidence_kind,
                    status,
                    evidence_payload_id,
                    evidence_payload_digest,
                    accepted,
                    previous,
                    resulting,
                    conflict_code,
                ),
            )
        ).fetchone()
        if transition:
            resolved = previous == "UNKNOWN"
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state=%s,settled_at=COALESCE(settled_at,clock_timestamp()),"
                    "resolved_at=CASE WHEN %s THEN clock_timestamp() ELSE resolved_at END "
                    "WHERE bot_id=%s AND attempt_id=%s"
                ).format(self._table("effect_attempts")),
                (resulting, resolved, self.store.bot_id, attempt_id),
            )
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state=%s,revision=revision+1,updated_at=clock_timestamp() "
                    "WHERE bot_id=%s AND effect_operation_id=%s"
                ).format(self._table("effect_operations")),
                (resulting, self.store.bot_id, effect_operation_id),
            )
        return dict(row)

    async def observe(self, **values) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.observe_in(conn, **values)

    async def abandon_prepared_in(
        self,
        conn,
        *,
        effect_operation_id: str,
        attempt_id: str,
        abandonment_operation_id: str,
        reason_code: str,
        cancel_effect: bool = False,
    ) -> dict:
        for field, value, limit in (
            ("effect_operation_id", effect_operation_id, 256),
            ("attempt_id", attempt_id, 256),
            ("abandonment_operation_id", abandonment_operation_id, 256),
            ("reason_code", reason_code, 128),
        ):
            _bounded(value, field, limit)
        if type(cancel_effect) is not bool:
            raise ForestError("EFFECT_INPUT_INVALID", "Invalid cancellation flag", 422)
        input_digest = _digest([attempt_id, reason_code, cancel_effect])
        existing = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND abandonment_operation_id=%s"
                ).format(self._table("attempt_abandonments")),
                (self.store.bot_id, abandonment_operation_id),
            )
        ).fetchone()
        if existing is not None:
            if existing["input_digest"] != input_digest:
                raise ForestError("EFFECT_ABANDONMENT_CONFLICT", "Abandonment input differs", 409)
            return dict(existing)
        effect = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND effect_operation_id=%s FOR UPDATE"
                ).format(self._table("effect_operations")),
                (self.store.bot_id, effect_operation_id),
            )
        ).fetchone()
        attempt = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND attempt_id=%s FOR UPDATE").format(
                    self._table("effect_attempts")
                ),
                (self.store.bot_id, attempt_id),
            )
        ).fetchone()
        existing = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND abandonment_operation_id=%s"
                ).format(self._table("attempt_abandonments")),
                (self.store.bot_id, abandonment_operation_id),
            )
        ).fetchone()
        if existing is not None:
            if existing["input_digest"] != input_digest:
                raise ForestError("EFFECT_ABANDONMENT_CONFLICT", "Abandonment input differs", 409)
            return dict(existing)
        if (
            effect is None
            or attempt is None
            or effect["current_attempt_id"] != attempt_id
            or attempt["effect_operation_id"] != effect_operation_id
            or attempt["state"] != "PREPARED"
        ):
            raise ForestError("EFFECT_NOT_ABANDONABLE", "Only a prepared attempt can be abandoned", 409)
        row = await (
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,abandonment_operation_id,attempt_id,input_digest,"
                    "reason_code,cancel_effect) VALUES (%s,%s,%s,%s,%s,%s) RETURNING *"
                ).format(self._table("attempt_abandonments")),
                (
                    self.store.bot_id,
                    abandonment_operation_id,
                    attempt_id,
                    input_digest,
                    reason_code,
                    cancel_effect,
                ),
            )
        ).fetchone()
        await conn.execute(
            sql.SQL(
                "UPDATE {} SET state='ABANDONED',settled_at=clock_timestamp() "
                "WHERE bot_id=%s AND attempt_id=%s"
            ).format(self._table("effect_attempts")),
            (self.store.bot_id, attempt_id),
        )
        await conn.execute(
            sql.SQL(
                "UPDATE {} SET state=%s,current_attempt_id=NULL,revision=revision+1,"
                "updated_at=clock_timestamp() WHERE bot_id=%s AND effect_operation_id=%s"
            ).format(self._table("effect_operations")),
            ("CANCELLED" if cancel_effect else "PENDING", self.store.bot_id, effect_operation_id),
        )
        return dict(row)

    async def abandon_prepared(self, **values) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.abandon_prepared_in(conn, **values)
