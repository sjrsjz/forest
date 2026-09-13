"""Host-owned original-image capture, independent of Agent/model capabilities.

Capture does not grant recall eligibility. New pixels only enter PayloadStore;
the caller of materialize_in must already hold current source/cut authority.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, timedelta
from uuid import uuid4

import httpx
from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.models import MediaSnapshot
from forest.packages import digest_json
from forest.storage.media import _url, _validate_owned
from forest.storage.memory_config import MemoryConfig
from forest.storage.payloads import PayloadStore


@dataclass(frozen=True)
class CaptureClaim:
    bot_id: str
    capture_id: str
    attempt_id: str
    claim_epoch: int
    owner_id: str
    config_revision: int
    deletion_epoch: int
    storage_generation: int
    maintenance_epoch: int
    url: str = field(repr=False)


class MemoryMediaStore:
    def __init__(self, messages, config=None):
        self.messages, self.store = messages, messages.store
        self.config = config or MemoryConfig(self.store)
        self.payloads = PayloadStore(self.store)

    @staticmethod
    def capture_id(bot_id, source_unit_id, part_id):
        return "memory-capture:" + digest_json([bot_id, source_unit_id, part_id])

    @staticmethod
    def _public(row):
        return {
            key: row[key]
            for key in (
                "capture_id",
                "source_unit_id",
                "part_id",
                "status",
                "attempt_count",
                "config_revision",
                "error_code",
                "next_attempt_at",
                "payload_id",
                "blob_digest",
            )
        }

    async def _quota_in(self, conn):
        await conn.execute(
            "INSERT INTO public.bot_memory_media_quota(bot_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (self.store.bot_id,),
        )
        return await (
            await conn.execute(
                "SELECT * FROM public.bot_memory_media_quota WHERE bot_id=%s FOR UPDATE",
                (self.store.bot_id,),
            )
        ).fetchone()

    async def _settle_in(self, conn, attempt, *, actual_bytes=None, retained_bytes=0):
        """Settle exactly this attempt once; unknown download cost stays charged."""
        if attempt["quota_settled"]:
            return
        await self._quota_in(conn)
        charge = attempt["reserved_download_bytes"] if actual_bytes is None else actual_bytes
        if not 0 <= charge <= attempt["reserved_download_bytes"]:
            raise ForestError(
                "MEDIA_ACCOUNTING_CONFLICT", "Capture cost exceeds its reservation", 409
            )
        await conn.execute(
            "UPDATE public.bot_memory_media_daily SET charged_bytes=charged_bytes-%s WHERE bot_id=%s AND budget_day=%s",
            (attempt["reserved_download_bytes"] - charge, self.store.bot_id, attempt["budget_day"]),
        )
        await conn.execute(
            "UPDATE public.bot_memory_media_quota SET reserved_bytes=reserved_bytes-%s,retained_bytes=retained_bytes+%s WHERE bot_id=%s",
            (attempt["reserved_storage_bytes"], retained_bytes, self.store.bot_id),
        )
        await conn.execute(
            "UPDATE public.bot_memory_media_attempts SET quota_settled=true,charged_bytes=%s WHERE bot_id=%s AND attempt_id=%s",
            (charge, self.store.bot_id, attempt["attempt_id"]),
        )

    async def _reap_in(self, conn, lease, limit):
        # Current host may close an expired/previous-incarnation reservation,
        # but can never publish its late bytes. A started GET remains charged.
        stale = await (
            await conn.execute(
                "SELECT a.* FROM public.bot_memory_media_attempts a WHERE a.bot_id=%s AND NOT quota_settled "
                "AND (deadline_at<=clock_timestamp() OR host_incarnation<>%s OR host_owner_epoch<>%s) "
                "ORDER BY deadline_at,attempt_id LIMIT %s FOR UPDATE",
                (self.store.bot_id, lease.incarnation, lease.owner_epoch, limit),
            )
        ).fetchall()
        for attempt in stale:
            await self._settle_in(
                conn, attempt, actual_bytes=0 if attempt["status"] == "CLAIMED" else None
            )
            await conn.execute(
                "UPDATE public.bot_memory_media_attempts SET status='ABANDONED',error_code='MEDIA_CAPTURE_ABANDONED',completed_at=clock_timestamp() WHERE bot_id=%s AND attempt_id=%s",
                (self.store.bot_id, attempt["attempt_id"]),
            )
            await conn.execute(
                "UPDATE public.bot_memory_media_heads SET status='RETRYABLE',active_attempt_id=NULL,error_code='MEDIA_CAPTURE_ABANDONED',next_attempt_at=clock_timestamp()+interval '1 second',updated_at=clock_timestamp() "
                "WHERE bot_id=%s AND capture_id=%s AND active_attempt_id=%s AND status='CLAIMED'",
                (self.store.bot_id, attempt["capture_id"], attempt["attempt_id"]),
            )
        # Actual body deletion, rather than revocation alone, releases storage.
        deleted = await (
            await conn.execute(
                "SELECT * FROM public.bot_memory_media_heads h WHERE h.bot_id=%s AND storage_accounted "
                "AND NOT EXISTS(SELECT 1 FROM public.bot_payload_bodies b WHERE b.bot_id=h.bot_id AND b.payload_id=h.payload_id) "
                "ORDER BY capture_id LIMIT %s FOR UPDATE",
                (self.store.bot_id, limit),
            )
        ).fetchall()
        if deleted:
            await self._quota_in(conn)
        for row in deleted:
            await conn.execute(
                "UPDATE public.bot_memory_media_quota SET retained_bytes=retained_bytes-%s WHERE bot_id=%s",
                (row["byte_length"], self.store.bot_id),
            )
            await conn.execute(
                "UPDATE public.bot_memory_media_heads SET storage_accounted=false,status='REVOKED' WHERE bot_id=%s AND capture_id=%s",
                (self.store.bot_id, row["capture_id"]),
            )

    async def ensure(self, lease, source_unit_ids):
        """Ensure pending captures for specific sources without a global scan.

        This creates capture metadata only. Capture authorization remains in
        claim/dispatch/final publication; it does not make queued input eligible.
        """
        return await self.scan(lease, limit=256, source_unit_ids=source_unit_ids)

    async def scan(self, lease, *, limit=32, source_unit_ids=None):
        if type(limit) is not int or not 1 <= limit <= 256:
            raise ValueError("Capture scan limit")
        selected = None
        if source_unit_ids is not None:
            if not isinstance(source_unit_ids, (list, tuple)):
                raise ValueError("Capture source IDs")
            selected = list(source_unit_ids)
            if (
                not 1 <= len(selected) <= 32
                or any(
                    not isinstance(value, str) or not 1 <= len(value) <= 256 for value in selected
                )
                or len(set(selected)) != len(selected)
            ):
                raise ValueError("Capture source IDs")
        try:
            saved = await self.config.runtime(lease)
        except ForestError as exc:
            if exc.code == "MEMORY_NOT_CONFIGURED":
                return []
            raise
        if not saved["values"]["enabled"]:
            return []
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            await self.config.guard_in(conn, lease, saved["revision"])
            await self.payloads.lock_in(conn)
            await self._reap_in(conn, lease, limit)
            missing = await (
                await conn.execute(
                    "SELECT DISTINCT m.source_unit_id,m.part_id,u.source_revision FROM public.bot_memory_source_media m "
                    "JOIN public.bot_memory_source_units u USING(bot_id,source_unit_id) "
                    "WHERE m.bot_id=%s AND (%s::text[] IS NULL OR m.source_unit_id=ANY(%s::text[])) "
                    "AND NOT EXISTS(SELECT 1 FROM public.bot_memory_media_heads h "
                    "WHERE h.bot_id=m.bot_id AND h.source_unit_id=m.source_unit_id AND h.part_id=m.part_id) "
                    "ORDER BY m.source_unit_id,m.part_id LIMIT %s",
                    (self.store.bot_id, selected, selected, limit),
                )
            ).fetchall()
            for row in missing:
                await conn.execute(
                    "INSERT INTO public.bot_memory_media_heads(bot_id,capture_id,source_unit_id,source_revision,part_id,media_ref) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (
                        self.store.bot_id,
                        self.capture_id(self.store.bot_id, row["source_unit_id"], row["part_id"]),
                        row["source_unit_id"],
                        row["source_revision"],
                        row["part_id"],
                        "source-media:" + digest_json([row["source_unit_id"], row["part_id"]]),
                    ),
                )
            result = await (
                await conn.execute(
                    "SELECT * FROM public.bot_memory_media_heads WHERE bot_id=%s "
                    "AND (%s::text[] IS NULL OR source_unit_id=ANY(%s::text[])) AND "
                    "(status IN ('PENDING','RETRYABLE','QUOTA_EXCEEDED') OR (status='REJECTED' AND config_revision<>%s)) "
                    "AND (next_attempt_at IS NULL OR next_attempt_at<=clock_timestamp()) ORDER BY updated_at,capture_id LIMIT %s",
                    (self.store.bot_id, selected, selected, saved["revision"], limit),
                )
            ).fetchall()
            await self.store._host(conn, lease)
            return [self._public(row) for row in result]

    async def _head_in(self, conn, capture_id):
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_memory_media_heads WHERE bot_id=%s AND capture_id=%s FOR UPDATE",
                (self.store.bot_id, capture_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError("MEDIA_CAPTURE_NOT_FOUND", "Capture does not exist in this Bot", 404)
        return row

    async def _source_in(self, conn, head, epoch, *, inbound_media_ref=None):
        unit = await (
            await conn.execute(
                "SELECT * FROM public.bot_memory_source_units WHERE bot_id=%s AND source_unit_id=%s",
                (self.store.bot_id, head["source_unit_id"]),
            )
        ).fetchone()
        value = await self.payloads.read_json_in(
            conn, unit["payload_id"], expected_deletion_epoch=epoch
        )
        part = next((p for p in value["parts"] if p["part_id"] == head["part_id"]), None)
        if not part or part["type"] != "image" or part["media_ref"] != head["media_ref"]:
            raise ForestError("MEDIA_SOURCE_MISMATCH", "Capture does not match a source image", 409)
        forbidden = await (
            await conn.execute(
                "SELECT 1 FROM public.bot_memory_source_occurrences o JOIN public.bot_message_inbox i USING(bot_id,event_id) "
                "LEFT JOIN public.bot_conversation_triggers t USING(bot_id,event_id) "
                "WHERE o.bot_id=%s AND o.source_unit_id=%s AND (i.context_disposition<>'READABLE' "
                "OR (i.origin='job' AND t.status='CANCELLED') OR EXISTS(SELECT 1 FROM public.bot_message_echo_aliases a WHERE a.bot_id=i.bot_id AND a.event_id=i.event_id)) LIMIT 1",
                (self.store.bot_id, head["source_unit_id"]),
            )
        ).fetchone()
        if forbidden:
            raise ForestError("MEDIA_SOURCE_REVOKED", "Source is no longer admissible", 403)
        observations = await (
            await conn.execute(
                "SELECT m.media_ref,m.source,m.part_index,o.occurrence_id,o.event_id,o.conversation_id,i.segments "
                "FROM public.bot_memory_source_media sm JOIN public.bot_memory_source_occurrences o USING(bot_id,occurrence_id) "
                "JOIN public.bot_inbound_media_refs m ON m.bot_id=sm.bot_id AND m.media_ref=sm.media_ref AND m.event_id=o.event_id "
                "JOIN public.bot_message_inbox i ON i.bot_id=o.bot_id AND i.event_id=o.event_id "
                "WHERE sm.bot_id=%s AND sm.source_unit_id=%s AND sm.part_id=%s AND (%s::text IS NULL OR m.media_ref=%s) "
                "ORDER BY o.accepted_at,o.occurrence_id LIMIT 32",
                (
                    self.store.bot_id,
                    head["source_unit_id"],
                    head["part_id"],
                    inbound_media_ref,
                    inbound_media_ref,
                ),
            )
        ).fetchall()
        valid = [
            row
            for row in observations
            if str(row["part_index"]) == head["part_id"]
            and row["part_index"] < len(row["segments"])
            and row["segments"][row["part_index"]].get("media_ref") == row["media_ref"]
        ]
        if not valid:
            raise ForestError("MEDIA_SOURCE_MISMATCH", "Image has no matching observation", 409)
        return valid

    async def _reject_in(self, conn, head, *, status, code, revision, delay=None):
        await conn.execute(
            "UPDATE public.bot_memory_media_heads SET status=%s,error_code=%s,config_revision=%s,next_attempt_at="
            "CASE WHEN %s::double precision IS NULL THEN NULL ELSE clock_timestamp()+(%s * interval '1 second') END,updated_at=clock_timestamp() WHERE bot_id=%s AND capture_id=%s",
            (status, code, revision, delay, delay, self.store.bot_id, head["capture_id"]),
        )

    async def claim(self, lease, capture_id, owner_id):
        if not isinstance(owner_id, str) or not 1 <= len(owner_id) <= 256:
            raise ValueError("Capture owner")
        saved = await self.config.runtime(lease)
        async with self.store.pool.connection() as conn, conn.transaction():
            runtime = await self.store._runtime(conn)
            await self.store._host(conn, lease, runtime)
            saved = await self.config.guard_in(conn, lease, saved["revision"])
            if not runtime["data_admission"]:
                return None
            settings = saved["values"]
            clock = await self.payloads.lock_in(conn)
            await self._reap_in(conn, lease, 32)
            head = await self._head_in(conn, capture_id)
            if (
                head["status"] in {"CAPTURED", "CLAIMED", "REVOKED", "UNAVAILABLE"}
                or (head["status"] == "REJECTED" and head["config_revision"] == saved["revision"])
                or (
                    head["next_attempt_at"]
                    and head["next_attempt_at"] > await self.store._now(conn)
                )
            ):
                await self.store._host(conn, lease)
                return None
            if head["attempt_count"] >= settings["capture_max_attempts"]:
                await self._reject_in(
                    conn,
                    head,
                    status="UNAVAILABLE",
                    code="MEDIA_ATTEMPTS_EXHAUSTED",
                    revision=saved["revision"],
                )
                await self.store._host(conn, lease)
                return None
            try:
                observations = await self._source_in(conn, head, clock["deletion_epoch"])
                origins = {
                    _url(value, origin_only=True)[1] for value in settings["media_allowed_origins"]
                }
                selected = None
                for observation in observations:
                    try:
                        url, origin = _url(observation["source"].get("uri"))
                    except (ForestError, AttributeError):
                        continue
                    if origin in origins:
                        selected = observation
                        break
                if selected is None:
                    raise ForestError("MEDIA_ORIGIN_DENIED", "No permitted image observation", 403)
            except ForestError as exc:
                await self._reject_in(
                    conn,
                    head,
                    status="REVOKED"
                    if exc.code in {"PAYLOAD_ERASED", "SOURCE_SUPPRESSED", "MEDIA_SOURCE_REVOKED"}
                    else "REJECTED",
                    code=exc.code,
                    revision=saved["revision"],
                )
                await self.store._host(conn, lease)
                return None
            active = await (
                await conn.execute(
                    "SELECT count(*) AS n FROM public.bot_memory_media_attempts WHERE bot_id=%s AND NOT quota_settled",
                    (self.store.bot_id,),
                )
            ).fetchone()
            if active["n"] >= settings["capture_concurrency"]:
                await self.store._host(conn, lease)
                return None
            maximum = settings["media_max_bytes"]
            # Raw streaming may return one final bounded chunk proving oversize.
            reserve = maximum + min(maximum, 65536)
            now = await self.store._now(conn)
            day = now.astimezone(UTC).date()
            await conn.execute(
                "INSERT INTO public.bot_memory_media_daily(bot_id,budget_day) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (self.store.bot_id, day),
            )
            daily = await (
                await conn.execute(
                    "SELECT charged_bytes FROM public.bot_memory_media_daily WHERE bot_id=%s AND budget_day=%s FOR UPDATE",
                    (self.store.bot_id, day),
                )
            ).fetchone()
            quota = await self._quota_in(conn)
            error = (
                "MEDIA_DAILY_QUOTA_EXCEEDED"
                if daily["charged_bytes"] + reserve > settings["capture_daily_bytes"]
                else "MEDIA_STORAGE_QUOTA_EXCEEDED"
                if quota["retained_bytes"] + quota["reserved_bytes"] + maximum
                > settings["capture_storage_bytes"]
                else None
            )
            if error:
                await self._reject_in(
                    conn,
                    head,
                    status="QUOTA_EXCEEDED",
                    code=error,
                    revision=saved["revision"],
                    delay=30,
                )
                await self.store._host(conn, lease)
                return None
            attempt_id, epoch = str(uuid4()), head["claim_epoch"] + 1
            await conn.execute(
                "INSERT INTO public.bot_memory_media_attempts(bot_id,attempt_id,capture_id,claim_epoch,owner_id,host_id,host_incarnation,host_owner_epoch,storage_generation,maintenance_epoch,config_revision,deletion_epoch,source_unit_id,source_revision,part_id,occurrence_id,inbound_media_ref,source_digest,origin,deadline_at,budget_day,reserved_download_bytes,reserved_storage_bytes) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id,
                    attempt_id,
                    capture_id,
                    epoch,
                    owner_id,
                    lease.host_id,
                    lease.incarnation,
                    lease.owner_epoch,
                    runtime["storage_generation"],
                    runtime["maintenance_epoch"],
                    saved["revision"],
                    clock["deletion_epoch"],
                    head["source_unit_id"],
                    head["source_revision"],
                    head["part_id"],
                    selected["occurrence_id"],
                    selected["media_ref"],
                    digest_json(selected["source"]),
                    origin,
                    now + timedelta(seconds=settings["capture_timeout"] + 30),
                    day,
                    reserve,
                    maximum,
                ),
            )
            await conn.execute(
                "UPDATE public.bot_memory_media_daily SET charged_bytes=charged_bytes+%s WHERE bot_id=%s AND budget_day=%s",
                (reserve, self.store.bot_id, day),
            )
            await conn.execute(
                "UPDATE public.bot_memory_media_quota SET reserved_bytes=reserved_bytes+%s WHERE bot_id=%s",
                (maximum, self.store.bot_id),
            )
            await conn.execute(
                "UPDATE public.bot_memory_media_heads SET status='CLAIMED',attempt_count=attempt_count+1,claim_epoch=%s,active_attempt_id=%s,config_revision=%s,next_attempt_at=NULL,error_code=NULL,updated_at=clock_timestamp() WHERE bot_id=%s AND capture_id=%s",
                (epoch, attempt_id, saved["revision"], self.store.bot_id, capture_id),
            )
            await self.store._host(conn, lease)
            return CaptureClaim(
                self.store.bot_id,
                capture_id,
                attempt_id,
                epoch,
                owner_id,
                saved["revision"],
                clock["deletion_epoch"],
                runtime["storage_generation"],
                runtime["maintenance_epoch"],
                url,
            )

    async def _attempt_in(self, conn, lease, claim):
        if type(claim) is not CaptureClaim or claim.bot_id != self.store.bot_id:
            raise ForestError("MEDIA_CAPTURE_STALE", "Capture belongs to another Bot", 409)
        head = await self._head_in(conn, claim.capture_id)
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_memory_media_attempts WHERE bot_id=%s AND attempt_id=%s FOR UPDATE",
                (self.store.bot_id, claim.attempt_id),
            )
        ).fetchone()
        if (
            row is None
            or row["capture_id"] != claim.capture_id
            or row["claim_epoch"] != claim.claim_epoch
            or row["owner_id"] != claim.owner_id
            or row["config_revision"] != claim.config_revision
            or row["deletion_epoch"] != claim.deletion_epoch
            or row["storage_generation"] != claim.storage_generation
            or row["maintenance_epoch"] != claim.maintenance_epoch
            or row["host_id"] != lease.host_id
            or row["host_incarnation"] != lease.incarnation
            or row["host_owner_epoch"] != lease.owner_epoch
        ):
            raise ForestError("MEDIA_CAPTURE_STALE", "Capture ownership differs", 409)
        return head, row

    async def _final_time_in(self, conn, lease, attempt):
        await self.store._host(conn, lease)
        if attempt["deadline_at"] <= await self.store._now(conn):
            raise ForestError(
                "MEDIA_CAPTURE_STALE", "Capture expired before transaction commit", 409
            )

    async def _permit(self, lease, claim):
        async with self.store.pool.connection() as conn, conn.transaction():
            runtime = await self.store._runtime(conn)
            await self.store._host(conn, lease, runtime)
            saved = await self.config.guard_in(conn, lease, claim.config_revision)
            if not runtime["data_admission"]:
                raise ForestError("DATA_ADMISSION_CLOSED", "Capture data admission is paused", 409)
            await self.payloads.lock_in(conn)
            head, attempt = await self._attempt_in(conn, lease, claim)
            self._maintenance(runtime, attempt)
            if (
                head["active_attempt_id"] != claim.attempt_id
                or attempt["status"] != "CLAIMED"
                or attempt["quota_settled"]
                or attempt["deadline_at"] <= await self.store._now(conn)
            ):
                raise ForestError("MEDIA_CAPTURE_STALE", "Capture is no longer dispatchable", 409)
            observations = await self._source_in(
                conn, head, claim.deletion_epoch, inbound_media_ref=attempt["inbound_media_ref"]
            )
            source = observations[0]
            url, origin = _url(source["source"].get("uri"))
            if (
                url != claim.url
                or origin != attempt["origin"]
                or digest_json(source["source"]) != attempt["source_digest"]
                or origin
                not in {
                    _url(value, origin_only=True)[1]
                    for value in saved["values"]["media_allowed_origins"]
                }
            ):
                raise ForestError("MEDIA_SOURCE_MISMATCH", "Capture source changed", 409)
            await conn.execute(
                "UPDATE public.bot_memory_media_attempts SET status='DISPATCHING' WHERE bot_id=%s AND attempt_id=%s",
                (self.store.bot_id, claim.attempt_id),
            )
            await self._final_time_in(conn, lease, attempt)
            return saved["values"]

    @staticmethod
    def _maintenance(runtime, attempt):
        if runtime["storage_generation"] != attempt["storage_generation"]:
            raise ForestError("MEDIA_STORAGE_REVOKED", "Capture storage generation changed", 409)
        if (
            not runtime["data_admission"]
            or runtime["maintenance_epoch"] != attempt["maintenance_epoch"]
        ):
            raise ForestError(
                "MEDIA_MAINTENANCE_REVOKED", "Capture maintenance permission changed", 409
            )

    async def _download(self, url, settings):
        maximum = settings["media_max_bytes"]
        async with (
            httpx.AsyncClient(
                timeout=settings["capture_timeout"],
                trust_env=False,
                follow_redirects=False,
                transport=httpx.AsyncHTTPTransport(retries=0, trust_env=False),
            ) as client,
            asyncio.timeout(settings["capture_timeout"]),
        ):
            async with client.stream(
                "GET", url, headers={"Accept-Encoding": "identity", "Accept": "image/*"}
            ) as response:
                if response.status_code != 200:
                    raise ForestError(
                        "MEDIA_HTTP_REJECTED", "Image server rejected the request", 422
                    )
                if response.headers.get("Content-Encoding", "identity").lower() not in {
                    "",
                    "identity",
                }:
                    raise ForestError(
                        "MEDIA_ENCODING_UNSUPPORTED", "Encoded image response is not accepted", 422
                    )
                length = response.headers.get("Content-Length")
                if length is not None and (not length.isdigit() or int(length) > maximum):
                    raise ForestError(
                        "MEDIA_SIZE_EXCEEDED", "Image response exceeds capture budget", 422
                    )
                content = bytearray()
                async for chunk in response.aiter_raw(chunk_size=min(maximum, 65536)):
                    if len(content) + len(chunk) > maximum:
                        raise ForestError(
                            "MEDIA_SIZE_EXCEEDED", "Image response exceeds capture budget", 422
                        )
                    content.extend(chunk)
                return bytes(content)

    async def finish(
        self, lease, claim, *, content=None, properties=None, error=None, actual_bytes=None
    ):
        async with self.store.pool.connection() as conn, conn.transaction():
            runtime = await self.store._runtime(conn)
            await self.store._host(conn, lease, runtime)
            try:
                saved = await self.config.guard_in(conn, lease, claim.config_revision)
            except ForestError as exc:
                saved, error = None, exc.code
            await self.payloads.lock_in(conn)
            head, attempt = await self._attempt_in(conn, lease, claim)
            try:
                self._maintenance(runtime, attempt)
            except ForestError as exc:
                error = exc.code
            if attempt["quota_settled"]:
                await self.store._host(conn, lease)
                return self._public(head)
            if (
                head["active_attempt_id"] != claim.attempt_id
                or attempt["status"] not in {"CLAIMED", "DISPATCHING"}
                or attempt["deadline_at"] <= await self.store._now(conn)
            ):
                raise ForestError("MEDIA_CAPTURE_STALE", "Capture result is stale", 409)
            if error is None:
                try:
                    source = (
                        await self._source_in(
                            conn,
                            head,
                            claim.deletion_epoch,
                            inbound_media_ref=attempt["inbound_media_ref"],
                        )
                    )[0]
                    if digest_json(source["source"]) != attempt["source_digest"]:
                        raise ForestError("MEDIA_SOURCE_MISMATCH", "Capture source changed", 409)
                except ForestError as exc:
                    error = exc.code
            if error is None and (
                attempt["status"] != "DISPATCHING"
                or not content
                or properties is None
                or len(content) > attempt["reserved_storage_bytes"]
            ):
                raise ForestError(
                    "MEDIA_CAPTURE_INVALID", "Capture has no complete validated pixels", 409
                )
            payload = None
            if error is None:
                payload = await self.payloads.put_bytes_in(
                    conn,
                    owner_kind="media.capture",
                    owner_id=head["capture_id"],
                    value=content,
                    sources=(
                        SourceRef(head["source_unit_id"], head["source_revision"], head["part_id"]),
                    ),
                    expected_deletion_epoch=claim.deletion_epoch,
                )
                if payload.state != "ACTIVE":
                    error = "SOURCE_SUPPRESSED"
            if error is None:
                status = "CAPTURED"
                await conn.execute(
                    "UPDATE public.bot_memory_media_heads SET payload_id=%s,blob_digest=%s,mime=%s,width=%s,height=%s,byte_length=%s,storage_accounted=true WHERE bot_id=%s AND capture_id=%s",
                    (
                        payload.payload_id,
                        payload.digest,
                        properties["mime"],
                        properties["width"],
                        properties["height"],
                        len(content),
                        self.store.bot_id,
                        head["capture_id"],
                    ),
                )
                await conn.execute(
                    "UPDATE public.bot_memory_clock SET fact_revision=fact_revision+1 WHERE bot_id=%s",
                    (self.store.bot_id,),
                )
            elif error in {
                "DELETION_CONFLICT",
                "SOURCE_SUPPRESSED",
                "PAYLOAD_ERASED",
                "MEDIA_SOURCE_REVOKED",
                "MEDIA_STORAGE_REVOKED",
            }:
                status = "REVOKED"
            elif error in {
                "MEDIA_TRANSPORT_FAILED",
                "MEDIA_CANCELLED",
                "DATA_ADMISSION_CLOSED",
                "MEDIA_MAINTENANCE_REVOKED",
            }:
                status = (
                    "RETRYABLE"
                    if saved and head["attempt_count"] < saved["values"]["capture_max_attempts"]
                    else "UNAVAILABLE"
                )
            else:
                status = "REJECTED"
            await self._settle_in(
                conn,
                attempt,
                actual_bytes=actual_bytes,
                retained_bytes=len(content) if status == "CAPTURED" else 0,
            )
            await conn.execute(
                "UPDATE public.bot_memory_media_attempts SET status=%s,error_code=%s,completed_at=clock_timestamp() WHERE bot_id=%s AND attempt_id=%s",
                (
                    "SUCCEEDED"
                    if status == "CAPTURED"
                    else "RETRYABLE"
                    if status in {"RETRYABLE", "UNAVAILABLE"}
                    else "REVOKED"
                    if status == "REVOKED"
                    else "REJECTED",
                    error,
                    self.store.bot_id,
                    claim.attempt_id,
                ),
            )
            await conn.execute(
                "UPDATE public.bot_memory_media_heads SET status=%s,active_attempt_id=NULL,error_code=%s,next_attempt_at="
                "CASE WHEN %s THEN clock_timestamp()+(%s * interval '1 second') ELSE NULL END,updated_at=clock_timestamp() WHERE bot_id=%s AND capture_id=%s",
                (
                    status,
                    error,
                    status == "RETRYABLE",
                    min(60, 2 ** head["attempt_count"]),
                    self.store.bot_id,
                    head["capture_id"],
                ),
            )
            result = self._public(await self._head_in(conn, claim.capture_id))
            await self._final_time_in(conn, lease, attempt)
            return result

    async def execute(self, lease, claim):
        content = properties = None
        started, actual_bytes, error = False, None, None
        try:
            settings = await self._permit(lease, claim)
            started = True
            content = await self._download(claim.url, settings)
            actual_bytes = len(content)
            properties = await _validate_owned(content)
        except asyncio.CancelledError:
            cleanup = asyncio.create_task(
                self.finish(
                    lease,
                    claim,
                    error="MEDIA_CANCELLED",
                    actual_bytes=actual_bytes if started else 0,
                )
            )
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    continue
            # Observe exceptions; a stale owner cannot settle another host's
            # reservation. Its claim remains for durable host recovery.
            if not cleanup.cancelled():
                cleanup.exception()
            raise
        except (httpx.HTTPError, TimeoutError):
            error = "MEDIA_TRANSPORT_FAILED"
        except ForestError as exc:
            if exc.code in {"STALE_HOST", "STORAGE_SEALED", "MEDIA_CAPTURE_STALE"}:
                raise
            error = exc.code
        return await self.finish(
            lease,
            claim,
            content=content,
            properties=properties,
            error=error,
            actual_bytes=actual_bytes if started else 0,
        )

    async def capture_once(self, lease, capture_id, owner_id="memory-media"):
        claim = await self.claim(lease, capture_id, owner_id)
        if claim is None:
            return None
        return await self.execute(lease, claim)

    async def materialize_in(
        self,
        conn,
        source_unit_ids,
        *,
        expected_deletion_epoch,
        part_refs=None,
        byte_limit=8 * 1024 * 1024,
    ):
        """Read pixels after caller's source authorization in the same transaction.

        This host-only helper grants no source/cut permission. It must never be
        wired directly to arbitrary Web or Worker IDs.
        """
        ids = list(source_unit_ids)
        if len(ids) > 256 or any(not isinstance(value, str) for value in ids):
            raise ValueError("Capture source limit")
        if type(byte_limit) is not int or not 1 <= byte_limit <= 32 * 1024 * 1024:
            raise ValueError("Capture materialization byte limit")
        requested = None if part_refs is None else [list(ref) for ref in part_refs]
        if requested is not None and (
            len(requested) > 32
            or any(
                len(ref) != 2 or ref[0] not in ids or not isinstance(ref[1], str)
                for ref in requested
            )
        ):
            raise ValueError("Capture part references")
        await self.payloads._epoch_in(conn, expected_deletion_epoch)
        rows = await (
            await conn.execute(
                "SELECT h.* FROM public.bot_memory_media_heads h JOIN public.bot_payload_headers p USING(bot_id,payload_id) "
                "WHERE h.bot_id=%s AND h.source_unit_id=ANY(%s) AND h.status='CAPTURED' AND p.state='ACTIVE' "
                "AND (%s::jsonb IS NULL OR EXISTS(SELECT 1 FROM jsonb_array_elements(%s::jsonb) q WHERE q->>0=h.source_unit_id AND q->>1=h.part_id)) "
                "ORDER BY h.source_unit_id,h.part_id LIMIT 256",
                (
                    self.store.bot_id,
                    ids,
                    Jsonb(requested) if requested is not None else None,
                    Jsonb(requested) if requested is not None else None,
                ),
            )
        ).fetchall()
        result, remaining = {}, byte_limit
        for row in rows:
            if row["byte_length"] > remaining or len(result) == 32:
                if requested is not None:
                    raise ForestError(
                        "MEDIA_MATERIALIZATION_BUDGET",
                        "Requested pixels exceed the bounded materialization budget",
                        413,
                    )
                continue
            content = await self.payloads.read_bytes_in(
                conn, row["payload_id"], expected_deletion_epoch=expected_deletion_epoch
            )
            result[(row["source_unit_id"], row["part_id"])] = MediaSnapshot(
                self.store.bot_id,
                row["media_ref"],
                1,
                row["source_unit_id"],
                row["part_id"],
                row["mime"],
                content,
                row["blob_digest"],
                row["width"],
                row["height"],
            )
            remaining -= len(content)
        return result
