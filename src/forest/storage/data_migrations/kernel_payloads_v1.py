"""Frozen 037: import Kernel/state bodies without rewriting existing facts."""

import hashlib
import json

from psycopg import sql
from psycopg.types.json import Jsonb

from forest.storage.migration_v1 import legacy_source_ref
from forest.storage.payloads import PayloadStore


def digest(value):
    return (
        "sha256:"
        + hashlib.sha256(
            json.dumps(
                value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
            ).encode()
        ).hexdigest()
    )


def verify(value, expected):
    actual = digest(value)
    if expected not in {actual, actual.removeprefix("sha256:")}:
        raise ValueError("Legacy Kernel content differs from its immutable digest")


async def pages(conn, bot_id, table, keys, fields):
    after = None
    identity = sql.SQL(",").join(sql.Identifier(key) for key in keys)
    present = sql.SQL(",").join(
        sql.SQL("{} IS NOT NULL AS {}").format(
            sql.Identifier(field), sql.Identifier("present_" + field)
        )
        for field in fields
    )
    while True:
        predicate = (
            sql.SQL("")
            if after is None
            else sql.SQL("AND ({})>({})").format(
                identity, sql.SQL(",").join(sql.Placeholder() for _ in keys)
            )
        )
        rows = await (
            await conn.execute(
                sql.SQL(
                    "SELECT *,{} FROM public.{} WHERE bot_id=%s {} ORDER BY {} LIMIT 128"
                ).format(present, sql.Identifier(table), predicate, identity),
                (bot_id,) if after is None else (bot_id, *after),
            )
        ).fetchall()
        if not rows:
            break
        for row in rows:
            yield row
        after = tuple(rows[-1][key] for key in keys)


async def migrate_in(conn, store):
    payloads = PayloadStore(store)
    sources = (legacy_source_ref(store.bot_id),)

    async def put(kind, owner, value):
        return await payloads.import_legacy_json_in(
            conn, owner_kind=kind, owner_id=owner, value=value, sources=sources
        )

    async for row in pages(conn, store.bot_id, "bot_calls", ("call_id",), ("result",)):
        if not row["present_result"]:
            if row["status"] == "COMPLETED":
                raise ValueError("Completed legacy Call has SQL NULL rather than a JSON result")
            continue
        ref = await put("kernel.result", "result:" + digest([row["call_id"]]), row["result"])
        await conn.execute(
            "UPDATE public.bot_calls SET result=NULL,result_payload_id=%s,result_digest=%s WHERE bot_id=%s AND call_id=%s",
            (ref.payload_id, ref.digest, store.bot_id, row["call_id"]),
        )

    # Current/history copies of an identical immutable state revision resolve
    # to one owner. Conflicting old copies fail the whole migration transaction.
    for table in ("bot_state_revisions", "bot_component_state"):
        keys = ("component_id", "key", "revision")
        async for row in pages(conn, store.bot_id, table, keys, ("value",)):
            if not row["present_value"]:
                if row["revision"] > 0 and not row["deleted"]:
                    raise ValueError("Live legacy state has SQL NULL rather than a JSON value")
                continue
            ref = await put(
                "state.value", "state:" + digest([row[key] for key in keys]), row["value"]
            )
            await conn.execute(
                sql.SQL(
                    'UPDATE public.{} SET value=NULL,value_payload_id=%s,value_digest=%s WHERE bot_id=%s AND component_id=%s AND "key"=%s AND revision=%s'
                ).format(sql.Identifier(table)),
                (ref.payload_id, ref.digest, store.bot_id, *(row[key] for key in keys)),
            )

    async for row in pages(
        conn, store.bot_id, "bot_state_steps", ("call_id", "operation_id"), ("receipt",)
    ):
        receipt = row["receipt"]
        if not isinstance(receipt, dict) or set(receipt) != {
            "operation_id",
            "call_operation_id",
            "component_id",
            "revision",
            "value",
        }:
            raise ValueError("Legacy state receipt has an unknown body/metadata shape")
        revisions = await (
            await conn.execute(
                "SELECT * FROM public.bot_state_revisions WHERE bot_id=%s AND call_id=%s AND operation_id=%s LIMIT 2",
                (store.bot_id, row["call_id"], row["operation_id"]),
            )
        ).fetchall()
        if len(revisions) != 1:
            raise ValueError("Legacy state receipt must identify one immutable state revision")
        revision = revisions[0]
        verify(receipt["value"], revision["value_digest"])
        if (receipt["component_id"], receipt["revision"], receipt["operation_id"]) != (
            revision["component_id"],
            revision["revision"],
            row["operation_id"],
        ):
            raise ValueError("Legacy state receipt identity differs from its state revision")
        await conn.execute(
            "UPDATE public.bot_state_steps SET receipt=NULL,receipt_metadata=%s,value_payload_id=%s,value_digest=%s WHERE bot_id=%s AND call_id=%s AND operation_id=%s",
            (
                Jsonb({k: v for k, v in receipt.items() if k != "value"}),
                revision["value_payload_id"],
                revision["value_digest"],
                store.bot_id,
                row["call_id"],
                row["operation_id"],
            ),
        )

    for table, field, ref_field, digest_field, owner in (
        (
            "bot_state_migration_sources",
            "source_rows",
            "source_payload_id",
            "source_digest",
            "source",
        ),
        (
            "bot_state_migration_results",
            "result_values",
            "result_payload_id",
            "result_digest",
            "candidate",
        ),
    ):
        async for row in pages(conn, store.bot_id, table, ("operation_id",), (field,)):
            if not row["present_" + field]:
                raise ValueError("Legacy state migration body is absent")
            verify(row[field], row[digest_field])
            ref = await put(
                "state.migration", owner + ":" + digest([row["operation_id"]]), row[field]
            )
            await conn.execute(
                sql.SQL(
                    "UPDATE public.{} SET {}=NULL,{}=%s WHERE bot_id=%s AND operation_id=%s"
                ).format(sql.Identifier(table), sql.Identifier(field), sql.Identifier(ref_field)),
                (ref.payload_id, store.bot_id, row["operation_id"]),
            )
    await conn.execute(FINAL_SQL)


FINAL_SQL = r"""
ALTER TABLE public.bot_calls ALTER COLUMN input_legacy_unknown SET DEFAULT false,
 ADD CHECK(result IS NULL),
 ADD CHECK((input_payload_id IS NULL)=input_legacy_unknown),
 ADD CHECK((result_payload_id IS NULL)=(result_digest IS NULL)),
 ADD CHECK(status<>'COMPLETED' OR result_payload_id IS NOT NULL);
ALTER TABLE public.bot_component_state ADD CHECK(value IS NULL),
 ADD CHECK((value_payload_id IS NULL)=(value_digest IS NULL)),
 ADD CHECK(revision=0 OR deleted OR value_payload_id IS NOT NULL);
ALTER TABLE public.bot_state_revisions ADD CHECK(value IS NULL),
 ADD CHECK((value_payload_id IS NULL)=(value_digest IS NULL)),
 ADD CHECK(deleted OR value_payload_id IS NOT NULL);
ALTER TABLE public.bot_state_steps ALTER COLUMN value_payload_id SET NOT NULL,
 ALTER COLUMN value_digest SET NOT NULL, ALTER COLUMN receipt_metadata SET NOT NULL,
 ADD CHECK(receipt IS NULL),
 ADD CHECK(COALESCE(jsonb_typeof(receipt_metadata)='object'
  AND receipt_metadata-ARRAY['operation_id','call_operation_id','component_id','revision']='{}'::jsonb
  AND receipt_metadata ?& ARRAY['operation_id','call_operation_id','component_id','revision']
  AND receipt_metadata->>'operation_id'=operation_id
  AND jsonb_typeof(receipt_metadata->'operation_id')='string'
  AND jsonb_typeof(receipt_metadata->'call_operation_id')='string'
  AND jsonb_typeof(receipt_metadata->'component_id')='string'
  AND jsonb_typeof(receipt_metadata->'revision')='number'
  AND receipt_metadata->>'revision' ~ '^[1-9][0-9]{0,18}$',false));
ALTER TABLE public.bot_state_migration_sources ALTER COLUMN source_payload_id SET NOT NULL, ADD CHECK(source_rows IS NULL);
ALTER TABLE public.bot_state_migration_results ALTER COLUMN result_payload_id SET NOT NULL, ADD CHECK(result_values IS NULL);

CREATE FUNCTION public.forest_kernel_payload_identity() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE body_ref text; fact_digest text; header record;
BEGIN
 IF TG_TABLE_NAME='bot_calls' THEN body_ref:=NEW.result_payload_id; fact_digest:=NEW.result_digest;
 ELSIF TG_TABLE_NAME='bot_state_migration_sources' THEN body_ref:=NEW.source_payload_id; fact_digest:=NEW.source_digest;
 ELSIF TG_TABLE_NAME='bot_state_migration_results' THEN body_ref:=NEW.result_payload_id; fact_digest:=NEW.result_digest;
 ELSE body_ref:=NEW.value_payload_id; fact_digest:=NEW.value_digest; END IF;
 IF body_ref IS NOT NULL THEN
  SELECT digest,encoding INTO header FROM public.bot_payload_headers WHERE bot_id=NEW.bot_id AND payload_id=body_ref;
  IF NOT FOUND OR header.encoding<>'json' OR fact_digest IS NULL
   OR fact_digest NOT IN (header.digest,replace(header.digest,'sha256:','')) THEN
   RAISE EXCEPTION 'Kernel payload reference and fact digest must match';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE FUNCTION public.forest_kernel_call_payload_once() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  IF NEW.input_legacy_unknown THEN RAISE EXCEPTION 'Only migrated Calls have unknown input'; END IF;
  RETURN NEW;
 END IF;
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Kernel Call facts are permanent'; END IF;
 IF (OLD.input_payload_id,OLD.input_legacy_unknown) IS DISTINCT FROM (NEW.input_payload_id,NEW.input_legacy_unknown) THEN
  RAISE EXCEPTION 'Kernel input payload identity is permanent';
 END IF;
 IF OLD.result_digest IS NOT NULL AND (OLD.result_payload_id,OLD.result_digest,OLD.status)
  IS DISTINCT FROM (NEW.result_payload_id,NEW.result_digest,NEW.status) THEN
  RAISE EXCEPTION 'Kernel result identity is permanent';
 END IF;
 RETURN NEW;
END $$;
CREATE FUNCTION public.forest_kernel_state_head() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'State revision fences are permanent'; END IF;
 IF (OLD.bot_id,OLD.component_id,OLD.key) IS DISTINCT FROM (NEW.bot_id,NEW.component_id,NEW.key)
  OR (NEW.revision<>OLD.revision AND NEW.revision<>OLD.revision+1)
  OR (NEW.revision=OLD.revision AND (OLD.deleted,OLD.value_payload_id,OLD.value_digest)
    IS DISTINCT FROM (NEW.deleted,NEW.value_payload_id,NEW.value_digest)) THEN
  RAISE EXCEPTION 'State head must preserve identity and advance exactly one revision';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER bot_kernel_call_payload_once BEFORE INSERT OR UPDATE OR DELETE ON public.bot_calls
 FOR EACH ROW EXECUTE FUNCTION public.forest_kernel_call_payload_once();
CREATE TRIGGER bot_kernel_state_head BEFORE UPDATE OR DELETE ON public.bot_component_state
 FOR EACH ROW EXECUTE FUNCTION public.forest_kernel_state_head();
CREATE TRIGGER bot_kernel_call_digest BEFORE INSERT OR UPDATE ON public.bot_calls
 FOR EACH ROW EXECUTE FUNCTION public.forest_kernel_payload_identity();
CREATE TRIGGER bot_kernel_state_digest BEFORE INSERT OR UPDATE ON public.bot_component_state
 FOR EACH ROW EXECUTE FUNCTION public.forest_kernel_payload_identity();
CREATE TRIGGER bot_kernel_revision_digest BEFORE INSERT ON public.bot_state_revisions
 FOR EACH ROW EXECUTE FUNCTION public.forest_kernel_payload_identity();
CREATE TRIGGER bot_kernel_step_digest BEFORE INSERT ON public.bot_state_steps
 FOR EACH ROW EXECUTE FUNCTION public.forest_kernel_payload_identity();
CREATE TRIGGER bot_kernel_migration_source_digest BEFORE INSERT ON public.bot_state_migration_sources
 FOR EACH ROW EXECUTE FUNCTION public.forest_kernel_payload_identity();
CREATE TRIGGER bot_kernel_migration_result_digest BEFORE INSERT ON public.bot_state_migration_results
 FOR EACH ROW EXECUTE FUNCTION public.forest_kernel_payload_identity();
CREATE TRIGGER bot_state_steps_immutable BEFORE UPDATE OR DELETE ON public.bot_state_steps
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_state_revisions_immutable BEFORE UPDATE OR DELETE ON public.bot_state_revisions
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_state_migration_sources_immutable BEFORE UPDATE OR DELETE ON public.bot_state_migration_sources
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_state_migration_results_immutable BEFORE UPDATE OR DELETE ON public.bot_state_migration_results
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
"""
