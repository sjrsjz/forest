"""Frozen 033 original semantic parts, exact wire and full receipt import."""

import re

from psycopg.types.json import Jsonb

from forest.storage.migration_v1 import _digest, legacy_source_ref
from forest.storage.payloads import PayloadStore


def receipt_metadata(value):
    if not isinstance(value, dict):
        return {}
    result = {}
    for key in ("message_id", "error_code"):
        item = value.get(key)
        if isinstance(item, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,256}", item):
            result[key] = item
    if value.get("status") in ("SENT", "NOT_SENT", "UNKNOWN", "ok", "failed", "async"):
        result["status"] = value["status"]
    if type(value.get("retcode")) is int and -(2**31) <= value["retcode"] < 2**31:
        result["retcode"] = value["retcode"]
    return result


def public_result(value):
    if not isinstance(value, dict):
        return {}
    result = {}
    for key in ("intent_id", "attempt_id", "error_code"):
        item = value.get(key)
        if isinstance(item, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,256}", item):
            result[key] = item
        elif key in ("attempt_id", "error_code") and key in value and item is None:
            result[key] = None
    if value.get("state") in (
        "PENDING", "CLAIMED", "DISPATCHING", "SENT", "FAILED", "UNKNOWN", "CANCELLED", "SKIPPED",
    ):
        result["state"] = value["state"]
    for key in ("part_index", "revision", "attempt_count"):
        item = value.get(key)
        minimum, maximum = (1, 2**63 - 1) if key == "revision" else (0, 2**31 - 1)
        if type(item) is int and minimum <= item <= maximum:
            result[key] = item
    if "receipt" in value:
        result["receipt"] = (
            receipt_metadata(value["receipt"]) if value["receipt"] is not None else None
        )
    return result


FINAL_SQL = """
CREATE FUNCTION public.forest_delivery_receipt_metadata_valid(value jsonb) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE field text; item jsonb;
BEGIN
 IF value IS NULL OR jsonb_typeof(value)<>'object' OR
  value-ARRAY['message_id','status','retcode','error_code']<>'{}'::jsonb THEN RETURN false; END IF;
 FOR field,item IN SELECT * FROM jsonb_each(value) LOOP
  IF field='retcode' THEN
   IF jsonb_typeof(item)<>'number' OR item::text !~ '^-?[0-9]+$' THEN RETURN false; END IF;
   IF item::text::numeric < -2147483648 OR item::text::numeric > 2147483647 THEN RETURN false; END IF;
  ELSE
   IF jsonb_typeof(item)<>'string' OR length(item#>>'{}') NOT BETWEEN 1 AND 256
    OR (item#>>'{}') !~ '^[A-Za-z0-9_.:-]+$' THEN RETURN false; END IF;
   IF field='status' AND (item#>>'{}') NOT IN ('SENT','NOT_SENT','UNKNOWN','ok','failed','async') THEN RETURN false; END IF;
  END IF;
 END LOOP;
 RETURN true;
END $$;
CREATE FUNCTION public.forest_delivery_result_metadata_valid(value jsonb) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE field text; item jsonb; minimum numeric; maximum numeric;
BEGIN
 IF value IS NULL OR jsonb_typeof(value)<>'object' OR
  value-ARRAY['intent_id','part_index','state','revision','attempt_id','attempt_count','error_code','receipt']<>'{}'::jsonb THEN RETURN false; END IF;
 FOR field,item IN SELECT * FROM jsonb_each(value) LOOP
  IF field='receipt' THEN
   IF item<>'null'::jsonb AND NOT public.forest_delivery_receipt_metadata_valid(item) THEN RETURN false; END IF;
  ELSIF field IN ('part_index','revision','attempt_count') THEN
   IF jsonb_typeof(item)<>'number' OR item::text !~ '^[0-9]+$' THEN RETURN false; END IF;
   minimum := CASE WHEN field='revision' THEN 1 ELSE 0 END;
   maximum := CASE WHEN field='revision' THEN 9223372036854775807 ELSE 2147483647 END;
   IF item::text::numeric < minimum OR item::text::numeric > maximum THEN RETURN false; END IF;
  ELSIF field IN ('attempt_id','error_code') AND item='null'::jsonb THEN
   CONTINUE;
  ELSE
   IF jsonb_typeof(item)<>'string' OR length(item#>>'{}') NOT BETWEEN 1 AND 256
    OR (item#>>'{}') !~ '^[A-Za-z0-9_.:-]+$' THEN RETURN false; END IF;
   IF field='state' AND (item#>>'{}') NOT IN ('PENDING','CLAIMED','DISPATCHING','SENT','FAILED','UNKNOWN','CANCELLED','SKIPPED') THEN RETURN false; END IF;
  END IF;
 END LOOP;
 RETURN true;
END $$;
ALTER TABLE public.bot_send_parts ALTER COLUMN payload_id SET NOT NULL,
 ADD CHECK(payload IS NULL),
 ADD CHECK(receipt IS NULL OR public.forest_delivery_receipt_metadata_valid(receipt));
ALTER TABLE public.bot_send_attempts ALTER COLUMN wire_payload_id SET NOT NULL,
 ADD CHECK(wire_body IS NULL),
 ADD CHECK(receipt IS NULL OR public.forest_delivery_receipt_metadata_valid(receipt));
ALTER TABLE public.bot_delivery_resolutions ADD CHECK(reason IS NULL AND result IS NULL),
 ADD CHECK(public.forest_delivery_result_metadata_valid(result_metadata));
CREATE OR REPLACE FUNCTION public.bot_delivery_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR TG_TABLE_NAME IN ('bot_send_intents','bot_delivery_resolutions') THEN
  RAISE EXCEPTION 'Delivery fact is immutable';
 ELSIF TG_TABLE_NAME='bot_send_parts' THEN
  IF ROW(OLD.bot_id,OLD.intent_id,OLD.part_index,OLD.part_operation_id,OLD.payload,OLD.payload_id,OLD.payload_digest)
   IS DISTINCT FROM ROW(NEW.bot_id,NEW.intent_id,NEW.part_index,NEW.part_operation_id,NEW.payload,NEW.payload_id,NEW.payload_digest) THEN
   RAISE EXCEPTION 'Delivery semantic part is immutable';
  END IF;
 ELSE
  IF (to_jsonb(OLD)-ARRAY['state','receipt','receipt_payload_id','observed_at','started_at']) IS DISTINCT FROM
   (to_jsonb(NEW)-ARRAY['state','receipt','receipt_payload_id','observed_at','started_at']) THEN
   RAISE EXCEPTION 'Delivery transport attempt is immutable';
  END IF;
  IF OLD.started_at IS NOT NULL AND NEW.started_at IS DISTINCT FROM OLD.started_at THEN
   RAISE EXCEPTION 'Delivery transport start is immutable';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER bot_send_parts_immutable BEFORE UPDATE OR DELETE ON public.bot_send_parts
 FOR EACH ROW EXECUTE FUNCTION public.bot_delivery_immutable();
CREATE TRIGGER bot_send_attempts_immutable BEFORE UPDATE OR DELETE ON public.bot_send_attempts
 FOR EACH ROW EXECUTE FUNCTION public.bot_delivery_immutable();
CREATE TRIGGER bot_delivery_resolutions_immutable BEFORE UPDATE OR DELETE ON public.bot_delivery_resolutions
 FOR EACH ROW EXECUTE FUNCTION public.bot_delivery_immutable();
"""


async def migrate_in(conn, store):
    payloads = PayloadStore(store)
    legacy = (legacy_source_ref(store.bot_id),)
    cursor = ("", -1)
    while True:
        rows = await (
            await conn.execute(
                "SELECT p.*,i.command_id,i.send_lane FROM public.bot_send_parts p JOIN public.bot_send_intents i USING(bot_id,intent_id) "
                "WHERE p.bot_id=%s AND (p.intent_id,p.part_index)>(%s,%s) ORDER BY p.intent_id,p.part_index LIMIT 64",
                (store.bot_id, *cursor),
            )
        ).fetchall()
        if not rows:
            break
        for row in rows:
            parents = await (
                await conn.execute(
                    "SELECT payload_id FROM public.bot_conversation_replies WHERE bot_id=%s AND delivery->>'intent_id'=%s "
                    "UNION SELECT i.payload_id FROM public.bot_shell_requests s JOIN public.bot_message_inbox i USING(bot_id,event_id) "
                    "WHERE s.bot_id=%s AND s.command_id=%s",
                    (store.bot_id, row["intent_id"], store.bot_id, row["command_id"]),
                )
            ).fetchall()
            body = await payloads.import_legacy_json_in(
                conn,
                owner_kind="delivery.payload",
                owner_id=row["part_operation_id"],
                value=row["payload"],
                sources=legacy,
                parents=tuple(parent["payload_id"] for parent in parents if parent["payload_id"]),
            )
            receipt = None
            if row["receipt"] is not None:
                receipt = await payloads.import_legacy_json_in(
                    conn,
                    owner_kind="delivery.receipt",
                    owner_id="part:" + row["part_operation_id"] + ":legacy",
                    value=row["receipt"],
                    parents=(body.payload_id,),
                )
            await conn.execute(
                "UPDATE public.bot_send_parts SET payload=NULL,payload_id=%s,receipt=%s,receipt_payload_id=%s "
                "WHERE bot_id=%s AND intent_id=%s AND part_index=%s",
                (
                    body.payload_id,
                    Jsonb(receipt_metadata(row["receipt"])) if row["receipt"] is not None else None,
                    receipt.payload_id if receipt else None,
                    store.bot_id,
                    row["intent_id"],
                    row["part_index"],
                ),
            )
        cursor = (rows[-1]["intent_id"], rows[-1]["part_index"])
    cursor = ""
    while True:
        rows = await (
            await conn.execute(
                "SELECT a.*,p.payload_id FROM public.bot_send_attempts a JOIN public.bot_send_parts p USING(bot_id,intent_id,part_index) "
                "WHERE a.bot_id=%s AND a.attempt_id>%s ORDER BY a.attempt_id LIMIT 32",
                (store.bot_id, cursor),
            )
        ).fetchall()
        if not rows:
            break
        for row in rows:
            body = await payloads.import_legacy_bytes_in(
                conn,
                owner_kind="delivery.request",
                owner_id=row["attempt_id"],
                value=bytes(row["wire_body"]),
                parents=(row["payload_id"],),
            )
            receipt = None
            if row["receipt"] is not None:
                receipt = await payloads.import_legacy_json_in(
                    conn,
                    owner_kind="delivery.receipt",
                    owner_id="attempt:" + row["attempt_id"] + ":legacy",
                    value=row["receipt"],
                    parents=(body.payload_id,),
                )
            await conn.execute(
                "UPDATE public.bot_send_attempts SET wire_body=NULL,wire_payload_id=%s,receipt=%s,receipt_payload_id=%s,"
                "deletion_epoch=(SELECT deletion_epoch FROM public.bot_memory_clock WHERE bot_id=%s) WHERE bot_id=%s AND attempt_id=%s",
                (
                    body.payload_id,
                    Jsonb(receipt_metadata(row["receipt"])) if row["receipt"] is not None else None,
                    receipt.payload_id if receipt else None,
                    store.bot_id,
                    store.bot_id,
                    row["attempt_id"],
                ),
            )
        cursor = rows[-1]["attempt_id"]
    cursor = ""
    while True:
        rows = await (
            await conn.execute(
                "SELECT r.*,p.payload_id AS parent FROM public.bot_delivery_resolutions r JOIN public.bot_send_parts p USING(bot_id,intent_id,part_index) "
                "WHERE r.bot_id=%s AND r.operation_id>%s ORDER BY r.operation_id LIMIT 64",
                (store.bot_id, cursor),
            )
        ).fetchall()
        if not rows:
            break
        for row in rows:
            body = await payloads.import_legacy_json_in(
                conn,
                owner_kind="delivery.receipt",
                owner_id="resolution:" + _digest(row["operation_id"]).removeprefix("sha256:"),
                value={"reason": row["reason"], "result": row["result"]},
                parents=(row["parent"],),
            )
            await conn.execute(
                "UPDATE public.bot_delivery_resolutions SET reason=NULL,result=NULL,payload_id=%s,result_metadata=%s WHERE bot_id=%s AND operation_id=%s",
                (
                    body.payload_id,
                    Jsonb(public_result(row["result"])),
                    store.bot_id,
                    row["operation_id"],
                ),
            )
        cursor = rows[-1]["operation_id"]
    await conn.execute(FINAL_SQL)
