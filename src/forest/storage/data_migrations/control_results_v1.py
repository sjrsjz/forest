"""Frozen 040: preserve old result facts without inventing their absent inputs."""

from forest.storage.migration_v1 import _digest, legacy_source_ref
from forest.storage.payloads import PayloadStore


async def migrate_in(conn, store):
    payloads = PayloadStore(store)
    after = None
    while True:
        rows = await (await conn.execute(
            "SELECT operation_id,action,result FROM public.bot_web_operations "
            "WHERE bot_id=%s AND (%s::text IS NULL OR operation_id>%s) "
            "ORDER BY operation_id LIMIT 64", (store.bot_id, after, after),
        )).fetchall()
        if not rows:
            break
        for row in rows:
            # The old column is NOT NULL, so Python None is a genuine JSON null.
            # No business input or independent input domain is fabricated.
            ref = await payloads.import_legacy_json_in(
                conn, owner_kind="control.result",
                owner_id="result:" + _digest([row["operation_id"]]),
                value={"action": row["action"], "operation_id": row["operation_id"], "value": row["result"]},
                sources=(legacy_source_ref(store.bot_id),),
            )
            await conn.execute(
                "UPDATE public.bot_web_operations SET result=NULL,result_payload_id=%s,"
                "result_payload_digest=%s WHERE bot_id=%s AND operation_id=%s",
                (ref.payload_id, ref.digest, store.bot_id, row["operation_id"]),
            )
        after = rows[-1]["operation_id"]
    await conn.execute(FINAL_SQL)


FINAL_SQL = """
ALTER TABLE public.bot_web_operations
 ALTER COLUMN input_legacy_unknown SET DEFAULT false,
 ALTER COLUMN result_payload_id SET NOT NULL,
 ALTER COLUMN result_payload_digest SET NOT NULL,
 ADD CONSTRAINT bot_control_result_external CHECK(result IS NULL),
 ADD CONSTRAINT bot_control_input_identity CHECK(
  (input_legacy_unknown AND input_payload_id IS NULL AND input_payload_digest IS NULL)
  OR (NOT input_legacy_unknown AND input_payload_id IS NOT NULL AND input_payload_digest IS NOT NULL)),
 ADD CHECK(input_payload_digest IS NULL OR input_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
 ADD CHECK(result_payload_digest ~ '^sha256:[a-f0-9]{64}$');

-- Only immutable rows imported inside 040 authorize the legacy read exception.
-- The INSERT guard below forbids adding this exception during runtime.
CREATE TRIGGER bot_web_operations_immutable BEFORE UPDATE OR DELETE ON public.bot_web_operations
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE FUNCTION public.forest_control_payload_value(
 selected_bot text, selected_payload text, selected_digest text,
 selected_kind text, selected_operation text, selected_action text)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE h record; envelope jsonb; sources_count bigint; suppressed boolean; expected_owner text;
BEGIN
 PERFORM 1 FROM public.bot_memory_clock WHERE bot_id=selected_bot FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'CONTROL_CONTENT_REVOKED' USING ERRCODE='P0001'; END IF;
 SELECT p.*,b.content INTO h FROM public.bot_payload_headers p
 LEFT JOIN public.bot_payload_bodies b USING(bot_id,payload_id)
 WHERE p.bot_id=selected_bot AND p.payload_id=selected_payload;
 expected_owner := selected_kind || ':sha256:' || encode(sha256(convert_to(
  jsonb_build_array(selected_operation)::text,'UTF8')),'hex');
 IF NOT FOUND OR h.owner_kind IS DISTINCT FROM ('control.' || selected_kind)
  OR h.owner_id IS DISTINCT FROM expected_owner OR h.encoding IS DISTINCT FROM 'json'
  OR h.digest IS DISTINCT FROM selected_digest THEN
  RAISE EXCEPTION 'CONTROL_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 IF h.state<>'ACTIVE' OR h.content IS NULL THEN
  RAISE EXCEPTION 'CONTROL_CONTENT_REVOKED' USING ERRCODE='P0001';
 END IF;
 IF h.digest IS DISTINCT FROM ('sha256:' || encode(sha256(h.content),'hex')) THEN
  RAISE EXCEPTION 'CONTROL_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 SELECT count(*),COALESCE(bool_or(EXISTS(
  SELECT 1 FROM public.bot_payload_suppression s WHERE s.bot_id=selected_bot
   AND s.source_unit_id=d.source_unit_id
   AND (s.part_id='*' OR d.part_id='*' OR s.part_id=d.part_id))),false)
 INTO sources_count,suppressed
 FROM public.forest_source_dependency_closure(selected_bot,h.manifest->'sources',4096) d;
 IF sources_count>4096 OR suppressed THEN
  RAISE EXCEPTION 'CONTROL_CONTENT_REVOKED' USING ERRCODE='P0001';
 END IF;
 envelope := convert_from(h.content,'UTF8')::jsonb;
 IF jsonb_typeof(envelope) IS DISTINCT FROM 'object'
  OR NOT (envelope ?& ARRAY['action','operation_id','value'])
  OR envelope-ARRAY['action','operation_id','value']<>'{}'::jsonb
  OR envelope->'action' IS DISTINCT FROM to_jsonb(selected_action)
  OR envelope->'operation_id' IS DISTINCT FROM to_jsonb(selected_operation) THEN
  RAISE EXCEPTION 'CONTROL_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 IF selected_kind='input' AND NOT EXISTS(
  SELECT 1 FROM public.bot_payload_sources s JOIN public.bot_payload_domains d
   ON d.bot_id=s.bot_id AND d.source_unit_id=s.source_unit_id
  WHERE s.bot_id=selected_bot AND s.payload_id=selected_payload AND s.source_revision=1 AND s.part_id='*'
   AND d.kind='CONTROL_INPUT' AND d.owner_id=expected_owner) THEN
  RAISE EXCEPTION 'CONTROL_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 RETURN envelope->'value';
END;
$$;

CREATE FUNCTION public.forest_control_operation_payload_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.input_legacy_unknown THEN
  RAISE EXCEPTION 'CONTROL_INPUT_LEGACY_FORBIDDEN' USING ERRCODE='23514';
 END IF;
 PERFORM public.forest_control_payload_value(NEW.bot_id,NEW.input_payload_id,NEW.input_payload_digest,
  'input',NEW.operation_id,NEW.action);
 PERFORM public.forest_control_payload_value(NEW.bot_id,NEW.result_payload_id,NEW.result_payload_digest,
  'result',NEW.operation_id,NEW.action);
 IF NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations d
  WHERE d.bot_id=NEW.bot_id AND d.payload_id=NEW.result_payload_id
   AND d.parent_payload_id=NEW.input_payload_id) THEN
  RAISE EXCEPTION 'CONTROL_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_control_operation_payload_guard BEFORE INSERT ON public.bot_web_operations
 FOR EACH ROW EXECUTE FUNCTION public.forest_control_operation_payload_guard();
"""
