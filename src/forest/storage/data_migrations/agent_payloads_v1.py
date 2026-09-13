"""Frozen 031 migration: exact legacy Agent bodies, conservative provenance."""

import hashlib
import json

from psycopg import sql

from forest.storage.migration_v1 import legacy_source_ref
from forest.storage.payloads import PayloadStore


async def migrate_in(conn, store):
    payloads = PayloadStore(store)
    sources = (legacy_source_ref(store.bot_id),)
    families = (
        (
            "bot_agent_steps",
            "logical_step_id",
            (
                ("snapshot", "snapshot_payload_id", "model.snapshot", False),
                ("output", "output_payload_id", "model.result", False),
                ("closed_parts", "closed_parts_payload_id", "context.parts", False),
            ),
        ),
        (
            "bot_model_request_manifests",
            "model_request_id",
            (("manifest", "manifest_payload_id", "model.manifest", False),),
        ),
        (
            "bot_model_transport_attempts",
            "transport_attempt_id",
            (
                ("request_body", "request_payload_id", "model.request", True),
                ("result", "result_payload_id", "model.result", False),
            ),
        ),
        (
            "bot_agent_tools",
            "operation_id",
            (
                ("arguments", "arguments_payload_id", "agent.tool", False),
                ("result", "result_payload_id", "agent.tool", False),
            ),
        ),
        (
            "bot_agent_tool_observations",
            "observation_id",
            (("observation", "observation_payload_id", "agent.observation", False),),
        ),
    )
    for table, key, fields in families:
        present_columns = sql.SQL(",").join(
            sql.SQL("{} IS NOT NULL AS {}").format(
                sql.Identifier(field), sql.Identifier("__present_" + field)
            )
            for field, _, _, _ in fields
        )
        after = None
        while True:
            predicate = (
                sql.SQL("") if after is None else sql.SQL(" AND {}>%s").format(sql.Identifier(key))
            )
            rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT *,{} FROM public.{} WHERE bot_id=%s {} ORDER BY {} LIMIT 128"
                    ).format(
                        present_columns, sql.Identifier(table), predicate, sql.Identifier(key)
                    ),
                    (store.bot_id,) if after is None else (store.bot_id, after),
                )
            ).fetchall()
            if not rows:
                break
            for row in rows:
                if table == "bot_agent_tools" and row["result_digest"] is not None:
                    # A retry/cancel may change scheduling status while keeping
                    # the previous NOT_SENT observation. Recover its own outcome
                    # from the immutable semantic digest, never from arrival order.
                    outcome = next(
                        (
                            status
                            for status in ("SUCCEEDED", "FAILED", "NOT_SENT", "UNKNOWN")
                            if "sha256:"
                            + hashlib.sha256(
                                json.dumps(
                                    [status, row["result"]],
                                    sort_keys=True,
                                    ensure_ascii=False,
                                    separators=(",", ":"),
                                    allow_nan=False,
                                ).encode()
                            ).hexdigest()
                            == row["result_digest"]
                        ),
                        None,
                    )
                    if outcome is None:
                        raise ValueError("Legacy Agent tool observation digest does not match")
                    await conn.execute(
                        "UPDATE public.bot_agent_tools SET result_outcome=%s WHERE bot_id=%s AND operation_id=%s",
                        (outcome, store.bot_id, row[key]),
                    )
                for field, ref_field, kind, binary in fields:
                    value = row[field]
                    # JSONB null and SQL NULL both decode as Python None. Only
                    # SQL absence skips import; a JSON null owns real b'null'.
                    if not row["__present_" + field]:
                        continue
                    writer = (
                        payloads.import_legacy_bytes_in
                        if binary
                        else payloads.import_legacy_json_in
                    )
                    ref = await writer(
                        conn,
                        owner_kind=kind,
                        owner_id="legacy-agent:"
                        + hashlib.sha256(
                            json.dumps(
                                [table, row[key], field], ensure_ascii=False, separators=(",", ":")
                            ).encode()
                        ).hexdigest(),
                        value=bytes(value) if binary else value,
                        sources=sources,
                    )
                    await conn.execute(
                        sql.SQL(
                            "UPDATE public.{} SET {}=NULL,{}=%s WHERE bot_id=%s AND {}=%s"
                        ).format(
                            sql.Identifier(table),
                            sql.Identifier(field),
                            sql.Identifier(ref_field),
                            sql.Identifier(key),
                        ),
                        (ref.payload_id, store.bot_id, row[key]),
                    )
            after = rows[-1][key]
    await conn.execute(FINAL_SQL)


FINAL_SQL = r"""
ALTER TABLE public.bot_agent_steps ALTER COLUMN snapshot_payload_id SET NOT NULL,
 ADD CHECK(snapshot IS NULL AND output IS NULL AND closed_parts IS NULL),
 ADD FOREIGN KEY(bot_id,snapshot_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ADD FOREIGN KEY(bot_id,output_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ADD FOREIGN KEY(bot_id,closed_parts_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
ALTER TABLE public.bot_model_request_manifests ALTER COLUMN manifest_payload_id SET NOT NULL,
 ADD CHECK(manifest IS NULL),
 ADD FOREIGN KEY(bot_id,manifest_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
ALTER TABLE public.bot_model_transport_attempts ALTER COLUMN request_payload_id SET NOT NULL,
 ADD CHECK(request_body IS NULL AND result IS NULL),
 ADD FOREIGN KEY(bot_id,request_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ADD FOREIGN KEY(bot_id,result_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
ALTER TABLE public.bot_agent_tools ALTER COLUMN arguments_payload_id SET NOT NULL,
 ADD CHECK(arguments IS NULL AND result IS NULL),
 ADD CHECK(COALESCE((result_digest IS NULL AND result_outcome IS NULL AND result_payload_id IS NULL)
  OR (result_digest IS NOT NULL AND result_outcome IN ('SUCCEEDED','FAILED','NOT_SENT','UNKNOWN')),false)),
 ADD CHECK(status NOT IN ('SUCCEEDED','FAILED','NOT_SENT') OR
  COALESCE(result_digest IS NOT NULL AND result_outcome=status,false)),
 ADD CHECK(result_metadata IN ('{}'::jsonb,'{"discarded_reason":"AGENT_MEMORY_REVOKED"}'::jsonb)),
 ADD CHECK(result_metadata='{}'::jsonb OR (result_digest IS NOT NULL AND result_payload_id IS NULL)),
 ADD FOREIGN KEY(bot_id,arguments_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ADD FOREIGN KEY(bot_id,result_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
ALTER TABLE public.bot_agent_tool_observations ADD CHECK(observation IS NULL),
 ADD FOREIGN KEY(bot_id,observation_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);

CREATE TRIGGER bot_agent_step_identity BEFORE UPDATE OR DELETE ON public.bot_agent_steps
 FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields(
 '{status,model_call_epoch,model_request_id,transport_attempt_id,output_payload_id,output_digest,closed_parts_payload_id,published_context_revision}');
CREATE TRIGGER bot_model_manifests_immutable BEFORE UPDATE OR DELETE ON public.bot_model_request_manifests
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_model_attempt_identity BEFORE UPDATE OR DELETE ON public.bot_model_transport_attempts
 FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields('{status,result_payload_id,result_digest,result_metadata}');
CREATE TRIGGER bot_agent_tool_identity BEFORE UPDATE OR DELETE ON public.bot_agent_tools
 FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields(
 '{authority_trigger_id,principal_ref,status,requires_approval,attempt_count,owner_epoch,actor_epoch,host_incarnation,result_payload_id,result_digest,result_metadata,result_outcome}');
CREATE FUNCTION public.forest_agent_usage_metadata(value jsonb) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE item record;
BEGIN
 IF value='null'::jsonb THEN RETURN true; END IF;
 IF jsonb_typeof(value) IS DISTINCT FROM 'object'
  OR value-ARRAY['prompt_tokens','completion_tokens','total_tokens']<>'{}'::jsonb THEN RETURN false; END IF;
 FOR item IN SELECT * FROM jsonb_each(value) LOOP
  IF jsonb_typeof(item.value)<>'number' OR item.value::text !~ '^(0|[1-9][0-9]{0,9})$'
   OR (item.value::text)::numeric>1000000000 THEN RETURN false; END IF;
 END LOOP;
 RETURN true;
END $$;
ALTER TABLE public.bot_model_transport_attempts ADD CHECK(result_metadata='{}'::jsonb OR COALESCE(
 jsonb_typeof(result_metadata)='object'
 AND result_payload_id IS NULL AND result_digest IS NOT NULL AND status IN ('RESPONDED','NOT_SENT','UNKNOWN')
 AND result_metadata-ARRAY['status','request_id','transport_attempt_id','payload_digest','usage','discarded_reason']='{}'::jsonb
 AND result_metadata ?& ARRAY['status','request_id','transport_attempt_id','payload_digest','usage','discarded_reason']
 AND jsonb_typeof(result_metadata->'status')='string'
 AND jsonb_typeof(result_metadata->'request_id')='string'
 AND jsonb_typeof(result_metadata->'transport_attempt_id')='string'
 AND jsonb_typeof(result_metadata->'payload_digest')='string'
 AND jsonb_typeof(result_metadata->'discarded_reason')='string'
 AND result_metadata->>'status'=status
 AND result_metadata->>'request_id'=model_request_id
 AND result_metadata->>'transport_attempt_id'=transport_attempt_id
 AND result_metadata->>'payload_digest'=wire_digest
 AND result_metadata->>'discarded_reason'='AGENT_MEMORY_REVOKED'
 AND public.forest_agent_usage_metadata(result_metadata->'usage'),false));
CREATE FUNCTION public.forest_agent_tool_result_once() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE changed boolean;
BEGIN
 changed := (OLD.result_payload_id,OLD.result_digest,OLD.result_metadata)
  IS DISTINCT FROM (NEW.result_payload_id,NEW.result_digest,NEW.result_metadata);
 IF OLD.status<>NEW.status AND NOT (
  (OLD.status='PROPOSED' AND NEW.status IN ('WAIT_APPROVAL','DISPATCHING','WAIT_CHILD','CANCELLED'))
  OR (OLD.status='WAIT_APPROVAL' AND NEW.status IN ('DISPATCHING','CANCELLED'))
  OR (OLD.status='DISPATCHING' AND NEW.status IN ('SUCCEEDED','FAILED','NOT_SENT','UNKNOWN'))
  OR (OLD.status='WAIT_CHILD' AND NEW.status IN ('SUCCEEDED','FAILED','UNKNOWN','CANCELLED'))
  OR (OLD.status='NOT_SENT' AND NEW.status IN ('WAIT_APPROVAL','DISPATCHING','CANCELLED'))
  OR (OLD.status='UNKNOWN' AND NEW.status IN ('SUCCEEDED','FAILED'))
 ) THEN RAISE EXCEPTION 'Tool status transition is invalid'; END IF;
 IF OLD.status IN ('SUCCEEDED','FAILED','CANCELLED') AND
  (changed OR (OLD.status,OLD.attempt_count) IS DISTINCT FROM (NEW.status,NEW.attempt_count)) THEN
  RAISE EXCEPTION 'Terminal tool observation is permanent';
 END IF;
 IF NEW.attempt_count IS DISTINCT FROM OLD.attempt_count AND NOT
  (OLD.status IN ('PROPOSED','WAIT_APPROVAL','NOT_SENT') AND NEW.status='DISPATCHING'
   AND NEW.attempt_count=OLD.attempt_count+1 AND NEW.attempt_count<=3) THEN
  RAISE EXCEPTION 'Tool attempt transition is invalid';
 END IF;
 IF changed THEN
  IF NOT ((OLD.status='DISPATCHING' AND NEW.status IN ('SUCCEEDED','FAILED','NOT_SENT','UNKNOWN'))
   OR (OLD.status IN ('UNKNOWN','WAIT_CHILD') AND NEW.status IN ('SUCCEEDED','FAILED')))
   OR NEW.result_digest IS NULL THEN
   RAISE EXCEPTION 'Tool result requires a new valid observation';
  END IF;
  NEW.result_outcome := NEW.status;
 ELSIF NEW.result_outcome IS DISTINCT FROM OLD.result_outcome THEN
  RAISE EXCEPTION 'Tool result outcome is permanent without a new observation';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER bot_agent_tool_result_once BEFORE UPDATE ON public.bot_agent_tools
 FOR EACH ROW EXECUTE FUNCTION public.forest_agent_tool_result_once();
CREATE FUNCTION public.forest_agent_payload_once() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_TABLE_NAME='bot_agent_steps' THEN
  IF OLD.output_digest IS NOT NULL AND (OLD.output_payload_id,OLD.output_digest)
     IS DISTINCT FROM (NEW.output_payload_id,NEW.output_digest) THEN
   RAISE EXCEPTION 'Agent output payload identity is permanent';
  END IF;
  IF OLD.closed_parts_payload_id IS NOT NULL AND OLD.closed_parts_payload_id IS DISTINCT FROM NEW.closed_parts_payload_id THEN
   RAISE EXCEPTION 'Closed tool group is permanent';
  END IF;
 ELSIF OLD.result_digest IS NOT NULL AND (OLD.status,OLD.result_digest,OLD.result_payload_id,OLD.result_metadata)
     IS DISTINCT FROM (NEW.status,NEW.result_digest,NEW.result_payload_id,NEW.result_metadata) THEN
  RAISE EXCEPTION 'Model observation identity is permanent';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER bot_model_observation_once BEFORE UPDATE ON public.bot_model_transport_attempts
 FOR EACH ROW EXECUTE FUNCTION public.forest_agent_payload_once();
CREATE TRIGGER bot_agent_step_output_once BEFORE UPDATE ON public.bot_agent_steps
 FOR EACH ROW EXECUTE FUNCTION public.forest_agent_payload_once();
CREATE OR REPLACE FUNCTION public.forest_agent_tool_observe() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF (OLD.status,OLD.attempt_count,OLD.result_digest,OLD.result_payload_id,OLD.result_metadata,OLD.result_outcome)
  IS DISTINCT FROM (NEW.status,NEW.attempt_count,NEW.result_digest,NEW.result_payload_id,NEW.result_metadata,NEW.result_outcome) THEN
  INSERT INTO public.bot_agent_tool_observations(bot_id,run_id,tool_call_id,operation_id,attempt_count,status,observation_refs)
  VALUES(NEW.bot_id,NEW.run_id,NEW.tool_call_id,NEW.operation_id,NEW.attempt_count,NEW.status,
   jsonb_build_object('arguments_payload_id',NEW.arguments_payload_id,'arguments_digest',NEW.arguments_digest,
    'result_payload_id',NEW.result_payload_id,'result_digest',NEW.result_digest,
    'result_outcome',NEW.result_outcome,'result_metadata',NEW.result_metadata));
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER bot_agent_tool_observe AFTER UPDATE ON public.bot_agent_tools
 FOR EACH ROW EXECUTE FUNCTION public.forest_agent_tool_observe();
CREATE TRIGGER bot_agent_tool_observations_immutable BEFORE UPDATE OR DELETE ON public.bot_agent_tool_observations
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
"""
