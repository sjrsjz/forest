"""Frozen 032 conversion: Child exact bytes and unproved legacy lineage."""

from psycopg.types.json import Jsonb

from forest.storage.migration_v1 import legacy_source_ref
from forest.storage.payloads import PayloadStore

VALUE_METADATA = frozenset(
    {
        "children_enabled",
        "child_concurrency",
        "max_children",
        "child_timeout",
        "child_max_output_tokens",
        "run_token_budget",
        "daily_token_budget",
        "max_output_tokens",
        "model_timeout",
        "stream",
    }
)


async def migrate_in(conn, store):
    payloads = PayloadStore(store)
    sources = (legacy_source_ref(store.bot_id),)
    after = ""
    while True:
        rows = await (
            await conn.execute(
                "SELECT c.*,s.snapshot_payload_id AS parent_payload_id,t.arguments_payload_id "
                "FROM public.bot_agent_children c JOIN public.bot_agent_steps s "
                "ON s.bot_id=c.bot_id AND s.logical_step_id=c.parent_step_id "
                "JOIN public.bot_agent_tools t ON t.bot_id=c.bot_id AND t.run_id=c.parent_run_id "
                "AND t.tool_call_id=c.tool_call_id WHERE c.bot_id=%s AND c.child_id>%s "
                "ORDER BY c.child_id LIMIT 64",
                (store.bot_id, after),
            )
        ).fetchall()
        if not rows:
            break
        for row in rows:
            parents = (row["parent_payload_id"], row["arguments_payload_id"])
            refs = []
            for field in ("snapshot", "spec", "result"):
                if row[field] is None:
                    refs.append(None)
                    continue
                ref = await payloads.import_legacy_json_in(
                    conn,
                    owner_kind="agent.child",
                    owner_id="legacy-child:" + row["child_id"] + ":" + field,
                    value=row[field],
                    sources=sources,
                    parents=parents,
                )
                refs.append(ref.payload_id)
            spec = row["spec"]
            metadata = {
                "config_revision": spec["config_revision"],
                "depth": spec["depth"],
                "delegation_policy_revision": spec["delegation_policy_revision"],
                "values": {
                    k: v
                    for k, v in spec["values"].items()
                    if k in VALUE_METADATA
                    and (
                        (k in {"children_enabled", "stream"} and type(v) is bool)
                        or (
                            k not in {"children_enabled", "stream"}
                            and type(v) is int
                            and 0 < v <= 1000000000
                        )
                    )
                },
            }
            await conn.execute(
                "UPDATE public.bot_agent_children SET snapshot=NULL,spec=NULL,result=NULL,"
                "snapshot_payload_id=%s,spec_payload_id=%s,result_payload_id=%s,spec_metadata=%s "
                "WHERE bot_id=%s AND child_id=%s",
                (*refs, Jsonb(metadata), store.bot_id, row["child_id"]),
            )
        after = rows[-1]["child_id"]
    after = ""
    while True:
        rows = await (
            await conn.execute(
                "SELECT a.*,c.snapshot_payload_id,c.spec_payload_id FROM public.bot_child_model_attempts a "
                "JOIN public.bot_agent_children c USING(bot_id,child_id) WHERE a.bot_id=%s AND a.attempt_id>%s "
                "ORDER BY a.attempt_id LIMIT 64",
                (store.bot_id, after),
            )
        ).fetchall()
        if not rows:
            break
        for row in rows:
            refs = []
            for field in ("request_body", "manifest", "result"):
                if row[field] is None:
                    refs.append(None)
                    continue
                binary = field == "request_body"
                writer = (
                    payloads.import_legacy_bytes_in if binary else payloads.import_legacy_json_in
                )
                ref = await writer(
                    conn,
                    owner_kind="model.request"
                    if binary
                    else "model.manifest"
                    if field == "manifest"
                    else "model.result",
                    owner_id="legacy-child-attempt:" + row["attempt_id"] + ":" + field,
                    value=bytes(row[field]) if binary else row[field],
                    sources=sources,
                    parents=(row["snapshot_payload_id"], row["spec_payload_id"]),
                )
                refs.append(ref.payload_id)
            metadata = None
            if row["result"] is not None:
                metadata = {"status": row["status"]}
                usage = row["result"].get("usage")
                if isinstance(usage, dict):
                    metadata["usage"] = {
                        k: v
                        for k, v in usage.items()
                        if k in {"total_tokens", "prompt_tokens", "completion_tokens"}
                        and type(v) is int
                        and 0 <= v <= 1000000000
                    }
            await conn.execute(
                "UPDATE public.bot_child_model_attempts SET request_body=NULL,manifest=NULL,result=NULL,"
                "request_payload_id=%s,manifest_payload_id=%s,result_payload_id=%s,result_metadata=%s "
                "WHERE bot_id=%s AND attempt_id=%s",
                (
                    *refs,
                    Jsonb(metadata) if metadata is not None else None,
                    store.bot_id,
                    row["attempt_id"],
                ),
            )
        after = rows[-1]["attempt_id"]
    await conn.execute(FINAL_SQL)


FINAL_SQL = r"""
CREATE FUNCTION public.forest_child_spec_metadata_valid(body jsonb) RETURNS boolean
 LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE k text; v jsonb;
BEGIN
 IF jsonb_typeof(body) IS DISTINCT FROM 'object'
    OR body-ARRAY['config_revision','depth','delegation_policy_revision','values']<>'{}'::jsonb
    OR NOT body ?& ARRAY['config_revision','depth','delegation_policy_revision','values']
    OR body->'depth' IS DISTINCT FROM '1'::jsonb
    OR jsonb_typeof(body->'values') IS DISTINCT FROM 'object' THEN RETURN false; END IF;
 FOREACH k IN ARRAY ARRAY['config_revision','delegation_policy_revision'] LOOP
  IF jsonb_typeof(body->k) IS DISTINCT FROM 'number'
     OR body->>k !~ '^(0|[1-9][0-9]{0,18})$' THEN RETURN false; END IF;
  IF (body->>k)::numeric>9223372036854775807 THEN RETURN false; END IF;
 END LOOP;
 FOR k,v IN SELECT * FROM jsonb_each(body->'values') LOOP
  IF k IN ('children_enabled','stream') THEN
   IF jsonb_typeof(v) IS DISTINCT FROM 'boolean' THEN RETURN false; END IF;
  ELSIF k IN ('child_concurrency','max_children','child_timeout','child_max_output_tokens',
             'run_token_budget','daily_token_budget','max_output_tokens','model_timeout') THEN
   IF jsonb_typeof(v) IS DISTINCT FROM 'number'
      OR v::text !~ '^[1-9][0-9]{0,9}$' THEN RETURN false; END IF;
   IF v::text::numeric>1000000000 THEN RETURN false; END IF;
  ELSE RETURN false;
  END IF;
 END LOOP;
 RETURN true;
END $$;
CREATE FUNCTION public.forest_child_result_metadata_valid(body jsonb) RETURNS boolean
 LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE k text; v jsonb;
BEGIN
 IF body IS NULL THEN RETURN true; END IF;
 IF jsonb_typeof(body) IS DISTINCT FROM 'object'
    OR body-ARRAY['status','usage','discarded_reason']<>'{}'::jsonb
    OR NOT body ? 'status'
    OR jsonb_typeof(body->'status') IS DISTINCT FROM 'string' THEN RETURN false; END IF;
 IF body->>'status' NOT IN ('RESPONDED','NOT_SENT','UNKNOWN') THEN RETURN false; END IF;
 IF body ? 'discarded_reason'
    AND body->'discarded_reason' IS DISTINCT FROM '"AGENT_MEMORY_REVOKED"'::jsonb THEN
  RETURN false;
 END IF;
 IF body ? 'usage' THEN
  IF jsonb_typeof(body->'usage') IS DISTINCT FROM 'object' THEN RETURN false; END IF;
  FOR k,v IN SELECT * FROM jsonb_each(body->'usage') LOOP
   IF k NOT IN ('total_tokens','prompt_tokens','completion_tokens')
      OR jsonb_typeof(v) IS DISTINCT FROM 'number'
      OR v::text !~ '^(0|[1-9][0-9]{0,9})$' THEN RETURN false; END IF;
   IF v::text::numeric>1000000000 THEN RETURN false; END IF;
  END LOOP;
 END IF;
 RETURN true;
END $$;
ALTER TABLE public.bot_agent_children
 ALTER COLUMN snapshot_payload_id SET NOT NULL, ALTER COLUMN spec_payload_id SET NOT NULL,
 ADD CHECK(snapshot IS NULL AND spec IS NULL AND result IS NULL),
 ADD CONSTRAINT bot_child_spec_metadata_closed
     CHECK(public.forest_child_spec_metadata_valid(spec_metadata)),
 ADD FOREIGN KEY(bot_id,snapshot_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ADD FOREIGN KEY(bot_id,spec_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ADD FOREIGN KEY(bot_id,result_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
ALTER TABLE public.bot_child_model_attempts
 ALTER COLUMN request_payload_id SET NOT NULL, ALTER COLUMN manifest_payload_id SET NOT NULL,
 ADD CHECK(request_body IS NULL AND manifest IS NULL AND result IS NULL),
 ADD CONSTRAINT bot_child_result_metadata_closed
     CHECK(public.forest_child_result_metadata_valid(result_metadata)),
 ADD FOREIGN KEY(bot_id,request_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ADD FOREIGN KEY(bot_id,manifest_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ADD FOREIGN KEY(bot_id,result_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
CREATE TRIGGER bot_child_identity BEFORE UPDATE OR DELETE ON public.bot_agent_children
 FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields(
 '{status,owner_id,lease_epoch,lease_until,host_incarnation,host_owner_epoch,policy_revision,maintenance_epoch,storage_generation,attempt_id,attempt_count,result_payload_id,result_digest,error_code,accepted_checkpoint}');
CREATE TRIGGER bot_child_attempt_identity BEFORE UPDATE OR DELETE ON public.bot_child_model_attempts
 FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields('{status,result_payload_id,result_metadata,result_digest,charged_tokens,settled}');
CREATE FUNCTION public.forest_child_payload_once() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.result_digest IS NOT NULL AND (OLD.result_payload_id,OLD.result_digest)
     IS DISTINCT FROM (NEW.result_payload_id,NEW.result_digest) THEN
  RAISE EXCEPTION 'Child observation identity is immutable' USING ERRCODE='55000';
 END IF;
 IF TG_TABLE_NAME='bot_child_model_attempts' THEN
  IF OLD.result_digest IS NOT NULL AND OLD.result_metadata IS DISTINCT FROM NEW.result_metadata THEN
   RAISE EXCEPTION 'Child observation metadata is immutable' USING ERRCODE='55000';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER bot_child_result_once BEFORE UPDATE ON public.bot_agent_children
 FOR EACH ROW EXECUTE FUNCTION public.forest_child_payload_once();
CREATE TRIGGER bot_child_attempt_result_once BEFORE UPDATE ON public.bot_child_model_attempts
 FOR EACH ROW EXECUTE FUNCTION public.forest_child_payload_once();
CREATE FUNCTION public.forest_child_payload_digest() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_TABLE_NAME='bot_agent_children' THEN
  IF NOT EXISTS(SELECT 1 FROM public.bot_payload_headers h WHERE h.bot_id=NEW.bot_id
      AND h.payload_id=NEW.snapshot_payload_id AND h.encoding='json' AND h.digest=NEW.snapshot_digest) THEN
   RAISE EXCEPTION 'Child snapshot digest differs from its payload' USING ERRCODE='55000';
  END IF;
 ELSE
  IF NOT EXISTS(SELECT 1 FROM public.bot_payload_headers h WHERE h.bot_id=NEW.bot_id
      AND h.payload_id=NEW.request_payload_id AND h.encoding='bytes' AND h.digest=NEW.wire_digest) THEN
   RAISE EXCEPTION 'Child wire digest differs from its payload' USING ERRCODE='55000';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER bot_child_snapshot_digest BEFORE INSERT ON public.bot_agent_children
 FOR EACH ROW EXECUTE FUNCTION public.forest_child_payload_digest();
CREATE TRIGGER bot_child_wire_digest BEFORE INSERT ON public.bot_child_model_attempts
 FOR EACH ROW EXECUTE FUNCTION public.forest_child_payload_digest();
"""
