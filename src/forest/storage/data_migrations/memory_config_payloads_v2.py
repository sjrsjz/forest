"""Frozen 046 Memory configuration/profile import using exact 041 provenance."""

from forest.storage.migration_v1 import _digest
from forest.storage.payloads import PayloadStore


async def _parent(conn, bot_id, kind, row):
    if kind == "REVISION":
        operation = await (await conn.execute(
            "SELECT result_payload_id FROM public.bot_web_operations WHERE bot_id=%s "
            "AND operation_id=%s AND action='memory.config.apply'",
            (bot_id, row["operation_id"]),
        )).fetchone()
        if operation:
            return operation["result_payload_id"]
    source_id = row["plan_id"] if kind == "PLAN" else _digest(["", row["revision"]])
    bridge = await (await conn.execute(
        "SELECT payload_id FROM public.bot_control_value_sources WHERE bot_id=%s "
        "AND family='memory.config' AND source_kind=%s AND source_id=%s",
        (bot_id, kind, source_id),
    )).fetchone()
    if bridge is None:
        raise ValueError("Memory configuration has no immutable 041 source")
    return bridge["payload_id"]


async def migrate_in(conn, store):
    payloads = PayloadStore(store)
    for kind, table, key in (
        ("PLAN", "bot_memory_config_plans", "plan_id"),
        ("REVISION", "bot_memory_config_revisions", "revision"),
    ):
        after = None
        while True:
            rows = await (await conn.execute(
                f"SELECT * FROM public.{table} WHERE bot_id=%s AND (%s::{('text' if kind == 'PLAN' else 'bigint')} "
                f"IS NULL OR {key}>%s) ORDER BY {key} LIMIT 64", (store.bot_id, after, after),
            )).fetchall()
            if not rows:
                break
            for row in rows:
                identity = str(row[key])
                envelope = {"family": "memory.config", "kind": kind, "resource": "",
                            "identity": identity, "values": row["values"]}
                parent = await _parent(conn, store.bot_id, kind, row)
                ref = await payloads.import_legacy_json_in(
                    conn, owner_kind="control.config",
                    owner_id="config-values-v1:" + _digest(["memory.config", kind, "", identity]),
                    value=envelope, parents=(parent,),
                )
                await conn.execute(
                    f"UPDATE public.{table} SET values_metadata=public.forest_memory_config_metadata(values),"
                    f"values=NULL,values_payload_id=%s,values_payload_digest=%s WHERE bot_id=%s AND {key}=%s",
                    (ref.payload_id, ref.digest, store.bot_id, row[key]),
                )
                if kind == "REVISION":
                    binding = await (await conn.execute(
                        "INSERT INTO public.bot_memory_deployment_bindings "
                        "(bot_id,deployment_digest,endpoint_digest,first_revision) VALUES (%s,%s,%s,%s) "
                        "ON CONFLICT DO NOTHING RETURNING endpoint_digest",
                        (store.bot_id, _digest([row["values"]["deployment_id"]]),
                         _digest([row["values"]["endpoint"]]), row["revision"]),
                    )).fetchone()
                    if binding is None:
                        binding = await (await conn.execute(
                            "SELECT endpoint_digest FROM public.bot_memory_deployment_bindings "
                            "WHERE bot_id=%s AND deployment_digest=%s",
                            (store.bot_id, _digest([row["values"]["deployment_id"]])),
                        )).fetchone()
                    if binding["endpoint_digest"] != _digest([row["values"]["endpoint"]]):
                        raise ValueError("Legacy Memory deployment was rebound")
            after = rows[-1][key]
    after = None
    while True:
        rows = await (await conn.execute(
            "SELECT q.*,r.values_payload_id AS configuration_payload_id FROM public.bot_memory_embedding_requests q "
            "JOIN public.bot_memory_config_revisions r ON r.bot_id=q.bot_id AND r.revision=q.config_revision "
            "WHERE q.bot_id=%s AND (%s::text IS NULL OR q.request_id>%s) ORDER BY q.request_id LIMIT 64",
            (store.bot_id, after, after),
        )).fetchall()
        if not rows:
            break
        for row in rows:
            from forest.storage.data_migrations.config_dependencies_v2 import import_dependency_in

            # This exact request/config_revision join proves the old copies'
            # missing prerequisite; never edit their frozen manifest or digest.
            for old_payload_id in (row["body_payload_id"], row["manifest_payload_id"]):
                await import_dependency_in(
                    conn, store, old_payload_id, row["configuration_payload_id"],
                    proof_kind="memory.request", proof_id=row["request_id"],
                )
            ref = await payloads.import_legacy_json_in(
                conn, owner_kind="memory.embedding", owner_id="profile:" + _digest([row["request_id"]]),
                value=row["profile"], parents=(row["configuration_payload_id"],),
            )
            await conn.execute(
                "UPDATE public.bot_memory_embedding_requests SET profile_metadata=jsonb_build_object("
                "'dimensions',profile->'dimensions','protocol_revision',profile->'protocol_revision'),"
                "profile=NULL,profile_payload_id=%s,profile_payload_digest=%s WHERE bot_id=%s AND request_id=%s",
                (ref.payload_id, ref.digest, store.bot_id, row["request_id"]),
            )
        after = rows[-1]["request_id"]
    await conn.execute(FINAL_SQL)


FINAL_SQL = """
ALTER TABLE public.bot_memory_config_plans
 ALTER COLUMN values_payload_id SET NOT NULL, ALTER COLUMN values_payload_digest SET NOT NULL,
 ALTER COLUMN values_metadata SET NOT NULL, ADD CHECK(values IS NULL),
 ADD CHECK(values_payload_digest ~ '^sha256:[0-9a-f]{64}$'),
 ADD CHECK(public.forest_memory_config_metadata_valid(values_metadata));
ALTER TABLE public.bot_memory_config_revisions
 ALTER COLUMN values_payload_id SET NOT NULL, ALTER COLUMN values_payload_digest SET NOT NULL,
 ALTER COLUMN values_metadata SET NOT NULL, ADD CHECK(values IS NULL),
 ADD CHECK(values_payload_digest ~ '^sha256:[0-9a-f]{64}$'),
 ADD CHECK(public.forest_memory_config_metadata_valid(values_metadata));
ALTER TABLE public.bot_memory_embedding_requests
 ALTER COLUMN profile_payload_id SET NOT NULL, ALTER COLUMN profile_payload_digest SET NOT NULL,
 ALTER COLUMN profile_metadata SET NOT NULL, ADD CHECK(profile IS NULL),
 ADD CHECK(profile_payload_digest ~ '^sha256:[0-9a-f]{64}$'),
 ADD CHECK(jsonb_typeof(profile_metadata)='object'
  AND profile_metadata ?& ARRAY['dimensions','protocol_revision']
  AND profile_metadata-ARRAY['dimensions','protocol_revision']='{}'::jsonb
  AND profile_metadata->'dimensions' IN ('512'::jsonb,'1024'::jsonb)
  AND profile_metadata->'protocol_revision'='"jina-clip-v2-http-2026-09-v1"'::jsonb);
CREATE TRIGGER bot_memory_config_plans_immutable BEFORE UPDATE OR DELETE ON public.bot_memory_config_plans
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_memory_config_revisions_immutable BEFORE UPDATE OR DELETE ON public.bot_memory_config_revisions
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER memory_embedding_request_fixed BEFORE UPDATE OR DELETE ON public.bot_memory_embedding_requests
 FOR EACH ROW EXECUTE FUNCTION public.forest_memory_index_fixed();
CREATE TRIGGER memory_deployment_binding_fixed BEFORE UPDATE OR DELETE ON public.bot_memory_deployment_bindings
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE FUNCTION public.forest_memory_config_values_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE selected_kind text; selected_identity text; value jsonb; p record; op record; h record; input jsonb; result jsonb;
BEGIN
 SELECT * INTO h FROM public.bot_memory_config_heads WHERE bot_id=NEW.bot_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'MEMORY_CONFIG_HEAD_REQUIRED' USING ERRCODE='23514'; END IF;
 selected_kind := CASE TG_TABLE_NAME WHEN 'bot_memory_config_plans' THEN 'PLAN' ELSE 'REVISION' END;
 selected_identity := CASE selected_kind WHEN 'PLAN' THEN NEW.plan_id ELSE to_jsonb(NEW)->>'revision' END;
 value := public.forest_config_payload_values(NEW.bot_id,NEW.values_payload_id,NEW.values_payload_digest,
  'memory.config',selected_kind,'',selected_identity);
 IF NEW.values IS NOT NULL OR NEW.values_metadata IS DISTINCT FROM public.forest_memory_config_metadata(value) THEN
  RAISE EXCEPTION 'MEMORY_CONFIG_METADATA_CONFLICT' USING ERRCODE='23514';
 END IF;
 IF selected_kind='PLAN' THEN
  IF NEW.expected_revision IS DISTINCT FROM h.revision THEN
   RAISE EXCEPTION 'MEMORY_CONFIG_HEAD_CONFLICT' USING ERRCODE='23514';
  END IF;
 END IF;
 IF selected_kind='REVISION' THEN
  SELECT * INTO p FROM public.bot_memory_config_plans WHERE bot_id=NEW.bot_id AND plan_id=NEW.plan_id;
  SELECT * INTO op FROM public.bot_web_operations WHERE bot_id=NEW.bot_id AND operation_id=NEW.operation_id;
  IF p.plan_id IS NULL OR op.operation_id IS NULL OR op.action<>'memory.config.apply'
   OR ROW(p.subject_id,p.issuer_id) IS DISTINCT FROM ROW(op.subject_id,op.issuer_id)
   OR NEW.previous_revision IS DISTINCT FROM p.expected_revision
   OR NEW.secret_ciphertext IS DISTINCT FROM p.secret_ciphertext
   OR p.expires_at<=clock_timestamp()
   OR to_jsonb(NEW.policy_revision) IS DISTINCT FROM op.ticket_basis->'policy_revision'
   OR h.revision IS DISTINCT FROM NEW.previous_revision
   OR NEW.secret_revision IS DISTINCT FROM h.secret_revision +
    (CASE WHEN p.credentials_changed OR h.revision=0 THEN 1 ELSE 0 END) THEN
   RAISE EXCEPTION 'MEMORY_CONFIG_REVISION_SOURCE_CONFLICT' USING ERRCODE='23514';
  END IF;
  IF value IS DISTINCT FROM public.forest_config_payload_values(NEW.bot_id,p.values_payload_id,p.values_payload_digest,
   'memory.config','PLAN','',p.plan_id) OR NOT EXISTS(
   SELECT 1 FROM public.bot_control_value_sources WHERE bot_id=NEW.bot_id AND family='memory.config'
    AND source_kind='PLAN' AND source_id=p.plan_id) THEN
   RAISE EXCEPTION 'MEMORY_CONFIG_REVISION_SOURCE_CONFLICT' USING ERRCODE='23514';
  END IF;
  input := public.forest_control_payload_value(NEW.bot_id,op.input_payload_id,op.input_payload_digest,
   'input',op.operation_id,op.action);
  result := public.forest_control_payload_value(NEW.bot_id,op.result_payload_id,op.result_payload_digest,
   'result',op.operation_id,op.action);
  IF input->'plan_id' IS DISTINCT FROM to_jsonb(p.plan_id)
   OR input->'plan_digest' IS DISTINCT FROM to_jsonb(p.plan_digest)
   OR input->'expected_revision' IS DISTINCT FROM to_jsonb(p.expected_revision)
   OR result IS DISTINCT FROM jsonb_build_object('revision',NEW.revision,'status','APPLIED','secret_revision',NEW.secret_revision)
   OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
    AND payload_id=NEW.values_payload_id AND parent_payload_id=p.values_payload_id)
   OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
    AND payload_id=NEW.values_payload_id AND parent_payload_id=op.input_payload_id)
   OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
    AND payload_id=op.result_payload_id AND parent_payload_id=NEW.values_payload_id) THEN
   RAISE EXCEPTION 'MEMORY_CONFIG_REVISION_SOURCE_CONFLICT' USING ERRCODE='23514';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER memory_config_plan_values_guard BEFORE INSERT ON public.bot_memory_config_plans
 FOR EACH ROW EXECUTE FUNCTION public.forest_memory_config_values_guard();
CREATE TRIGGER memory_config_revision_values_guard BEFORE INSERT ON public.bot_memory_config_revisions
 FOR EACH ROW EXECUTE FUNCTION public.forest_memory_config_values_guard();

CREATE FUNCTION public.forest_memory_config_head_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r record;
BEGIN
 IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
 SELECT * INTO r FROM public.bot_memory_config_revisions WHERE bot_id=NEW.bot_id AND revision=NEW.revision;
 IF NOT FOUND OR NEW.bot_id IS DISTINCT FROM OLD.bot_id OR NEW.revision<>OLD.revision+1
  OR r.previous_revision IS DISTINCT FROM OLD.revision OR NEW.secret_revision IS DISTINCT FROM r.secret_revision THEN
  RAISE EXCEPTION 'MEMORY_CONFIG_HEAD_CONFLICT' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER memory_config_head_guard BEFORE UPDATE ON public.bot_memory_config_heads
 FOR EACH ROW EXECUTE FUNCTION public.forest_memory_config_head_guard();

CREATE FUNCTION public.forest_memory_plan_receipt_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.bot_control_value_sources b JOIN public.bot_web_operations w
  ON w.bot_id=b.bot_id AND w.operation_id=b.operation_id WHERE b.bot_id=NEW.bot_id
  AND b.family='memory.config' AND b.source_kind='PLAN' AND b.source_id=NEW.plan_id
  AND NOT b.legacy_untracked AND w.action='memory.config.plan'
  AND ROW(w.result_payload_id,w.result_payload_digest) IS NOT DISTINCT FROM ROW(b.payload_id,b.payload_digest)
  AND ROW(w.subject_id,w.issuer_id) IS NOT DISTINCT FROM ROW(NEW.subject_id,NEW.issuer_id)) THEN
  RAISE EXCEPTION 'MEMORY_PLAN_RECEIPT_REQUIRED' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER memory_plan_receipt_complete AFTER INSERT ON public.bot_memory_config_plans
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.forest_memory_plan_receipt_complete();

CREATE FUNCTION public.forest_memory_deployment_identity_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r record; value jsonb;
BEGIN
 SELECT * INTO r FROM public.bot_memory_config_revisions WHERE bot_id=NEW.bot_id AND revision=NEW.first_revision;
 IF NOT FOUND THEN RAISE EXCEPTION 'MEMORY_DEPLOYMENT_SOURCE_MISSING' USING ERRCODE='23514'; END IF;
 value := public.forest_config_payload_values(NEW.bot_id,r.values_payload_id,r.values_payload_digest,
  'memory.config','REVISION','',r.revision::text);
 IF NEW.deployment_digest IS DISTINCT FROM public.forest_memory_identity_digest(value->>'deployment_id')
  OR NEW.endpoint_digest IS DISTINCT FROM public.forest_memory_identity_digest(value->>'endpoint') THEN
  RAISE EXCEPTION 'MEMORY_DEPLOYMENT_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER memory_deployment_identity_guard BEFORE INSERT ON public.bot_memory_deployment_bindings
 FOR EACH ROW EXECUTE FUNCTION public.forest_memory_deployment_identity_guard();
CREATE FUNCTION public.forest_memory_deployment_register() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE value jsonb; deployment text; endpoint text; prior text;
BEGIN
 value := public.forest_config_payload_values(NEW.bot_id,NEW.values_payload_id,NEW.values_payload_digest,
  'memory.config','REVISION','',NEW.revision::text);
 deployment := public.forest_memory_identity_digest(value->>'deployment_id');
 endpoint := public.forest_memory_identity_digest(value->>'endpoint');
 INSERT INTO public.bot_memory_deployment_bindings(bot_id,deployment_digest,endpoint_digest,first_revision)
  VALUES(NEW.bot_id,deployment,endpoint,NEW.revision) ON CONFLICT DO NOTHING;
 SELECT endpoint_digest INTO prior FROM public.bot_memory_deployment_bindings
  WHERE bot_id=NEW.bot_id AND deployment_digest=deployment;
 IF prior IS DISTINCT FROM endpoint THEN
  RAISE EXCEPTION 'MEMORY_DEPLOYMENT_CONFLICT' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER memory_deployment_register AFTER INSERT ON public.bot_memory_config_revisions
 FOR EACH ROW EXECUTE FUNCTION public.forest_memory_deployment_register();

CREATE FUNCTION public.forest_memory_request_profile_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r record; h record; value jsonb; expected jsonb; owner text; parent_id text;
BEGIN
 SELECT * INTO r FROM public.bot_memory_config_revisions WHERE bot_id=NEW.bot_id AND revision=NEW.config_revision;
 IF NOT FOUND THEN RAISE EXCEPTION 'MEMORY_CONFIG_SOURCE_MISSING' USING ERRCODE='23514'; END IF;
 value := public.forest_config_payload_values(NEW.bot_id,r.values_payload_id,r.values_payload_digest,
  'memory.config','REVISION','',r.revision::text);
 owner := 'profile:' || public.forest_memory_identity_digest(NEW.request_id);
 SELECT p.*,b.content INTO h FROM public.bot_payload_headers p LEFT JOIN public.bot_payload_bodies b USING(bot_id,payload_id)
  WHERE p.bot_id=NEW.bot_id AND p.payload_id=NEW.profile_payload_id;
 IF NOT FOUND OR h.owner_kind IS DISTINCT FROM 'memory.embedding' OR h.owner_id IS DISTINCT FROM owner
  OR h.encoding IS DISTINCT FROM 'json' OR h.digest IS DISTINCT FROM NEW.profile_payload_digest
  OR NEW.space_id IS DISTINCT FROM h.digest OR h.state IS DISTINCT FROM 'ACTIVE' OR h.content IS NULL
  OR h.digest IS DISTINCT FROM ('sha256:' || encode(sha256(h.content),'hex')) THEN
  RAISE EXCEPTION 'MEMORY_PROFILE_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 expected := jsonb_build_object('deployment_id',value->'deployment_id','model_revision',value->'model_revision',
  'model',value->'model','dimensions',value->'dimensions','protocol_revision','jina-clip-v2-http-2026-09-v1',
  'document_task',NULL,'query_task','retrieval.query','normalized',true,'truncate',false,
  'image_input','authorized_capture_bytes_v1','postprocessing','l2_float32_v1','mixed_strategy','independent_facets_v1');
 IF convert_from(h.content,'UTF8')::jsonb IS DISTINCT FROM expected OR NEW.profile_metadata IS DISTINCT FROM
  jsonb_build_object('dimensions',value->'dimensions','protocol_revision','jina-clip-v2-http-2026-09-v1') THEN
  RAISE EXCEPTION 'MEMORY_PROFILE_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 FOREACH parent_id IN ARRAY ARRAY[NEW.profile_payload_id,NEW.body_payload_id,NEW.manifest_payload_id] LOOP
  IF NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=parent_id AND parent_payload_id=r.values_payload_id) THEN
   RAISE EXCEPTION 'MEMORY_PROFILE_SOURCE_CONFLICT' USING ERRCODE='23514';
  END IF;
 END LOOP;
 RETURN NEW;
END $$;
CREATE TRIGGER memory_request_profile_guard BEFORE INSERT ON public.bot_memory_embedding_requests
 FOR EACH ROW EXECUTE FUNCTION public.forest_memory_request_profile_guard();
"""
