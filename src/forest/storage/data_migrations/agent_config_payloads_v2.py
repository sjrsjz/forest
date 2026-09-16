"""Frozen 045: Agent configuration and original Run specifications only."""

import math

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.storage.data_migrations.config_dependencies_v2 import import_dependency_in
from forest.storage.migration_v1 import _digest, import_payload_in, legacy_source_ref

LIMITS = {
    "max_steps": (16, 128), "run_timeout": (600, 86400),
    "model_timeout": (60, 86400), "daily_token_budget": (1000000, 10000000000),
    "run_token_budget": (1000000, 10000000000), "max_output_tokens": (4096, 262144),
}


def _metadata(values):
    result = {}
    for key, (default, maximum) in LIMITS.items():
        value = values.get(key, default)
        if key == "model_timeout" and type(value) in {int, float} and math.isfinite(value) and 0 < value <= maximum:
            result[key] = value
            continue
        if type(value) is not int or not 1 <= value <= maximum:
            raise ForestError("AGENT_SPEC_INVALID", "Invalid legacy Agent limits", 422)
        result[key] = value
    for key, default in (("enabled", True),):
        value = values.get(key, default)
        if type(value) is not bool:
            raise ForestError("AGENT_SPEC_INVALID", "Invalid legacy Agent switch", 422)
        result[key] = value
    return result


async def _source(conn, bot_id, row, kind):
    if kind == "REVISION":
        operation = await (await conn.execute(
            "SELECT * FROM public.bot_web_operations WHERE bot_id=%s AND operation_id=%s",
            (bot_id, row["operation_id"]),
        )).fetchone()
        if operation and operation["action"] == "agent.config.apply":
            return operation["result_payload_id"]
    identity = row["plan_id"] if kind == "PLAN" else _digest(["", row["revision"]])
    bridge = await (await conn.execute(
        "SELECT * FROM public.bot_control_value_sources WHERE bot_id=%s AND family='agent.config' "
        "AND source_kind=%s AND source_id=%s", (bot_id, kind, identity),
    )).fetchone()
    if bridge is None:
        raise ForestError("CONTROL_SOURCE_MISSING", "Legacy Agent configuration source is absent", 409)
    return bridge["payload_id"]


async def migrate_in(conn, store):
    for table, kind, key in (
        ("bot_agent_config_plans", "PLAN", "plan_id"),
        ("bot_agent_config_revisions", "REVISION", "revision"),
    ):
        after = None
        while True:
            rows = await (await conn.execute(
                f"SELECT * FROM public.{table} WHERE bot_id=%s "
                f"AND (%s::{'text' if key == 'plan_id' else 'bigint'} IS NULL OR {key}>%s) ORDER BY {key} LIMIT 64",
                (store.bot_id, after, after),
            )).fetchall()
            if not rows:
                break
            for row in rows:
                parent = await _source(conn, store.bot_id, row, kind)
                identity = str(row[key])
                value = {"family": "agent.config", "kind": kind, "resource": "", "identity": identity, "values": row["values"]}
                ref = await import_payload_in(
                    conn, store, owner_kind="control.config",
                    owner_id="config-values-v1:" + _digest(["agent.config", kind, "", identity]),
                    value=value, encoding="json", parents=(parent,),
                )
                await conn.execute(
                    f"UPDATE public.{table} SET values=NULL,values_payload_id=%s,values_payload_digest=%s,values_metadata=%s "
                    f"WHERE bot_id=%s AND {key}=%s",
                    (ref.payload_id, ref.digest, Jsonb(_metadata(row["values"])), store.bot_id, row[key]),
                )
            after = rows[-1][key]
    after = None
    while True:
        rows = await (await conn.execute(
            "SELECT * FROM public.bot_agent_runs WHERE bot_id=%s AND (%s::text IS NULL OR run_id>%s) ORDER BY run_id LIMIT 64",
            (store.bot_id, after, after),
        )).fetchall()
        if not rows:
            break
        for row in rows:
            spec = row["spec"]
            if not isinstance(spec, dict) or _digest(spec) != row["spec_digest"]:
                raise ForestError("AGENT_SPEC_INVALID", "Legacy Run specification differs from its digest", 409)
            meta = {"config_revision": spec.get("config_revision", 0), "secret_revision": spec.get("secret_revision", 0),
                    "values": _metadata(spec.get("values", spec))}
            parents = ()
            if meta["config_revision"]:
                config = await (await conn.execute(
                    "SELECT values_payload_id FROM public.bot_agent_config_revisions WHERE bot_id=%s AND revision=%s",
                    (store.bot_id, meta["config_revision"]),
                )).fetchone()
                if config is None:
                    raise ForestError("AGENT_SPEC_INVALID", "Legacy Run has no actual configuration revision", 409)
                parents = (config["values_payload_id"],)
            ref = await import_payload_in(
                conn, store, owner_kind="control.config", owner_id="agent-run-spec-v1:" + _digest([row["run_id"]]),
                value=spec, encoding="json", parents=parents, sources=(legacy_source_ref(store.bot_id),),
            )
            await conn.execute(
                "UPDATE public.bot_agent_runs SET spec=NULL,spec_payload_id=%s,spec_payload_digest=%s,spec_metadata=%s,"
                "spec_imported_legacy=true WHERE bot_id=%s AND run_id=%s",
                (ref.payload_id, ref.digest, Jsonb(meta), store.bot_id, row["run_id"]),
            )
            step_after = ""
            while True:
                steps = await (await conn.execute(
                    "SELECT logical_step_id,snapshot_payload_id FROM public.bot_agent_steps "
                    "WHERE bot_id=%s AND run_id=%s AND logical_step_id>%s ORDER BY logical_step_id LIMIT 64",
                    (store.bot_id, row["run_id"], step_after),
                )).fetchall()
                if not steps:
                    break
                for step in steps:
                    await import_dependency_in(
                        conn, store, step["snapshot_payload_id"], ref.payload_id,
                        proof_kind="agent.spec", proof_id=row["run_id"],
                    )
                step_after = steps[-1]["logical_step_id"]
        after = rows[-1]["run_id"]
    await conn.execute(FINAL_SQL)


FINAL_SQL = r"""
CREATE FUNCTION public.forest_agent_values_metadata(v jsonb) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE result jsonb := '{}'::jsonb; item record; raw jsonb;
BEGIN
 IF jsonb_typeof(v) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION 'AGENT_SPEC_INVALID' USING ERRCODE='55000'; END IF;
 FOR item IN SELECT * FROM (VALUES
  ('max_steps',16::bigint,128::bigint),('run_timeout',600,86400),('model_timeout',60,86400),
  ('daily_token_budget',1000000,10000000000),('run_token_budget',1000000,10000000000),
  ('max_output_tokens',4096,262144)) AS limits(key,fallback,maximum)
 LOOP
  raw := COALESCE(v->item.key,to_jsonb(item.fallback));
  IF jsonb_typeof(raw) IS DISTINCT FROM 'number' THEN
   RAISE EXCEPTION 'AGENT_SPEC_INVALID' USING ERRCODE='55000';
  END IF;
  IF (item.key<>'model_timeout' AND (raw::text !~ '^[0-9]{1,11}$' OR raw::text::numeric<1))
   OR raw::text::numeric<=0 OR raw::text::numeric>item.maximum THEN
   RAISE EXCEPTION 'AGENT_SPEC_INVALID' USING ERRCODE='55000';
  END IF;
  result := result || jsonb_build_object(item.key,raw);
 END LOOP;
 FOR item IN SELECT * FROM (VALUES ('enabled',true)) AS switches(key,fallback)
 LOOP
  raw := COALESCE(v->item.key,to_jsonb(item.fallback));
  IF jsonb_typeof(raw) IS DISTINCT FROM 'boolean' THEN RAISE EXCEPTION 'AGENT_SPEC_INVALID' USING ERRCODE='55000'; END IF;
  result := result || jsonb_build_object(item.key,raw);
 END LOOP;
 RETURN result;
END $$;
CREATE FUNCTION public.forest_agent_spec_metadata(v jsonb) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE c jsonb := COALESCE(v->'config_revision','0'::jsonb); s jsonb := COALESCE(v->'secret_revision','0'::jsonb);
BEGIN
 IF jsonb_typeof(v) IS DISTINCT FROM 'object' OR jsonb_typeof(c) IS DISTINCT FROM 'number'
  OR jsonb_typeof(s) IS DISTINCT FROM 'number' OR c::text !~ '^[0-9]{1,19}$' OR s::text !~ '^[0-9]{1,19}$'
  OR c::text::numeric>9223372036854775807 OR s::text::numeric>9223372036854775807 THEN
  RAISE EXCEPTION 'AGENT_SPEC_INVALID' USING ERRCODE='55000';
 END IF;
 RETURN jsonb_build_object('config_revision',c,'secret_revision',s,
  'values',public.forest_agent_values_metadata(COALESCE(v->'values',v)));
END $$;
ALTER TABLE public.bot_agent_config_plans
 ALTER COLUMN values_payload_id SET NOT NULL, ALTER COLUMN values_payload_digest SET NOT NULL, ALTER COLUMN values_metadata SET NOT NULL,
 ADD CHECK(values IS NULL), ADD CHECK(values_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
 ADD CHECK(values_metadata=public.forest_agent_values_metadata(values_metadata));
ALTER TABLE public.bot_agent_config_revisions
 ALTER COLUMN values_payload_id SET NOT NULL, ALTER COLUMN values_payload_digest SET NOT NULL, ALTER COLUMN values_metadata SET NOT NULL,
 ADD CHECK(values IS NULL), ADD CHECK(values_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
 ADD CHECK(values_metadata=public.forest_agent_values_metadata(values_metadata));
ALTER TABLE public.bot_agent_runs
 ALTER COLUMN spec_payload_id SET NOT NULL, ALTER COLUMN spec_payload_digest SET NOT NULL, ALTER COLUMN spec_metadata SET NOT NULL,
 ADD CHECK(spec IS NULL), ADD CHECK(spec_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
 ADD CHECK(spec_metadata=public.forest_agent_spec_metadata(spec_metadata));
CREATE TRIGGER bot_agent_config_plans_immutable BEFORE UPDATE OR DELETE ON public.bot_agent_config_plans
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_agent_config_revisions_immutable BEFORE UPDATE OR DELETE ON public.bot_agent_config_revisions
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_agent_run_identity BEFORE UPDATE OR DELETE ON public.bot_agent_runs
 FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields('{owner_epoch,actor_epoch,actor_host_incarnation,checkpoint_seq,next_step_seq,published_step_seq,current_step_id,phase,input_revision,selected_input_end_seq,wait_classified_seq,final_step_id,final_input_revision,final_reply_id,error_code,tool_iteration_count,thinking_iteration_count,protocol_repair_count}');

CREATE FUNCTION public.forest_agent_config_values_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE v jsonb; p record; op record; input jsonb; result jsonb; original jsonb; kind text; identity text;
BEGIN
 IF TG_TABLE_NAME='bot_agent_config_plans' THEN kind := 'PLAN'; identity := NEW.plan_id;
 ELSE kind := 'REVISION'; identity := NEW.revision::text; END IF;
 v := public.forest_config_payload_values(NEW.bot_id,NEW.values_payload_id,NEW.values_payload_digest,
  'agent.config',kind,'',identity);
 IF NEW.values IS NOT NULL OR NEW.values_metadata IS DISTINCT FROM public.forest_agent_values_metadata(v) THEN
  RAISE EXCEPTION 'AGENT_CONFIG_PAYLOAD_CONFLICT' USING ERRCODE='55000';
 END IF;
 IF TG_TABLE_NAME='bot_agent_config_plans' THEN RETURN NEW; END IF;
 SELECT * INTO p FROM public.bot_agent_config_plans WHERE bot_id=NEW.bot_id AND plan_id=NEW.plan_id;
 SELECT * INTO op FROM public.bot_web_operations WHERE bot_id=NEW.bot_id AND operation_id=NEW.operation_id;
 IF p IS NULL OR op IS NULL OR op.action IS DISTINCT FROM 'agent.config.apply'
  OR op.subject_id IS DISTINCT FROM p.subject_id OR op.issuer_id IS DISTINCT FROM p.issuer_id
  OR NEW.previous_revision IS DISTINCT FROM p.expected_revision OR p.expires_at<=clock_timestamp()
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=NEW.values_payload_id AND parent_payload_id=p.values_payload_id)
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=NEW.values_payload_id AND parent_payload_id=op.input_payload_id)
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=op.result_payload_id AND parent_payload_id=NEW.values_payload_id) THEN
  RAISE EXCEPTION 'AGENT_CONFIG_SOURCE_CONFLICT' USING ERRCODE='55000';
 END IF;
 original := public.forest_config_payload_values(NEW.bot_id,p.values_payload_id,p.values_payload_digest,'agent.config','PLAN','',p.plan_id);
 input := public.forest_control_payload_value(NEW.bot_id,op.input_payload_id,op.input_payload_digest,'input',op.operation_id,op.action);
 result := public.forest_control_payload_value(NEW.bot_id,op.result_payload_id,op.result_payload_digest,'result',op.operation_id,op.action);
 IF v IS DISTINCT FROM original OR input IS DISTINCT FROM jsonb_build_object('expected_revision',p.expected_revision,'plan_id',p.plan_id,'plan_digest',p.plan_digest)
  OR result IS DISTINCT FROM jsonb_build_object('revision',NEW.revision,'secret_revision',NEW.secret_revision,'status','APPLIED') THEN
  RAISE EXCEPTION 'AGENT_CONFIG_SOURCE_CONFLICT' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER bot_agent_config_plan_values BEFORE INSERT ON public.bot_agent_config_plans
 FOR EACH ROW EXECUTE FUNCTION public.forest_agent_config_values_insert();
CREATE TRIGGER bot_agent_config_revision_values BEFORE INSERT ON public.bot_agent_config_revisions
 FOR EACH ROW EXECUTE FUNCTION public.forest_agent_config_values_insert();

CREATE FUNCTION public.forest_agent_plan_committed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.bot_control_value_sources b JOIN public.bot_web_operations op
  ON op.bot_id=b.bot_id AND op.operation_id=b.operation_id WHERE b.bot_id=NEW.bot_id AND b.family='agent.config'
  AND b.source_kind='PLAN' AND b.source_id=NEW.plan_id AND NOT b.legacy_untracked
  AND op.action='agent.config.plan' AND op.subject_id=NEW.subject_id AND op.issuer_id=NEW.issuer_id
  AND op.result_payload_id=b.payload_id AND op.result_payload_digest=b.payload_digest
  AND EXISTS(SELECT 1 FROM public.bot_payload_derivations d WHERE d.bot_id=NEW.bot_id
   AND d.payload_id=NEW.values_payload_id AND d.parent_payload_id=op.input_payload_id)
  AND EXISTS(SELECT 1 FROM public.bot_payload_derivations d WHERE d.bot_id=NEW.bot_id
   AND d.payload_id=op.result_payload_id AND d.parent_payload_id=NEW.values_payload_id)) THEN
  RAISE EXCEPTION 'AGENT_CONFIG_SOURCE_CONFLICT' USING ERRCODE='55000';
 END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER bot_agent_plan_complete AFTER INSERT ON public.bot_agent_config_plans
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.forest_agent_plan_committed();

CREATE FUNCTION public.forest_agent_spec_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE h record; body jsonb; expected_owner text; cfg record; config_value jsonb; domain_owner text;
BEGIN
 PERFORM 1 FROM public.bot_memory_clock WHERE bot_id=NEW.bot_id FOR UPDATE;
 SELECT p.*,b.content INTO h FROM public.bot_payload_headers p LEFT JOIN public.bot_payload_bodies b USING(bot_id,payload_id)
  WHERE p.bot_id=NEW.bot_id AND p.payload_id=NEW.spec_payload_id;
 expected_owner := 'agent-run-spec-v1:sha256:'||encode(sha256(convert_to('['||to_jsonb(NEW.run_id)::text||']','UTF8')),'hex');
 IF NEW.spec_imported_legacy OR NEW.spec IS NOT NULL OR h IS NULL OR h.owner_kind IS DISTINCT FROM 'control.config'
  OR h.owner_id IS DISTINCT FROM expected_owner OR h.encoding IS DISTINCT FROM 'json' OR h.state IS DISTINCT FROM 'ACTIVE'
  OR NEW.spec_payload_id IS DISTINCT FROM 'payload:sha256:'||encode(sha256(convert_to(
   '['||to_jsonb(NEW.bot_id)::text||',"control.config",'||to_jsonb(expected_owner)::text||']','UTF8')),'hex')
  OR h.content IS NULL OR h.digest IS DISTINCT FROM NEW.spec_payload_digest OR h.digest IS DISTINCT FROM NEW.spec_digest
  OR NOT public.forest_payload_dependencies_available(NEW.bot_id,NEW.spec_payload_id)
  OR h.digest IS DISTINCT FROM 'sha256:'||encode(sha256(h.content),'hex') THEN
  RAISE EXCEPTION 'AGENT_SPEC_INVALID' USING ERRCODE='55000';
 END IF;
 body := convert_from(h.content,'UTF8')::jsonb;
 IF NEW.spec_metadata IS DISTINCT FROM public.forest_agent_spec_metadata(body) THEN
  RAISE EXCEPTION 'AGENT_SPEC_INVALID' USING ERRCODE='55000';
 END IF;
 IF (NEW.spec_metadata->>'config_revision')::bigint>0 THEN
  SELECT * INTO cfg FROM public.bot_agent_config_revisions WHERE bot_id=NEW.bot_id
   AND revision=(NEW.spec_metadata->>'config_revision')::bigint;
  IF cfg IS NULL OR cfg.secret_revision IS DISTINCT FROM (NEW.spec_metadata->>'secret_revision')::bigint
   OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations d WHERE d.bot_id=NEW.bot_id
    AND d.payload_id=NEW.spec_payload_id AND d.parent_payload_id=cfg.values_payload_id) THEN
   RAISE EXCEPTION 'AGENT_SPEC_INVALID' USING ERRCODE='55000';
  END IF;
  config_value := public.forest_config_payload_values(NEW.bot_id,cfg.values_payload_id,cfg.values_payload_digest,
   'agent.config','REVISION','',cfg.revision::text);
  IF COALESCE(body->'values',body) IS DISTINCT FROM config_value THEN
   RAISE EXCEPTION 'AGENT_SPEC_INVALID' USING ERRCODE='55000';
  END IF;
 ELSE
  domain_owner := 'agent-spec:sha256:'||encode(sha256(convert_to('['||to_jsonb(NEW.bot_id)::text||','||to_jsonb(NEW.run_id)::text||']','UTF8')),'hex');
  IF NOT EXISTS(SELECT 1 FROM public.bot_payload_domains d JOIN public.bot_payload_sources s
   ON s.bot_id=d.bot_id AND s.source_unit_id=d.source_unit_id WHERE d.bot_id=NEW.bot_id
   AND d.kind='AGENT_SPEC' AND d.owner_id=domain_owner AND s.payload_id=NEW.spec_payload_id
   AND s.source_revision=1 AND s.part_id='*') THEN
   RAISE EXCEPTION 'AGENT_SPEC_INVALID' USING ERRCODE='55000';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER bot_agent_spec_body BEFORE INSERT ON public.bot_agent_runs
 FOR EACH ROW EXECUTE FUNCTION public.forest_agent_spec_insert();
"""
