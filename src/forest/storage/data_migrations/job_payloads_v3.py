"""Frozen 061 Job settings/delegation import and post-cutover guards."""

from psycopg.types.json import Jsonb

from forest.storage.config_payloads import ConfigPayloadStore
from forest.storage.job_payloads import delegation_metadata, normalize_settings, settings_metadata
from forest.storage.migration_v1 import _digest
from forest.storage.payloads import PayloadStore


async def _source(conn, bot_id, row, kind):
    if kind == "REVISION":
        operation = await (await conn.execute(
            "SELECT result_payload_id FROM public.bot_web_operations WHERE bot_id=%s "
            "AND operation_id=%s AND action='job.apply'",
            (bot_id, row["operation_id"]),
        )).fetchone()
        if operation:
            return operation["result_payload_id"]
    source_id = row["plan_id"] if kind == "PLAN" else _digest([row["job_id"], row["revision"]])
    bridge = await (await conn.execute(
        "SELECT payload_id FROM public.bot_control_value_sources WHERE bot_id=%s "
        "AND family='job' AND source_kind=%s AND source_id=%s",
        (bot_id, kind, source_id),
    )).fetchone()
    if bridge is None:
        raise ValueError("Job definition has no immutable 041 source")
    return bridge["payload_id"]


async def _import_config(payloads, conn, store, *, family, kind, job_id, identity, value, parent):
    return await payloads.import_legacy_json_in(
        conn,
        owner_kind="control.config",
        owner_id=ConfigPayloadStore.owner_id(
            family=family, kind=kind, resource=job_id, identity=identity,
        ),
        value={
            "family": family,
            "kind": kind,
            "resource": job_id,
            "identity": identity,
            "values": value,
        },
        parents=(parent,),
    )


async def migrate_in(conn, store):
    payloads = PayloadStore(store)
    after = None
    while True:
        rows = await (await conn.execute(
            "SELECT * FROM public.bot_job_plans WHERE bot_id=%s AND "
            "(%s::text IS NULL OR plan_id>%s) ORDER BY plan_id LIMIT 64",
            (store.bot_id, after, after),
        )).fetchall()
        if not rows:
            break
        for row in rows:
            normalized = normalize_settings(row["values"])
            if normalized != row["values"]:
                raise ValueError("Legacy Job plan is not a normalized JobSettings value")
            parent = await _source(conn, store.bot_id, row, "PLAN")
            ref = await _import_config(
                payloads,
                conn,
                store,
                family="job",
                kind="PLAN",
                job_id=row["job_id"],
                identity=row["plan_id"],
                value=row["values"],
                parent=parent,
            )
            await conn.execute(
                "UPDATE public.bot_job_plans SET values=NULL,values_payload_id=%s,"
                "values_payload_digest=%s,values_metadata=%s WHERE bot_id=%s AND plan_id=%s",
                (ref.payload_id, ref.digest, Jsonb(settings_metadata(row["values"])),
                 store.bot_id, row["plan_id"]),
            )
        after = rows[-1]["plan_id"]

    after = None
    while True:
        rows = await (await conn.execute(
            "SELECT * FROM public.bot_job_revisions WHERE bot_id=%s AND "
            "(%s::text IS NULL OR (job_id,revision)>(%s,%s::bigint)) "
            "ORDER BY job_id,revision LIMIT 64",
            (store.bot_id, after[0] if after else None, *(after or (None, None))),
        )).fetchall()
        if not rows:
            break
        for row in rows:
            normalized = normalize_settings(row["values"])
            if normalized != row["values"]:
                raise ValueError("Legacy Job revision is not a normalized JobSettings value")
            grant_metadata = delegation_metadata(row["delegation"])
            parent = await _source(conn, store.bot_id, row, "REVISION")
            settings_ref = await _import_config(
                payloads,
                conn,
                store,
                family="job",
                kind="REVISION",
                job_id=row["job_id"],
                identity=str(row["revision"]),
                value=row["values"],
                parent=parent,
            )
            delegation_ref = await _import_config(
                payloads,
                conn,
                store,
                family="job.delegation",
                kind="REVISION",
                job_id=row["job_id"],
                identity=str(row["revision"]),
                value=row["delegation"],
                parent=parent,
            )
            await conn.execute(
                "UPDATE public.bot_job_revisions SET values=NULL,delegation=NULL,"
                "values_payload_id=%s,values_payload_digest=%s,values_metadata=%s,"
                "delegation_payload_id=%s,delegation_payload_digest=%s,delegation_metadata=%s "
                "WHERE bot_id=%s AND job_id=%s AND revision=%s",
                (
                    settings_ref.payload_id,
                    settings_ref.digest,
                    Jsonb(settings_metadata(row["values"])),
                    delegation_ref.payload_id,
                    delegation_ref.digest,
                    Jsonb(grant_metadata),
                    store.bot_id,
                    row["job_id"],
                    row["revision"],
                ),
            )
        after = (rows[-1]["job_id"], rows[-1]["revision"])
    await conn.execute(FINAL_SQL)


FINAL_SQL = r"""
CREATE FUNCTION public.forest_job_settings_metadata(v jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE result jsonb;
BEGIN
 IF jsonb_typeof(v) IS DISTINCT FROM 'object'
  OR NOT (v ?& ARRAY['name','enabled','kind','conversation_id','prompt','schedule','misfire',
   'misfire_grace_seconds','catch_up_limit','max_overlap','delegation_expires_at','max_runs'])
  OR v-ARRAY['name','enabled','kind','conversation_id','prompt','schedule','misfire',
   'misfire_grace_seconds','catch_up_limit','max_overlap','delegation_expires_at','max_runs']<>'{}'::jsonb
  OR jsonb_typeof(v->'name') IS DISTINCT FROM 'string'
  OR octet_length(v->>'name') NOT BETWEEN 1 AND 800
  OR jsonb_typeof(v->'prompt') IS DISTINCT FROM 'string'
  OR btrim(v->>'prompt')='' OR octet_length(v->>'prompt')>32768
  OR jsonb_typeof(v->'enabled') IS DISTINCT FROM 'boolean'
  OR v->>'kind'<>'conversation.trigger'
  OR jsonb_typeof(v->'conversation_id') IS DISTINCT FROM 'string'
  OR octet_length(v->>'conversation_id') NOT BETWEEN 1 AND 256
  OR jsonb_typeof(v->'schedule') IS DISTINCT FROM 'object'
  OR v->>'misfire' NOT IN ('skip','coalesce','catch_up')
  OR (v->>'misfire_grace_seconds') !~ '^[0-9]{1,4}$'
  OR (v->>'misfire_grace_seconds')::int NOT BETWEEN 0 AND 3600
  OR (v->>'catch_up_limit') !~ '^[0-9]{1,2}$'
  OR (v->>'catch_up_limit')::int NOT BETWEEN 1 AND 10
  OR (v->>'max_overlap') !~ '^[0-9]$'
  OR (v->>'max_overlap')::int NOT BETWEEN 1 AND 8
  OR (v->>'max_runs') !~ '^[0-9]{1,6}$'
  OR (v->>'max_runs')::int NOT BETWEEN 1 AND 100000 THEN
  RAISE EXCEPTION 'JOB_SETTINGS_INVALID' USING ERRCODE='23514';
 END IF;
 PERFORM (v->>'delegation_expires_at')::timestamptz;
 SELECT jsonb_object_agg(key,v->key) INTO result FROM unnest(ARRAY[
  'enabled','kind','conversation_id','schedule','misfire','misfire_grace_seconds',
  'catch_up_limit','max_overlap','delegation_expires_at','max_runs'
 ]) key;
 RETURN result;
END $$;

CREATE FUNCTION public.forest_job_delegation_metadata(v jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE epoch jsonb;
BEGIN
 IF jsonb_typeof(v) IS DISTINCT FROM 'object'
  OR jsonb_typeof(v->'issuer_id') IS DISTINCT FROM 'string'
  OR octet_length(v->>'issuer_id') NOT BETWEEN 1 AND 4096
  OR (v->>'key_revision') !~ '^[1-9][0-9]{0,18}$'
  OR (v->>'key_revision')::numeric>=9223372036854775808
  OR (v->>'policy_revision') !~ '^[1-9][0-9]{0,18}$'
  OR (v->>'policy_revision')::numeric>=9223372036854775808 THEN
  RAISE EXCEPTION 'JOB_DELEGATION_INVALID' USING ERRCODE='23514';
 END IF;
 PERFORM (v->>'expires_at')::timestamptz;
 epoch := COALESCE(v->'delegation_epoch','null'::jsonb);
 IF epoch<>'null'::jsonb AND ((v->>'delegation_epoch') !~ '^[1-9][0-9]{0,18}$'
  OR (v->>'delegation_epoch')::numeric>=9223372036854775808) THEN
  RAISE EXCEPTION 'JOB_DELEGATION_INVALID' USING ERRCODE='23514';
 END IF;
 RETURN jsonb_build_object(
  'issuer_id',v->'issuer_id','key_revision',v->'key_revision',
  'policy_revision',v->'policy_revision','delegation_epoch',epoch,
  'expires_at',v->'expires_at'
 );
END $$;

ALTER TABLE public.bot_job_plans
 ALTER COLUMN values_payload_id SET NOT NULL,
 ALTER COLUMN values_payload_digest SET NOT NULL,
 ALTER COLUMN values_metadata SET NOT NULL,
 ADD CHECK(values IS NULL),
 ADD CHECK(values_payload_digest ~ '^sha256:[0-9a-f]{64}$'),
 ADD CHECK(jsonb_typeof(values_metadata)='object');
ALTER TABLE public.bot_job_revisions
 ALTER COLUMN values_payload_id SET NOT NULL,
 ALTER COLUMN values_payload_digest SET NOT NULL,
 ALTER COLUMN values_metadata SET NOT NULL,
 ALTER COLUMN delegation_payload_id SET NOT NULL,
 ALTER COLUMN delegation_payload_digest SET NOT NULL,
 ALTER COLUMN delegation_metadata SET NOT NULL,
 ADD CHECK(values IS NULL AND delegation IS NULL),
 ADD CHECK(values_payload_digest ~ '^sha256:[0-9a-f]{64}$'),
 ADD CHECK(delegation_payload_digest ~ '^sha256:[0-9a-f]{64}$'),
 ADD CHECK(jsonb_typeof(values_metadata)='object' AND jsonb_typeof(delegation_metadata)='object');

CREATE TRIGGER bot_job_revision_immutable BEFORE UPDATE OR DELETE ON public.bot_job_revisions
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_job_plan_immutable BEFORE UPDATE OR DELETE ON public.bot_job_plans
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE FUNCTION public.forest_job_values_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE selected_kind text; selected_identity text; settings jsonb; grant_value jsonb;
 p record; op record; h record; plan_settings jsonb; input jsonb; result jsonb; expected_grant jsonb;
BEGIN
 selected_kind := CASE TG_TABLE_NAME WHEN 'bot_job_plans' THEN 'PLAN' ELSE 'REVISION' END;
 selected_identity := CASE selected_kind WHEN 'PLAN' THEN to_jsonb(NEW)->>'plan_id'
  ELSE to_jsonb(NEW)->>'revision' END;
 SELECT * INTO h FROM public.bot_jobs WHERE bot_id=NEW.bot_id AND job_id=NEW.job_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'JOB_HEAD_REQUIRED' USING ERRCODE='23514'; END IF;
 settings := public.forest_config_payload_values(
  NEW.bot_id,NEW.values_payload_id,NEW.values_payload_digest,
  'job',selected_kind,NEW.job_id,selected_identity);
 IF NEW.values IS NOT NULL OR NEW.values_metadata IS DISTINCT FROM public.forest_job_settings_metadata(settings) THEN
  RAISE EXCEPTION 'JOB_SETTINGS_METADATA_CONFLICT' USING ERRCODE='23514';
 END IF;
 IF selected_kind='PLAN' THEN
  IF NEW.expected_revision IS DISTINCT FROM h.revision THEN
   RAISE EXCEPTION 'JOB_HEAD_CONFLICT' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
 END IF;

 grant_value := public.forest_config_payload_values(
  NEW.bot_id,NEW.delegation_payload_id,NEW.delegation_payload_digest,
  'job.delegation','REVISION',NEW.job_id,NEW.revision::text);
 IF NEW.delegation IS NOT NULL
  OR NEW.delegation_metadata IS DISTINCT FROM public.forest_job_delegation_metadata(grant_value) THEN
  RAISE EXCEPTION 'JOB_DELEGATION_METADATA_CONFLICT' USING ERRCODE='23514';
 END IF;
 SELECT * INTO p FROM public.bot_job_plans WHERE bot_id=NEW.bot_id AND plan_id=NEW.plan_id;
 SELECT * INTO op FROM public.bot_web_operations WHERE bot_id=NEW.bot_id AND operation_id=NEW.operation_id;
 IF p.plan_id IS NULL OR op.operation_id IS NULL OR op.action<>'job.apply'
  OR ROW(p.job_id,p.subject_id,p.issuer_id,p.expected_revision)
   IS DISTINCT FROM ROW(NEW.job_id,op.subject_id,op.issuer_id,h.revision)
  OR NEW.revision<>h.revision+1 OR p.expires_at<=clock_timestamp() THEN
  RAISE EXCEPTION 'JOB_REVISION_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 plan_settings := public.forest_config_payload_values(
  NEW.bot_id,p.values_payload_id,p.values_payload_digest,'job','PLAN',NEW.job_id,p.plan_id);
 input := public.forest_control_payload_value(
  NEW.bot_id,op.input_payload_id,op.input_payload_digest,'input',op.operation_id,op.action);
 result := public.forest_control_payload_value(
  NEW.bot_id,op.result_payload_id,op.result_payload_digest,'result',op.operation_id,op.action);
 expected_grant := jsonb_build_object(
  'issuer_id',op.ticket_basis->'issuer_id','subject_id',op.ticket_basis->'subject_id',
  'key_revision',op.ticket_basis->'key_revision','account_revision',op.ticket_basis->'account_revision',
  'policy_revision',op.ticket_basis->'policy_revision',
  'delegation_id',to_jsonb('job-delegation:sha256:' || encode(sha256(convert_to(
   '[' || to_jsonb(NEW.bot_id)::text || ',' || to_jsonb(NEW.job_id)::text || ',' || NEW.revision::text || ']','UTF8')),'hex')),
  'delegation_epoch',to_jsonb(h.delegation_epoch+1),
  'origin_kind',to_jsonb(COALESCE(op.ticket_basis->>'origin_kind','web')),
  'principal_ref',to_jsonb('delegation:' || op.subject_id),
  'expires_at',plan_settings->'delegation_expires_at',
  'capabilities','["conversation.trigger"]'::jsonb,'tool_capabilities','[]'::jsonb,
  'schedule_algorithm','"forest-calendar-v1"'::jsonb
 );
 IF op.ticket_basis ? 'source_binding_id' THEN
  expected_grant := expected_grant || jsonb_build_object('source_binding_id',op.ticket_basis->'source_binding_id');
 END IF;
 IF op.ticket_basis ? 'source_principal' THEN
  expected_grant := expected_grant || jsonb_build_object('source_principal',op.ticket_basis->'source_principal');
 END IF;
 IF settings IS DISTINCT FROM plan_settings OR grant_value IS DISTINCT FROM expected_grant
  OR input IS DISTINCT FROM jsonb_build_object(
   'job_id',NEW.job_id,'expected_revision',p.expected_revision,
   'plan_id',p.plan_id,'plan_digest',p.plan_digest)
  OR result->>'job_id' IS DISTINCT FROM NEW.job_id
  OR result->'revision' IS DISTINCT FROM to_jsonb(NEW.revision)
  OR result->>'status' IS DISTINCT FROM 'APPLIED'
  OR result->'delegation' IS DISTINCT FROM grant_value
  OR result-ARRAY['job_id','revision','status','delegation','next_due_at']<>'{}'::jsonb
  OR NOT (result ?& ARRAY['job_id','revision','status','delegation','next_due_at'])
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=NEW.values_payload_id AND parent_payload_id=p.values_payload_id)
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=NEW.values_payload_id AND parent_payload_id=op.input_payload_id)
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=NEW.delegation_payload_id AND parent_payload_id=p.values_payload_id)
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=NEW.delegation_payload_id AND parent_payload_id=op.input_payload_id)
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=op.result_payload_id AND parent_payload_id=NEW.values_payload_id)
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=op.result_payload_id AND parent_payload_id=NEW.delegation_payload_id) THEN
  RAISE EXCEPTION 'JOB_REVISION_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER job_plan_values_guard BEFORE INSERT ON public.bot_job_plans
 FOR EACH ROW EXECUTE FUNCTION public.forest_job_values_guard();
CREATE TRIGGER job_revision_values_guard BEFORE INSERT ON public.bot_job_revisions
 FOR EACH ROW EXECUTE FUNCTION public.forest_job_values_guard();

CREATE FUNCTION public.forest_job_plan_receipt_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.bot_control_value_sources b JOIN public.bot_web_operations w
  ON w.bot_id=b.bot_id AND w.operation_id=b.operation_id WHERE b.bot_id=NEW.bot_id
  AND b.family='job' AND b.source_kind='PLAN' AND b.source_id=NEW.plan_id
  AND NOT b.legacy_untracked AND w.action='job.plan'
  AND ROW(w.result_payload_id,w.result_payload_digest) IS NOT DISTINCT FROM ROW(b.payload_id,b.payload_digest)
  AND ROW(w.subject_id,w.issuer_id) IS NOT DISTINCT FROM ROW(NEW.subject_id,NEW.issuer_id)) THEN
  RAISE EXCEPTION 'JOB_PLAN_RECEIPT_REQUIRED' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER job_plan_receipt_complete AFTER INSERT ON public.bot_job_plans
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.forest_job_plan_receipt_complete();

CREATE OR REPLACE FUNCTION public.forest_control_plan_source_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE op record; plan jsonb; value jsonb; selected_table text; actual_values jsonb; resource text;
BEGIN
 IF NEW.legacy_untracked OR NEW.source_kind<>'PLAN' THEN
  RAISE EXCEPTION 'CONTROL_LEGACY_SOURCE_FORBIDDEN' USING ERRCODE='23514';
 END IF;
 SELECT * INTO op FROM public.bot_web_operations WHERE bot_id=NEW.bot_id AND operation_id=NEW.operation_id;
 IF NOT FOUND OR op.action<>NEW.family || '.plan'
  OR ROW(op.result_payload_id,op.result_payload_digest) IS DISTINCT FROM ROW(NEW.payload_id,NEW.payload_digest) THEN
  RAISE EXCEPTION 'CONTROL_PLAN_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 selected_table := CASE NEW.family WHEN 'config' THEN 'bot_config_plans' WHEN 'agent.config' THEN 'bot_agent_config_plans'
  WHEN 'memory.config' THEN 'bot_memory_config_plans' WHEN 'binding' THEN 'bot_binding_plans' WHEN 'job' THEN 'bot_job_plans' END;
 IF selected_table IS NULL THEN RAISE EXCEPTION 'CONTROL_PLAN_SOURCE_CONFLICT' USING ERRCODE='23514'; END IF;
 EXECUTE format('SELECT to_jsonb(p) FROM public.%I p WHERE bot_id=$1 AND plan_id=$2',selected_table)
 INTO plan USING NEW.bot_id,NEW.source_id;
 IF plan->>'plan_id' IS DISTINCT FROM NEW.source_id
  OR ROW(plan->>'subject_id',plan->>'issuer_id') IS DISTINCT FROM ROW(op.subject_id,op.issuer_id) THEN
  RAISE EXCEPTION 'CONTROL_PLAN_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 value := public.forest_control_payload_value(NEW.bot_id,NEW.payload_id,NEW.payload_digest,'result',NEW.operation_id,op.action);
 IF plan ? 'values_payload_id' THEN
  resource := CASE NEW.family WHEN 'config' THEN plan->>'component_id'
   WHEN 'binding' THEN plan->>'binding_id' WHEN 'job' THEN plan->>'job_id' ELSE '' END;
  actual_values := public.forest_config_payload_values(NEW.bot_id,plan->>'values_payload_id',plan->>'values_payload_digest',
   NEW.family,'PLAN',resource,NEW.source_id);
  IF NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=plan->>'values_payload_id' AND parent_payload_id=op.input_payload_id)
   OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
    AND payload_id=NEW.payload_id AND parent_payload_id=plan->>'values_payload_id') THEN
   RAISE EXCEPTION 'CONTROL_PLAN_SOURCE_CONFLICT' USING ERRCODE='23514';
  END IF;
 ELSE actual_values := plan->'values'; END IF;
 IF value->>'plan_id' IS DISTINCT FROM plan->>'plan_id' OR value->>'plan_digest' IS DISTINCT FROM plan->>'plan_digest'
  OR value->'values' IS DISTINCT FROM actual_values THEN
  RAISE EXCEPTION 'CONTROL_PLAN_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION public.forest_job_payload_available_unlocked(
 selected_bot text,selected_payload text,selected_digest text)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE h record; n bigint; suppressed boolean; initial jsonb;
BEGIN
 SELECT p.*,b.content INTO h FROM public.bot_payload_headers p
 LEFT JOIN public.bot_payload_bodies b USING(bot_id,payload_id)
 WHERE p.bot_id=selected_bot AND p.payload_id=selected_payload;
 IF NOT FOUND OR h.state<>'ACTIVE' OR h.content IS NULL OR h.digest IS DISTINCT FROM selected_digest
  OR h.byte_length IS DISTINCT FROM octet_length(h.content)::bigint
  OR h.digest IS DISTINCT FROM ('sha256:' || encode(sha256(h.content),'hex'))
  OR EXISTS(SELECT 1 FROM public.bot_payload_derivations d
   JOIN public.bot_payload_headers p ON p.bot_id=d.bot_id AND p.payload_id=d.parent_payload_id
   LEFT JOIN public.bot_payload_bodies b ON b.bot_id=p.bot_id AND b.payload_id=p.payload_id
   WHERE d.bot_id=selected_bot AND d.payload_id=selected_payload
    AND (p.state<>'ACTIVE' OR b.payload_id IS NULL))
  OR EXISTS(SELECT 1 FROM public.bot_payload_legacy_closure c
   JOIN public.bot_payload_headers p ON p.bot_id=c.bot_id AND p.payload_id=c.parent_payload_id
   LEFT JOIN public.bot_payload_bodies b ON b.bot_id=p.bot_id AND b.payload_id=p.payload_id
   WHERE c.bot_id=selected_bot AND c.payload_id=selected_payload
    AND (p.state<>'ACTIVE' OR b.payload_id IS NULL)) THEN
  RETURN false;
 END IF;
 SELECT COALESCE(jsonb_agg(to_jsonb(s)-ARRAY['bot_id','payload_id']),'[]'::jsonb) INTO initial FROM (
  SELECT * FROM public.forest_payload_effective_sources
  WHERE bot_id=selected_bot AND payload_id=selected_payload LIMIT 4097
 ) s;
 IF jsonb_array_length(initial)>4096 THEN RETURN false; END IF;
 SELECT count(*),COALESCE(bool_or(EXISTS(SELECT 1 FROM public.bot_payload_suppression x
  WHERE x.bot_id=selected_bot AND x.source_unit_id=s.source_unit_id
   AND (x.part_id='*' OR s.part_id='*' OR x.part_id=s.part_id))),false)
 INTO n,suppressed FROM public.forest_source_dependency_closure(selected_bot,initial,4096) s;
 RETURN n<=4096 AND NOT suppressed;
END $$;

CREATE FUNCTION public.forest_job_revision_payloads_available(
 selected_bot text,selected_job text,selected_revision bigint,
 settings_id text,settings_digest text,settings_metadata jsonb,
 delegation_id text,delegation_digest text,delegation_metadata jsonb)
RETURNS boolean LANGUAGE sql VOLATILE AS $$
 SELECT selected_job IS NOT NULL AND selected_revision>0
  AND jsonb_typeof(settings_metadata)='object' AND jsonb_typeof(delegation_metadata)='object'
  AND public.forest_job_payload_available_unlocked(selected_bot,settings_id,settings_digest)
  AND public.forest_job_payload_available_unlocked(selected_bot,delegation_id,delegation_digest)
$$;

CREATE OR REPLACE FUNCTION public.forest_job_occurrence_authorized(target_bot text,target_occurrence text)
RETURNS boolean LANGUAGE sql VOLATILE AS $$
 SELECT COALESCE((
  SELECT o.expires_at>clock_timestamp()
   AND o.delegation_epoch=j.delegation_epoch
   AND (d.delegation_metadata->>'expires_at')::timestamptz>clock_timestamp()
   AND (d.delegation_metadata->>'policy_revision')::bigint=r.policy_revision
   AND i.enabled AND i.actions ? 'job.run'
   AND i.key_revision=(d.delegation_metadata->>'key_revision')::bigint
   AND public.forest_job_revision_payloads_available(
    d.bot_id,d.job_id,d.revision,d.values_payload_id,d.values_payload_digest,d.values_metadata,
    d.delegation_payload_id,d.delegation_payload_digest,d.delegation_metadata)
  FROM public.bot_job_occurrences o JOIN public.bot_jobs j USING(bot_id,job_id)
  JOIN public.bot_job_revisions d
   ON d.bot_id=o.bot_id AND d.job_id=o.job_id AND d.revision=o.job_revision
  JOIN public.bot_runtime_state r ON r.bot_id=o.bot_id
  LEFT JOIN public.bot_web_issuers i
   ON i.bot_id=o.bot_id AND i.issuer_id=d.delegation_metadata->>'issuer_id'
  WHERE o.bot_id=target_bot AND o.occurrence_id=target_occurrence
 ),false)
$$;
"""
