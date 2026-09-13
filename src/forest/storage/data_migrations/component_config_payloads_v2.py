"""Frozen 048 component configuration and local Watch specification cutover."""

from forest.storage.data_migrations.config_dependencies_v2 import import_dependency_in
from forest.storage.migration_v1 import _digest, legacy_source_ref
from forest.storage.payloads import PayloadStore


async def _source(conn, bot_id, row, kind):
    if kind == "REVISION":
        operation = await (
            await conn.execute(
                "SELECT result_payload_id FROM public.bot_web_operations WHERE bot_id=%s AND operation_id=%s AND action='config.apply'",
                (bot_id, row["operation_id"]),
            )
        ).fetchone()
        if operation:
            return operation["result_payload_id"]
    identity = row["plan_id"] if kind == "PLAN" else _digest([row["component_id"], row["revision"]])
    bridge = await (
        await conn.execute(
            "SELECT payload_id FROM public.bot_control_value_sources WHERE bot_id=%s AND family='config' AND source_kind=%s AND source_id=%s",
            (bot_id, kind, identity),
        )
    ).fetchone()
    if bridge is None:
        raise ValueError("Component configuration has no immutable source")
    return bridge["payload_id"]


async def migrate_in(conn, store):
    payloads = PayloadStore(store)
    for kind, table, ordering in (
        ("PLAN", "bot_config_plans", "plan_id"),
        ("REVISION", "bot_config_revisions", "component_id,revision"),
    ):
        after = None
        while True:
            predicate = "plan_id>%s" if kind == "PLAN" else "(component_id,revision)>(%s,%s)"
            params = [store.bot_id, *([] if after is None else after)]
            batch = await (
                await conn.execute(
                    f"SELECT * FROM public.{table} WHERE bot_id=%s "
                    + ("AND " + predicate if after is not None else "")
                    + f" ORDER BY {ordering} LIMIT 64",
                    params,
                )
            ).fetchall()
            if not batch:
                break
            for row in batch:
                key = row["plan_id"] if kind == "PLAN" else str(row["revision"])
                ref = await payloads.import_legacy_json_in(
                    conn,
                    owner_kind="control.config",
                    owner_id="config-values-v1:"
                    + _digest(["config", kind, row["component_id"], key]),
                    value={
                        "family": "config",
                        "kind": kind,
                        "resource": row["component_id"],
                        "identity": key,
                        "values": row["values"],
                    },
                    parents=(await _source(conn, store.bot_id, row, kind),),
                )
                where = "plan_id=%s" if kind == "PLAN" else "component_id=%s AND revision=%s"
                keys = (
                    [row["plan_id"]] if kind == "PLAN" else [row["component_id"], row["revision"]]
                )
                await conn.execute(
                    f"UPDATE public.{table} SET values_metadata=public.forest_component_values_metadata(values),"
                    f"values=NULL,values_payload_id=%s,values_payload_digest=%s WHERE bot_id=%s AND {where}",
                    [ref.payload_id, ref.digest, store.bot_id, *keys],
                )
            last = batch[-1]
            after = (
                [last["plan_id"]] if kind == "PLAN" else [last["component_id"], last["revision"]]
            )
    after = ""
    while True:
        batch = await (
            await conn.execute(
                "SELECT * FROM public.bot_dev_watch_heads WHERE bot_id=%s AND watch_id>%s ORDER BY watch_id LIMIT 64",
                (store.bot_id, after),
            )
        ).fetchall()
        if not batch:
            break
        for row in batch:
            spec = {key: row[key] for key in ("source_identity", "config", "grants")}
            legacy = await payloads.import_legacy_json_in(
                conn,
                owner_kind="control.config",
                owner_id="legacy-watch-input:" + _digest([row["watch_id"]]),
                value=spec,
                sources=(legacy_source_ref(store.bot_id),),
            )
            ref = await payloads.import_legacy_json_in(
                conn,
                owner_kind="control.config",
                owner_id="config-values-v1:"
                + _digest(["watch", "INITIAL", row["watch_id"], row["watch_id"]]),
                value={
                    "family": "watch",
                    "kind": "INITIAL",
                    "resource": row["watch_id"],
                    "identity": row["watch_id"],
                    "values": spec,
                },
                parents=(legacy.payload_id,),
            )
            await conn.execute(
                "UPDATE public.bot_dev_watch_heads SET source_identity=NULL,config=NULL,grants=NULL,spec_payload_id=%s,spec_payload_digest=%s WHERE bot_id=%s AND watch_id=%s",
                (ref.payload_id, ref.digest, store.bot_id, row["watch_id"]),
            )
        after = batch[-1]["watch_id"]
    # Actual operation/candidate joins prove generation copies, without guessing
    # provenance from equal config text or assigning a CLI input a Web issuer.
    after = ("", "")
    while True:
        batch = await (
            await conn.execute(
                "SELECT g.generation_id,o.operation_id,g.config_payload_id,r.values_payload_id AS component_parent,w.spec_payload_id AS watch_parent "
                "FROM public.bot_generations g JOIN public.bot_lifecycle_operations o "
                "ON o.bot_id=g.bot_id AND o.candidate_generation_id=g.generation_id AND o.component_id=g.component_id "
                "LEFT JOIN public.bot_config_activations a ON a.bot_id=o.bot_id AND a.operation_id=o.operation_id AND a.component_id=o.component_id "
                "LEFT JOIN public.bot_config_revisions r ON r.bot_id=a.bot_id AND r.component_id=a.component_id AND r.revision=a.revision "
                "LEFT JOIN public.bot_dev_watch_activations d ON d.bot_id=o.bot_id AND d.operation_id=o.operation_id "
                "LEFT JOIN public.bot_dev_watch_heads w ON w.bot_id=d.bot_id AND w.watch_id=d.watch_id AND w.component_id=o.component_id "
                "WHERE g.bot_id=%s AND (g.generation_id,o.operation_id)>(%s,%s) ORDER BY g.generation_id,o.operation_id LIMIT 64",
                (store.bot_id, *after),
            )
        ).fetchall()
        if not batch:
            break
        for row in batch:
            for column in ("component_parent", "watch_parent"):
                if row[column]:
                    await import_dependency_in(
                        conn,
                        store,
                        row["config_payload_id"],
                        row[column],
                        proof_kind="generation.config"
                        if column == "component_parent"
                        else "watch.config",
                        proof_id=row["generation_id"],
                    )
        after = (batch[-1]["generation_id"], batch[-1]["operation_id"])
    await conn.execute(FINAL_SQL)


FINAL_SQL = """
ALTER TABLE public.bot_config_plans
 ALTER COLUMN values_payload_id SET NOT NULL, ALTER COLUMN values_payload_digest SET NOT NULL,
 ALTER COLUMN values_metadata SET NOT NULL, ADD CHECK(values IS NULL),
 ADD CHECK(values_payload_digest ~ '^sha256:[0-9a-f]{64}$'),
 ADD CHECK(public.forest_component_metadata_valid(values_metadata));
ALTER TABLE public.bot_config_revisions
 ALTER COLUMN values_payload_id SET NOT NULL, ALTER COLUMN values_payload_digest SET NOT NULL,
 ALTER COLUMN values_metadata SET NOT NULL, ADD CHECK(values IS NULL),
 ADD CHECK(values_payload_digest ~ '^sha256:[0-9a-f]{64}$'),
 ADD CHECK(public.forest_component_metadata_valid(values_metadata));
ALTER TABLE public.bot_dev_watch_heads
 ALTER COLUMN spec_payload_id SET NOT NULL, ALTER COLUMN spec_payload_digest SET NOT NULL,
 ADD CHECK(source_identity IS NULL AND config IS NULL AND grants IS NULL),
 ADD CHECK(spec_payload_digest ~ '^sha256:[0-9a-f]{64}$');
CREATE TRIGGER bot_config_plans_immutable BEFORE UPDATE OR DELETE ON public.bot_config_plans
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_config_revisions_immutable BEFORE UPDATE OR DELETE ON public.bot_config_revisions
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE FUNCTION public.forest_component_values_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE k text; identity text; value jsonb; p record; h record; op record; input jsonb; result jsonb;
BEGIN
 SELECT * INTO h FROM public.bot_config_heads WHERE bot_id=NEW.bot_id AND component_id=NEW.component_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'CONFIGURATION_HEAD_REQUIRED' USING ERRCODE='23514'; END IF;
 k:=CASE TG_TABLE_NAME WHEN 'bot_config_plans' THEN 'PLAN' ELSE 'REVISION' END;
 identity:=CASE k WHEN 'PLAN' THEN NEW.plan_id ELSE to_jsonb(NEW)->>'revision' END;
 value:=public.forest_config_payload_values(NEW.bot_id,NEW.values_payload_id,NEW.values_payload_digest,
  'config',k,NEW.component_id,identity);
 IF NEW.values IS NOT NULL OR NEW.values_metadata IS DISTINCT FROM public.forest_component_values_metadata(value) THEN
  RAISE EXCEPTION 'CONFIGURATION_METADATA_CONFLICT' USING ERRCODE='23514'; END IF;
 IF k='PLAN' THEN
  IF NEW.expected_revision IS DISTINCT FROM h.desired_revision THEN
   RAISE EXCEPTION 'CONFIGURATION_SUPERSEDED' USING ERRCODE='23514'; END IF;
 ELSE
  SELECT * INTO p FROM public.bot_config_plans WHERE bot_id=NEW.bot_id AND plan_id=NEW.plan_id;
  SELECT * INTO op FROM public.bot_web_operations WHERE bot_id=NEW.bot_id AND operation_id=NEW.operation_id;
  IF p.plan_id IS NULL OR op.operation_id IS NULL OR op.action<>'config.apply'
   OR p.component_id IS DISTINCT FROM NEW.component_id
   OR ROW(p.subject_id,p.issuer_id) IS DISTINCT FROM ROW(op.subject_id,op.issuer_id)
   OR NEW.previous_revision IS DISTINCT FROM p.expected_revision
   OR NEW.previous_revision IS DISTINCT FROM h.desired_revision
   OR NEW.revision IS DISTINCT FROM h.desired_revision+1 OR p.expires_at<=clock_timestamp()
   OR to_jsonb(NEW.policy_revision) IS DISTINCT FROM op.ticket_basis->'policy_revision' THEN
   RAISE EXCEPTION 'CONFIGURATION_REVISION_SOURCE_CONFLICT' USING ERRCODE='23514'; END IF;
  IF value IS DISTINCT FROM public.forest_config_payload_values(NEW.bot_id,p.values_payload_id,p.values_payload_digest,
   'config','PLAN',NEW.component_id,p.plan_id) OR NOT EXISTS(
   SELECT 1 FROM public.bot_control_value_sources WHERE bot_id=NEW.bot_id AND family='config'
    AND source_kind='PLAN' AND source_id=p.plan_id) THEN
   RAISE EXCEPTION 'CONFIGURATION_REVISION_SOURCE_CONFLICT' USING ERRCODE='23514'; END IF;
  input:=public.forest_control_payload_value(NEW.bot_id,op.input_payload_id,op.input_payload_digest,'input',op.operation_id,op.action);
  result:=public.forest_control_payload_value(NEW.bot_id,op.result_payload_id,op.result_payload_digest,'result',op.operation_id,op.action);
  IF input->'component_id' IS DISTINCT FROM to_jsonb(NEW.component_id)
   OR input->'plan_id' IS DISTINCT FROM to_jsonb(p.plan_id)
   OR input->'plan_digest' IS DISTINCT FROM to_jsonb(p.plan_digest)
   OR input->'expected_revision' IS DISTINCT FROM to_jsonb(p.expected_revision)
   OR result IS DISTINCT FROM jsonb_build_object('operation_id',NEW.operation_id,'desired_revision',NEW.revision,
     'effective_revision',h.effective_revision,'status','ACCEPTED')
   OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id AND payload_id=NEW.values_payload_id AND parent_payload_id=p.values_payload_id)
   OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id AND payload_id=NEW.values_payload_id AND parent_payload_id=op.input_payload_id)
   OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id AND payload_id=op.result_payload_id AND parent_payload_id=NEW.values_payload_id) THEN
   RAISE EXCEPTION 'CONFIGURATION_REVISION_SOURCE_CONFLICT' USING ERRCODE='23514'; END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER component_plan_values_guard BEFORE INSERT ON public.bot_config_plans
 FOR EACH ROW EXECUTE FUNCTION public.forest_component_values_guard();
CREATE TRIGGER component_revision_values_guard BEFORE INSERT ON public.bot_config_revisions
 FOR EACH ROW EXECUTE FUNCTION public.forest_component_values_guard();
CREATE FUNCTION public.forest_component_plan_complete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE op record; input jsonb; source record;
BEGIN
 SELECT w.* INTO op FROM public.bot_control_value_sources b JOIN public.bot_web_operations w
  ON w.bot_id=b.bot_id AND w.operation_id=b.operation_id WHERE b.bot_id=NEW.bot_id AND b.family='config'
  AND b.source_kind='PLAN' AND b.source_id=NEW.plan_id AND NOT b.legacy_untracked AND w.action='config.plan'
  AND ROW(w.result_payload_id,w.result_payload_digest) IS NOT DISTINCT FROM ROW(b.payload_id,b.payload_digest)
  AND ROW(w.subject_id,w.issuer_id) IS NOT DISTINCT FROM ROW(NEW.subject_id,NEW.issuer_id);
 IF NOT FOUND THEN
  RAISE EXCEPTION 'CONFIGURATION_PLAN_RECEIPT_REQUIRED' USING ERRCODE='23514'; END IF;
 input:=public.forest_control_payload_value(NEW.bot_id,op.input_payload_id,op.input_payload_digest,'input',op.operation_id,op.action);
 IF input ? 'source_revision' THEN
  IF jsonb_typeof(input->'source_revision') IS DISTINCT FROM 'number'
   OR (input->>'source_revision') !~ '^[1-9][0-9]{0,18}$'
   OR (input->>'source_revision')::numeric>9223372036854775807 THEN
   RAISE EXCEPTION 'CONFIGURATION_SOURCE_INVALID' USING ERRCODE='23514'; END IF;
  SELECT * INTO source FROM public.bot_config_revisions WHERE bot_id=NEW.bot_id
   AND component_id=NEW.component_id AND revision=(input->>'source_revision')::bigint;
  IF NOT FOUND OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=op.input_payload_id AND parent_payload_id=source.values_payload_id) THEN
   RAISE EXCEPTION 'CONFIGURATION_SOURCE_CONFLICT' USING ERRCODE='23514'; END IF;
  PERFORM public.forest_config_payload_values(NEW.bot_id,source.values_payload_id,source.values_payload_digest,
   'config','REVISION',NEW.component_id,source.revision::text);
 END IF;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER component_plan_complete AFTER INSERT ON public.bot_config_plans
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.forest_component_plan_complete();

CREATE FUNCTION public.forest_component_head_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  IF NEW.desired_revision<>0 OR NEW.effective_revision<>0 THEN
   RAISE EXCEPTION 'CONFIGURATION_HEAD_CONFLICT' USING ERRCODE='23514'; END IF;
 ELSE
  IF ROW(NEW.bot_id,NEW.component_id) IS DISTINCT FROM ROW(OLD.bot_id,OLD.component_id)
   OR NEW.desired_revision NOT IN (OLD.desired_revision,OLD.desired_revision+1)
   OR NEW.effective_revision<OLD.effective_revision
   OR (NEW.effective_revision<>OLD.effective_revision AND NEW.effective_revision<>NEW.desired_revision)
   OR (NEW.desired_revision<>OLD.desired_revision AND NOT EXISTS(
    SELECT 1 FROM public.bot_config_revisions WHERE bot_id=NEW.bot_id AND component_id=NEW.component_id
     AND revision=NEW.desired_revision AND previous_revision=OLD.desired_revision)) THEN
   RAISE EXCEPTION 'CONFIGURATION_HEAD_CONFLICT' USING ERRCODE='23514'; END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER component_head_guard BEFORE INSERT OR UPDATE ON public.bot_config_heads
 FOR EACH ROW EXECUTE FUNCTION public.forest_component_head_guard();

CREATE FUNCTION public.forest_watch_spec(selected_bot text,selected_watch text) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE h record; value jsonb; identity text;
BEGIN
 SELECT * INTO h FROM public.bot_dev_watch_heads WHERE bot_id=selected_bot AND watch_id=selected_watch FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'WATCH_NOT_FOUND' USING ERRCODE='P0001'; END IF;
 value:=public.forest_config_payload_values(selected_bot,h.spec_payload_id,h.spec_payload_digest,'watch','INITIAL',selected_watch,selected_watch);
 identity:='sha256:'||encode(sha256(convert_to('{"source":'||to_jsonb(value->>'source_identity')::text||'}','UTF8')),'hex');
 IF jsonb_typeof(value) IS DISTINCT FROM 'object' OR NOT(value ?& ARRAY['source_identity','config','grants'])
  OR value-ARRAY['source_identity','config','grants']<>'{}'::jsonb
  OR jsonb_typeof(value->'source_identity') IS DISTINCT FROM 'string'
  OR identity IS DISTINCT FROM selected_watch THEN
  RAISE EXCEPTION 'WATCH_AUTHORIZATION_CONFLICT' USING ERRCODE='23514'; END IF;
 PERFORM public.forest_component_values_metadata(jsonb_build_object('package_digest','watch', 'config',value->'config','grants',value->'grants'));
 RETURN value;
END $$;
CREATE FUNCTION public.forest_watch_head_payload_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE value jsonb; input record; operation text;
BEGIN
 IF TG_OP='UPDATE' THEN
  IF ROW(NEW.bot_id,NEW.watch_id,NEW.policy_revision,NEW.spec_payload_id,NEW.spec_payload_digest)
   IS DISTINCT FROM ROW(OLD.bot_id,OLD.watch_id,OLD.policy_revision,OLD.spec_payload_id,OLD.spec_payload_digest)
   OR (OLD.component_id IS NOT NULL AND NEW.component_id IS DISTINCT FROM OLD.component_id) THEN
   RAISE EXCEPTION 'WATCH_AUTHORIZATION_CONFLICT' USING ERRCODE='23514'; END IF;
  RETURN NEW;
 END IF;
 value:=public.forest_config_payload_values(NEW.bot_id,NEW.spec_payload_id,NEW.spec_payload_digest,
  'watch','INITIAL',NEW.watch_id,NEW.watch_id);
 IF NEW.watch_id IS DISTINCT FROM ('sha256:'||encode(sha256(convert_to(
  '{"source":'||to_jsonb(value->>'source_identity')::text||'}','UTF8')),'hex'))
  OR jsonb_typeof(value) IS DISTINCT FROM 'object' OR NOT(value ?& ARRAY['source_identity','config','grants'])
  OR value-ARRAY['source_identity','config','grants']<>'{}'::jsonb THEN
  RAISE EXCEPTION 'WATCH_AUTHORIZATION_CONFLICT' USING ERRCODE='23514'; END IF;
 PERFORM public.forest_component_values_metadata(jsonb_build_object('package_digest','watch','config',value->'config','grants',value->'grants'));
 operation:='watch-initialize:'||NEW.watch_id;
 SELECT p.* INTO input FROM public.bot_payload_derivations d JOIN public.bot_payload_headers p
  ON p.bot_id=d.bot_id AND p.payload_id=d.parent_payload_id WHERE d.bot_id=NEW.bot_id AND d.payload_id=NEW.spec_payload_id
  AND p.owner_kind='control.input' AND p.owner_id='input:'||public.forest_memory_identity_digest(operation);
 IF NOT FOUND OR value IS DISTINCT FROM public.forest_control_payload_value(NEW.bot_id,input.payload_id,input.digest,'input',operation,'watch.initialize') THEN
  RAISE EXCEPTION 'WATCH_AUTHORIZATION_CONFLICT' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER watch_head_payload_guard BEFORE INSERT OR UPDATE ON public.bot_dev_watch_heads
 FOR EACH ROW EXECUTE FUNCTION public.forest_watch_head_payload_guard();

CREATE OR REPLACE FUNCTION public.forest_publish_configuration() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE activation record; head record; cfg_revision record; candidate record; candidate_config jsonb; cfg_values jsonb;
BEGIN
    IF NEW.phase <> 'COMMITTED' OR OLD.phase = 'COMMITTED' THEN RETURN NEW; END IF;
    SELECT * INTO activation FROM public.bot_config_activations
        WHERE bot_id=NEW.bot_id AND operation_id=NEW.operation_id;
    IF NOT FOUND THEN RETURN NEW; END IF;
    SELECT * INTO head FROM public.bot_config_heads
        WHERE bot_id=NEW.bot_id AND component_id=activation.component_id FOR UPDATE;
    SELECT * INTO cfg_revision FROM public.bot_config_revisions
        WHERE bot_id=NEW.bot_id AND component_id=activation.component_id AND revision=activation.revision;
    SELECT * INTO candidate FROM public.bot_generations
        WHERE bot_id=NEW.bot_id AND generation_id=NEW.candidate_generation_id;
    cfg_values := public.forest_config_payload_values(NEW.bot_id,cfg_revision.values_payload_id,cfg_revision.values_payload_digest,
        'config','REVISION',activation.component_id,activation.revision::text);
    candidate_config := public.forest_generation_configuration(NEW.bot_id,NEW.candidate_generation_id);
    IF (CASE WHEN activation.recover_effective THEN head.effective_revision ELSE head.desired_revision END)
            IS DISTINCT FROM activation.revision
        OR activation.component_id IS DISTINCT FROM NEW.component_id
        OR activation.host_incarnation IS DISTINCT FROM NEW.host_incarnation
        OR candidate.package_digest IS DISTINCT FROM (cfg_values->>'package_digest')
        OR candidate_config IS DISTINCT FROM (cfg_values->'config')
        OR candidate.capability_grants IS DISTINCT FROM (cfg_values->'grants')
        OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
            AND payload_id=candidate.config_payload_id AND parent_payload_id=cfg_revision.values_payload_id
            UNION ALL SELECT 1 FROM public.bot_payload_legacy_dependencies WHERE bot_id=NEW.bot_id
            AND payload_id=candidate.config_payload_id AND parent_payload_id=cfg_revision.values_payload_id)
        OR cfg_revision.policy_revision IS DISTINCT FROM (SELECT policy_revision FROM public.bot_runtime_state
                                                        WHERE bot_id=NEW.bot_id) THEN
        RAISE EXCEPTION 'CONFIGURATION_SUPERSEDED' USING ERRCODE='P0001';
    END IF;
    IF NOT activation.recover_effective THEN
        UPDATE public.bot_config_heads SET effective_revision=activation.revision
            WHERE bot_id=NEW.bot_id AND component_id=activation.component_id;
    END IF;
    INSERT INTO public.bot_outbox_events(bot_id,event_id,topic,operation_id,payload)
        VALUES (NEW.bot_id,'config:'||NEW.operation_id,
                CASE WHEN activation.recover_effective THEN 'config.recovered' ELSE 'config.effective' END,
                NEW.operation_id,jsonb_build_object('component_id',NEW.component_id,'revision',activation.revision));
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.forest_publish_dev_watch() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE activation record; head record; build record; candidate record; host record; candidate_config jsonb; watch_spec jsonb;
BEGIN
    IF NEW.phase <> 'COMMITTED' OR OLD.phase = 'COMMITTED' THEN RETURN NEW; END IF;
    SELECT * INTO activation FROM public.bot_dev_watch_activations
        WHERE bot_id=NEW.bot_id AND operation_id=NEW.operation_id;
    IF NOT FOUND THEN
        IF EXISTS (SELECT 1 FROM public.bot_dev_watch_heads
                   WHERE bot_id=NEW.bot_id AND component_id=NEW.component_id) THEN
            RAISE EXCEPTION 'WATCH_ACTIVATION_REQUIRED' USING ERRCODE='P0001';
        END IF;
        RETURN NEW;
    END IF;
    -- Kernel holds route/runtime/host/state locks. This is a business lock only.
    SELECT * INTO head FROM public.bot_dev_watch_heads
        WHERE bot_id=NEW.bot_id AND watch_id=activation.watch_id FOR UPDATE;
    SELECT * INTO build FROM public.bot_dev_watch_builds
        WHERE bot_id=NEW.bot_id AND request_id=activation.request_id;
    SELECT * INTO candidate FROM public.bot_generations
        WHERE bot_id=NEW.bot_id AND generation_id=NEW.candidate_generation_id;
    watch_spec := public.forest_watch_spec(NEW.bot_id,activation.watch_id);
    candidate_config := public.forest_generation_configuration(NEW.bot_id,NEW.candidate_generation_id);
    IF head.desired_request_id IS DISTINCT FROM activation.expected_desired_request_id
        OR (NOT activation.recovering AND activation.request_id IS DISTINCT FROM head.desired_request_id)
        OR (activation.recovering AND activation.request_id IS DISTINCT FROM head.good_request_id)
        OR activation.host_incarnation IS DISTINCT FROM NEW.host_incarnation
        OR head.component_id IS DISTINCT FROM NEW.component_id
        OR build.component_id IS DISTINCT FROM NEW.component_id
        OR candidate.package_digest IS DISTINCT FROM build.package_digest
        OR candidate_config IS DISTINCT FROM (watch_spec->'config')
        OR candidate.capability_grants IS DISTINCT FROM (watch_spec->'grants')
        OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
            AND payload_id=candidate.config_payload_id AND parent_payload_id=head.spec_payload_id
            UNION ALL SELECT 1 FROM public.bot_payload_legacy_dependencies WHERE bot_id=NEW.bot_id
            AND payload_id=candidate.config_payload_id AND parent_payload_id=head.spec_payload_id)
        OR head.policy_revision IS DISTINCT FROM (SELECT policy_revision FROM public.bot_runtime_state WHERE bot_id=NEW.bot_id)
        THEN RAISE EXCEPTION 'WATCH_SUPERSEDED' USING ERRCODE='P0001';
    END IF;
    IF NOT activation.recovering THEN
        UPDATE public.bot_dev_watch_heads SET good_request_id=activation.request_id
            WHERE bot_id=NEW.bot_id AND watch_id=activation.watch_id;
        INSERT INTO public.bot_dev_watch_results(bot_id,request_id,status)
            VALUES(NEW.bot_id,activation.request_id,'COMMITTED');
    END IF;
    -- Recheck elapsed authority AFTER the head lock, even if a caller bypasses
    -- a preflight check. Kernel additionally checks again after all triggers.
    SELECT * INTO host FROM public.bot_host_lease WHERE bot_id=NEW.bot_id;
    IF host.incarnation IS DISTINCT FROM NEW.host_incarnation
        OR host.expires_at <= clock_timestamp() OR NEW.lease_until <= clock_timestamp()
        THEN RAISE EXCEPTION 'WATCH_AUTHORITY_EXPIRED' USING ERRCODE='P0001';
    END IF;
    RETURN NEW;
END;
$$;

"""
