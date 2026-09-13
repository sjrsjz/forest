"""Frozen 038: independent generation configuration bodies, never guessed chat lineage."""

from forest.memory_types import SourceRef
from forest.storage.migration_v1 import _digest
from forest.storage.payloads import PayloadStore


async def migrate_in(conn, store):
    payloads = PayloadStore(store)
    after = ""
    while True:
        rows = await (
            await conn.execute(
                "SELECT generation_id,config,state_value_schema FROM public.bot_generations "
                "WHERE bot_id=%s AND generation_id>%s ORDER BY generation_id LIMIT 64",
                (store.bot_id, after),
            )
        ).fetchall()
        if not rows:
            break
        for row in rows:
            generation_id = row["generation_id"]
            source = SourceRef(
                "domain:" + _digest([store.bot_id, "GENERATION_CONFIG", generation_id])
            )
            await conn.execute(
                "INSERT INTO public.bot_payload_domains(bot_id,kind,owner_id,source_unit_id) "
                "VALUES (%s,'GENERATION_CONFIG',%s,%s)",
                (store.bot_id, generation_id, source.source_unit_id),
            )
            ref = await payloads.import_legacy_json_in(
                conn,
                owner_kind="control.config",
                owner_id="generation-config:" + _digest([store.bot_id, generation_id]),
                value={"config": row["config"], "state_value_schema": row["state_value_schema"]},
                sources=(source,),
            )
            await conn.execute(
                "UPDATE public.bot_generations SET config=NULL,state_value_schema=NULL,"
                "config_payload_id=%s,config_payload_digest=%s WHERE bot_id=%s AND generation_id=%s",
                (ref.payload_id, ref.digest, store.bot_id, generation_id),
            )
        after = rows[-1]["generation_id"]
    await conn.execute(FINAL_SQL)


FINAL_SQL = """
ALTER TABLE public.bot_generations ALTER COLUMN config_payload_id SET NOT NULL,
 ALTER COLUMN config_payload_digest SET NOT NULL;
ALTER TABLE public.bot_generations ADD CONSTRAINT bot_generation_config_body_external
 CHECK(config IS NULL AND state_value_schema IS NULL);
CREATE TRIGGER bot_generation_identity BEFORE UPDATE ON public.bot_generations
 FOR EACH ROW EXECUTE FUNCTION public.forest_generation_identity();
CREATE TRIGGER bot_generation_state_identity BEFORE UPDATE ON public.bot_generations
 FOR EACH ROW EXECUTE FUNCTION public.forest_generation_state_identity();
CREATE FUNCTION public.forest_generation_config_payload_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE p record;
BEGIN
 IF TG_OP='UPDATE' AND ROW(NEW.config_payload_id,NEW.config_payload_digest)
  IS DISTINCT FROM ROW(OLD.config_payload_id,OLD.config_payload_digest) THEN
  RAISE EXCEPTION 'GENERATION_CONFIG_REFERENCE_IMMUTABLE' USING ERRCODE='55000';
 END IF;
 IF TG_OP='INSERT' THEN
  SELECT * INTO p FROM public.bot_payload_headers WHERE bot_id=NEW.bot_id AND payload_id=NEW.config_payload_id;
  IF NOT FOUND OR p.owner_kind<>'control.config' OR p.encoding<>'json' OR p.digest<>NEW.config_payload_digest
   OR NOT EXISTS(SELECT 1 FROM public.bot_payload_sources s JOIN public.bot_payload_domains d
    ON d.bot_id=s.bot_id AND d.source_unit_id=s.source_unit_id
    WHERE s.bot_id=NEW.bot_id AND s.payload_id=NEW.config_payload_id
     AND d.kind='GENERATION_CONFIG' AND d.owner_id=NEW.generation_id
     AND s.source_revision=1 AND s.part_id='*') THEN
   RAISE EXCEPTION 'GENERATION_CONFIG_REFERENCE_INVALID' USING ERRCODE='23514';
  END IF;
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_generation_config_payload_guard BEFORE INSERT OR UPDATE ON public.bot_generations
 FOR EACH ROW EXECUTE FUNCTION public.forest_generation_config_payload_guard();

-- Publication compares the real immutable payload, never the cleared legacy
-- columns. The caller already owns lifecycle/state/exposure locks; the clock
-- also fences a direct SQL publication against concurrent source suppression.
CREATE FUNCTION public.forest_generation_configuration(selected_bot text,selected_generation text)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE p record; source_count bigint; suppressed boolean; value jsonb;
BEGIN
 PERFORM 1 FROM public.bot_memory_clock WHERE bot_id=selected_bot FOR UPDATE;
 IF NOT FOUND THEN
  RAISE EXCEPTION 'GENERATION_CONFIG_UNAVAILABLE' USING ERRCODE='P0001';
 END IF;
 SELECT h.*,b.content,g.config_payload_digest INTO p FROM public.bot_generations g
  JOIN public.bot_payload_headers h ON h.bot_id=g.bot_id AND h.payload_id=g.config_payload_id
  LEFT JOIN public.bot_payload_bodies b ON b.bot_id=h.bot_id AND b.payload_id=h.payload_id
  WHERE g.bot_id=selected_bot AND g.generation_id=selected_generation;
 IF NOT FOUND OR p.state<>'ACTIVE' OR p.encoding<>'json' OR p.owner_kind<>'control.config'
  OR p.content IS NULL OR p.digest IS DISTINCT FROM p.config_payload_digest
  OR p.digest IS DISTINCT FROM ('sha256:' || encode(sha256(p.content),'hex')) THEN
  RAISE EXCEPTION 'GENERATION_CONFIG_UNAVAILABLE' USING ERRCODE='P0001';
 END IF;
 SELECT count(*),COALESCE(bool_or(EXISTS(
  SELECT 1 FROM public.bot_payload_suppression s WHERE s.bot_id=selected_bot
   AND s.source_unit_id=d.source_unit_id
   AND (s.part_id='*' OR d.part_id='*' OR s.part_id=d.part_id))),false)
 INTO source_count,suppressed
 FROM public.forest_source_dependency_closure(selected_bot,p.manifest->'sources',4096) d;
 IF source_count>4096 OR suppressed THEN
  RAISE EXCEPTION 'GENERATION_CONFIG_UNAVAILABLE' USING ERRCODE='P0001';
 END IF;
 value := convert_from(p.content,'UTF8')::jsonb;
 IF jsonb_typeof(value) IS DISTINCT FROM 'object' OR NOT (value ? 'config') THEN
  RAISE EXCEPTION 'GENERATION_CONFIG_UNAVAILABLE' USING ERRCODE='P0001';
 END IF;
 RETURN value->'config';
END;
$$;

CREATE OR REPLACE FUNCTION public.forest_publish_configuration() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE activation record; head record; cfg_revision record; candidate record; candidate_config jsonb;
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
    candidate_config := public.forest_generation_configuration(NEW.bot_id,NEW.candidate_generation_id);
    IF (CASE WHEN activation.recover_effective THEN head.effective_revision ELSE head.desired_revision END)
            IS DISTINCT FROM activation.revision
        OR activation.component_id IS DISTINCT FROM NEW.component_id
        OR activation.host_incarnation IS DISTINCT FROM NEW.host_incarnation
        OR candidate.package_digest IS DISTINCT FROM (cfg_revision.values->>'package_digest')
        OR candidate_config IS DISTINCT FROM (cfg_revision.values->'config')
        OR candidate.capability_grants IS DISTINCT FROM (cfg_revision.values->'grants')
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
DECLARE activation record; head record; build record; candidate record; host record; candidate_config jsonb;
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
    candidate_config := public.forest_generation_configuration(NEW.bot_id,NEW.candidate_generation_id);
    IF head.desired_request_id IS DISTINCT FROM activation.expected_desired_request_id
        OR (NOT activation.recovering AND activation.request_id IS DISTINCT FROM head.desired_request_id)
        OR (activation.recovering AND activation.request_id IS DISTINCT FROM head.good_request_id)
        OR activation.host_incarnation IS DISTINCT FROM NEW.host_incarnation
        OR head.component_id IS DISTINCT FROM NEW.component_id
        OR build.component_id IS DISTINCT FROM NEW.component_id
        OR candidate.package_digest IS DISTINCT FROM build.package_digest
        OR candidate_config IS DISTINCT FROM head.config
        OR candidate.capability_grants IS DISTINCT FROM head.grants
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
