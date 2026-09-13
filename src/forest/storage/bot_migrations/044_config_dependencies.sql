-- Supplemental, migration-proven prerequisites for already frozen bodies.
-- Original manifests and their hashes stay immutable. These edges are never
-- source-to-source aliases: independent original chat bodies remain independent.
ALTER TABLE public.bot_payload_domains DROP CONSTRAINT bot_payload_domains_kind_check;
ALTER TABLE public.bot_payload_domains ADD CONSTRAINT bot_payload_domains_kind_check
 CHECK(kind IN ('SERVICE_INPUT','GENERATION_CONFIG','STATE_CONTRACT','CONTROL_INPUT','AGENT_SPEC'));

CREATE TABLE public.bot_payload_legacy_dependencies (
 bot_id text NOT NULL, payload_id text NOT NULL, parent_payload_id text NOT NULL,
 proof_kind text NOT NULL CHECK(proof_kind IN ('agent.spec','agent.child','memory.request','binding.initial','binding.unknown','generation.config','watch.config')),
 proof_id text NOT NULL CHECK(octet_length(proof_id) BETWEEN 1 AND 4096),
 PRIMARY KEY(bot_id,payload_id,parent_payload_id), CHECK(payload_id<>parent_payload_id),
 FOREIGN KEY(bot_id,payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 FOREIGN KEY(bot_id,parent_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id)
);
CREATE TRIGGER bot_legacy_dependencies_frozen BEFORE INSERT OR UPDATE OR DELETE
 ON public.bot_payload_legacy_dependencies FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE TABLE public.bot_payload_legacy_closure (
 bot_id text NOT NULL, payload_id text NOT NULL, root_payload_id text NOT NULL, parent_payload_id text NOT NULL,
 PRIMARY KEY(bot_id,payload_id,root_payload_id,parent_payload_id), CHECK(payload_id<>parent_payload_id),
 FOREIGN KEY(bot_id,payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 FOREIGN KEY(bot_id,root_payload_id,parent_payload_id)
  REFERENCES public.bot_payload_legacy_dependencies(bot_id,payload_id,parent_payload_id)
);
CREATE INDEX bot_legacy_closure_parent ON public.bot_payload_legacy_closure(bot_id,parent_payload_id,payload_id);
CREATE TRIGGER bot_legacy_closure_immutable BEFORE UPDATE OR DELETE
 ON public.bot_payload_legacy_closure FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE FUNCTION public.forest_legacy_closure_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 -- Runtime inheritance can only copy a prerequisite from an actual immutable
 -- direct parent. The migration importer alone installs original closure rows.
 IF NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations d
  JOIN public.bot_payload_legacy_closure c ON c.bot_id=d.bot_id AND c.payload_id=d.parent_payload_id
  WHERE d.bot_id=NEW.bot_id AND d.payload_id=NEW.payload_id
   AND c.root_payload_id=NEW.root_payload_id AND c.parent_payload_id=NEW.parent_payload_id) THEN
  RAISE EXCEPTION 'LEGACY_DEPENDENCY_IMPORT_REQUIRED' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER bot_legacy_closure_guard BEFORE INSERT ON public.bot_payload_legacy_closure
 FOR EACH ROW EXECUTE FUNCTION public.forest_legacy_closure_guard();
CREATE FUNCTION public.forest_inherit_legacy_dependencies() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO public.bot_payload_legacy_closure(bot_id,payload_id,root_payload_id,parent_payload_id)
 SELECT NEW.bot_id,NEW.payload_id,root_payload_id,parent_payload_id
 FROM public.bot_payload_legacy_closure WHERE bot_id=NEW.bot_id AND payload_id=NEW.parent_payload_id
 ON CONFLICT DO NOTHING;
 RETURN NEW;
END $$;
CREATE TRIGGER bot_payload_inherit_legacy AFTER INSERT ON public.bot_payload_derivations
 FOR EACH ROW EXECUTE FUNCTION public.forest_inherit_legacy_dependencies();

CREATE VIEW public.forest_payload_effective_sources AS
 SELECT bot_id,payload_id,source_unit_id,source_revision,part_id FROM public.bot_payload_sources
 UNION
 SELECT c.bot_id,c.payload_id,s.source_unit_id,s.source_revision,s.part_id
 FROM public.bot_payload_legacy_closure c JOIN public.bot_payload_sources s
 ON s.bot_id=c.bot_id AND s.payload_id=c.parent_payload_id;

CREATE FUNCTION public.forest_payload_dependencies_available(selected_bot text,selected_payload text)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE n bigint; suppressed boolean; initial jsonb;
BEGIN
 PERFORM 1 FROM public.bot_memory_clock WHERE bot_id=selected_bot FOR UPDATE;
 IF NOT FOUND OR EXISTS(SELECT 1 FROM public.bot_payload_legacy_closure c
  JOIN public.bot_payload_headers h ON h.bot_id=c.bot_id AND h.payload_id=c.parent_payload_id
  LEFT JOIN public.bot_payload_bodies b ON b.bot_id=h.bot_id AND b.payload_id=h.payload_id
  WHERE c.bot_id=selected_bot AND c.payload_id=selected_payload AND (h.state<>'ACTIVE' OR b.payload_id IS NULL)) THEN
  RETURN false;
 END IF;
 SELECT COALESCE(jsonb_agg(to_jsonb(s)-ARRAY['bot_id','payload_id']),'[]'::jsonb) INTO initial FROM (
  SELECT * FROM public.forest_payload_effective_sources WHERE bot_id=selected_bot AND payload_id=selected_payload LIMIT 4097
 ) s;
 IF jsonb_array_length(initial)>4096 THEN RETURN false; END IF;
 SELECT count(*),COALESCE(bool_or(EXISTS(SELECT 1 FROM public.bot_payload_suppression x
  WHERE x.bot_id=selected_bot AND x.source_unit_id=s.source_unit_id
   AND (x.part_id='*' OR s.part_id='*' OR x.part_id=s.part_id))),false) INTO n,suppressed
 FROM public.forest_source_dependency_closure(selected_bot,initial,4096) s;
 RETURN n<=4096 AND NOT suppressed;
END $$;
CREATE FUNCTION public.forest_payload_legacy_body_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT public.forest_payload_dependencies_available(NEW.bot_id,NEW.payload_id) THEN
  RAISE EXCEPTION 'PAYLOAD_DEPENDENCY_UNAVAILABLE' USING ERRCODE='P0001';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER bot_payload_legacy_body_guard BEFORE INSERT ON public.bot_payload_bodies
 FOR EACH ROW EXECUTE FUNCTION public.forest_payload_legacy_body_guard();

-- Public readers retain their original identity/body checks and additionally
-- check the proven supplemental prerequisites before returning any content.
ALTER FUNCTION public.forest_config_payload_values(text,text,text,text,text,text,text)
 RENAME TO forest_config_payload_values_v043;
CREATE FUNCTION public.forest_config_payload_values(selected_bot text,selected_payload text,selected_digest text,
 selected_family text,selected_kind text,selected_resource text,selected_identity text)
RETURNS jsonb LANGUAGE plpgsql AS $$
BEGIN
 IF NOT public.forest_payload_dependencies_available(selected_bot,selected_payload) THEN
  RAISE EXCEPTION 'CONTROL_CONTENT_REVOKED' USING ERRCODE='P0001';
 END IF;
 RETURN public.forest_config_payload_values_v043(selected_bot,selected_payload,selected_digest,
  selected_family,selected_kind,selected_resource,selected_identity);
END $$;
ALTER FUNCTION public.forest_control_payload_value(text,text,text,text,text,text)
 RENAME TO forest_control_payload_value_v040;
CREATE FUNCTION public.forest_control_payload_value(selected_bot text,selected_payload text,selected_digest text,
 selected_kind text,selected_operation text,selected_action text)
RETURNS jsonb LANGUAGE plpgsql AS $$
BEGIN
 IF NOT public.forest_payload_dependencies_available(selected_bot,selected_payload) THEN
  RAISE EXCEPTION 'CONTROL_CONTENT_REVOKED' USING ERRCODE='P0001';
 END IF;
 RETURN public.forest_control_payload_value_v040(selected_bot,selected_payload,selected_digest,
  selected_kind,selected_operation,selected_action);
END $$;
ALTER FUNCTION public.forest_generation_configuration(text,text) RENAME TO forest_generation_configuration_v038;
CREATE FUNCTION public.forest_generation_configuration(selected_bot text,selected_generation text)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE selected_payload text;
BEGIN
 SELECT config_payload_id INTO selected_payload FROM public.bot_generations
  WHERE bot_id=selected_bot AND generation_id=selected_generation;
 IF NOT public.forest_payload_dependencies_available(selected_bot,selected_payload) THEN
  RAISE EXCEPTION 'GENERATION_CONFIG_UNAVAILABLE' USING ERRCODE='P0001';
 END IF;
 RETURN public.forest_generation_configuration_v038(selected_bot,selected_generation);
END $$;

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
  resource := CASE NEW.family WHEN 'config' THEN plan->>'component_id' WHEN 'binding' THEN plan->>'binding_id' ELSE '' END;
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

-- Same immutable decision rules, including the supplemental prerequisites.
CREATE OR REPLACE FUNCTION public.forest_payload_header_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' OR (to_jsonb(NEW)-ARRAY['state','forget_operation_id']) IS DISTINCT FROM
                           (to_jsonb(OLD)-ARRAY['state','forget_operation_id']) THEN
        RAISE EXCEPTION 'Payload identity and manifest are immutable' USING ERRCODE='55000';
    END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    IF OLD.state='ACTIVE' AND NEW.state='REVOKED' AND EXISTS(
        SELECT 1 FROM public.bot_payload_forget_requests f,
             jsonb_array_elements(f.targets) t,
             public.forest_payload_effective_sources s
        WHERE f.bot_id=NEW.bot_id AND f.operation_id=NEW.forget_operation_id
          AND s.bot_id=NEW.bot_id AND s.payload_id=NEW.payload_id
          AND s.source_unit_id=t->>'source_unit_id'
          AND (s.part_id='*' OR t->>'part_id'='*' OR s.part_id=t->>'part_id')
    ) THEN RETURN NEW; END IF;
    IF OLD.state='REVOKED' AND NEW.state='SCRUBBED'
       AND NEW.forget_operation_id=OLD.forget_operation_id AND NOT EXISTS(
           SELECT 1 FROM public.bot_payload_bodies WHERE bot_id=NEW.bot_id AND payload_id=NEW.payload_id
       ) THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Payload state cannot be revived or scrubbed without a request' USING ERRCODE='55000';
END $$;

-- Same immutable decision rules, including the supplemental prerequisites.
CREATE OR REPLACE FUNCTION public.forest_source_dependency_closure(selected_bot text,initial_sources jsonb,maximum integer)
RETURNS TABLE(source_unit_id text,source_revision bigint,part_id text)
LANGUAGE sql STABLE AS $$
 WITH RECURSIVE dependencies(unit,revision,part) AS (
  SELECT j->>'source_unit_id',(j->>'source_revision')::bigint,j->>'part_id'
   FROM jsonb_array_elements(initial_sources) j
  UNION
  SELECT p.source_unit_id,p.source_revision,p.part_id FROM dependencies d
   JOIN public.bot_payload_source_links l ON l.bot_id=selected_bot AND l.dependent_source_unit_id=d.unit
   JOIN public.forest_payload_effective_sources p ON p.bot_id=l.bot_id AND p.payload_id=l.parent_payload_id
 ) SELECT d.unit,d.revision,d.part FROM dependencies d LIMIT maximum+1
$$;

-- Same immutable decision rules, including the supplemental prerequisites.
CREATE OR REPLACE FUNCTION public.forest_source_dependent_closure(selected_bot text,initial_targets jsonb,maximum integer)
RETURNS TABLE(source_unit_id text,part_id text)
LANGUAGE sql STABLE AS $$
 WITH RECURSIVE targets(unit,part) AS (
  SELECT j->>'source_unit_id',j->>'part_id' FROM jsonb_array_elements(initial_targets) j
  UNION
  SELECT l.dependent_source_unit_id,'*' FROM targets t
   JOIN public.forest_payload_effective_sources p ON p.bot_id=selected_bot AND p.source_unit_id=t.unit
    AND (p.part_id='*' OR t.part='*' OR p.part_id=t.part)
   JOIN public.bot_payload_source_links l ON l.bot_id=p.bot_id AND l.parent_payload_id=p.payload_id
 ) SELECT t.unit,t.part FROM targets t LIMIT maximum+1
$$;

-- Same immutable decision rules, including the supplemental prerequisites.
CREATE OR REPLACE FUNCTION public.forest_revoke_source_link(selected_bot text,selected_event_id text)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE current_epoch bigint; cause text; targets jsonb;
        new_operation text; detail jsonb; link public.bot_payload_source_links;
BEGIN
 SELECT deletion_epoch INTO current_epoch FROM public.bot_memory_clock WHERE bot_id=selected_bot FOR UPDATE;
 SELECT * INTO link FROM public.bot_payload_source_links WHERE bot_id=selected_bot AND event_id=selected_event_id;
 IF link.bot_id IS NULL THEN RETURN false; END IF;
 SELECT forget_operation_id INTO cause FROM public.bot_payload_headers
  WHERE bot_id=selected_bot AND payload_id=link.parent_payload_id AND state IN ('REVOKED','SCRUBBED');
 IF cause IS NULL OR EXISTS(SELECT 1 FROM public.bot_payload_suppression x
  WHERE x.bot_id=selected_bot AND x.source_unit_id=link.dependent_source_unit_id AND x.part_id='*') THEN
  RETURN false;
 END IF;
 -- Each durable decision is deliberately one source. Updating all of its flat
 -- copies exposes revoked parent heads for the next bounded propagation batch.
 -- No real link or existing deletion decision is dropped on a graph-size limit.
 targets := jsonb_build_array(jsonb_build_object('source_unit_id',link.dependent_source_unit_id,'part_id','*'));
 new_operation := 'echo-forget:' || link.link_id;
 INSERT INTO public.bot_payload_forget_requests(bot_id,operation_id,input_digest,targets,deletion_epoch)
 VALUES (selected_bot,new_operation,'sha256:' || encode(sha256(convert_to(
  'forest.echo.forget.v1:' || link.link_id || ':' || cause,'UTF8')),'hex'),targets,current_epoch+1);
 INSERT INTO public.bot_payload_suppression(bot_id,source_unit_id,part_id,operation_id,deletion_epoch)
 VALUES(selected_bot,link.dependent_source_unit_id,'*',new_operation,current_epoch+1);
 UPDATE public.bot_memory_clock SET deletion_epoch=current_epoch+1,fact_revision=fact_revision+1 WHERE bot_id=selected_bot;
 UPDATE public.bot_payload_headers h SET state='REVOKED',forget_operation_id=new_operation
  WHERE h.bot_id=selected_bot AND h.state='ACTIVE' AND EXISTS(
   SELECT 1 FROM public.forest_payload_effective_sources s WHERE s.bot_id=h.bot_id AND s.payload_id=h.payload_id
    AND s.source_unit_id=link.dependent_source_unit_id);
 detail := jsonb_build_object('link_id',link.link_id,'event_id',link.event_id,
  'source_unit_id',link.dependent_source_unit_id,'cause_operation_id',cause,
  'deletion_epoch',current_epoch+1,'source_count',1);
 INSERT INTO public.bot_audit_records(bot_id,audit_id,action,operation_id,details)
 VALUES(selected_bot,new_operation,'memory.exact_echo_revoked',new_operation,detail);
 INSERT INTO public.bot_outbox_events(bot_id,event_id,topic,operation_id,payload)
 VALUES(selected_bot,new_operation,'memory.exact_echo_revoked',new_operation,detail);
 RETURN true;
END $$;
