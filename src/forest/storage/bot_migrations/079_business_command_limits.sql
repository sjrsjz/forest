-- Durable Bot-wide business-command admission limits and principal buckets.
DO $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.bot_schema_writer_gate
               WHERE singleton AND minimum_generation>=18) THEN
  RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
 END IF;
END;
$$;

ALTER TABLE public.bot_control_value_sources
 DROP CONSTRAINT bot_control_value_sources_family_check;
ALTER TABLE public.bot_control_value_sources
 ADD CONSTRAINT bot_control_value_sources_family_check CHECK(family IN (
  'config','agent.config','memory.config','binding','job','command.policy','command.limit'
));

UPDATE public.bot_web_issuers i SET actions=(
 SELECT jsonb_agg(action ORDER BY action) FROM (
  SELECT DISTINCT action FROM jsonb_array_elements_text(
   i.actions ||
   CASE WHEN i.actions ?| ARRAY['command.execution.read','command.policy.read']
    THEN '["command.limit.read"]'::jsonb ELSE '[]'::jsonb END ||
   CASE WHEN i.actions ?| ARRAY['command.execution.recover','command.policy.apply']
    THEN '["command.limit.plan","command.limit.apply"]'::jsonb ELSE '[]'::jsonb END
  ) AS action
 ) AS expanded
);

CREATE TABLE public.bot_command_limit_heads (
 bot_id text PRIMARY KEY,
 revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE public.bot_command_limit_plans (
 bot_id text NOT NULL,
 plan_id text NOT NULL,
 subject_id text NOT NULL,
 issuer_id text NOT NULL,
 expected_revision bigint NOT NULL CHECK(expected_revision>=0),
 max_argument_bytes integer NOT NULL CHECK(max_argument_bytes BETWEEN 1 AND 1048576),
 max_pending_bot integer NOT NULL CHECK(max_pending_bot BETWEEN 1 AND 100000),
 max_pending_binding integer NOT NULL CHECK(max_pending_binding BETWEEN 1 AND 10000),
 max_pending_principal integer NOT NULL CHECK(max_pending_principal BETWEEN 1 AND 1000),
 principal_burst integer NOT NULL CHECK(principal_burst BETWEEN 1 AND 1000),
 principal_refill_seconds integer NOT NULL CHECK(principal_refill_seconds BETWEEN 1 AND 86400),
 plan_digest text NOT NULL CHECK(plan_digest ~ '^sha256:[a-f0-9]{64}$'),
 expires_at timestamptz NOT NULL,
 values_payload_id text NOT NULL,
 values_payload_digest text NOT NULL CHECK(values_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,plan_id),
 CHECK(max_pending_principal<=max_pending_binding AND max_pending_binding<=max_pending_bot),
 FOREIGN KEY(bot_id,values_payload_id)
  REFERENCES public.bot_payload_headers(bot_id,payload_id)
);

CREATE TABLE public.bot_command_limit_revisions (
 bot_id text NOT NULL,
 revision bigint NOT NULL CHECK(revision>0),
 previous_revision bigint NOT NULL CHECK(previous_revision=revision-1),
 max_argument_bytes integer NOT NULL CHECK(max_argument_bytes BETWEEN 1 AND 1048576),
 max_pending_bot integer NOT NULL CHECK(max_pending_bot BETWEEN 1 AND 100000),
 max_pending_binding integer NOT NULL CHECK(max_pending_binding BETWEEN 1 AND 10000),
 max_pending_principal integer NOT NULL CHECK(max_pending_principal BETWEEN 1 AND 1000),
 principal_burst integer NOT NULL CHECK(principal_burst BETWEEN 1 AND 1000),
 principal_refill_seconds integer NOT NULL CHECK(principal_refill_seconds BETWEEN 1 AND 86400),
 policy_revision bigint NOT NULL CHECK(policy_revision>0),
 plan_id text NOT NULL,
 operation_id text NOT NULL,
 values_payload_id text NOT NULL,
 values_payload_digest text NOT NULL CHECK(values_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,revision),
 UNIQUE(bot_id,operation_id),
 CHECK(max_pending_principal<=max_pending_binding AND max_pending_binding<=max_pending_bot),
 FOREIGN KEY(bot_id,plan_id) REFERENCES public.bot_command_limit_plans(bot_id,plan_id),
 FOREIGN KEY(bot_id,operation_id) REFERENCES public.bot_web_operations(bot_id,operation_id),
 FOREIGN KEY(bot_id,values_payload_id)
  REFERENCES public.bot_payload_headers(bot_id,payload_id)
);

CREATE TABLE public.bot_command_principal_buckets (
 bot_id text NOT NULL,
 principal_ref text NOT NULL,
 tokens numeric(24,9) NOT NULL CHECK(tokens>=0),
 updated_at timestamptz NOT NULL,
 PRIMARY KEY(bot_id,principal_ref)
);

CREATE TRIGGER bot_command_limit_plan_immutable
 BEFORE UPDATE OR DELETE ON public.bot_command_limit_plans
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_command_limit_revision_immutable
 BEFORE UPDATE OR DELETE ON public.bot_command_limit_revisions
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE FUNCTION public.forest_command_limit_head_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  RAISE EXCEPTION 'COMMAND_LIMIT_HEAD_DELETE_FORBIDDEN' USING ERRCODE='55000';
 END IF;
 IF NEW.bot_id IS DISTINCT FROM OLD.bot_id OR NEW.revision<>OLD.revision+1
  OR NOT EXISTS(SELECT 1 FROM public.bot_command_limit_revisions r
      WHERE (r.bot_id,r.revision,r.previous_revision)=
            (NEW.bot_id,NEW.revision,OLD.revision)) THEN
  RAISE EXCEPTION 'COMMAND_LIMIT_HEAD_CONFLICT' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_command_limit_head_guard
 BEFORE UPDATE OR DELETE ON public.bot_command_limit_heads
 FOR EACH ROW EXECUTE FUNCTION public.forest_command_limit_head_guard();

CREATE FUNCTION public.forest_command_limit_payload_values(
 selected_bot text, selected_payload text, selected_digest text,
 selected_kind text, selected_identity text)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE h record; envelope jsonb; expected_owner text; expected_payload text;
 identity_json text; actual_parents jsonb;
BEGIN
 IF selected_kind NOT IN ('PLAN','REVISION')
  OR selected_identity IS NULL OR selected_identity='' OR octet_length(selected_identity)>4096
  OR selected_identity ~ '[\x01-\x1f\x7f]'
  OR (selected_kind='REVISION' AND selected_identity !~ '^(0|[1-9][0-9]*)$')
  OR selected_digest IS NULL OR selected_digest !~ '^sha256:[a-f0-9]{64}$'
  OR selected_bot IS NULL OR selected_payload IS NULL THEN
  RAISE EXCEPTION 'CONFIG_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 identity_json := '["command.limit",' || to_jsonb(selected_kind)::text || ',"",'
  || to_jsonb(selected_identity)::text || ']';
 expected_owner := 'config-values-v1:sha256:' || encode(sha256(convert_to(identity_json,'UTF8')),'hex');
 expected_payload := 'payload:sha256:' || encode(sha256(convert_to(
  '[' || to_jsonb(selected_bot)::text || ',"control.config",' || to_jsonb(expected_owner)::text || ']',
  'UTF8')),'hex');
 IF selected_payload IS DISTINCT FROM expected_payload THEN
  RAISE EXCEPTION 'CONFIG_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 IF NOT public.forest_payload_dependencies_available(selected_bot,selected_payload) THEN
  RAISE EXCEPTION 'CONTROL_CONTENT_REVOKED' USING ERRCODE='P0001';
 END IF;
 SELECT p.*,b.content INTO h FROM public.bot_payload_headers p
  LEFT JOIN public.bot_payload_bodies b USING(bot_id,payload_id)
  WHERE p.bot_id=selected_bot AND p.payload_id=selected_payload;
 IF NOT FOUND OR h.owner_kind<>'control.config' OR h.owner_id<>expected_owner
  OR h.encoding<>'json' OR h.digest<>selected_digest OR h.state<>'ACTIVE' OR h.content IS NULL
  OR jsonb_typeof(h.manifest)<>'object' OR NOT (h.manifest ?& ARRAY['parents','sources'])
  OR h.manifest-ARRAY['parents','sources']<>'{}'::jsonb
  OR jsonb_typeof(h.manifest->'parents')<>'array'
  OR jsonb_typeof(h.manifest->'sources')<>'array'
  OR jsonb_array_length(h.manifest->'parents') NOT BETWEEN 1 AND 256
  OR jsonb_array_length(h.manifest->'sources')>4096
  OR h.byte_length<>octet_length(h.content)::bigint
  OR h.digest<>('sha256:' || encode(sha256(h.content),'hex')) THEN
  RAISE EXCEPTION 'CONFIG_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 SELECT COALESCE(jsonb_agg(parent_payload_id ORDER BY parent_payload_id COLLATE "C"),'[]'::jsonb)
  INTO actual_parents FROM public.bot_payload_derivations
  WHERE bot_id=selected_bot AND payload_id=selected_payload;
 IF actual_parents IS DISTINCT FROM h.manifest->'parents'
  OR (SELECT COALESCE(jsonb_agg(jsonb_build_object(
       'source_unit_id',source_unit_id,'source_revision',source_revision,'part_id',part_id)
       ORDER BY source_unit_id,source_revision,part_id),'[]'::jsonb)
      FROM public.bot_payload_sources
      WHERE bot_id=selected_bot AND payload_id=selected_payload)
     IS DISTINCT FROM
     (SELECT COALESCE(jsonb_agg(source ORDER BY source->>'source_unit_id',
       source->>'source_revision',source->>'part_id'),'[]'::jsonb)
      FROM jsonb_array_elements(h.manifest->'sources') source)
  OR EXISTS(
   SELECT 1 FROM public.bot_payload_derivations d
   JOIN public.bot_payload_headers p ON (p.bot_id,p.payload_id)=(d.bot_id,d.parent_payload_id)
   LEFT JOIN public.bot_payload_bodies b ON (b.bot_id,b.payload_id)=(p.bot_id,p.payload_id)
   WHERE d.bot_id=selected_bot AND d.payload_id=selected_payload
    AND (p.state<>'ACTIVE' OR b.payload_id IS NULL)
  ) THEN
  RAISE EXCEPTION 'CONTROL_CONTENT_REVOKED' USING ERRCODE='P0001';
 END IF;
 BEGIN envelope := convert_from(h.content,'UTF8')::jsonb;
 EXCEPTION WHEN invalid_text_representation OR character_not_in_repertoire OR untranslatable_character THEN
  RAISE EXCEPTION 'CONFIG_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END;
 IF jsonb_typeof(envelope)<>'object'
  OR envelope-ARRAY['family','kind','resource','identity','values']<>'{}'::jsonb
  OR envelope->>'family'<>'command.limit' OR envelope->>'kind'<>selected_kind
  OR envelope->>'resource'<>'' OR envelope->>'identity'<>selected_identity THEN
  RAISE EXCEPTION 'CONFIG_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 RETURN envelope->'values';
END;
$$;
CREATE FUNCTION public.forest_command_limit_values_valid(
 selected_values jsonb,
 selected_max_argument_bytes integer,
 selected_max_pending_bot integer,
 selected_max_pending_binding integer,
 selected_max_pending_principal integer,
 selected_principal_burst integer,
 selected_principal_refill_seconds integer)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
 RETURN jsonb_typeof(selected_values)='object'
  AND selected_values ?& ARRAY[
   'max_argument_bytes','max_pending_bot','max_pending_binding',
   'max_pending_principal','principal_burst','principal_refill_seconds'
  ]
  AND selected_values-ARRAY[
   'max_argument_bytes','max_pending_bot','max_pending_binding',
   'max_pending_principal','principal_burst','principal_refill_seconds'
  ]='{}'::jsonb
  AND jsonb_typeof(selected_values->'max_argument_bytes')='number'
  AND jsonb_typeof(selected_values->'max_pending_bot')='number'
  AND jsonb_typeof(selected_values->'max_pending_binding')='number'
  AND jsonb_typeof(selected_values->'max_pending_principal')='number'
  AND jsonb_typeof(selected_values->'principal_burst')='number'
  AND jsonb_typeof(selected_values->'principal_refill_seconds')='number'
  AND selected_values->>'max_argument_bytes'=selected_max_argument_bytes::text
  AND selected_values->>'max_pending_bot'=selected_max_pending_bot::text
  AND selected_values->>'max_pending_binding'=selected_max_pending_binding::text
  AND selected_values->>'max_pending_principal'=selected_max_pending_principal::text
  AND selected_values->>'principal_burst'=selected_principal_burst::text
  AND selected_values->>'principal_refill_seconds'=selected_principal_refill_seconds::text
  AND selected_max_argument_bytes BETWEEN 1 AND 1048576
  AND selected_max_pending_bot BETWEEN 1 AND 100000
  AND selected_max_pending_binding BETWEEN 1 AND 10000
  AND selected_max_pending_principal BETWEEN 1 AND 1000
  AND selected_principal_burst BETWEEN 1 AND 1000
  AND selected_principal_refill_seconds BETWEEN 1 AND 86400
  AND selected_max_pending_principal<=selected_max_pending_binding
  AND selected_max_pending_binding<=selected_max_pending_bot;
END;
$$;

CREATE OR REPLACE FUNCTION public.forest_control_plan_source_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
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
 selected_table := CASE NEW.family WHEN 'config' THEN 'bot_config_plans'
  WHEN 'agent.config' THEN 'bot_agent_config_plans'
  WHEN 'memory.config' THEN 'bot_memory_config_plans'
  WHEN 'binding' THEN 'bot_binding_plans' WHEN 'job' THEN 'bot_job_plans'
  WHEN 'command.policy' THEN 'bot_command_policy_plans'
  WHEN 'command.limit' THEN 'bot_command_limit_plans' END;
 IF selected_table IS NULL THEN
  RAISE EXCEPTION 'CONTROL_PLAN_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 EXECUTE format('SELECT to_jsonb(p) FROM public.%I p WHERE bot_id=$1 AND plan_id=$2',selected_table)
  INTO plan USING NEW.bot_id,NEW.source_id;
 IF plan->>'plan_id' IS DISTINCT FROM NEW.source_id
  OR ROW(plan->>'subject_id',plan->>'issuer_id') IS DISTINCT FROM ROW(op.subject_id,op.issuer_id) THEN
  RAISE EXCEPTION 'CONTROL_PLAN_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 value := public.forest_control_payload_value(
  NEW.bot_id,NEW.payload_id,NEW.payload_digest,'result',NEW.operation_id,op.action
 );
 resource := CASE NEW.family WHEN 'config' THEN plan->>'component_id'
  WHEN 'binding' THEN plan->>'binding_id'
  WHEN 'job' THEN plan->>'job_id'
  WHEN 'command.policy' THEN plan->>'resource_id' ELSE '' END;
 IF NEW.family='command.policy' THEN
  actual_values := public.forest_command_policy_payload_values(
   NEW.bot_id,plan->>'values_payload_id',plan->>'values_payload_digest',
   'PLAN',resource,NEW.source_id
  );
  IF NOT public.forest_command_policy_values_valid(
   actual_values,plan->>'policy_mode',(plan->>'user_count')::integer
  ) THEN
   RAISE EXCEPTION 'CONTROL_PLAN_SOURCE_CONFLICT' USING ERRCODE='23514';
  END IF;
 ELSIF NEW.family='command.limit' THEN
  actual_values := public.forest_command_limit_payload_values(
   NEW.bot_id,plan->>'values_payload_id',plan->>'values_payload_digest','PLAN',NEW.source_id
  );
  IF NOT public.forest_command_limit_values_valid(
   actual_values,(plan->>'max_argument_bytes')::integer,
   (plan->>'max_pending_bot')::integer,(plan->>'max_pending_binding')::integer,
   (plan->>'max_pending_principal')::integer,(plan->>'principal_burst')::integer,
   (plan->>'principal_refill_seconds')::integer
  ) THEN
   RAISE EXCEPTION 'CONTROL_PLAN_SOURCE_CONFLICT' USING ERRCODE='23514';
  END IF;
 ELSE
  actual_values := public.forest_config_payload_values(
   NEW.bot_id,plan->>'values_payload_id',plan->>'values_payload_digest',
   NEW.family,'PLAN',resource,NEW.source_id
  );
 END IF;
 IF NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=plan->>'values_payload_id' AND parent_payload_id=op.input_payload_id)
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=NEW.payload_id AND parent_payload_id=plan->>'values_payload_id')
  OR value->>'plan_id' IS DISTINCT FROM plan->>'plan_id'
  OR value->>'plan_digest' IS DISTINCT FROM plan->>'plan_digest'
  OR value->'values' IS DISTINCT FROM actual_values THEN
  RAISE EXCEPTION 'CONTROL_PLAN_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END;
$$;

CREATE FUNCTION public.forest_command_limit_revision_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE plan record; op record; plan_values jsonb; revision_values jsonb;
 input_value jsonb; result_value jsonb;
BEGIN
 SELECT * INTO plan FROM public.bot_command_limit_plans
  WHERE bot_id=NEW.bot_id AND plan_id=NEW.plan_id;
 SELECT * INTO op FROM public.bot_web_operations
  WHERE bot_id=NEW.bot_id AND operation_id=NEW.operation_id;
 IF plan.bot_id IS NULL OR op.bot_id IS NULL OR op.action<>'command.limit.apply'
  OR ROW(NEW.previous_revision,NEW.max_argument_bytes,NEW.max_pending_bot,
         NEW.max_pending_binding,NEW.max_pending_principal,NEW.principal_burst,
         NEW.principal_refill_seconds)
     IS DISTINCT FROM
     ROW(plan.expected_revision,plan.max_argument_bytes,plan.max_pending_bot,
         plan.max_pending_binding,plan.max_pending_principal,plan.principal_burst,
         plan.principal_refill_seconds)
  OR ROW(plan.subject_id,plan.issuer_id) IS DISTINCT FROM ROW(op.subject_id,op.issuer_id)
  OR (op.ticket_basis->>'policy_revision')::bigint IS DISTINCT FROM NEW.policy_revision THEN
  RAISE EXCEPTION 'COMMAND_LIMIT_REVISION_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 plan_values := public.forest_command_limit_payload_values(
  NEW.bot_id,plan.values_payload_id,plan.values_payload_digest,'PLAN',plan.plan_id
 );
 revision_values := public.forest_command_limit_payload_values(
  NEW.bot_id,NEW.values_payload_id,NEW.values_payload_digest,'REVISION',NEW.revision::text
 );
 input_value := public.forest_control_payload_value(
  NEW.bot_id,op.input_payload_id,op.input_payload_digest,
  'input',op.operation_id,op.action
 );
 result_value := public.forest_control_payload_value(
  NEW.bot_id,op.result_payload_id,op.result_payload_digest,
  'result',op.operation_id,op.action
 );
 IF revision_values IS DISTINCT FROM plan_values
  OR NOT public.forest_command_limit_values_valid(
   revision_values,NEW.max_argument_bytes,NEW.max_pending_bot,
   NEW.max_pending_binding,NEW.max_pending_principal,NEW.principal_burst,
   NEW.principal_refill_seconds
  )
  OR input_value IS DISTINCT FROM jsonb_build_object(
   'expected_revision',NEW.previous_revision,'plan_id',NEW.plan_id,
   'plan_digest',plan.plan_digest
  )
  OR result_value IS DISTINCT FROM jsonb_build_object(
   'revision',NEW.revision,'status','APPLIED'
  )
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations
   WHERE bot_id=NEW.bot_id AND payload_id=NEW.values_payload_id
    AND parent_payload_id=plan.values_payload_id)
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations
   WHERE bot_id=NEW.bot_id AND payload_id=NEW.values_payload_id
    AND parent_payload_id=op.input_payload_id)
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations
   WHERE bot_id=NEW.bot_id AND payload_id=op.result_payload_id
    AND parent_payload_id=NEW.values_payload_id) THEN
  RAISE EXCEPTION 'COMMAND_LIMIT_REVISION_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_command_limit_revision_source_guard
 BEFORE INSERT ON public.bot_command_limit_revisions
 FOR EACH ROW EXECUTE FUNCTION public.forest_command_limit_revision_guard();

ALTER TABLE public.bot_business_command_requests
 ADD COLUMN admission_limit_revision bigint,
 ADD FOREIGN KEY(bot_id,admission_limit_revision)
  REFERENCES public.bot_command_limit_revisions(bot_id,revision);
CREATE INDEX bot_business_command_admission_counts
 ON public.bot_business_command_requests(bot_id,binding_id,principal_ref)
 WHERE status IN ('PENDING','CLAIMED');

