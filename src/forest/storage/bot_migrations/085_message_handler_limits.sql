-- Durable Bot-wide ordinary-message Handler admission limits and principal buckets.
DO $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.bot_schema_writer_gate
               WHERE singleton AND minimum_generation>=24) THEN
  RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
 END IF;
END;
$$;

ALTER TABLE public.bot_control_value_sources
 DROP CONSTRAINT bot_control_value_sources_family_check;
ALTER TABLE public.bot_control_value_sources
 ADD CONSTRAINT bot_control_value_sources_family_check CHECK(family IN (
  'config','agent.config','memory.config','binding','job','command.policy','command.limit',
  'message.handler.policy','message.handler.limit'
));

UPDATE public.bot_web_issuers i SET actions=(
 SELECT jsonb_agg(action ORDER BY action) FROM (
  SELECT DISTINCT action FROM jsonb_array_elements_text(
   i.actions ||
   CASE WHEN i.actions ? 'message.read'
    THEN '["message.handler.limit.read"]'::jsonb ELSE '[]'::jsonb END ||
   CASE WHEN i.actions ? 'config.plan'
    THEN '["message.handler.limit.plan"]'::jsonb ELSE '[]'::jsonb END ||
   CASE WHEN i.actions ?& ARRAY['config.apply','message.cancel']
    THEN '["message.handler.limit.apply"]'::jsonb ELSE '[]'::jsonb END
  ) AS action
 ) AS expanded
);

CREATE TABLE public.bot_message_handler_limit_heads (
 bot_id text PRIMARY KEY,
 revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE public.bot_message_handler_limit_plans (
 bot_id text NOT NULL,
 plan_id text NOT NULL,
 subject_id text NOT NULL,
 issuer_id text NOT NULL,
 expected_revision bigint NOT NULL CHECK(expected_revision>=0),
 max_event_bytes integer NOT NULL CHECK(max_event_bytes BETWEEN 1 AND 1048576),
 max_pending_bot integer NOT NULL CHECK(max_pending_bot BETWEEN 1 AND 100000),
 max_pending_binding integer NOT NULL CHECK(max_pending_binding BETWEEN 1 AND 10000),
 max_pending_principal integer NOT NULL CHECK(max_pending_principal BETWEEN 1 AND 1000),
 max_handlers_per_chain integer NOT NULL CHECK(max_handlers_per_chain BETWEEN 1 AND 256),
 principal_burst integer NOT NULL CHECK(principal_burst BETWEEN 1 AND 1000),
 principal_refill_seconds integer NOT NULL CHECK(principal_refill_seconds BETWEEN 1 AND 86400),
 deadline_seconds integer NOT NULL CHECK(deadline_seconds BETWEEN 1 AND 3600),
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

CREATE TABLE public.bot_message_handler_limit_revisions (
 bot_id text NOT NULL,
 revision bigint NOT NULL CHECK(revision>0),
 previous_revision bigint NOT NULL CHECK(previous_revision=revision-1),
 max_event_bytes integer NOT NULL CHECK(max_event_bytes BETWEEN 1 AND 1048576),
 max_pending_bot integer NOT NULL CHECK(max_pending_bot BETWEEN 1 AND 100000),
 max_pending_binding integer NOT NULL CHECK(max_pending_binding BETWEEN 1 AND 10000),
 max_pending_principal integer NOT NULL CHECK(max_pending_principal BETWEEN 1 AND 1000),
 max_handlers_per_chain integer NOT NULL CHECK(max_handlers_per_chain BETWEEN 1 AND 256),
 principal_burst integer NOT NULL CHECK(principal_burst BETWEEN 1 AND 1000),
 principal_refill_seconds integer NOT NULL CHECK(principal_refill_seconds BETWEEN 1 AND 86400),
 deadline_seconds integer NOT NULL CHECK(deadline_seconds BETWEEN 1 AND 3600),
 policy_revision bigint NOT NULL CHECK(policy_revision>0),
 plan_id text NOT NULL,
 operation_id text NOT NULL,
 values_payload_id text NOT NULL,
 values_payload_digest text NOT NULL CHECK(values_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,revision),
 UNIQUE(bot_id,operation_id),
 CHECK(max_pending_principal<=max_pending_binding AND max_pending_binding<=max_pending_bot),
 FOREIGN KEY(bot_id,plan_id) REFERENCES public.bot_message_handler_limit_plans(bot_id,plan_id),
 FOREIGN KEY(bot_id,operation_id) REFERENCES public.bot_web_operations(bot_id,operation_id),
 FOREIGN KEY(bot_id,values_payload_id)
  REFERENCES public.bot_payload_headers(bot_id,payload_id)
);

CREATE TABLE public.bot_message_handler_principal_buckets (
 bot_id text NOT NULL,
 principal_ref text NOT NULL,
 tokens numeric(24,9) NOT NULL CHECK(tokens>=0),
 updated_at timestamptz NOT NULL,
 PRIMARY KEY(bot_id,principal_ref)
);

CREATE TRIGGER bot_message_handler_limit_plan_immutable
 BEFORE UPDATE OR DELETE ON public.bot_message_handler_limit_plans
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_message_handler_limit_revision_immutable
 BEFORE UPDATE OR DELETE ON public.bot_message_handler_limit_revisions
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE FUNCTION public.forest_message_handler_limit_head_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_LIMIT_HEAD_DELETE_FORBIDDEN' USING ERRCODE='55000';
 END IF;
 IF NEW.bot_id IS DISTINCT FROM OLD.bot_id OR NEW.revision<>OLD.revision+1
  OR NOT EXISTS(SELECT 1 FROM public.bot_message_handler_limit_revisions r
      WHERE (r.bot_id,r.revision,r.previous_revision)=
            (NEW.bot_id,NEW.revision,OLD.revision)) THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_LIMIT_HEAD_CONFLICT' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_message_handler_limit_head_guard
 BEFORE UPDATE OR DELETE ON public.bot_message_handler_limit_heads
 FOR EACH ROW EXECUTE FUNCTION public.forest_message_handler_limit_head_guard();

CREATE FUNCTION public.forest_message_handler_limit_payload_values(
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
 identity_json := '["message.handler.limit",' || to_jsonb(selected_kind)::text || ',"",'
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
  OR envelope->>'family'<>'message.handler.limit' OR envelope->>'kind'<>selected_kind
  OR envelope->>'resource'<>'' OR envelope->>'identity'<>selected_identity THEN
  RAISE EXCEPTION 'CONFIG_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 RETURN envelope->'values';
END;
$$;
CREATE FUNCTION public.forest_message_handler_limit_values_valid(
 selected_values jsonb,
 selected_max_event_bytes integer,
 selected_max_pending_bot integer,
 selected_max_pending_binding integer,
 selected_max_pending_principal integer,
 selected_max_handlers_per_chain integer,
 selected_principal_burst integer,
 selected_principal_refill_seconds integer,
 selected_deadline_seconds integer)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
 RETURN jsonb_typeof(selected_values)='object'
  AND selected_values ?& ARRAY[
   'max_event_bytes','max_pending_bot','max_pending_binding',
   'max_pending_principal','max_handlers_per_chain','principal_burst','principal_refill_seconds','deadline_seconds'
  ]
  AND selected_values-ARRAY[
   'max_event_bytes','max_pending_bot','max_pending_binding',
   'max_pending_principal','max_handlers_per_chain','principal_burst','principal_refill_seconds','deadline_seconds'
  ]='{}'::jsonb
  AND jsonb_typeof(selected_values->'max_event_bytes')='number'
  AND jsonb_typeof(selected_values->'max_pending_bot')='number'
  AND jsonb_typeof(selected_values->'max_pending_binding')='number'
  AND jsonb_typeof(selected_values->'max_pending_principal')='number'
  AND jsonb_typeof(selected_values->'max_handlers_per_chain')='number'
  AND jsonb_typeof(selected_values->'principal_burst')='number'
  AND jsonb_typeof(selected_values->'principal_refill_seconds')='number'
  AND jsonb_typeof(selected_values->'deadline_seconds')='number'
  AND selected_values->>'max_event_bytes'=selected_max_event_bytes::text
  AND selected_values->>'max_pending_bot'=selected_max_pending_bot::text
  AND selected_values->>'max_pending_binding'=selected_max_pending_binding::text
  AND selected_values->>'max_pending_principal'=selected_max_pending_principal::text
  AND selected_values->>'max_handlers_per_chain'=selected_max_handlers_per_chain::text
  AND selected_values->>'principal_burst'=selected_principal_burst::text
  AND selected_values->>'principal_refill_seconds'=selected_principal_refill_seconds::text
  AND selected_values->>'deadline_seconds'=selected_deadline_seconds::text
  AND selected_max_event_bytes BETWEEN 1 AND 1048576
  AND selected_max_pending_bot BETWEEN 1 AND 100000
  AND selected_max_pending_binding BETWEEN 1 AND 10000
  AND selected_max_pending_principal BETWEEN 1 AND 1000
  AND selected_max_handlers_per_chain BETWEEN 1 AND 256
  AND selected_principal_burst BETWEEN 1 AND 1000
  AND selected_principal_refill_seconds BETWEEN 1 AND 86400
  AND selected_deadline_seconds BETWEEN 1 AND 3600
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
  WHEN 'command.limit' THEN 'bot_command_limit_plans'
  WHEN 'message.handler.policy' THEN 'bot_message_handler_policy_plans'
  WHEN 'message.handler.limit' THEN 'bot_message_handler_limit_plans' END;
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
  WHEN 'command.policy' THEN plan->>'resource_id'
  WHEN 'message.handler.policy' THEN plan->>'resource_id' ELSE '' END;
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
 ELSIF NEW.family='message.handler.limit' THEN
  actual_values := public.forest_message_handler_limit_payload_values(
   NEW.bot_id,plan->>'values_payload_id',plan->>'values_payload_digest','PLAN',NEW.source_id
  );
  IF NOT public.forest_message_handler_limit_values_valid(
   actual_values,(plan->>'max_event_bytes')::integer,
   (plan->>'max_pending_bot')::integer,(plan->>'max_pending_binding')::integer,
   (plan->>'max_pending_principal')::integer,(plan->>'max_handlers_per_chain')::integer,
   (plan->>'principal_burst')::integer,(plan->>'principal_refill_seconds')::integer,
   (plan->>'deadline_seconds')::integer
  ) THEN
   RAISE EXCEPTION 'CONTROL_PLAN_SOURCE_CONFLICT' USING ERRCODE='23514';
  END IF;
 ELSIF NEW.family='message.handler.policy' THEN
  actual_values := public.forest_message_handler_policy_payload_values(
   NEW.bot_id,plan->>'values_payload_id',plan->>'values_payload_digest',
   'PLAN',resource,NEW.source_id
  );
  IF NOT public.forest_message_handler_policy_values_valid(
   actual_values,plan->>'policy_mode',(plan->>'user_count')::integer
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

CREATE FUNCTION public.forest_message_handler_limit_revision_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE plan record; op record; plan_values jsonb; revision_values jsonb;
 input_value jsonb; result_value jsonb;
BEGIN
 SELECT * INTO plan FROM public.bot_message_handler_limit_plans
  WHERE bot_id=NEW.bot_id AND plan_id=NEW.plan_id;
 SELECT * INTO op FROM public.bot_web_operations
  WHERE bot_id=NEW.bot_id AND operation_id=NEW.operation_id;
 IF plan.bot_id IS NULL OR op.bot_id IS NULL OR op.action<>'message.handler.limit.apply'
  OR ROW(NEW.previous_revision,NEW.max_event_bytes,NEW.max_pending_bot,
         NEW.max_pending_binding,NEW.max_pending_principal,NEW.max_handlers_per_chain,
         NEW.principal_burst,NEW.principal_refill_seconds,NEW.deadline_seconds)
     IS DISTINCT FROM
     ROW(plan.expected_revision,plan.max_event_bytes,plan.max_pending_bot,
         plan.max_pending_binding,plan.max_pending_principal,plan.max_handlers_per_chain,
         plan.principal_burst,plan.principal_refill_seconds,plan.deadline_seconds)
  OR ROW(plan.subject_id,plan.issuer_id) IS DISTINCT FROM ROW(op.subject_id,op.issuer_id)
  OR (op.ticket_basis->>'policy_revision')::bigint IS DISTINCT FROM NEW.policy_revision THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_LIMIT_REVISION_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 plan_values := public.forest_message_handler_limit_payload_values(
  NEW.bot_id,plan.values_payload_id,plan.values_payload_digest,'PLAN',plan.plan_id
 );
 revision_values := public.forest_message_handler_limit_payload_values(
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
  OR NOT public.forest_message_handler_limit_values_valid(
   revision_values,NEW.max_event_bytes,NEW.max_pending_bot,
   NEW.max_pending_binding,NEW.max_pending_principal,NEW.max_handlers_per_chain,
   NEW.principal_burst,NEW.principal_refill_seconds,NEW.deadline_seconds
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
  RAISE EXCEPTION 'MESSAGE_HANDLER_LIMIT_REVISION_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_message_handler_limit_revision_source_guard
 BEFORE INSERT ON public.bot_message_handler_limit_revisions
 FOR EACH ROW EXECUTE FUNCTION public.forest_message_handler_limit_revision_guard();

ALTER TABLE public.bot_message_handler_chains
 ADD COLUMN admission_limit_revision bigint,
 ADD COLUMN event_bytes integer NOT NULL DEFAULT 0 CHECK(event_bytes BETWEEN 0 AND 1048576),
 ADD COLUMN admission_exempt boolean NOT NULL DEFAULT true,
 ADD FOREIGN KEY(bot_id,admission_limit_revision)
  REFERENCES public.bot_message_handler_limit_revisions(bot_id,revision);
ALTER TABLE public.bot_message_handler_chains ALTER COLUMN event_bytes DROP DEFAULT;
ALTER TABLE public.bot_message_handler_chains ALTER COLUMN admission_exempt DROP DEFAULT;
CREATE INDEX bot_message_handler_admission_counts
 ON public.bot_message_handler_chains(bot_id,binding_id,principal_ref)
 WHERE status='PENDING' AND admission_exempt=false;

CREATE OR REPLACE FUNCTION public.forest_message_handler_chain_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE inbox record; conversation record; result_disposition text; failure_retryable boolean;
 failure_retry_at timestamptz; has_attempt boolean; call_status text;
BEGIN
 IF TG_OP='INSERT' THEN
  SELECT * INTO inbox FROM public.bot_message_inbox
   WHERE bot_id=NEW.bot_id AND event_id=NEW.event_id;
  IF inbox.bot_id IS NULL OR inbox.trigger_decision<>'PENDING'
   OR ROW(inbox.conversation_id,inbox.ingress_seq,inbox.payload_id,
          inbox.author->>'principal_ref',inbox.author->>'external_user_id') IS DISTINCT FROM
      ROW(NEW.conversation_id,NEW.ingress_seq,NEW.input_payload_id,
          NEW.principal_ref,NEW.external_user_id)
   OR NEW.chain_id<>'message-handler-chain:sha256:' || encode(sha256(convert_to(
      to_jsonb(NEW.event_id)::text,'UTF8')),'hex') THEN
   RAISE EXCEPTION 'MESSAGE_HANDLER_CHAIN_PROOF_INVALID' USING ERRCODE='55000';
  END IF;
  RETURN NEW;
 END IF;
 IF TG_OP='DELETE' THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_CHAIN_DELETE_FORBIDDEN' USING ERRCODE='55000';
 END IF;
 -- A business-classification barrier becomes capacity-governed only when
 -- deterministic classification confirms that ordinary Handler routing owns it.
 IF OLD.status='PENDING' AND OLD.admission_exempt AND NOT NEW.admission_exempt
  AND NEW.revision=OLD.revision+1
  AND NEW.deadline_at>OLD.created_at
  AND (to_jsonb(NEW)-ARRAY['status','revision','terminal_reason','completed_at',
       'admission_limit_revision','admission_exempt','deadline_at']) IS NOT DISTINCT FROM
      (to_jsonb(OLD)-ARRAY['status','revision','terminal_reason','completed_at',
       'admission_limit_revision','admission_exempt','deadline_at'])
  AND ((NEW.status='PENDING' AND NEW.terminal_reason IS NULL
        AND NEW.completed_at IS NULL)
    OR (NEW.status='FAILED' AND NEW.terminal_reason IN (
        'MESSAGE_HANDLER_LIMITS_UNAVAILABLE','MESSAGE_HANDLER_EVENT_TOO_LARGE',
        'MESSAGE_HANDLER_CHAIN_TOO_LARGE','MESSAGE_HANDLER_BOT_QUEUE_FULL',
        'MESSAGE_HANDLER_BINDING_QUEUE_FULL','MESSAGE_HANDLER_PRINCIPAL_QUEUE_FULL',
        'MESSAGE_HANDLER_RATE_LIMITED') AND NEW.completed_at IS NOT NULL)) THEN
  RETURN NEW;
 END IF;
 IF (to_jsonb(NEW)-ARRAY['fallback_decision','fallback_rule_revision','fallback_reason',
      'fallback_digest','status','current_index','revision','terminal_reason','completed_at',
      'claim_actor_id','claim_actor_epoch','claim_host_incarnation','attempt_count',
      'next_attempt_at','last_error_code']) IS DISTINCT FROM
    (to_jsonb(OLD)-ARRAY['fallback_decision','fallback_rule_revision','fallback_reason',
      'fallback_digest','status','current_index','revision','terminal_reason','completed_at',
      'claim_actor_id','claim_actor_epoch','claim_host_incarnation','attempt_count',
      'next_attempt_at','last_error_code'])
  OR NEW.revision<>OLD.revision+1 THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_CHAIN_IMMUTABLE' USING ERRCODE='55000';
 END IF;
 -- The sole legacy compatibility mutation fills a previously unknown fallback.
 IF OLD.fallback_decision='PENDING' AND NEW.status=OLD.status
  AND ROW(NEW.current_index,NEW.claim_actor_id,NEW.claim_actor_epoch,
          NEW.claim_host_incarnation,NEW.attempt_count,NEW.next_attempt_at,
          NEW.last_error_code,NEW.terminal_reason,NEW.completed_at) IS NOT DISTINCT FROM
      ROW(OLD.current_index,OLD.claim_actor_id,OLD.claim_actor_epoch,
          OLD.claim_host_incarnation,OLD.attempt_count,OLD.next_attempt_at,
          OLD.last_error_code,OLD.terminal_reason,OLD.completed_at)
  AND NEW.fallback_decision IN ('TRIGGER','NO_TRIGGER') THEN
  RETURN NEW;
 END IF;
 IF ROW(NEW.fallback_decision,NEW.fallback_rule_revision,NEW.fallback_reason,
        NEW.fallback_digest) IS DISTINCT FROM
    ROW(OLD.fallback_decision,OLD.fallback_rule_revision,OLD.fallback_reason,
        OLD.fallback_digest) THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_FALLBACK_INVALID' USING ERRCODE='55000';
 END IF;
 SELECT * INTO conversation FROM public.bot_conversations
  WHERE bot_id=NEW.bot_id AND conversation_id=NEW.conversation_id;
 SELECT c.status INTO call_status FROM public.bot_message_handler_attempts a
  LEFT JOIN public.bot_calls c ON c.bot_id=a.bot_id AND c.operation_id=a.call_operation_id
  WHERE (a.bot_id,a.chain_id,a.attempt_no)=(OLD.bot_id,OLD.chain_id,OLD.attempt_count);
 -- Revocation after a claim suppresses a completed result or abandons work that
 -- was never durably admitted. An ADMITTED call must settle before cancellation.
 IF OLD.status='PENDING' AND OLD.claim_actor_id IS NOT NULL
  AND NEW.status='CANCELLED' AND NEW.current_index=OLD.current_index
  AND NEW.attempt_count=OLD.attempt_count AND NEW.claim_actor_id IS NULL
  AND ROW(NEW.next_attempt_at,NEW.last_error_code) IS NOT DISTINCT FROM
      ROW(OLD.next_attempt_at,OLD.last_error_code)
  AND NEW.completed_at IS NOT NULL
  AND ((call_status='COMPLETED' AND NEW.terminal_reason='POLICY_REVOKED_AFTER_CALL')
    OR (call_status IS DISTINCT FROM 'ADMITTED' AND call_status IS DISTINCT FROM 'COMPLETED'
        AND NEW.terminal_reason='POLICY_REVOKED_AFTER_CLAIM')) THEN
  RETURN NEW;
 END IF;
 -- Cancellation is allowed only before an item has begun.
 IF OLD.status IN ('PENDING','PASSED') AND OLD.claim_actor_id IS NULL
  AND NEW.status='CANCELLED' AND NEW.current_index=OLD.current_index
  AND NEW.attempt_count=OLD.attempt_count
  AND ROW(NEW.claim_actor_id,NEW.claim_actor_epoch,NEW.claim_host_incarnation,
          NEW.next_attempt_at,NEW.last_error_code) IS NOT DISTINCT FROM
      ROW(OLD.claim_actor_id,OLD.claim_actor_epoch,OLD.claim_host_incarnation,
          OLD.next_attempt_at,OLD.last_error_code)
  AND NEW.terminal_reason IN ('BUSINESS_COMMAND_OWNED','BUSINESS_CLASSIFICATION_FAILED',
      'POLICY_REVOKED') AND NEW.completed_at IS NOT NULL THEN
  RETURN NEW;
 END IF;
 -- Claim a new attempt or transfer an unfinished claim to the replacement Actor.
 IF OLD.status='PENDING' AND NEW.status='PENDING'
  AND NEW.current_index=OLD.current_index AND NEW.completed_at IS NULL
  AND NEW.claim_actor_id IS NOT NULL AND NEW.terminal_reason IS NOT DISTINCT FROM OLD.terminal_reason
  AND NEW.last_error_code IS NOT DISTINCT FROM OLD.last_error_code
  AND NEW.next_attempt_at IS NOT DISTINCT FROM OLD.next_attempt_at
  AND ROW(NEW.claim_actor_id,NEW.claim_actor_epoch,NEW.claim_host_incarnation)
      IS NOT DISTINCT FROM ROW(conversation.actor_id,conversation.actor_epoch,
                               conversation.actor_host_incarnation) THEN
  IF OLD.claim_actor_id IS NULL THEN
   SELECT EXISTS(SELECT 1 FROM public.bot_message_handler_attempts a
    WHERE (a.bot_id,a.chain_id,a.attempt_no,a.route_index)=
     (NEW.bot_id,NEW.chain_id,NEW.attempt_count,NEW.current_index)) INTO has_attempt;
   IF NEW.attempt_count=OLD.attempt_count+1 AND has_attempt THEN RETURN NEW; END IF;
  ELSIF NEW.attempt_count=OLD.attempt_count THEN
   RETURN NEW;
  END IF;
 END IF;
 SELECT r.disposition FROM public.bot_message_handler_results r
  WHERE (r.bot_id,r.chain_id,r.route_index,r.attempt_no)=
   (NEW.bot_id,NEW.chain_id,OLD.current_index,OLD.attempt_count)
  INTO result_disposition;
 SELECT f.retryable,f.retry_at FROM public.bot_message_handler_failures f
  WHERE (f.bot_id,f.chain_id,f.route_index,f.attempt_no)=
   (NEW.bot_id,NEW.chain_id,OLD.current_index,OLD.attempt_count)
  INTO failure_retryable,failure_retry_at;
 -- One Pass advances exactly one item and returns to the fair queue or completes.
 IF OLD.status='PENDING' AND OLD.claim_actor_id IS NOT NULL AND result_disposition='PASS'
  AND NEW.current_index=OLD.current_index+1 AND NEW.attempt_count=OLD.attempt_count
  AND NEW.claim_actor_id IS NULL AND NEW.last_error_code IS NULL
  AND NEW.next_attempt_at IS NOT DISTINCT FROM OLD.next_attempt_at
  AND ((NEW.status='PENDING' AND NEW.current_index<NEW.route_count AND NEW.completed_at IS NULL)
    AND NEW.terminal_reason IS NULL
    OR (NEW.status='PASSED' AND NEW.current_index=NEW.route_count
        AND NEW.terminal_reason='ALL_PASSED' AND NEW.completed_at IS NOT NULL)) THEN
  RETURN NEW;
 END IF;
 -- Consumed and Reply terminate after the current item.
 IF OLD.status='PENDING' AND OLD.claim_actor_id IS NOT NULL
  AND ((result_disposition='CONSUMED' AND NEW.status='CONSUMED')
    OR (result_disposition='REPLY' AND NEW.status='REPLIED'))
  AND NEW.current_index=OLD.current_index+1
  AND NEW.attempt_count=OLD.attempt_count AND NEW.claim_actor_id IS NULL
  AND NEW.next_attempt_at IS NOT DISTINCT FROM OLD.next_attempt_at
  AND NEW.terminal_reason=result_disposition AND NEW.completed_at IS NOT NULL THEN
  RETURN NEW;
 END IF;
 -- A retryable failure releases the claim without changing the item index.
 IF OLD.status='PENDING' AND OLD.claim_actor_id IS NOT NULL AND failure_retryable
  AND NEW.status='PENDING' AND NEW.current_index=OLD.current_index
  AND NEW.attempt_count=OLD.attempt_count AND NEW.claim_actor_id IS NULL
  AND NEW.last_error_code IS NOT NULL AND NEW.next_attempt_at=failure_retry_at
  AND NEW.terminal_reason IS NOT DISTINCT FROM OLD.terminal_reason
  AND NEW.completed_at IS NULL THEN
  RETURN NEW;
 END IF;
 -- A terminal failure closes the chain and never falls back to Agent.
 IF OLD.status='PENDING' AND OLD.claim_actor_id IS NOT NULL AND failure_retryable=false
  AND NEW.status='FAILED' AND NEW.current_index=OLD.current_index
  AND NEW.attempt_count=OLD.attempt_count AND NEW.claim_actor_id IS NULL
  AND NEW.last_error_code IS NOT NULL AND NEW.terminal_reason=NEW.last_error_code
  AND NEW.next_attempt_at IS NOT DISTINCT FROM OLD.next_attempt_at
  AND NEW.completed_at IS NOT NULL THEN
  RETURN NEW;
 END IF;
 RAISE EXCEPTION 'MESSAGE_HANDLER_CHAIN_TRANSITION_INVALID' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION public.forest_message_handler_admission_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.event_bytes<=0
  OR (NEW.route_count=0 AND NEW.admission_limit_revision IS NOT NULL)
  OR (NEW.admission_exempt AND NEW.status<>'PENDING')
  OR (NEW.status='FAILED' AND NEW.terminal_reason NOT IN (
   'MESSAGE_HANDLER_LIMITS_UNAVAILABLE','MESSAGE_HANDLER_EVENT_TOO_LARGE',
   'MESSAGE_HANDLER_CHAIN_TOO_LARGE','MESSAGE_HANDLER_BOT_QUEUE_FULL',
   'MESSAGE_HANDLER_BINDING_QUEUE_FULL','MESSAGE_HANDLER_PRINCIPAL_QUEUE_FULL',
   'MESSAGE_HANDLER_RATE_LIMITED')) THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_ADMISSION_PROOF_INVALID' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_message_handler_admission_guard
 BEFORE INSERT ON public.bot_message_handler_chains
 FOR EACH ROW EXECUTE FUNCTION public.forest_message_handler_admission_guard();

ALTER TABLE public.bot_conversation_notices
 DROP CONSTRAINT bot_conversation_notices_kind_check;
ALTER TABLE public.bot_conversation_notices
 ADD CONSTRAINT bot_conversation_notices_kind_check CHECK(kind IN (
  'BUSINESS_COMMAND_DENIED','BUSINESS_COMMAND_FAILED','BUSINESS_CLASSIFICATION_FAILED',
  'MESSAGE_HANDLER_REJECTED'
 ));
DO $$
DECLARE selected_name text;
BEGIN
 SELECT conname INTO selected_name FROM pg_constraint
  WHERE conrelid='public.bot_conversation_notices'::regclass AND contype='c'
   AND pg_get_constraintdef(oid) LIKE '%request_id IS NULL%kind%'
 LIMIT 1;
 IF selected_name IS NULL THEN
  RAISE EXCEPTION 'CONVERSATION_NOTICE_REQUEST_CONSTRAINT_MISSING' USING ERRCODE='55000';
 END IF;
 EXECUTE format('ALTER TABLE public.bot_conversation_notices DROP CONSTRAINT %I',selected_name);
END;
$$;
ALTER TABLE public.bot_conversation_notices
 ADD CONSTRAINT bot_conversation_notices_request_kind_check_v2 CHECK(
  (request_id IS NULL)=(kind IN ('BUSINESS_CLASSIFICATION_FAILED','MESSAGE_HANDLER_REJECTED'))
 );

CREATE FUNCTION public.forest_message_handler_rejection_notice_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE chain record;
BEGIN
 IF NEW.kind<>'MESSAGE_HANDLER_REJECTED' THEN RETURN NEW; END IF;
 SELECT * INTO chain FROM public.bot_message_handler_chains
  WHERE bot_id=NEW.bot_id AND event_id=NEW.event_id;
 IF chain.bot_id IS NULL OR chain.status<>'FAILED'
  OR chain.conversation_id<>NEW.conversation_id OR chain.ingress_seq<>NEW.ingress_seq
  OR chain.input_payload_id<>NEW.input_payload_id
  OR chain.terminal_reason<>NEW.error_code THEN
  RAISE EXCEPTION 'CONVERSATION_NOTICE_PROOF_INVALID' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_message_handler_rejection_notice_guard
 BEFORE INSERT ON public.bot_conversation_notices
 FOR EACH ROW EXECUTE FUNCTION public.forest_message_handler_rejection_notice_guard();


CREATE OR REPLACE FUNCTION public.forest_conversation_notice_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE inbox record; request record; conversation record; chain record;
BEGIN
 IF TG_OP='DELETE' THEN
  RAISE EXCEPTION 'CONVERSATION_NOTICE_IMMUTABLE' USING ERRCODE='55000';
 END IF;
 IF TG_OP='UPDATE' THEN
  IF (to_jsonb(NEW)-ARRAY['status','completed_at','claim_actor_id','claim_actor_epoch',
       'claim_host_incarnation','claim_input_batch_id']) IS DISTINCT FROM
     (to_jsonb(OLD)-ARRAY['status','completed_at','claim_actor_id','claim_actor_epoch',
       'claim_host_incarnation','claim_input_batch_id'])
   OR OLD.status IN ('COMPLETED','CANCELLED')
   OR NOT ((OLD.status='PENDING' AND NEW.status IN ('CLAIMED','CANCELLED'))
        OR (OLD.status='CLAIMED' AND NEW.status IN ('CLAIMED','COMPLETED','CANCELLED'))) THEN
   RAISE EXCEPTION 'CONVERSATION_NOTICE_IMMUTABLE' USING ERRCODE='55000';
  END IF;
  IF NEW.status IN ('CLAIMED','COMPLETED') THEN
   SELECT * INTO conversation FROM public.bot_conversations
    WHERE bot_id=NEW.bot_id AND conversation_id=NEW.conversation_id;
   IF conversation.bot_id IS NULL OR
      ROW(conversation.actor_id,conversation.actor_epoch,conversation.actor_host_incarnation)
      IS DISTINCT FROM
      ROW(NEW.claim_actor_id,NEW.claim_actor_epoch,NEW.claim_host_incarnation) THEN
    RAISE EXCEPTION 'CONVERSATION_NOTICE_ACTOR_FENCE' USING ERRCODE='55000';
   END IF;
  END IF;
  IF OLD.status='CLAIMED' AND NEW.status='COMPLETED' AND NOT EXISTS(
      SELECT 1 FROM public.bot_conversation_notice_results r
       WHERE r.bot_id=NEW.bot_id AND r.notice_id=NEW.notice_id) THEN
   RAISE EXCEPTION 'CONVERSATION_NOTICE_RESULT_REQUIRED' USING ERRCODE='55000';
  END IF;
  RETURN NEW;
 END IF;
 SELECT * INTO inbox FROM public.bot_message_inbox
  WHERE bot_id=NEW.bot_id AND event_id=NEW.event_id;
 IF inbox.bot_id IS NULL
  OR ROW(inbox.conversation_id,inbox.ingress_seq,inbox.payload_id) IS DISTINCT FROM
     ROW(NEW.conversation_id,NEW.ingress_seq,NEW.input_payload_id) THEN
  RAISE EXCEPTION 'CONVERSATION_NOTICE_PROOF_INVALID' USING ERRCODE='55000';
 END IF;
 IF NEW.kind='BUSINESS_CLASSIFICATION_FAILED' THEN
  IF inbox.trigger_decision<>'PENDING' OR NOT inbox.business_classification_pending THEN
   RAISE EXCEPTION 'CONVERSATION_NOTICE_PROOF_INVALID' USING ERRCODE='55000';
  END IF;
 ELSIF NEW.kind='MESSAGE_HANDLER_REJECTED' THEN
  SELECT * INTO chain FROM public.bot_message_handler_chains
   WHERE bot_id=NEW.bot_id AND event_id=NEW.event_id;
  IF chain.bot_id IS NULL OR chain.status<>'FAILED'
   OR chain.conversation_id<>NEW.conversation_id OR chain.ingress_seq<>NEW.ingress_seq
   OR chain.input_payload_id<>NEW.input_payload_id
   OR chain.terminal_reason<>NEW.error_code THEN
   RAISE EXCEPTION 'CONVERSATION_NOTICE_PROOF_INVALID' USING ERRCODE='55000';
  END IF;
 ELSE
  SELECT * INTO request FROM public.bot_business_command_requests
   WHERE bot_id=NEW.bot_id AND request_id=NEW.request_id;
  IF request.bot_id IS NULL OR request.event_id<>NEW.event_id
   OR (NEW.kind='BUSINESS_COMMAND_DENIED' AND
       (request.status<>'DENIED' OR request.terminal_reason<>NEW.error_code))
   OR (NEW.kind='BUSINESS_COMMAND_FAILED' AND
       (request.status<>'FAILED' OR request.terminal_reason<>NEW.error_code)) THEN
   RAISE EXCEPTION 'CONVERSATION_NOTICE_PROOF_INVALID' USING ERRCODE='55000';
  END IF;
 END IF;
 RETURN NEW;
END;
$$;
