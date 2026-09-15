-- Per-Binding business-command policy is independently versioned. Command
-- declaration metadata remains in the route-bound registry; user selections
-- live only in removable configuration payloads.
ALTER TABLE public.bot_control_value_sources
 DROP CONSTRAINT bot_control_value_sources_family_check;
ALTER TABLE public.bot_control_value_sources
 ADD CONSTRAINT bot_control_value_sources_family_check CHECK(family IN (
  'config','agent.config','memory.config','binding','job','command.policy'
));

-- Existing Bots retain their issuer identity across upgrades. Admit the new
-- action names deterministically so the upgraded Web authority can use them.
UPDATE public.bot_web_issuers i SET actions=(
 SELECT jsonb_agg(action ORDER BY action) FROM (
  SELECT DISTINCT jsonb_array_elements_text(
   i.actions || '["command.policy.read","command.policy.plan","command.policy.apply"]'::jsonb
  ) AS action
 ) expanded
);

CREATE TABLE public.bot_command_policy_heads (
 bot_id text NOT NULL,
 binding_id text NOT NULL,
 command_id text NOT NULL,
 resource_id text NOT NULL CHECK(resource_id ~ '^sha256:[a-f0-9]{64}$'),
 revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,binding_id,command_id),
 UNIQUE(bot_id,resource_id),
 FOREIGN KEY(bot_id,binding_id) REFERENCES public.bot_bindings(bot_id,binding_id),
 FOREIGN KEY(bot_id,command_id) REFERENCES public.bot_command_heads(bot_id,command_id)
);

CREATE TABLE public.bot_command_policy_plans (
 bot_id text NOT NULL,
 binding_id text NOT NULL,
 command_id text NOT NULL,
 resource_id text NOT NULL,
 plan_id text NOT NULL,
 subject_id text NOT NULL,
 issuer_id text NOT NULL,
 expected_revision bigint NOT NULL CHECK(expected_revision>=0),
 command_spec_digest text NOT NULL CHECK(command_spec_digest ~ '^sha256:[a-f0-9]{64}$'),
 policy_mode text NOT NULL CHECK(policy_mode IN ('DISABLED','EVERYONE','ADMINS','USER_SET')),
 user_count integer NOT NULL CHECK(user_count BETWEEN 0 AND 64),
 plan_digest text NOT NULL CHECK(plan_digest ~ '^sha256:[a-f0-9]{64}$'),
 expires_at timestamptz NOT NULL,
 values_payload_id text NOT NULL,
 values_payload_digest text NOT NULL CHECK(values_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,plan_id),
 FOREIGN KEY(bot_id,binding_id,command_id)
  REFERENCES public.bot_command_policy_heads(bot_id,binding_id,command_id),
 FOREIGN KEY(bot_id,resource_id)
  REFERENCES public.bot_command_policy_heads(bot_id,resource_id),
 FOREIGN KEY(bot_id,values_payload_id)
  REFERENCES public.bot_payload_headers(bot_id,payload_id)
);

CREATE TABLE public.bot_command_policy_revisions (
 bot_id text NOT NULL,
 binding_id text NOT NULL,
 command_id text NOT NULL,
 resource_id text NOT NULL,
 revision bigint NOT NULL CHECK(revision>0),
 previous_revision bigint NOT NULL CHECK(previous_revision=revision-1),
 command_spec_digest text NOT NULL CHECK(command_spec_digest ~ '^sha256:[a-f0-9]{64}$'),
 policy_mode text NOT NULL CHECK(policy_mode IN ('DISABLED','EVERYONE','ADMINS','USER_SET')),
 user_count integer NOT NULL CHECK(user_count BETWEEN 0 AND 64),
 policy_revision bigint NOT NULL CHECK(policy_revision>0),
 plan_id text NOT NULL,
 operation_id text NOT NULL,
 values_payload_id text NOT NULL,
 values_payload_digest text NOT NULL CHECK(values_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,binding_id,command_id,revision),
 UNIQUE(bot_id,operation_id),
 FOREIGN KEY(bot_id,binding_id,command_id)
  REFERENCES public.bot_command_policy_heads(bot_id,binding_id,command_id),
 FOREIGN KEY(bot_id,plan_id) REFERENCES public.bot_command_policy_plans(bot_id,plan_id),
 FOREIGN KEY(bot_id,operation_id) REFERENCES public.bot_web_operations(bot_id,operation_id),
 FOREIGN KEY(bot_id,values_payload_id)
  REFERENCES public.bot_payload_headers(bot_id,payload_id)
);

CREATE TRIGGER bot_command_policy_plan_immutable
 BEFORE UPDATE OR DELETE ON public.bot_command_policy_plans
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_command_policy_revision_immutable
 BEFORE UPDATE OR DELETE ON public.bot_command_policy_revisions
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE FUNCTION public.forest_command_policy_head_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  RAISE EXCEPTION 'COMMAND_POLICY_HEAD_DELETE_FORBIDDEN' USING ERRCODE='55000';
 END IF;
 IF ROW(NEW.bot_id,NEW.binding_id,NEW.command_id,NEW.resource_id)
    IS DISTINCT FROM ROW(OLD.bot_id,OLD.binding_id,OLD.command_id,OLD.resource_id)
  OR NEW.revision<>OLD.revision+1
  OR NOT EXISTS(
   SELECT 1 FROM public.bot_command_policy_revisions r
   WHERE (r.bot_id,r.binding_id,r.command_id,r.revision,r.previous_revision)=
    (NEW.bot_id,NEW.binding_id,NEW.command_id,NEW.revision,OLD.revision)
  ) THEN
  RAISE EXCEPTION 'COMMAND_POLICY_HEAD_CONFLICT' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_command_policy_head_guard
 BEFORE UPDATE OR DELETE ON public.bot_command_policy_heads
 FOR EACH ROW EXECUTE FUNCTION public.forest_command_policy_head_guard();

-- Validate the new payload family without weakening the frozen validators used
-- by older configuration families.
CREATE FUNCTION public.forest_command_policy_payload_values(
 selected_bot text, selected_payload text, selected_digest text,
 selected_kind text, selected_resource text, selected_identity text)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE h record; envelope jsonb; expected_owner text; expected_payload text;
 identity_json text; actual_parents jsonb;
BEGIN
 IF selected_kind NOT IN ('PLAN','REVISION')
  OR selected_resource IS NULL OR selected_resource !~ '^sha256:[a-f0-9]{64}$'
  OR selected_identity IS NULL OR selected_identity='' OR octet_length(selected_identity)>4096
  OR selected_identity ~ '[\x01-\x1f\x7f]'
  OR (selected_kind='REVISION' AND selected_identity !~ '^(0|[1-9][0-9]*)$')
  OR selected_digest IS NULL OR selected_digest !~ '^sha256:[a-f0-9]{64}$'
  OR selected_bot IS NULL OR selected_payload IS NULL THEN
  RAISE EXCEPTION 'CONFIG_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 identity_json := '["command.policy",' || to_jsonb(selected_kind)::text || ','
  || to_jsonb(selected_resource)::text || ',' || to_jsonb(selected_identity)::text || ']';
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
  OR envelope->>'family'<>'command.policy' OR envelope->>'kind'<>selected_kind
  OR envelope->>'resource'<>selected_resource OR envelope->>'identity'<>selected_identity THEN
  RAISE EXCEPTION 'CONFIG_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 RETURN envelope->'values';
END;
$$;

CREATE FUNCTION public.forest_command_policy_values_valid(
 selected_values jsonb, selected_mode text, selected_user_count integer)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE actual_count integer; distinct_count integer; valid_users boolean;
BEGIN
 IF jsonb_typeof(selected_values)<>'object'
  OR NOT (selected_values ?& ARRAY['mode','user_ids'])
  OR selected_values-ARRAY['mode','user_ids']<>'{}'::jsonb
  OR selected_values->>'mode' IS DISTINCT FROM selected_mode
  OR selected_mode NOT IN ('DISABLED','EVERYONE','ADMINS','USER_SET')
  OR jsonb_typeof(selected_values->'user_ids')<>'array'
  OR selected_user_count NOT BETWEEN 0 AND 64 THEN RETURN false;
 END IF;
 SELECT count(*),count(DISTINCT item),COALESCE(bool_and(
  jsonb_typeof(item)='string' AND item#>>'{}' ~ '^[1-9][0-9]{0,19}$'
 ),true) INTO actual_count,distinct_count,valid_users
 FROM jsonb_array_elements(selected_values->'user_ids') item;
 RETURN valid_users AND actual_count=selected_user_count AND distinct_count=actual_count
  AND ((selected_mode='USER_SET')=(actual_count>0));
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
  WHEN 'command.policy' THEN 'bot_command_policy_plans' END;
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

CREATE FUNCTION public.forest_command_policy_revision_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE plan record; op record; plan_values jsonb; revision_values jsonb;
 input_value jsonb; result_value jsonb;
BEGIN
 SELECT * INTO plan FROM public.bot_command_policy_plans
  WHERE bot_id=NEW.bot_id AND plan_id=NEW.plan_id;
 SELECT * INTO op FROM public.bot_web_operations
  WHERE bot_id=NEW.bot_id AND operation_id=NEW.operation_id;
 IF plan.bot_id IS NULL OR op.bot_id IS NULL OR op.action<>'command.policy.apply'
  OR ROW(NEW.binding_id,NEW.command_id,NEW.resource_id,NEW.previous_revision,
         NEW.command_spec_digest,NEW.policy_mode,NEW.user_count)
     IS DISTINCT FROM
     ROW(plan.binding_id,plan.command_id,plan.resource_id,plan.expected_revision,
         plan.command_spec_digest,plan.policy_mode,plan.user_count)
  OR ROW(plan.subject_id,plan.issuer_id) IS DISTINCT FROM ROW(op.subject_id,op.issuer_id)
  OR (op.ticket_basis->>'policy_revision')::bigint IS DISTINCT FROM NEW.policy_revision THEN
  RAISE EXCEPTION 'COMMAND_POLICY_REVISION_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 plan_values := public.forest_command_policy_payload_values(
  NEW.bot_id,plan.values_payload_id,plan.values_payload_digest,
  'PLAN',plan.resource_id,plan.plan_id
 );
 revision_values := public.forest_command_policy_payload_values(
  NEW.bot_id,NEW.values_payload_id,NEW.values_payload_digest,
  'REVISION',NEW.resource_id,NEW.revision::text
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
  OR NOT public.forest_command_policy_values_valid(
   revision_values,NEW.policy_mode,NEW.user_count
  )
  OR input_value IS DISTINCT FROM jsonb_build_object(
   'binding_id',NEW.binding_id,'command_id',NEW.command_id,
   'expected_revision',NEW.previous_revision,'plan_id',NEW.plan_id,
   'plan_digest',plan.plan_digest
  )
  OR result_value IS DISTINCT FROM jsonb_build_object(
   'binding_id',NEW.binding_id,'command_id',NEW.command_id,
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
  RAISE EXCEPTION 'COMMAND_POLICY_REVISION_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_command_policy_revision_source_guard
 BEFORE INSERT ON public.bot_command_policy_revisions
 FOR EACH ROW EXECUTE FUNCTION public.forest_command_policy_revision_guard();
