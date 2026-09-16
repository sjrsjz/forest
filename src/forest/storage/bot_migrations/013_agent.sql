CREATE TABLE public.bot_agent_runs (
    bot_id text NOT NULL,
    run_id text NOT NULL,
    conversation_id text NOT NULL,
    session_id text NOT NULL,
    session_epoch bigint NOT NULL,
    spec jsonb NOT NULL,
    spec_digest text NOT NULL,
    owner_epoch bigint NOT NULL DEFAULT 1,
    actor_epoch bigint NOT NULL,
    actor_host_incarnation text NOT NULL,
    checkpoint_seq bigint NOT NULL DEFAULT 0,
    next_step_seq bigint NOT NULL DEFAULT 1,
    published_step_seq bigint NOT NULL DEFAULT 0,
    current_step_id text,
    phase text NOT NULL DEFAULT 'READY_MODEL',
    input_revision bigint NOT NULL,
    selected_input_end_seq bigint NOT NULL,
    wait_classified_seq bigint,
    final_step_id text,
    final_input_revision bigint,
    final_reply_id text,
    error_code text,
    tool_iteration_count integer NOT NULL DEFAULT 0 CHECK(tool_iteration_count>=0),
    thinking_iteration_count integer NOT NULL DEFAULT 0 CHECK(thinking_iteration_count>=0),
    protocol_repair_count integer NOT NULL DEFAULT 0 CHECK(protocol_repair_count>=0),
    deadline_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,run_id),
    FOREIGN KEY (bot_id,run_id) REFERENCES public.bot_conversation_runs(bot_id,run_id),
    CHECK (phase IN ('READY_MODEL','PREPARING_MODEL','MODEL_PENDING','RETRY_MODEL','WAIT_TOOL',
      'WAIT_APPROVAL','BLOCKED_UNKNOWN','WAIT_INPUT','FINAL_READY','COMPLETED','CANCELLED','FAILED'))
);
CREATE TABLE public.bot_agent_checkpoints (
    bot_id text NOT NULL, run_id text NOT NULL, checkpoint_seq bigint NOT NULL,
    phase text NOT NULL, payload jsonb NOT NULL, checksum text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,run_id,checkpoint_seq),
    FOREIGN KEY(bot_id,run_id) REFERENCES public.bot_agent_runs(bot_id,run_id)
);
CREATE TABLE public.bot_agent_steps (
    bot_id text NOT NULL, run_id text NOT NULL, logical_step_id text NOT NULL,
    step_seq bigint NOT NULL, input_revision bigint NOT NULL,
    selected_input_end_seq bigint NOT NULL, snapshot jsonb NOT NULL, snapshot_digest text NOT NULL,
    status text NOT NULL DEFAULT 'PREPARED', model_call_epoch bigint NOT NULL DEFAULT 0,
    model_request_id text, transport_attempt_id text, output jsonb, output_digest text,
    closed_parts jsonb, published_context_revision bigint,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,logical_step_id), UNIQUE(bot_id,run_id,step_seq),
    FOREIGN KEY(bot_id,run_id) REFERENCES public.bot_agent_runs(bot_id,run_id),
    CHECK(status IN ('PREPARED','MODEL_PENDING','RETRY_MODEL','WAIT_TOOL','CLOSED','FINAL_READY','FINAL_COMMITTED','FAILED'))
);
CREATE TABLE public.bot_model_request_manifests (
    bot_id text NOT NULL, model_request_id text NOT NULL, logical_step_id text NOT NULL,
    manifest jsonb NOT NULL, manifest_digest text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,model_request_id),
    FOREIGN KEY(bot_id,logical_step_id) REFERENCES public.bot_agent_steps(bot_id,logical_step_id)
);
CREATE TABLE public.bot_model_transport_attempts (
    bot_id text NOT NULL, transport_attempt_id text NOT NULL, operation_id text NOT NULL,
    model_request_id text NOT NULL, run_id text NOT NULL, logical_step_id text NOT NULL,
    model_call_epoch bigint NOT NULL, owner_epoch bigint NOT NULL,
    actor_epoch bigint NOT NULL, host_incarnation text NOT NULL,
    wire_digest text NOT NULL, request_body bytea NOT NULL, prepared_metadata jsonb NOT NULL,
    status text NOT NULL DEFAULT 'PERMITTED',
    result jsonb, result_digest text, deadline_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,transport_attempt_id), UNIQUE(bot_id,operation_id),
    FOREIGN KEY(bot_id,model_request_id) REFERENCES public.bot_model_request_manifests(bot_id,model_request_id),
    FOREIGN KEY(bot_id,logical_step_id) REFERENCES public.bot_agent_steps(bot_id,logical_step_id),
    CHECK(status IN ('PERMITTED','DISPATCHING','RESPONDED','NOT_SENT','UNKNOWN','ABANDONED'))
);
CREATE TABLE public.bot_agent_budget_days (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id), budget_day date NOT NULL,
    reserved_tokens bigint NOT NULL DEFAULT 0 CHECK(reserved_tokens>=0),
    charged_tokens bigint NOT NULL DEFAULT 0 CHECK(charged_tokens>=0),
    PRIMARY KEY(bot_id,budget_day)
);
CREATE TABLE public.bot_agent_run_budgets (
    bot_id text NOT NULL, run_id text NOT NULL,
    reserved_tokens bigint NOT NULL DEFAULT 0 CHECK(reserved_tokens>=0),
    charged_tokens bigint NOT NULL DEFAULT 0 CHECK(charged_tokens>=0),
    PRIMARY KEY(bot_id,run_id),
    FOREIGN KEY(bot_id,run_id) REFERENCES public.bot_agent_runs(bot_id,run_id)
);
CREATE TABLE public.bot_model_budget_reservations (
    bot_id text NOT NULL, transport_attempt_id text NOT NULL, budget_day date NOT NULL,
    reserved_tokens bigint NOT NULL CHECK(reserved_tokens>=0), charged_tokens bigint,
    status text NOT NULL DEFAULT 'RESERVED' CHECK(status IN ('RESERVED','UNKNOWN','SETTLED')),
    PRIMARY KEY(bot_id,transport_attempt_id),
    FOREIGN KEY(bot_id,transport_attempt_id) REFERENCES public.bot_model_transport_attempts(bot_id,transport_attempt_id),
    FOREIGN KEY(bot_id,budget_day) REFERENCES public.bot_agent_budget_days(bot_id,budget_day)
);
CREATE TABLE public.bot_agent_tools (
    bot_id text NOT NULL, run_id text NOT NULL, logical_step_id text NOT NULL, tool_call_id text NOT NULL,
    call_index integer NOT NULL, tool_name text NOT NULL, arguments jsonb NOT NULL, arguments_digest text NOT NULL,
    operation_id text NOT NULL, authority_trigger_id text, principal_ref text,
    status text NOT NULL DEFAULT 'PROPOSED', requires_approval boolean NOT NULL DEFAULT false,
    attempt_count integer NOT NULL DEFAULT 0,
    owner_epoch bigint, actor_epoch bigint, host_incarnation text, result jsonb, result_digest text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,run_id,tool_call_id), UNIQUE(bot_id,logical_step_id,call_index), UNIQUE(bot_id,operation_id),
    FOREIGN KEY(bot_id,logical_step_id) REFERENCES public.bot_agent_steps(bot_id,logical_step_id),
    CHECK(status IN ('PROPOSED','WAIT_APPROVAL','DISPATCHING','SUCCEEDED','FAILED','NOT_SENT','UNKNOWN','CANCELLED'))
);
CREATE TABLE public.bot_agent_tool_approvals (
    bot_id text NOT NULL, approval_id text NOT NULL, run_id text NOT NULL, tool_call_id text NOT NULL,
    arguments_digest text NOT NULL, authority_trigger_id text NOT NULL, principal_ref text NOT NULL,
    policy_revision bigint NOT NULL, decision text, decided_by text, operation_id text,
    expires_at timestamptz NOT NULL, decided_at timestamptz,
    PRIMARY KEY(bot_id,approval_id), UNIQUE(bot_id,run_id,tool_call_id),
    FOREIGN KEY(bot_id,run_id,tool_call_id) REFERENCES public.bot_agent_tools(bot_id,run_id,tool_call_id),
    CHECK(decision IS NULL OR decision IN ('APPROVE','REJECT'))
);
ALTER TABLE public.bot_context_units DROP CONSTRAINT bot_context_units_kind_check;
ALTER TABLE public.bot_context_units ADD CONSTRAINT bot_context_units_kind_check CHECK(kind IN ('INPUT','REPLY','CONTROL','RUN_STEP'));
ALTER TABLE public.bot_context_units ADD COLUMN logical_step_id text, ADD COLUMN step_parts jsonb;
ALTER TABLE public.bot_context_units ADD UNIQUE(bot_id,logical_step_id);
ALTER TABLE public.bot_conversation_replies ADD COLUMN logical_step_id text;
ALTER TABLE public.bot_conversation_replies ADD UNIQUE(bot_id,logical_step_id);

CREATE TRIGGER bot_agent_checkpoints_immutable BEFORE UPDATE OR DELETE ON public.bot_agent_checkpoints
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_model_manifests_immutable BEFORE UPDATE OR DELETE ON public.bot_model_request_manifests
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE FUNCTION public.forest_agent_fixed_fields() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' OR (to_jsonb(OLD) - TG_ARGV[0]::text[]) IS DISTINCT FROM (to_jsonb(NEW) - TG_ARGV[0]::text[]) THEN
        RAISE EXCEPTION 'Forest Agent identity and frozen input are immutable' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER bot_agent_run_identity BEFORE UPDATE OR DELETE ON public.bot_agent_runs
    FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields('{owner_epoch,actor_epoch,actor_host_incarnation,checkpoint_seq,next_step_seq,published_step_seq,current_step_id,phase,input_revision,selected_input_end_seq,wait_classified_seq,final_step_id,final_input_revision,final_reply_id,error_code,tool_iteration_count,thinking_iteration_count,protocol_repair_count}');
CREATE TRIGGER bot_agent_step_identity BEFORE UPDATE OR DELETE ON public.bot_agent_steps
    FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields('{status,model_call_epoch,model_request_id,transport_attempt_id,output,output_digest,closed_parts,published_context_revision}');
CREATE TRIGGER bot_model_attempt_identity BEFORE UPDATE OR DELETE ON public.bot_model_transport_attempts
    FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields('{status,result,result_digest}');
CREATE TRIGGER bot_agent_tool_identity BEFORE UPDATE OR DELETE ON public.bot_agent_tools
    FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields('{authority_trigger_id,principal_ref,status,requires_approval,attempt_count,owner_epoch,actor_epoch,host_incarnation,result,result_digest}');
CREATE TRIGGER bot_context_unit_identity BEFORE UPDATE OR DELETE ON public.bot_context_units
    FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields('{visible}');
CREATE TRIGGER bot_agent_approval_identity BEFORE UPDATE OR DELETE ON public.bot_agent_tool_approvals
    FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields('{decision,decided_by,operation_id,decided_at}');
CREATE FUNCTION public.forest_agent_observation_once() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE previous jsonb := to_jsonb(OLD); replacement jsonb := to_jsonb(NEW);
BEGIN
    IF TG_TABLE_NAME='bot_model_transport_attempts' AND previous->>'result_digest' IS NOT NULL
       AND (previous->'result',previous->'result_digest',previous->'status') IS DISTINCT FROM (replacement->'result',replacement->'result_digest',replacement->'status') THEN
        RAISE EXCEPTION 'Model transport observation is immutable' USING ERRCODE='55000';
    ELSIF TG_TABLE_NAME='bot_agent_tool_approvals' AND previous->>'decision' IS NOT NULL AND previous IS DISTINCT FROM replacement THEN
        RAISE EXCEPTION 'Tool approval decision is immutable' USING ERRCODE='55000';
    ELSIF TG_TABLE_NAME='bot_agent_steps' AND previous->>'output_digest' IS NOT NULL
       AND (previous->'output',previous->'output_digest') IS DISTINCT FROM (replacement->'output',replacement->'output_digest') THEN
        RAISE EXCEPTION 'Model step output is immutable' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER bot_model_observation_once BEFORE UPDATE ON public.bot_model_transport_attempts
    FOR EACH ROW EXECUTE FUNCTION public.forest_agent_observation_once();
CREATE TRIGGER bot_agent_approval_once BEFORE UPDATE ON public.bot_agent_tool_approvals
    FOR EACH ROW EXECUTE FUNCTION public.forest_agent_observation_once();
CREATE TRIGGER bot_agent_step_output_once BEFORE UPDATE ON public.bot_agent_steps
    FOR EACH ROW EXECUTE FUNCTION public.forest_agent_observation_once();
CREATE TABLE public.bot_agent_tool_observations (
    observation_id bigserial PRIMARY KEY, bot_id text NOT NULL, run_id text NOT NULL,
    tool_call_id text NOT NULL, operation_id text NOT NULL, attempt_count integer NOT NULL,
    status text NOT NULL, observation jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY(bot_id,run_id,tool_call_id) REFERENCES public.bot_agent_tools(bot_id,run_id,tool_call_id)
);
CREATE TRIGGER bot_agent_tool_observations_immutable BEFORE UPDATE OR DELETE ON public.bot_agent_tool_observations
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE FUNCTION public.forest_agent_tool_observe() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (OLD.status,OLD.attempt_count,OLD.result_digest) IS DISTINCT FROM (NEW.status,NEW.attempt_count,NEW.result_digest) THEN
        INSERT INTO public.bot_agent_tool_observations(bot_id,run_id,tool_call_id,operation_id,attempt_count,status,observation)
        VALUES(NEW.bot_id,NEW.run_id,NEW.tool_call_id,NEW.operation_id,NEW.attempt_count,NEW.status,to_jsonb(NEW));
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER bot_agent_tool_observe AFTER UPDATE ON public.bot_agent_tools
    FOR EACH ROW EXECUTE FUNCTION public.forest_agent_tool_observe();
