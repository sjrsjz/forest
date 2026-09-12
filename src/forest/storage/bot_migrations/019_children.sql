ALTER TABLE public.bot_agent_tools DROP CONSTRAINT bot_agent_tools_status_check;
ALTER TABLE public.bot_agent_tools ADD CHECK(status IN ('PROPOSED','WAIT_APPROVAL','WAIT_CHILD','DISPATCHING','SUCCEEDED','FAILED','NOT_SENT','UNKNOWN','CANCELLED'));
CREATE TABLE public.bot_agent_tree_budgets (
    bot_id text NOT NULL, run_id text NOT NULL, reserved_tokens bigint NOT NULL DEFAULT 0 CHECK(reserved_tokens>=0),
    charged_tokens bigint NOT NULL DEFAULT 0 CHECK(charged_tokens>=0),
    PRIMARY KEY(bot_id,run_id), FOREIGN KEY(bot_id,run_id) REFERENCES public.bot_agent_runs(bot_id,run_id)
);
CREATE TABLE public.bot_agent_children (
    bot_id text NOT NULL, child_id text NOT NULL, parent_run_id text NOT NULL, parent_step_id text NOT NULL,
    tool_call_id text NOT NULL, parent_input_revision bigint NOT NULL, parent_snapshot_digest text NOT NULL,
    authority_trigger_id text NOT NULL, principal_ref text NOT NULL, source_unit_ids jsonb NOT NULL,
    snapshot jsonb NOT NULL, snapshot_digest text NOT NULL, spec jsonb NOT NULL,
    status text NOT NULL DEFAULT 'QUEUED' CHECK(status IN ('QUEUED','RUNNING','RETRY','RESULT_READY','UNKNOWN','FAILED','CANCELLED','ACCEPTED')),
    owner_id text, lease_epoch bigint NOT NULL DEFAULT 0, lease_until timestamptz,
    host_incarnation text, host_owner_epoch bigint, policy_revision bigint, maintenance_epoch bigint, storage_generation bigint,
    attempt_id text, attempt_count integer NOT NULL DEFAULT 0,
    result jsonb, result_digest text, error_code text, accepted_checkpoint bigint,
    deadline_at timestamptz NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,child_id), UNIQUE(bot_id,parent_run_id,tool_call_id),
    FOREIGN KEY(bot_id,parent_step_id) REFERENCES public.bot_agent_steps(bot_id,logical_step_id),
    FOREIGN KEY(bot_id,parent_run_id,tool_call_id) REFERENCES public.bot_agent_tools(bot_id,run_id,tool_call_id)
);
CREATE INDEX bot_child_pending ON public.bot_agent_children(bot_id,status,created_at);
CREATE TABLE public.bot_child_model_attempts (
    bot_id text NOT NULL, attempt_id text NOT NULL, child_id text NOT NULL,
    request_id text NOT NULL, request_body bytea NOT NULL, wire_digest text NOT NULL,
    manifest jsonb NOT NULL, metadata jsonb NOT NULL, owner_id text NOT NULL, lease_epoch bigint NOT NULL,
    host_incarnation text NOT NULL, host_owner_epoch bigint NOT NULL,
    status text NOT NULL CHECK(status IN ('PERMITTED','DISPATCHING','RESPONDED','NOT_SENT','UNKNOWN')),
    result jsonb, result_digest text, budget_day date NOT NULL, reserved_tokens bigint NOT NULL CHECK(reserved_tokens>=0),
    charged_tokens bigint CHECK(charged_tokens>=0), settled boolean NOT NULL DEFAULT false,
    deadline_at timestamptz NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,attempt_id), FOREIGN KEY(bot_id,child_id) REFERENCES public.bot_agent_children(bot_id,child_id),
    CHECK(wire_digest='sha256:' || encode(sha256(request_body),'hex'))
);
CREATE TRIGGER bot_child_identity BEFORE UPDATE OR DELETE ON public.bot_agent_children
    FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields('{status,owner_id,lease_epoch,lease_until,host_incarnation,host_owner_epoch,policy_revision,maintenance_epoch,storage_generation,attempt_id,attempt_count,result,result_digest,error_code,accepted_checkpoint}');
CREATE TRIGGER bot_child_attempt_identity BEFORE UPDATE OR DELETE ON public.bot_child_model_attempts
    FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields('{status,result,result_digest,charged_tokens,settled}');
CREATE FUNCTION public.forest_child_observation_once() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.result_digest IS NOT NULL AND (OLD.result,OLD.result_digest) IS DISTINCT FROM (NEW.result,NEW.result_digest) THEN
        RAISE EXCEPTION 'Child model observation is immutable' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER bot_child_result_once BEFORE UPDATE ON public.bot_agent_children FOR EACH ROW EXECUTE FUNCTION public.forest_child_observation_once();
CREATE TRIGGER bot_child_attempt_result_once BEFORE UPDATE ON public.bot_child_model_attempts FOR EACH ROW EXECUTE FUNCTION public.forest_child_observation_once();
