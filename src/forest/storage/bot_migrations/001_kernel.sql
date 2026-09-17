CREATE TABLE public.forest_bot_identity (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    bot_id text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE public.bot_runtime_state (
    bot_id text PRIMARY KEY REFERENCES public.forest_bot_identity(bot_id),
    storage_generation bigint NOT NULL DEFAULT 1 CHECK (storage_generation > 0),
    status text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'SEALED')),
    maintenance_epoch bigint NOT NULL DEFAULT 1 CHECK (maintenance_epoch > 0),
    policy_revision bigint NOT NULL DEFAULT 1 CHECK (policy_revision > 0),
    data_admission boolean NOT NULL DEFAULT true,
    control_admission boolean NOT NULL DEFAULT true
);
CREATE TABLE public.bot_host_lease (
    bot_id text PRIMARY KEY REFERENCES public.forest_bot_identity(bot_id),
    host_id text,
    incarnation text,
    owner_epoch bigint NOT NULL DEFAULT 0 CHECK (owner_epoch >= 0),
    expires_at timestamptz NOT NULL DEFAULT '1970-01-01 UTC'
);
CREATE TABLE public.bot_host_incarnations (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    incarnation text NOT NULL,
    host_id text NOT NULL,
    owner_epoch bigint NOT NULL,
    PRIMARY KEY (bot_id, incarnation),
    UNIQUE (bot_id, owner_epoch)
);
CREATE TABLE public.bot_component_resources (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    component_id text NOT NULL,
    operation_id text,
    op_epoch bigint NOT NULL DEFAULT 0,
    lease_until timestamptz NOT NULL DEFAULT '1970-01-01 UTC',
    PRIMARY KEY (bot_id, component_id)
);
CREATE TABLE public.bot_generations (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    generation_id text NOT NULL,
    component_id text NOT NULL,
    package_digest text NOT NULL,
    config_digest text NOT NULL,
    config jsonb NOT NULL,
    instance_id text NOT NULL,
    worker_incarnation text NOT NULL,
    host_incarnation text NOT NULL,
    host_owner_epoch bigint NOT NULL,
    status text NOT NULL CHECK (status IN ('PREPARING', 'READY', 'ACTIVE', 'QUIESCING', 'RETIRED', 'FAILED', 'DISPOSED')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, generation_id),
    UNIQUE (bot_id, worker_incarnation)
);
CREATE TABLE public.bot_component_routes (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    component_id text NOT NULL,
    generation_id text,
    route_epoch bigint NOT NULL DEFAULT 0 CHECK (route_epoch >= 0),
    admission boolean NOT NULL DEFAULT false,
    PRIMARY KEY (bot_id, component_id),
    FOREIGN KEY (bot_id, generation_id) REFERENCES public.bot_generations(bot_id, generation_id)
);
CREATE TABLE public.bot_lifecycle_operations (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    operation_id text NOT NULL,
    component_id text NOT NULL,
    input_digest text NOT NULL,
    candidate_generation_id text NOT NULL,
    old_generation_id text,
    expected_route_epoch bigint NOT NULL,
    phase text NOT NULL CHECK (phase IN ('PLAN', 'FROZEN', 'COMMITTED', 'FAILED', 'ABANDONED')),
    host_incarnation text NOT NULL,
    host_owner_epoch bigint NOT NULL,
    op_epoch bigint NOT NULL,
    lease_until timestamptz NOT NULL,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, operation_id),
    FOREIGN KEY (bot_id, candidate_generation_id) REFERENCES public.bot_generations(bot_id, generation_id)
);
CREATE TABLE public.bot_calls (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    call_id text NOT NULL,
    operation_id text NOT NULL,
    component_id text NOT NULL,
    generation_id text NOT NULL,
    route_epoch bigint NOT NULL,
    worker_incarnation text NOT NULL,
    host_id text NOT NULL,
    host_incarnation text NOT NULL,
    owner_epoch bigint NOT NULL,
    storage_generation bigint NOT NULL,
    maintenance_epoch bigint NOT NULL,
    policy_revision bigint NOT NULL,
    cancel_epoch bigint NOT NULL DEFAULT 0,
    deadline_at timestamptz NOT NULL,
    input_digest text NOT NULL,
    causal_root_kind text NOT NULL CHECK (causal_root_kind IN (
        'KERNEL_OPERATION','BUSINESS_COMMAND_REQUEST','MESSAGE_HANDLER_CHAIN','CONVERSATION_RUN'
    )),
    causal_root_id text NOT NULL,
    status text NOT NULL DEFAULT 'ADMITTED' CHECK (status IN ('ADMITTED', 'COMPLETED', 'FAILED', 'CANCELLED')),
    result jsonb,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, call_id),
    UNIQUE (bot_id, operation_id),
    FOREIGN KEY (bot_id, generation_id) REFERENCES public.bot_generations(bot_id, generation_id)
);
CREATE INDEX bot_calls_drain ON public.bot_calls(bot_id, component_id, status);
CREATE TABLE public.bot_component_state (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    component_id text NOT NULL,
    key text NOT NULL,
    revision bigint NOT NULL CHECK (revision >= 0),
    value jsonb,
    PRIMARY KEY (bot_id, component_id, key)
);
CREATE TABLE public.bot_state_steps (
    bot_id text NOT NULL,
    call_id text NOT NULL,
    operation_id text NOT NULL,
    input_digest text NOT NULL,
    payload_digest text NOT NULL,
    receipt jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, call_id, operation_id),
    FOREIGN KEY (bot_id, call_id) REFERENCES public.bot_calls(bot_id, call_id)
);
CREATE TABLE public.bot_state_revisions (
    bot_id text NOT NULL,
    component_id text NOT NULL,
    key text NOT NULL,
    revision bigint NOT NULL,
    previous_revision bigint NOT NULL CHECK (previous_revision = revision - 1),
    value jsonb NOT NULL,
    call_id text NOT NULL,
    operation_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, component_id, key, revision),
    FOREIGN KEY (bot_id, call_id, operation_id) REFERENCES public.bot_state_steps(bot_id, call_id, operation_id)
);
CREATE TABLE public.bot_audit_records (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    audit_id text NOT NULL,
    action text NOT NULL,
    operation_id text,
    details jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, audit_id)
);
CREATE TABLE public.bot_outbox_events (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    event_id text NOT NULL,
    topic text NOT NULL,
    operation_id text,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    delivered_at timestamptz,
    PRIMARY KEY (bot_id, event_id)
);
CREATE FUNCTION public.forest_bot_reject_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Forest immutable Bot fact cannot be changed' USING ERRCODE = '55000';
END;
$$;
CREATE TRIGGER bot_identity_immutable BEFORE UPDATE OR DELETE ON public.forest_bot_identity
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_incarnations_immutable BEFORE UPDATE OR DELETE ON public.bot_host_incarnations
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_state_steps_immutable BEFORE UPDATE OR DELETE ON public.bot_state_steps
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_state_revisions_immutable BEFORE UPDATE OR DELETE ON public.bot_state_revisions
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_audit_immutable BEFORE UPDATE OR DELETE ON public.bot_audit_records
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE FUNCTION public.forest_generation_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(OLD.bot_id, OLD.generation_id, OLD.component_id, OLD.package_digest,
        OLD.config_digest, OLD.config, OLD.instance_id, OLD.worker_incarnation,
        OLD.host_incarnation, OLD.host_owner_epoch, OLD.created_at)
      IS DISTINCT FROM ROW(NEW.bot_id, NEW.generation_id, NEW.component_id, NEW.package_digest,
        NEW.config_digest, NEW.config, NEW.instance_id, NEW.worker_incarnation,
        NEW.host_incarnation, NEW.host_owner_epoch, NEW.created_at) THEN
        RAISE EXCEPTION 'Forest generation identity is immutable' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER bot_generation_identity BEFORE UPDATE ON public.bot_generations
    FOR EACH ROW EXECUTE FUNCTION public.forest_generation_identity();
