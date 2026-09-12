CREATE TABLE public.bot_web_issuers (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    issuer_id text NOT NULL,
    key_revision bigint NOT NULL,
    public_key text NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    actions jsonb NOT NULL,
    PRIMARY KEY (bot_id, issuer_id)
);
CREATE TABLE public.bot_config_heads (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    component_id text NOT NULL,
    desired_revision bigint NOT NULL DEFAULT 0,
    effective_revision bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (bot_id, component_id),
    CHECK (effective_revision BETWEEN 0 AND desired_revision)
);
CREATE TABLE public.bot_config_plans (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    plan_id text NOT NULL,
    component_id text NOT NULL,
    subject_id text NOT NULL,
    issuer_id text NOT NULL,
    expected_revision bigint NOT NULL,
    values jsonb NOT NULL,
    plan_digest text NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (bot_id, plan_id)
);
CREATE TABLE public.bot_web_operations (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    operation_id text NOT NULL,
    action text NOT NULL,
    input_digest text NOT NULL,
    subject_id text NOT NULL,
    issuer_id text NOT NULL,
    ticket_basis jsonb NOT NULL,
    result jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, operation_id)
);
CREATE TABLE public.bot_config_revisions (
    bot_id text NOT NULL,
    component_id text NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    previous_revision bigint NOT NULL CHECK (previous_revision = revision - 1),
    values jsonb NOT NULL,
    policy_revision bigint NOT NULL,
    plan_id text NOT NULL,
    operation_id text NOT NULL,
    PRIMARY KEY (bot_id, component_id, revision),
    FOREIGN KEY (bot_id, plan_id) REFERENCES public.bot_config_plans(bot_id, plan_id),
    FOREIGN KEY (bot_id, operation_id) REFERENCES public.bot_web_operations(bot_id, operation_id)
);
CREATE TABLE public.bot_config_activations (
    bot_id text NOT NULL,
    operation_id text NOT NULL,
    component_id text NOT NULL,
    revision bigint NOT NULL,
    host_incarnation text NOT NULL,
    PRIMARY KEY (bot_id, operation_id),
    FOREIGN KEY (bot_id, component_id, revision)
        REFERENCES public.bot_config_revisions(bot_id, component_id, revision)
);
CREATE TRIGGER bot_config_plans_immutable BEFORE UPDATE OR DELETE ON public.bot_config_plans
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_config_revisions_immutable BEFORE UPDATE OR DELETE ON public.bot_config_revisions
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_config_activations_immutable BEFORE UPDATE OR DELETE ON public.bot_config_activations
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_web_operations_immutable BEFORE UPDATE OR DELETE ON public.bot_web_operations
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

-- The kernel already holds resource -> route -> runtime -> host -> call locks.
-- Acquire only the configuration business head here, never an earlier lock layer.
CREATE FUNCTION public.forest_publish_configuration() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE activation record; desired bigint; cfg_revision record; candidate record;
BEGIN
    IF NEW.phase <> 'COMMITTED' OR OLD.phase = 'COMMITTED' THEN RETURN NEW; END IF;
    SELECT * INTO activation FROM public.bot_config_activations
        WHERE bot_id=NEW.bot_id AND operation_id=NEW.operation_id;
    IF NOT FOUND THEN RETURN NEW; END IF;
    SELECT desired_revision INTO desired FROM public.bot_config_heads
        WHERE bot_id=NEW.bot_id AND component_id=activation.component_id FOR UPDATE;
    SELECT * INTO cfg_revision FROM public.bot_config_revisions
        WHERE bot_id=NEW.bot_id AND component_id=activation.component_id
        AND revision=activation.revision;
    SELECT * INTO candidate FROM public.bot_generations
        WHERE bot_id=NEW.bot_id AND generation_id=NEW.candidate_generation_id;
    -- Missing fields must reject publication: ordinary <> would propagate SQL
    -- NULL and let a malformed desired revision pass this authority check.
    IF desired IS DISTINCT FROM activation.revision
        OR activation.component_id IS DISTINCT FROM NEW.component_id
        OR activation.host_incarnation IS DISTINCT FROM NEW.host_incarnation
        OR candidate.package_digest IS DISTINCT FROM (cfg_revision.values->>'package_digest')
        OR candidate.config IS DISTINCT FROM (cfg_revision.values->'config')
        OR candidate.capability_grants IS DISTINCT FROM (cfg_revision.values->'grants')
        OR cfg_revision.policy_revision IS DISTINCT FROM (SELECT policy_revision FROM public.bot_runtime_state
                                       WHERE bot_id=NEW.bot_id) THEN
        RAISE EXCEPTION 'CONFIGURATION_SUPERSEDED' USING ERRCODE='P0001';
    END IF;
    UPDATE public.bot_config_heads SET effective_revision=activation.revision
        WHERE bot_id=NEW.bot_id AND component_id=activation.component_id;
    INSERT INTO public.bot_outbox_events(bot_id,event_id,topic,operation_id,payload)
        VALUES (NEW.bot_id,'config:'||NEW.operation_id,'config.effective',NEW.operation_id,
                jsonb_build_object('component_id',NEW.component_id,'revision',activation.revision));
    RETURN NEW;
END;
$$;
CREATE TRIGGER bot_configuration_publication AFTER UPDATE ON public.bot_lifecycle_operations
    FOR EACH ROW EXECUTE FUNCTION public.forest_publish_configuration();
