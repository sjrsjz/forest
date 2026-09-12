-- Local development uses host authority, never fabricated Web tickets. Accepted
-- edits and immutable activations fence publication in the Bot transaction.
CREATE TABLE public.bot_dev_watch_heads (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    watch_id text NOT NULL,
    source_identity text NOT NULL,
    component_id text,
    config jsonb NOT NULL,
    grants jsonb NOT NULL,
    policy_revision bigint NOT NULL,
    desired_sequence bigint NOT NULL DEFAULT 0,
    desired_request_id text,
    good_request_id text,
    PRIMARY KEY (bot_id, watch_id),
    UNIQUE (bot_id, source_identity),
    UNIQUE (bot_id, component_id)
);
CREATE TABLE public.bot_dev_watch_requests (
    bot_id text NOT NULL,
    watch_id text NOT NULL,
    request_id text NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    source_digest text NOT NULL,
    host_incarnation text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, request_id),
    UNIQUE (bot_id, watch_id, sequence),
    FOREIGN KEY (bot_id, watch_id) REFERENCES public.bot_dev_watch_heads(bot_id, watch_id)
);
CREATE TABLE public.bot_dev_watch_builds (
    bot_id text NOT NULL,
    request_id text NOT NULL,
    package_digest text NOT NULL,
    artifact text NOT NULL,
    component_id text NOT NULL,
    PRIMARY KEY (bot_id, request_id),
    FOREIGN KEY (bot_id, request_id) REFERENCES public.bot_dev_watch_requests(bot_id, request_id)
);
CREATE TABLE public.bot_dev_watch_activations (
    bot_id text NOT NULL,
    operation_id text NOT NULL,
    watch_id text NOT NULL,
    request_id text NOT NULL,
    expected_desired_request_id text NOT NULL,
    recovering boolean NOT NULL,
    host_incarnation text NOT NULL,
    PRIMARY KEY (bot_id, operation_id),
    FOREIGN KEY (bot_id, watch_id) REFERENCES public.bot_dev_watch_heads(bot_id, watch_id),
    FOREIGN KEY (bot_id, request_id) REFERENCES public.bot_dev_watch_builds(bot_id, request_id)
);
CREATE TABLE public.bot_dev_watch_results (
    bot_id text NOT NULL,
    request_id text NOT NULL,
    status text NOT NULL CHECK (status IN ('FAILED','SUPERSEDED','COMMITTED')),
    error_code text,
    PRIMARY KEY (bot_id, request_id),
    FOREIGN KEY (bot_id, request_id) REFERENCES public.bot_dev_watch_requests(bot_id, request_id)
);
CREATE TRIGGER bot_dev_watch_requests_immutable BEFORE UPDATE OR DELETE ON public.bot_dev_watch_requests
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_dev_watch_builds_immutable BEFORE UPDATE OR DELETE ON public.bot_dev_watch_builds
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_dev_watch_activations_immutable BEFORE UPDATE OR DELETE ON public.bot_dev_watch_activations
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_dev_watch_results_immutable BEFORE UPDATE OR DELETE ON public.bot_dev_watch_results
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE FUNCTION public.forest_publish_dev_watch() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE activation record; head record; build record; candidate record; host record;
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
    IF head.desired_request_id IS DISTINCT FROM activation.expected_desired_request_id
        OR (NOT activation.recovering AND activation.request_id IS DISTINCT FROM head.desired_request_id)
        OR (activation.recovering AND activation.request_id IS DISTINCT FROM head.good_request_id)
        OR activation.host_incarnation IS DISTINCT FROM NEW.host_incarnation
        OR head.component_id IS DISTINCT FROM NEW.component_id
        OR build.component_id IS DISTINCT FROM NEW.component_id
        OR candidate.package_digest IS DISTINCT FROM build.package_digest
        OR candidate.config IS DISTINCT FROM head.config
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
CREATE TRIGGER bot_dev_watch_publication AFTER UPDATE ON public.bot_lifecycle_operations
    FOR EACH ROW EXECUTE FUNCTION public.forest_publish_dev_watch();
