-- Durable module intent is independent from immutable component configuration.
-- DISABLE uses the ordinary resource/route/host lifecycle with no candidate.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.bot_schema_writer_gate
        WHERE singleton AND minimum_generation >= 9
    ) THEN
        RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
    END IF;
END;
$$;

ALTER TABLE public.bot_lifecycle_operations
    ALTER COLUMN candidate_generation_id DROP NOT NULL,
    ADD COLUMN operation_kind text NOT NULL DEFAULT 'INSTALL',
    ADD CONSTRAINT bot_lifecycle_operation_kind_check CHECK (
        (operation_kind='INSTALL' AND candidate_generation_id IS NOT NULL)
        OR (operation_kind='DISABLE' AND candidate_generation_id IS NULL)
    );

CREATE FUNCTION public.forest_module_lifecycle_identity_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.operation_id,NEW.component_id,NEW.input_digest,
            NEW.candidate_generation_id,NEW.old_generation_id,
            NEW.expected_route_epoch,NEW.operation_kind,NEW.created_at)
        IS DISTINCT FROM
        ROW(OLD.bot_id,OLD.operation_id,OLD.component_id,OLD.input_digest,
            OLD.candidate_generation_id,OLD.old_generation_id,
            OLD.expected_route_epoch,OLD.operation_kind,OLD.created_at) THEN
        RAISE EXCEPTION 'Lifecycle operation identity is immutable' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER bot_module_lifecycle_identity_guard
    BEFORE UPDATE ON public.bot_lifecycle_operations
    FOR EACH ROW EXECUTE FUNCTION public.forest_module_lifecycle_identity_guard();

ALTER TABLE public.bot_shell_requests
    ADD CONSTRAINT bot_shell_requests_v11_priority_check CHECK (
        gate_version<>'shell-gate-v11' OR (
            priority_policy='current-binding-admin-v1'
            AND priority_basis_binding_epoch IS NOT NULL
            AND priority_basis_binding_epoch>=1
            AND priority_basis_config_revision IS NOT NULL
            AND priority_basis_config_revision>=1
            AND priority_eligible=(requested_priority=0 AND priority=0)
            AND priority=(CASE
                WHEN requested_priority=0 AND priority_eligible THEN 0
                WHEN requested_priority=0 THEN 1
                ELSE requested_priority
            END)
        )
    );

-- Preserve existing issuer restrictions. Module reads follow configuration reads;
-- reload needs both plan/apply authority, enable needs apply authority, and the
-- disruptive disable operation additionally requires message cancellation.
UPDATE public.bot_web_issuers i SET actions=COALESCE((
    SELECT jsonb_agg(action ORDER BY action) FROM (
        SELECT jsonb_array_elements_text(i.actions) AS action
        UNION SELECT 'module.read' WHERE i.actions ? 'config.read'
        UNION SELECT 'module.validate' WHERE i.actions ? 'config.read'
        UNION SELECT 'module.reload' WHERE i.actions ?& ARRAY['config.plan','config.apply']
        UNION SELECT 'module.enable' WHERE i.actions ? 'config.apply'
        UNION SELECT 'module.disable'
            WHERE i.actions ?& ARRAY['config.apply','message.cancel']
    ) permitted
), '[]'::jsonb);

CREATE TABLE public.bot_module_state_heads (
    bot_id text NOT NULL,
    component_id text NOT NULL,
    desired_revision bigint NOT NULL DEFAULT 0 CHECK (desired_revision>=0),
    effective_revision bigint NOT NULL DEFAULT 0 CHECK (
        effective_revision BETWEEN 0 AND desired_revision
    ),
    PRIMARY KEY (bot_id,component_id),
    FOREIGN KEY (bot_id,component_id)
        REFERENCES public.bot_config_heads(bot_id,component_id)
);

CREATE TABLE public.bot_module_state_revisions (
    bot_id text NOT NULL,
    component_id text NOT NULL,
    revision bigint NOT NULL CHECK (revision>0),
    previous_revision bigint NOT NULL CHECK (previous_revision=revision-1),
    enabled boolean NOT NULL,
    reason text NOT NULL CHECK (length(reason) BETWEEN 1 AND 256),
    policy_revision bigint NOT NULL CHECK (policy_revision>0),
    operation_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,component_id,revision),
    FOREIGN KEY (bot_id,operation_id)
        REFERENCES public.bot_web_operations(bot_id,operation_id)
);

CREATE TABLE public.bot_module_state_activations (
    bot_id text NOT NULL,
    operation_id text NOT NULL,
    component_id text NOT NULL,
    revision bigint NOT NULL,
    enabled boolean NOT NULL,
    host_incarnation text NOT NULL,
    PRIMARY KEY (bot_id,operation_id),
    FOREIGN KEY (bot_id,component_id,revision)
        REFERENCES public.bot_module_state_revisions(bot_id,component_id,revision)
);

CREATE TRIGGER bot_module_state_revisions_immutable
    BEFORE UPDATE OR DELETE ON public.bot_module_state_revisions
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_module_state_activations_immutable
    BEFORE UPDATE OR DELETE ON public.bot_module_state_activations
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE FUNCTION public.forest_module_state_head_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.desired_revision<>0 OR NEW.effective_revision<>0 THEN
            RAISE EXCEPTION 'MODULE_STATE_HEAD_CONFLICT' USING ERRCODE='23514';
        END IF;
    ELSIF ROW(NEW.bot_id,NEW.component_id)
            IS DISTINCT FROM ROW(OLD.bot_id,OLD.component_id)
        OR NEW.desired_revision NOT IN (OLD.desired_revision,OLD.desired_revision+1)
        OR NEW.effective_revision<OLD.effective_revision
        OR (NEW.effective_revision<>OLD.effective_revision
            AND NEW.effective_revision<>NEW.desired_revision)
        OR (NEW.desired_revision<>OLD.desired_revision AND NOT EXISTS (
            SELECT 1 FROM public.bot_module_state_revisions r
            WHERE (r.bot_id,r.component_id,r.revision,r.previous_revision)=
                (NEW.bot_id,NEW.component_id,NEW.desired_revision,OLD.desired_revision)
        )) THEN
        RAISE EXCEPTION 'MODULE_STATE_HEAD_CONFLICT' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER bot_module_state_head_guard
    BEFORE INSERT OR UPDATE ON public.bot_module_state_heads
    FOR EACH ROW EXECUTE FUNCTION public.forest_module_state_head_guard();

CREATE FUNCTION public.forest_module_state_revision_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE op record; h record; cfg record; input jsonb; result jsonb;
BEGIN
    SELECT * INTO h FROM public.bot_module_state_heads
        WHERE bot_id=NEW.bot_id AND component_id=NEW.component_id FOR UPDATE;
    SELECT * INTO op FROM public.bot_web_operations
        WHERE bot_id=NEW.bot_id AND operation_id=NEW.operation_id;
    SELECT * INTO cfg FROM public.bot_config_heads
        WHERE bot_id=NEW.bot_id AND component_id=NEW.component_id;
    IF op.operation_id IS NOT NULL THEN
        input:=public.forest_control_payload_value(
            NEW.bot_id,op.input_payload_id,op.input_payload_digest,
            'input',op.operation_id,op.action
        );
        result:=public.forest_control_payload_value(
            NEW.bot_id,op.result_payload_id,op.result_payload_digest,
            'result',op.operation_id,op.action
        );
    END IF;
    IF op.operation_id IS NULL OR cfg.component_id IS NULL
        OR h.desired_revision IS DISTINCT FROM NEW.previous_revision
        OR NEW.revision IS DISTINCT FROM NEW.previous_revision+1
        OR op.action IS DISTINCT FROM (CASE
            WHEN NEW.enabled THEN 'module.enable' ELSE 'module.disable' END)
        OR op.ticket_basis->>'policy_revision' IS DISTINCT FROM NEW.policy_revision::text
        OR input IS DISTINCT FROM jsonb_build_object(
            'component_id',NEW.component_id,
            'expected_state_revision',NEW.previous_revision,
            'enabled',NEW.enabled,
            'reason',NEW.reason
        )
        OR result IS DISTINCT FROM jsonb_build_object(
            'operation_id',NEW.operation_id,
            'component_id',NEW.component_id,
            'desired_config_revision',cfg.desired_revision,
            'effective_config_revision',cfg.effective_revision,
            'desired_state_revision',NEW.revision,
            'effective_state_revision',h.effective_revision,
            'enabled',NEW.enabled,
            'status','ACCEPTED'
        ) THEN
        RAISE EXCEPTION 'MODULE_STATE_REVISION_SOURCE_CONFLICT' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER bot_module_state_revision_guard
    BEFORE INSERT ON public.bot_module_state_revisions
    FOR EACH ROW EXECUTE FUNCTION public.forest_module_state_revision_guard();

CREATE FUNCTION public.forest_module_state_activation_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE h record; r record; host record;
BEGIN
    SELECT * INTO h FROM public.bot_module_state_heads
        WHERE bot_id=NEW.bot_id AND component_id=NEW.component_id FOR UPDATE;
    SELECT * INTO r FROM public.bot_module_state_revisions
        WHERE bot_id=NEW.bot_id AND component_id=NEW.component_id
        AND revision=NEW.revision;
    SELECT * INTO host FROM public.bot_host_lease WHERE bot_id=NEW.bot_id;
    IF h.desired_revision IS DISTINCT FROM NEW.revision
        OR r.enabled IS DISTINCT FROM NEW.enabled
        OR host.incarnation IS DISTINCT FROM NEW.host_incarnation
        OR host.expires_at<=clock_timestamp() THEN
        RAISE EXCEPTION 'MODULE_STATE_SUPERSEDED' USING ERRCODE='P0001';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER bot_module_state_activation_guard
    BEFORE INSERT ON public.bot_module_state_activations
    FOR EACH ROW EXECUTE FUNCTION public.forest_module_state_activation_guard();

CREATE FUNCTION public.forest_publish_module_state() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE activation record; h record; r record; route record;
BEGIN
    IF NEW.phase<>'COMMITTED' OR OLD.phase='COMMITTED' THEN RETURN NEW; END IF;
    SELECT * INTO activation FROM public.bot_module_state_activations
        WHERE bot_id=NEW.bot_id AND operation_id=NEW.operation_id;
    IF NOT FOUND THEN RETURN NEW; END IF;
    SELECT * INTO h FROM public.bot_module_state_heads
        WHERE bot_id=NEW.bot_id AND component_id=activation.component_id FOR UPDATE;
    SELECT * INTO r FROM public.bot_module_state_revisions
        WHERE bot_id=NEW.bot_id AND component_id=activation.component_id
        AND revision=activation.revision;
    SELECT * INTO route FROM public.bot_component_routes
        WHERE bot_id=NEW.bot_id AND component_id=activation.component_id;
    IF h.desired_revision IS DISTINCT FROM activation.revision
        OR r.enabled IS DISTINCT FROM activation.enabled
        OR activation.component_id IS DISTINCT FROM NEW.component_id
        OR activation.host_incarnation IS DISTINCT FROM NEW.host_incarnation
        OR NEW.operation_kind IS DISTINCT FROM
            (CASE WHEN activation.enabled THEN 'INSTALL' ELSE 'DISABLE' END)
        OR (activation.enabled AND route.generation_id IS DISTINCT FROM NEW.candidate_generation_id)
        OR (NOT activation.enabled AND
            (route.generation_id IS NOT NULL OR route.admission OR route.control_admission)) THEN
        RAISE EXCEPTION 'MODULE_STATE_SUPERSEDED' USING ERRCODE='P0001';
    END IF;
    UPDATE public.bot_module_state_heads SET effective_revision=activation.revision
        WHERE bot_id=NEW.bot_id AND component_id=activation.component_id;
    INSERT INTO public.bot_outbox_events(bot_id,event_id,topic,operation_id,payload)
        VALUES (NEW.bot_id,'module-state:'||NEW.operation_id,'module.state.effective',
            NEW.operation_id,jsonb_build_object(
                'component_id',NEW.component_id,'revision',activation.revision,
                'enabled',activation.enabled));
    RETURN NEW;
END;
$$;
CREATE TRIGGER bot_module_state_publication
    AFTER UPDATE ON public.bot_lifecycle_operations
    FOR EACH ROW EXECUTE FUNCTION public.forest_publish_module_state();
