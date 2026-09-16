-- Reviewed, reversible source-to-source associations over the existing graph ledger.
CREATE TABLE public.bot_memory_association_heads (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    association_id text NOT NULL,
    left_source_unit_id text NOT NULL,
    right_source_unit_id text NOT NULL,
    scope_id text NOT NULL,
    revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0),
    active boolean NOT NULL DEFAULT false,
    current_contribution_id text,
    PRIMARY KEY(bot_id,association_id),
    UNIQUE(bot_id,left_source_unit_id,right_source_unit_id),
    FOREIGN KEY(bot_id,left_source_unit_id)
        REFERENCES public.bot_memory_source_units(bot_id,source_unit_id),
    FOREIGN KEY(bot_id,right_source_unit_id)
        REFERENCES public.bot_memory_source_units(bot_id,source_unit_id),
    FOREIGN KEY(bot_id,scope_id)
        REFERENCES public.bot_memory_graph_scopes(bot_id,scope_id),
    CHECK(left_source_unit_id<right_source_unit_id),
    CHECK(active=(current_contribution_id IS NOT NULL))
);
CREATE TRIGGER bot_memory_association_head_identity
    BEFORE UPDATE OR DELETE ON public.bot_memory_association_heads
    FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields(
        '{revision,active,current_contribution_id}');
CREATE INDEX bot_memory_association_left_browse
    ON public.bot_memory_association_heads(bot_id,left_source_unit_id,association_id);
CREATE INDEX bot_memory_association_right_browse
    ON public.bot_memory_association_heads(bot_id,right_source_unit_id,association_id);
CREATE INDEX bot_memory_association_state_browse
    ON public.bot_memory_association_heads(bot_id,active,association_id);

CREATE TABLE public.bot_memory_association_changes (
    bot_id text NOT NULL,
    association_id text NOT NULL,
    revision bigint NOT NULL CHECK(revision>0),
    operation_id text NOT NULL,
    active boolean NOT NULL,
    graph_operation_id text NOT NULL,
    contribution_id text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,association_id,revision),
    UNIQUE(bot_id,operation_id),
    UNIQUE(bot_id,graph_operation_id),
    FOREIGN KEY(bot_id,association_id)
        REFERENCES public.bot_memory_association_heads(bot_id,association_id),
    CHECK(active=(contribution_id IS NOT NULL))
);
CREATE TRIGGER bot_memory_association_changes_immutable
    BEFORE UPDATE OR DELETE ON public.bot_memory_association_changes
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE TABLE public.bot_memory_association_plans (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    plan_id text NOT NULL,
    operation_id text NOT NULL,
    subject_id text NOT NULL,
    issuer_id text NOT NULL,
    association_id text NOT NULL,
    expected_revision bigint NOT NULL CHECK(expected_revision>=0),
    expected_deletion_epoch bigint NOT NULL CHECK(expected_deletion_epoch>=0),
    desired_active boolean NOT NULL,
    source_refs jsonb NOT NULL CHECK(
        jsonb_typeof(source_refs)='array' AND jsonb_array_length(source_refs)=2),
    scope jsonb NOT NULL CHECK(jsonb_typeof(scope)='object'),
    plan_digest text NOT NULL,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,plan_id),
    UNIQUE(bot_id,operation_id)
);
CREATE TRIGGER bot_memory_association_plans_immutable
    BEFORE UPDATE OR DELETE ON public.bot_memory_association_plans
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

UPDATE public.bot_web_issuers i SET actions=COALESCE((
    SELECT jsonb_agg(action ORDER BY action) FROM (
        SELECT jsonb_array_elements_text(i.actions) AS action
        UNION
        SELECT capability.action FROM (VALUES
            ('memory.association.read', '["memory.read"]'::jsonb),
            ('memory.association.plan', '["memory.read","memory.config.plan"]'::jsonb),
            ('memory.association.apply', '["memory.read","memory.config.apply"]'::jsonb)
        ) AS capability(action,required)
        WHERE i.actions @> capability.required
    ) permitted
), '[]'::jsonb);
