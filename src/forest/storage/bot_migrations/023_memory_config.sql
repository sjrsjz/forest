CREATE TABLE public.bot_memory_config_heads (
    bot_id text PRIMARY KEY REFERENCES public.forest_bot_identity(bot_id),
    revision bigint NOT NULL DEFAULT 0 CHECK(revision >= 0),
    secret_revision bigint NOT NULL DEFAULT 0 CHECK(secret_revision >= 0)
);
CREATE TABLE public.bot_memory_config_plans (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    plan_id text NOT NULL,
    subject_id text NOT NULL,
    issuer_id text NOT NULL,
    expected_revision bigint NOT NULL CHECK(expected_revision >= 0),
    values jsonb NOT NULL,
    secret_ciphertext text NOT NULL,
    credentials_changed boolean NOT NULL,
    plan_digest text NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY(bot_id,plan_id)
);
CREATE TABLE public.bot_memory_config_revisions (
    bot_id text NOT NULL,
    revision bigint NOT NULL CHECK(revision > 0),
    previous_revision bigint NOT NULL CHECK(previous_revision = revision-1),
    values jsonb NOT NULL,
    secret_revision bigint NOT NULL CHECK(secret_revision > 0),
    secret_ciphertext text NOT NULL,
    policy_revision bigint NOT NULL,
    plan_id text NOT NULL,
    operation_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,revision),
    FOREIGN KEY(bot_id,plan_id) REFERENCES public.bot_memory_config_plans(bot_id,plan_id),
    FOREIGN KEY(bot_id,operation_id) REFERENCES public.bot_web_operations(bot_id,operation_id)
);
CREATE INDEX bot_memory_config_deployment ON public.bot_memory_config_revisions
    (bot_id,(values->>'deployment_id'));
CREATE TRIGGER bot_memory_config_plans_immutable BEFORE UPDATE OR DELETE ON public.bot_memory_config_plans
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_memory_config_revisions_immutable BEFORE UPDATE OR DELETE ON public.bot_memory_config_revisions
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

-- Preserve restricted/disabled issuer grants. No read permission implies writes.
UPDATE public.bot_web_issuers i SET actions=COALESCE((
    SELECT jsonb_agg(action ORDER BY action) FROM (
        SELECT jsonb_array_elements_text(i.actions) AS action
        UNION
        SELECT capability.action FROM (VALUES
            ('memory.config.read', '["config.read"]'::jsonb),
            ('memory.config.plan', '["config.plan"]'::jsonb),
            ('memory.config.apply', '["config.apply"]'::jsonb),
            ('memory.read', '["message.read"]'::jsonb)
        ) AS capability(action, required)
        WHERE i.actions @> capability.required
    ) permitted
), '[]'::jsonb);
