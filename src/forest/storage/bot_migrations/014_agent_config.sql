CREATE TABLE public.bot_agent_config_heads (
    bot_id text PRIMARY KEY REFERENCES public.forest_bot_identity(bot_id),
    revision bigint NOT NULL DEFAULT 0 CHECK(revision >= 0),
    secret_revision bigint NOT NULL DEFAULT 0 CHECK(secret_revision >= 0)
);
CREATE TABLE public.bot_agent_config_plans (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    plan_id text NOT NULL,
    subject_id text NOT NULL,
    issuer_id text NOT NULL,
    expected_revision bigint NOT NULL,
    values jsonb NOT NULL,
    secret_ciphertext text NOT NULL,
    credentials_changed boolean NOT NULL,
    plan_digest text NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY(bot_id,plan_id)
);
CREATE TABLE public.bot_agent_config_revisions (
    bot_id text NOT NULL,
    revision bigint NOT NULL CHECK(revision > 0),
    previous_revision bigint NOT NULL CHECK(previous_revision = revision-1),
    values jsonb NOT NULL,
    secret_revision bigint NOT NULL,
    secret_ciphertext text NOT NULL,
    policy_revision bigint NOT NULL,
    plan_id text NOT NULL,
    operation_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,revision),
    FOREIGN KEY(bot_id,plan_id) REFERENCES public.bot_agent_config_plans(bot_id,plan_id),
    FOREIGN KEY(bot_id,operation_id) REFERENCES public.bot_web_operations(bot_id,operation_id)
);
CREATE TRIGGER bot_agent_config_plans_immutable BEFORE UPDATE OR DELETE ON public.bot_agent_config_plans
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_agent_config_revisions_immutable BEFORE UPDATE OR DELETE ON public.bot_agent_config_revisions
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
