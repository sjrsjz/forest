-- Reviewed public memory deletion plans; the effect remains in the 021 payload ledger.
CREATE TABLE public.bot_memory_forget_plans (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    plan_id text NOT NULL,
    operation_id text NOT NULL,
    subject_id text NOT NULL,
    issuer_id text NOT NULL,
    expected_deletion_epoch bigint NOT NULL CHECK(expected_deletion_epoch>=0),
    requested_targets jsonb NOT NULL CHECK(
        jsonb_typeof(requested_targets)='array'
        AND jsonb_array_length(requested_targets) BETWEEN 1 AND 32),
    resolved_targets jsonb NOT NULL CHECK(
        jsonb_typeof(resolved_targets)='array'
        AND jsonb_array_length(resolved_targets) BETWEEN 1 AND 4096),
    target_digest text NOT NULL,
    affected_payloads bigint NOT NULL CHECK(affected_payloads>=0),
    legacy_conservative boolean NOT NULL,
    plan_digest text NOT NULL,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,plan_id),
    UNIQUE(bot_id,operation_id)
);
CREATE TRIGGER bot_memory_forget_plans_immutable
    BEFORE UPDATE OR DELETE ON public.bot_memory_forget_plans
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

-- Preserve every issuer's existing restriction. Specialized memory rights are
-- prerequisites; a disabled issuer remains disabled.
UPDATE public.bot_web_issuers i SET actions=COALESCE((
    SELECT jsonb_agg(action ORDER BY action) FROM (
        SELECT jsonb_array_elements_text(i.actions) AS action
        UNION
        SELECT capability.action FROM (VALUES
            ('memory.forget.read', '["memory.read"]'::jsonb),
            ('memory.forget.plan', '["memory.read","memory.config.plan"]'::jsonb),
            ('memory.forget.apply', '["memory.read","memory.config.apply"]'::jsonb)
        ) AS capability(action,required)
        WHERE i.actions @> capability.required
    ) permitted
), '[]'::jsonb);
