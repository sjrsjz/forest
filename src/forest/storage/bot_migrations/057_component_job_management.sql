-- Explicit replay and retirement for plugin-owned durable work. Management
-- revisions move only on operator actions, never on runtime lease churn.
ALTER TABLE public.bot_component_job_heads
    ADD COLUMN active boolean NOT NULL DEFAULT true,
    ADD COLUMN management_revision bigint NOT NULL DEFAULT 0 CHECK (management_revision >= 0),
    ADD COLUMN retired_at timestamptz,
    ADD COLUMN retirement_operation_id text,
    ADD CONSTRAINT bot_component_job_head_retirement_shape CHECK (
        active = (retired_at IS NULL AND retirement_operation_id IS NULL)
    );

ALTER TABLE public.bot_component_jobs
    DROP CONSTRAINT bot_component_jobs_status_check,
    ADD COLUMN replay_epoch bigint NOT NULL DEFAULT 0 CHECK (replay_epoch >= 0),
    ADD COLUMN management_revision bigint NOT NULL DEFAULT 0 CHECK (management_revision >= 0),
    ADD COLUMN retired_at timestamptz,
    ADD CONSTRAINT bot_component_jobs_status_check
        CHECK (status IN ('PENDING','INFLIGHT','RETRY_WAIT','ACKED','DEAD','RETIRED')),
    ADD CONSTRAINT bot_component_job_retired_shape CHECK (
        (status='RETIRED') = (retired_at IS NOT NULL)
    );

DROP INDEX public.bot_component_jobs_handler_open;
CREATE INDEX bot_component_jobs_handler_open
    ON public.bot_component_jobs(bot_id,component_id,handler_id,job_seq)
    WHERE status NOT IN ('ACKED','DEAD','RETIRED');

CREATE TABLE public.bot_component_job_replays (
    bot_id text NOT NULL,
    job_id text NOT NULL,
    replay_epoch bigint NOT NULL CHECK (replay_epoch > 0),
    operation_id text NOT NULL,
    subject_id text NOT NULL,
    issuer_id text NOT NULL,
    previous_attempt_count integer NOT NULL CHECK (previous_attempt_count BETWEEN 0 AND 32),
    previous_dispatch_count bigint NOT NULL CHECK (previous_dispatch_count >= previous_attempt_count),
    previous_error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,job_id,replay_epoch),
    UNIQUE (bot_id,operation_id),
    FOREIGN KEY (bot_id,job_id) REFERENCES public.bot_component_jobs(bot_id,job_id)
);

CREATE TABLE public.bot_component_job_retirement_plans (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    plan_id text NOT NULL,
    operation_id text NOT NULL,
    subject_id text NOT NULL,
    issuer_id text NOT NULL,
    component_id text NOT NULL,
    handler_id text NOT NULL,
    expected_revision bigint NOT NULL CHECK (expected_revision >= 0),
    impact jsonb NOT NULL CHECK (jsonb_typeof(impact)='object'),
    impact_digest text NOT NULL,
    plan_digest text NOT NULL,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,plan_id),
    UNIQUE (bot_id,operation_id),
    FOREIGN KEY (bot_id,component_id,handler_id)
        REFERENCES public.bot_component_job_heads(bot_id,component_id,handler_id)
);

CREATE TABLE public.bot_component_job_handler_retirements (
    bot_id text NOT NULL,
    component_id text NOT NULL,
    handler_id text NOT NULL,
    management_revision bigint NOT NULL CHECK (management_revision > 0),
    operation_id text NOT NULL,
    plan_id text NOT NULL,
    subject_id text NOT NULL,
    issuer_id text NOT NULL,
    retired_job_count integer NOT NULL CHECK (retired_job_count >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,component_id,handler_id,management_revision),
    UNIQUE (bot_id,operation_id),
    FOREIGN KEY (bot_id,component_id,handler_id)
        REFERENCES public.bot_component_job_heads(bot_id,component_id,handler_id),
    FOREIGN KEY (bot_id,plan_id)
        REFERENCES public.bot_component_job_retirement_plans(bot_id,plan_id)
);

CREATE TRIGGER bot_component_job_replays_immutable
    BEFORE UPDATE OR DELETE ON public.bot_component_job_replays
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_component_job_retirement_plans_immutable
    BEFORE UPDATE OR DELETE ON public.bot_component_job_retirement_plans
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_component_job_handler_retirements_immutable
    BEFORE UPDATE OR DELETE ON public.bot_component_job_handler_retirements
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

-- Preserve least privilege for existing Web issuers.
UPDATE public.bot_web_issuers i SET actions=COALESCE((
    SELECT jsonb_agg(action ORDER BY action) FROM (
        SELECT jsonb_array_elements_text(i.actions) AS action
        UNION
        SELECT 'component_job.read' WHERE i.actions ?| ARRAY['config.read','message.read']
        UNION
        SELECT 'component_job.replay'
        WHERE i.actions ? 'service.invoke'
          AND i.actions ?| ARRAY['config.read','message.read']
        UNION
        SELECT 'component_job.retire.plan'
        WHERE i.actions ? 'config.plan'
          AND i.actions ?| ARRAY['config.read','message.read']
        UNION
        SELECT 'component_job.retire.apply'
        WHERE i.actions @> '["config.apply","message.cancel"]'::jsonb
          AND i.actions ?| ARRAY['config.read','message.read']
    ) permitted
), '[]'::jsonb);
