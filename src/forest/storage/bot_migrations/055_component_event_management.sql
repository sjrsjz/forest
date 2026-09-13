-- Explicit component-event replay and subscription retirement. Runtime lease
-- churn does not change management revisions, so operators CAS only deliberate
-- administrative transitions.
ALTER TABLE public.bot_event_subscription_heads
    ADD COLUMN active boolean NOT NULL DEFAULT true,
    ADD COLUMN management_revision bigint NOT NULL DEFAULT 0 CHECK (management_revision >= 0),
    ADD COLUMN retired_at timestamptz,
    ADD COLUMN retirement_operation_id text,
    ADD CONSTRAINT bot_event_subscription_retirement_shape CHECK (
        active = (retired_at IS NULL AND retirement_operation_id IS NULL)
    );

ALTER TABLE public.bot_event_deliveries
    DROP CONSTRAINT bot_event_deliveries_status_check,
    ADD COLUMN replay_epoch bigint NOT NULL DEFAULT 0 CHECK (replay_epoch >= 0),
    ADD COLUMN management_revision bigint NOT NULL DEFAULT 0 CHECK (management_revision >= 0),
    ADD COLUMN retired_at timestamptz,
    ADD CONSTRAINT bot_event_deliveries_status_check
        CHECK (status IN ('PENDING','INFLIGHT','RETRY_WAIT','ACKED','DEAD','RETIRED')),
    ADD CONSTRAINT bot_event_delivery_retired_shape CHECK (
        (status = 'RETIRED') = (retired_at IS NOT NULL)
    );

DROP INDEX public.bot_event_deliveries_open_order;
CREATE INDEX bot_event_deliveries_open_order
    ON public.bot_event_deliveries(bot_id,subscriber_id,contract_major,event_seq)
    WHERE status NOT IN ('ACKED','DEAD','RETIRED');

CREATE TABLE public.bot_event_delivery_replays (
    bot_id text NOT NULL,
    delivery_id text NOT NULL,
    replay_epoch bigint NOT NULL CHECK (replay_epoch > 0),
    operation_id text NOT NULL,
    subject_id text NOT NULL,
    issuer_id text NOT NULL,
    previous_attempt_count integer NOT NULL CHECK (previous_attempt_count BETWEEN 0 AND 32),
    previous_dispatch_count bigint NOT NULL CHECK (previous_dispatch_count >= previous_attempt_count),
    previous_error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,delivery_id,replay_epoch),
    UNIQUE (bot_id,operation_id),
    FOREIGN KEY (bot_id,delivery_id)
        REFERENCES public.bot_event_deliveries(bot_id,delivery_id)
);

CREATE TABLE public.bot_event_retirement_plans (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    plan_id text NOT NULL,
    operation_id text NOT NULL,
    subject_id text NOT NULL,
    issuer_id text NOT NULL,
    subscriber_id text NOT NULL,
    contract_major integer NOT NULL CHECK (contract_major > 0),
    expected_revision bigint NOT NULL CHECK (expected_revision >= 0),
    impact jsonb NOT NULL CHECK (jsonb_typeof(impact) = 'object'),
    impact_digest text NOT NULL,
    plan_digest text NOT NULL,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,plan_id),
    UNIQUE (bot_id,operation_id),
    FOREIGN KEY (bot_id,subscriber_id,contract_major)
        REFERENCES public.bot_event_subscription_heads(bot_id,subscriber_id,contract_major)
);

CREATE TABLE public.bot_event_subscription_retirements (
    bot_id text NOT NULL,
    subscriber_id text NOT NULL,
    contract_major integer NOT NULL CHECK (contract_major > 0),
    management_revision bigint NOT NULL CHECK (management_revision > 0),
    operation_id text NOT NULL,
    plan_id text NOT NULL,
    subject_id text NOT NULL,
    issuer_id text NOT NULL,
    retired_delivery_count integer NOT NULL CHECK (retired_delivery_count >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,subscriber_id,contract_major,management_revision),
    UNIQUE (bot_id,operation_id),
    FOREIGN KEY (bot_id,subscriber_id,contract_major)
        REFERENCES public.bot_event_subscription_heads(bot_id,subscriber_id,contract_major),
    FOREIGN KEY (bot_id,plan_id)
        REFERENCES public.bot_event_retirement_plans(bot_id,plan_id)
);

CREATE TRIGGER bot_event_delivery_replays_immutable
    BEFORE UPDATE OR DELETE ON public.bot_event_delivery_replays
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_event_retirement_plans_immutable
    BEFORE UPDATE OR DELETE ON public.bot_event_retirement_plans
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_event_subscription_retirements_immutable
    BEFORE UPDATE OR DELETE ON public.bot_event_subscription_retirements
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

-- Preserve least privilege when upgrading existing issuer records. Each write
-- action is derived directly from pre-migration capabilities in i.actions.
UPDATE public.bot_web_issuers i SET actions=COALESCE((
    SELECT jsonb_agg(action ORDER BY action) FROM (
        SELECT jsonb_array_elements_text(i.actions) AS action
        UNION
        SELECT 'event.read' WHERE i.actions ?| ARRAY['config.read','message.read']
        UNION
        SELECT 'event.replay'
        WHERE i.actions ? 'service.invoke'
          AND i.actions ?| ARRAY['config.read','message.read']
        UNION
        SELECT 'event.retire.plan'
        WHERE i.actions ? 'config.plan'
          AND i.actions ?| ARRAY['config.read','message.read']
        UNION
        SELECT 'event.retire.apply'
        WHERE i.actions @> '["config.apply","message.cancel"]'::jsonb
          AND i.actions ?| ARRAY['config.read','message.read']
    ) permitted
), '[]'::jsonb);
