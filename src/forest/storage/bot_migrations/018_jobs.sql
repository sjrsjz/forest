CREATE TABLE public.bot_jobs (
    bot_id text NOT NULL, job_id text NOT NULL, revision bigint NOT NULL DEFAULT 0,
    definition_revision bigint, conversation_id text, enabled boolean NOT NULL DEFAULT false,
    delegation_epoch bigint NOT NULL DEFAULT 0, next_due_at timestamptz,
    emitted_count bigint NOT NULL DEFAULT 0 CHECK(emitted_count>=0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,job_id),
    FOREIGN KEY(bot_id,conversation_id) REFERENCES public.bot_conversations(bot_id,conversation_id)
);
CREATE TABLE public.bot_job_revisions (
    bot_id text NOT NULL, job_id text NOT NULL, revision bigint NOT NULL,
    values jsonb NOT NULL, delegation jsonb NOT NULL, definition_digest text NOT NULL,
    operation_id text NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,job_id,revision), UNIQUE(bot_id,operation_id),
    FOREIGN KEY(bot_id,job_id) REFERENCES public.bot_jobs(bot_id,job_id)
);
CREATE TABLE public.bot_job_plans (
    bot_id text NOT NULL, job_id text NOT NULL, plan_id text NOT NULL,
    expected_revision bigint NOT NULL, subject_id text NOT NULL, issuer_id text NOT NULL,
    values jsonb NOT NULL, plan_digest text NOT NULL, expires_at timestamptz NOT NULL,
    PRIMARY KEY(bot_id,plan_id), FOREIGN KEY(bot_id,job_id) REFERENCES public.bot_jobs(bot_id,job_id)
);
CREATE TABLE public.bot_job_occurrences (
    bot_id text NOT NULL, occurrence_id text NOT NULL, job_id text NOT NULL,
    job_revision bigint NOT NULL, delegation_epoch bigint NOT NULL,
    origin text NOT NULL CHECK(origin IN ('SCHEDULE','MANUAL')),
    scheduled_for timestamptz NOT NULL, expires_at timestamptz NOT NULL,
    revision bigint NOT NULL DEFAULT 1,
    status text NOT NULL DEFAULT 'QUEUED' CHECK(status IN
        ('QUEUED','CLAIMED','ENQUEUED','SUCCEEDED','FAILED','CANCELLED','EXPIRED')),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(), attempt_count integer NOT NULL DEFAULT 0,
    owner_id text, host_incarnation text, lease_epoch bigint NOT NULL DEFAULT 0,
    lease_until timestamptz, error_code text,
    event_id text, trigger_id text, completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), dispatched_at timestamptz,
    PRIMARY KEY(bot_id,occurrence_id), UNIQUE(bot_id,event_id), UNIQUE(bot_id,trigger_id),
    FOREIGN KEY(bot_id,job_id,job_revision) REFERENCES public.bot_job_revisions(bot_id,job_id,revision),
    FOREIGN KEY(bot_id,event_id) REFERENCES public.bot_message_inbox(bot_id,event_id),
    FOREIGN KEY(bot_id,trigger_id) REFERENCES public.bot_conversation_triggers(bot_id,trigger_id)
);
CREATE INDEX bot_job_due ON public.bot_jobs(bot_id,next_due_at) WHERE enabled;
CREATE INDEX bot_job_pending ON public.bot_job_occurrences(bot_id,status,next_attempt_at,scheduled_for);
CREATE TABLE public.bot_job_expansions (
    bot_id text NOT NULL, expansion_id text NOT NULL, job_id text NOT NULL, job_revision bigint NOT NULL,
    from_due_at timestamptz NOT NULL, through_at timestamptz NOT NULL,
    selected_occurrences jsonb NOT NULL, misfired boolean NOT NULL,
    PRIMARY KEY(bot_id,expansion_id), FOREIGN KEY(bot_id,job_id) REFERENCES public.bot_jobs(bot_id,job_id)
);
CREATE TRIGGER bot_job_revision_immutable BEFORE UPDATE OR DELETE ON public.bot_job_revisions
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_job_plan_immutable BEFORE UPDATE OR DELETE ON public.bot_job_plans
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_job_expansion_immutable BEFORE UPDATE OR DELETE ON public.bot_job_expansions
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE FUNCTION public.forest_job_occurrence_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' OR ROW(NEW.bot_id,NEW.occurrence_id,NEW.job_id,NEW.job_revision,
       NEW.delegation_epoch,NEW.origin,NEW.scheduled_for,NEW.expires_at)
       IS DISTINCT FROM ROW(OLD.bot_id,OLD.occurrence_id,OLD.job_id,OLD.job_revision,
       OLD.delegation_epoch,OLD.origin,OLD.scheduled_for,OLD.expires_at)
       OR (OLD.event_id IS NOT NULL AND ROW(NEW.event_id,NEW.trigger_id) IS DISTINCT FROM ROW(OLD.event_id,OLD.trigger_id)) THEN
        RAISE EXCEPTION 'Job occurrence identity is immutable' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER bot_job_occurrence_identity BEFORE UPDATE OR DELETE ON public.bot_job_occurrences
    FOR EACH ROW EXECUTE FUNCTION public.forest_job_occurrence_immutable();
ALTER TABLE public.bot_message_inbox DROP CONSTRAINT bot_message_inbox_origin_check,
    ADD CONSTRAINT bot_message_inbox_origin_check CHECK(origin IN
        ('platform_input','chat_output','control_output','job'));
-- Runtime/policy and conversation locks are held by every Actor selection.
-- A Web account proof authorizes durable enqueue; it is not a browser session
-- lease on an already accepted occurrence. Local revocation still fences claim.
CREATE FUNCTION public.forest_job_occurrence_authorized(target_bot text,target_occurrence text)
RETURNS boolean LANGUAGE sql VOLATILE AS $$
    SELECT COALESCE((
        SELECT o.expires_at>clock_timestamp()
          AND o.delegation_epoch=j.delegation_epoch
          AND (d.delegation->>'expires_at')::timestamptz>clock_timestamp()
          AND (d.delegation->>'policy_revision')::bigint=r.policy_revision
          AND i.enabled AND i.actions ? 'job.run'
          AND i.key_revision=(d.delegation->>'key_revision')::bigint
        FROM public.bot_job_occurrences o JOIN public.bot_jobs j USING(bot_id,job_id)
        JOIN public.bot_job_revisions d ON d.bot_id=o.bot_id AND d.job_id=o.job_id AND d.revision=o.job_revision
        JOIN public.bot_runtime_state r ON r.bot_id=o.bot_id
        LEFT JOIN public.bot_web_issuers i ON i.bot_id=o.bot_id AND i.issuer_id=d.delegation->>'issuer_id'
        WHERE o.bot_id=target_bot AND o.occurrence_id=target_occurrence
    ),false)
$$;
CREATE FUNCTION public.forest_job_trigger_authorized(target_bot text,target_trigger text)
RETURNS boolean LANGUAGE sql VOLATILE AS $$
    SELECT COALESCE((SELECT o.status='ENQUEUED'
        AND public.forest_job_occurrence_authorized(o.bot_id,o.occurrence_id)
        FROM public.bot_job_occurrences o WHERE o.bot_id=target_bot AND o.trigger_id=target_trigger),false)
$$;
CREATE FUNCTION public.forest_job_trigger_claim_fence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status='PENDING' AND NEW.status='CLAIMED'
       AND EXISTS(SELECT 1 FROM public.bot_job_occurrences WHERE bot_id=NEW.bot_id AND trigger_id=NEW.trigger_id)
       AND NOT public.forest_job_trigger_authorized(NEW.bot_id,NEW.trigger_id) THEN
        RAISE EXCEPTION 'Job delegation expired before trigger claim' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER bot_job_trigger_claim_fence BEFORE UPDATE ON public.bot_conversation_triggers
    FOR EACH ROW EXECUTE FUNCTION public.forest_job_trigger_claim_fence();
UPDATE public.bot_web_issuers i SET actions=COALESCE((
    SELECT jsonb_agg(action ORDER BY action) FROM (
        SELECT jsonb_array_elements_text(i.actions) AS action UNION
        SELECT capability.action FROM (VALUES
            ('job.read','["message.read"]'::jsonb), ('job.plan','["config.plan"]'::jsonb),
            ('job.apply','["config.apply"]'::jsonb), ('job.run','["service.invoke"]'::jsonb),
            ('job.cancel','["message.cancel"]'::jsonb), ('job.replay','["service.invoke"]'::jsonb)
        ) capability(action,required) WHERE i.actions @> capability.required
    ) permitted
),'[]'::jsonb);
