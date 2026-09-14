CREATE TABLE public.bot_context_management_requests (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    request_id text NOT NULL,
    operation_id text NOT NULL,
    conversation_id text NOT NULL,
    kind text NOT NULL CHECK(kind IN ('COMPACT')),
    expected_view_revision bigint NOT NULL CHECK(expected_view_revision >= 0),
    status text NOT NULL DEFAULT 'PENDING'
        CHECK(status IN ('PENDING','APPLIED','CONFLICT')),
    applied_view_revision bigint,
    terminal_reason text,
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY(bot_id,request_id),
    UNIQUE(bot_id,operation_id),
    FOREIGN KEY(bot_id,conversation_id)
        REFERENCES public.bot_conversations(bot_id,conversation_id),
    FOREIGN KEY(bot_id,operation_id)
        REFERENCES public.bot_web_operations(bot_id,operation_id)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK(
        (status='PENDING' AND applied_view_revision IS NULL
            AND terminal_reason IS NULL AND completed_at IS NULL)
        OR
        (status='APPLIED' AND applied_view_revision IS NOT NULL
            AND applied_view_revision > expected_view_revision
            AND terminal_reason IS NULL AND completed_at IS NOT NULL)
        OR
        (status='CONFLICT' AND applied_view_revision IS NULL
            AND terminal_reason IS NOT NULL AND completed_at IS NOT NULL)
    )
);

CREATE INDEX bot_context_management_pending
    ON public.bot_context_management_requests(bot_id,conversation_id,requested_at,request_id)
    WHERE status='PENDING';

CREATE FUNCTION public.forest_context_management_request_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'CONTEXT_MANAGEMENT_IMMUTABLE' USING ERRCODE='55000';
    END IF;
    IF ROW(NEW.bot_id,NEW.request_id,NEW.operation_id,NEW.conversation_id,
           NEW.kind,NEW.expected_view_revision,NEW.requested_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.request_id,OLD.operation_id,OLD.conversation_id,
           OLD.kind,OLD.expected_view_revision,OLD.requested_at) THEN
        RAISE EXCEPTION 'CONTEXT_MANAGEMENT_IDENTITY_IMMUTABLE' USING ERRCODE='55000';
    END IF;
    IF OLD.status <> 'PENDING'
       AND ROW(NEW.status,NEW.applied_view_revision,NEW.terminal_reason,NEW.completed_at)
           IS DISTINCT FROM
           ROW(OLD.status,OLD.applied_view_revision,OLD.terminal_reason,OLD.completed_at) THEN
        RAISE EXCEPTION 'CONTEXT_MANAGEMENT_TERMINAL' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER bot_context_management_request_guard
    BEFORE UPDATE OR DELETE ON public.bot_context_management_requests
    FOR EACH ROW EXECUTE FUNCTION public.forest_context_management_request_guard();
