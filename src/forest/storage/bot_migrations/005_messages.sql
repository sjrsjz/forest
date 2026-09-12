CREATE TABLE public.bot_conversations (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    conversation_id text NOT NULL,
    binding_id text NOT NULL,
    platform text NOT NULL,
    channel_kind text NOT NULL CHECK (channel_kind IN ('group','private')),
    channel_id text NOT NULL,
    thread_id text NOT NULL DEFAULT '',
    group_source_id text NOT NULL,
    next_ingress_seq bigint NOT NULL DEFAULT 1,
    classified_through_seq bigint NOT NULL DEFAULT 0,
    context_applied_seq bigint NOT NULL DEFAULT 0,
    context_revision bigint NOT NULL DEFAULT 0,
    context_view_revision bigint NOT NULL DEFAULT 0,
    mailbox_revision bigint NOT NULL DEFAULT 0,
    processed_mailbox_revision bigint NOT NULL DEFAULT 0,
    next_response_seq bigint NOT NULL DEFAULT 1,
    actor_id text,
    actor_epoch bigint NOT NULL DEFAULT 0,
    actor_host_incarnation text,
    actor_expires_at timestamptz,
    active_session_id text,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,conversation_id),
    UNIQUE (bot_id,binding_id,platform,channel_kind,channel_id,thread_id),
    FOREIGN KEY (bot_id,binding_id) REFERENCES public.bot_bindings(bot_id,binding_id),
    CHECK (0 <= context_applied_seq AND context_applied_seq <= classified_through_seq
           AND classified_through_seq < next_ingress_seq),
    CHECK (processed_mailbox_revision <= mailbox_revision)
);
CREATE TABLE public.bot_message_inbox (
    bot_id text NOT NULL,
    conversation_id text NOT NULL,
    event_id text NOT NULL,
    ingress_seq bigint NOT NULL,
    platform_message_id text NOT NULL,
    source_event_id text NOT NULL,
    input_digest text NOT NULL,
    author jsonb NOT NULL,
    segments jsonb NOT NULL,
    sent_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    trigger_decision text NOT NULL DEFAULT 'PENDING'
        CHECK (trigger_decision IN ('PENDING','TRIGGER','NO_TRIGGER','CONTROL_COMMAND','FAILED')),
    classification_digest text,
    rule_revision text,
    classification_reason text,
    classification_deadline timestamptz NOT NULL DEFAULT clock_timestamp()+interval '60 seconds',
    context_disposition text NOT NULL DEFAULT 'READABLE'
        CHECK (context_disposition IN ('READABLE','RESTRICTED','INVALID')),
    estimated_tokens bigint NOT NULL,
    PRIMARY KEY (bot_id,event_id),
    UNIQUE (bot_id,conversation_id,ingress_seq),
    UNIQUE (bot_id,conversation_id,platform_message_id),
    FOREIGN KEY (bot_id,conversation_id) REFERENCES public.bot_conversations(bot_id,conversation_id)
);
CREATE INDEX bot_message_classification_pending ON public.bot_message_inbox
    (bot_id,classification_deadline) WHERE trigger_decision='PENDING';
CREATE TABLE public.bot_inbound_media_refs (
    bot_id text NOT NULL,
    media_ref text NOT NULL,
    event_id text NOT NULL,
    part_index integer NOT NULL,
    source jsonb NOT NULL,
    status text NOT NULL DEFAULT 'PENDING_FETCH',
    PRIMARY KEY (bot_id,media_ref),
    UNIQUE (bot_id,event_id,part_index),
    FOREIGN KEY (bot_id,event_id) REFERENCES public.bot_message_inbox(bot_id,event_id)
);
CREATE TABLE public.bot_conversation_sessions (
    bot_id text NOT NULL,
    session_id text NOT NULL,
    conversation_id text NOT NULL,
    status text NOT NULL CHECK (status IN ('RUNNING','READY_NEXT','WAIT_INPUT','DRAINING','CLOSED')),
    active_run_id text,
    session_epoch bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    closed_at timestamptz,
    PRIMARY KEY (bot_id,session_id),
    FOREIGN KEY (bot_id,conversation_id) REFERENCES public.bot_conversations(bot_id,conversation_id)
);
CREATE UNIQUE INDEX bot_one_live_session ON public.bot_conversation_sessions
    (bot_id,conversation_id) WHERE status <> 'CLOSED';
CREATE TABLE public.bot_conversation_runs (
    bot_id text NOT NULL,
    run_id text NOT NULL,
    session_id text NOT NULL,
    conversation_id text NOT NULL,
    status text NOT NULL DEFAULT 'RUNNING' CHECK (status IN ('RUNNING','COMPLETED','CANCELLED','FAILED')),
    input_revision bigint NOT NULL DEFAULT 0,
    selected_input_end_seq bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (bot_id,run_id),
    FOREIGN KEY (bot_id,session_id) REFERENCES public.bot_conversation_sessions(bot_id,session_id)
);
CREATE TABLE public.bot_conversation_triggers (
    bot_id text NOT NULL,
    trigger_id text NOT NULL,
    conversation_id text NOT NULL,
    event_id text NOT NULL,
    ingress_seq bigint NOT NULL,
    principal_ref text NOT NULL,
    status text NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','CLAIMED','ANSWERED','FAILED','CANCELLED')),
    claimed_session_id text,
    claimed_run_id text,
    input_batch_id text,
    answered_reply_id text,
    terminal_reason text,
    PRIMARY KEY (bot_id,trigger_id),
    UNIQUE (bot_id,event_id),
    FOREIGN KEY (bot_id,event_id) REFERENCES public.bot_message_inbox(bot_id,event_id)
);
CREATE INDEX bot_pending_triggers ON public.bot_conversation_triggers
    (bot_id,conversation_id,status,ingress_seq);
CREATE TABLE public.bot_context_input_batches (
    bot_id text NOT NULL,
    input_batch_id text NOT NULL,
    operation_id text NOT NULL,
    conversation_id text NOT NULL,
    session_id text,
    run_id text,
    kind text NOT NULL CHECK (kind IN ('TRIGGER_INPUT','SILENT_DRAIN','RECOVERY_CLAIM')),
    start_seq bigint,
    end_seq bigint,
    trigger_cut_seq bigint,
    selected_input_end_seq bigint NOT NULL,
    trigger_ids jsonb NOT NULL,
    context_revision bigint NOT NULL,
    input_revision bigint,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,input_batch_id),
    UNIQUE (bot_id,conversation_id,operation_id),
    FOREIGN KEY (bot_id,conversation_id) REFERENCES public.bot_conversations(bot_id,conversation_id),
    CHECK ((start_seq IS NULL AND end_seq IS NULL) OR (start_seq > 0 AND end_seq >= start_seq))
);
CREATE TABLE public.bot_context_units (
    bot_id text NOT NULL,
    conversation_id text NOT NULL,
    unit_id text NOT NULL,
    context_revision bigint NOT NULL,
    unit_index bigint NOT NULL,
    kind text NOT NULL CHECK (kind IN ('INPUT','REPLY')),
    event_id text,
    input_batch_id text,
    reply_id text,
    visible boolean NOT NULL DEFAULT true,
    estimated_tokens bigint NOT NULL,
    PRIMARY KEY (bot_id,unit_id),
    UNIQUE (bot_id,conversation_id,context_revision,unit_index),
    UNIQUE (bot_id,event_id),
    UNIQUE (bot_id,reply_id),
    FOREIGN KEY (bot_id,conversation_id) REFERENCES public.bot_conversations(bot_id,conversation_id)
);
CREATE TABLE public.bot_context_compactions (
    bot_id text NOT NULL,
    conversation_id text NOT NULL,
    view_revision bigint NOT NULL,
    context_revision bigint NOT NULL,
    policy_revision text NOT NULL,
    token_budget bigint NOT NULL,
    archive_through_seq bigint NOT NULL,
    source_manifest_digest text NOT NULL,
    removed_unit_ids jsonb NOT NULL,
    retained_unit_ids jsonb NOT NULL,
    PRIMARY KEY (bot_id,conversation_id,view_revision)
);
CREATE TABLE public.bot_conversation_replies (
    bot_id text NOT NULL,
    reply_id text NOT NULL,
    operation_id text NOT NULL,
    conversation_id text NOT NULL,
    run_id text NOT NULL,
    input_revision bigint NOT NULL,
    response_seq bigint NOT NULL,
    parts jsonb NOT NULL,
    input_digest text NOT NULL,
    delivery jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,reply_id),
    UNIQUE (bot_id,conversation_id,operation_id),
    UNIQUE (bot_id,run_id),
    UNIQUE (bot_id,conversation_id,response_seq),
    FOREIGN KEY (bot_id,run_id) REFERENCES public.bot_conversation_runs(bot_id,run_id)
);
CREATE TRIGGER bot_input_batches_immutable BEFORE UPDATE OR DELETE ON public.bot_context_input_batches
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_context_compactions_immutable BEFORE UPDATE OR DELETE ON public.bot_context_compactions
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_conversation_replies_immutable BEFORE UPDATE OR DELETE ON public.bot_conversation_replies
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE FUNCTION public.forest_inbox_source_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Inbox source deletion requires a dedicated retention migration' USING ERRCODE='55000';
    END IF;
    IF ROW(NEW.bot_id,NEW.conversation_id,NEW.event_id,NEW.ingress_seq,NEW.platform_message_id,
           NEW.source_event_id,NEW.input_digest,NEW.author,NEW.segments,NEW.sent_at,NEW.received_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.conversation_id,OLD.event_id,OLD.ingress_seq,OLD.platform_message_id,
           OLD.source_event_id,OLD.input_digest,OLD.author,OLD.segments,OLD.sent_at,OLD.received_at) THEN
        RAISE EXCEPTION 'Inbox source identity and content are immutable' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER bot_message_source_immutable BEFORE UPDATE OR DELETE ON public.bot_message_inbox
    FOR EACH ROW EXECUTE FUNCTION public.forest_inbox_source_immutable();
