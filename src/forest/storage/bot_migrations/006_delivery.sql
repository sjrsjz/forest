-- Delivery is a durable effect ledger.  Exact attempts and semantic routes are
-- immutable; only observations and explicit reconciliation change their state.
CREATE TABLE public.bot_delivery_roots (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    root_operation_id text NOT NULL,
    effects_cancelled boolean NOT NULL DEFAULT false,
    cancel_epoch bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (bot_id, root_operation_id)
);
CREATE TABLE public.bot_delivery_streams (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    stream_id text NOT NULL,
    binding_id text NOT NULL,
    lane text NOT NULL CHECK (lane IN ('CHAT','CONTROL')),
    last_seq bigint NOT NULL DEFAULT 0,
    last_selected_at timestamptz NOT NULL DEFAULT '1970-01-01 UTC',
    PRIMARY KEY (bot_id, stream_id)
);
CREATE TABLE public.bot_send_intents (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    intent_id text NOT NULL,
    operation_id text NOT NULL,
    input_digest text NOT NULL,
    root_operation_id text NOT NULL,
    cause_kind text NOT NULL CHECK (cause_kind IN (
        'SHELL_COMMAND','CONVERSATION_NOTICE','MESSAGE_HANDLER_CHAIN',
        'BUSINESS_COMMAND_REQUEST','CONVERSATION_RUN','KERNEL_CALL','DELIVERY_OPERATION'
    )),
    cause_id text NOT NULL,
    binding_id text NOT NULL,
    binding_epoch bigint NOT NULL,
    policy_revision bigint NOT NULL,
    storage_generation bigint NOT NULL,
    stream_id text NOT NULL,
    send_lane text NOT NULL CHECK (send_lane IN ('CHAT','CONTROL')),
    conversation_id text NOT NULL,
    response_seq bigint,
    command_id text,
    control_seq bigint,
    sequence bigint NOT NULL CHECK (sequence > 0),
    reply_route jsonb NOT NULL,
    adapter_contract text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, intent_id),
    UNIQUE (bot_id, operation_id),
    UNIQUE (bot_id, stream_id, sequence),
    FOREIGN KEY (bot_id, root_operation_id) REFERENCES public.bot_delivery_roots,
    FOREIGN KEY (bot_id, stream_id) REFERENCES public.bot_delivery_streams,
    CHECK ((send_lane='CHAT' AND response_seq=sequence AND command_id IS NULL AND control_seq IS NULL)
        OR (send_lane='CONTROL' AND control_seq=sequence AND command_id IS NOT NULL AND response_seq IS NULL))
);
CREATE TABLE public.bot_send_parts (
    bot_id text NOT NULL,
    intent_id text NOT NULL,
    part_index integer NOT NULL CHECK (part_index >= 0),
    part_operation_id text NOT NULL,
    payload jsonb NOT NULL,
    payload_digest text NOT NULL,
    state text NOT NULL DEFAULT 'PENDING' CHECK
        (state IN ('PENDING','CLAIMED','DISPATCHING','SENT','FAILED','UNKNOWN','CANCELLED','SKIPPED')),
    revision bigint NOT NULL DEFAULT 1,
    owner_epoch bigint NOT NULL DEFAULT 0,
    host_incarnation text,
    host_owner_epoch bigint,
    worker_id text,
    claim_binding_epoch bigint,
    claim_connection_epoch bigint,
    claim_binding_revision bigint,
    claim_until timestamptz,
    attempt_id text,
    attempt_count integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    error_code text,
    receipt jsonb,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, intent_id, part_index),
    UNIQUE (bot_id, part_operation_id),
    FOREIGN KEY (bot_id, intent_id) REFERENCES public.bot_send_intents
);
CREATE INDEX bot_send_intents_cause ON public.bot_send_intents(bot_id,cause_kind,cause_id);
CREATE INDEX bot_send_pending ON public.bot_send_parts (bot_id, available_at) WHERE state='PENDING';
CREATE TABLE public.bot_send_attempts (
    bot_id text NOT NULL,
    attempt_id text NOT NULL,
    intent_id text NOT NULL,
    part_index integer NOT NULL,
    owner_epoch bigint NOT NULL,
    host_incarnation text NOT NULL,
    host_owner_epoch bigint NOT NULL,
    policy_revision bigint NOT NULL,
    binding_epoch bigint NOT NULL,
    connection_epoch bigint NOT NULL,
    binding_revision bigint NOT NULL,
    cancel_epoch bigint NOT NULL,
    action text NOT NULL,
    wire_body bytea NOT NULL,
    wire_digest text NOT NULL,
    executor_ref jsonb NOT NULL,
    permitted_at timestamptz NOT NULL,
    deadline_at timestamptz NOT NULL,
    started_at timestamptz,
    state text NOT NULL DEFAULT 'DISPATCHING' CHECK (state IN ('DISPATCHING','SENT','NOT_SENT','UNKNOWN')),
    receipt jsonb,
    observed_at timestamptz,
    PRIMARY KEY (bot_id, attempt_id),
    FOREIGN KEY (bot_id, intent_id, part_index) REFERENCES public.bot_send_parts
);
CREATE TABLE public.bot_delivery_binding_limits (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    binding_id text NOT NULL,
    next_send_at timestamptz NOT NULL DEFAULT '1970-01-01 UTC',
    control_streak integer NOT NULL DEFAULT 0,
    PRIMARY KEY (bot_id, binding_id)
);
CREATE INDEX bot_send_receipt_echo ON public.bot_send_attempts
    (bot_id, (receipt->>'message_id')) WHERE state='SENT';
CREATE TABLE public.bot_delivery_resolutions (
    bot_id text NOT NULL,
    operation_id text NOT NULL,
    intent_id text NOT NULL,
    part_index integer NOT NULL,
    input_digest text NOT NULL,
    decision text NOT NULL CHECK (decision IN ('SENT','NOT_SENT','SKIP')),
    reason text NOT NULL,
    result jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, operation_id),
    FOREIGN KEY (bot_id, intent_id, part_index) REFERENCES public.bot_send_parts
);
CREATE FUNCTION public.bot_delivery_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME IN ('bot_send_intents','bot_delivery_resolutions') THEN
        RAISE EXCEPTION 'Delivery fact is immutable';
    ELSIF TG_TABLE_NAME='bot_send_parts' THEN
        IF (OLD.bot_id,OLD.intent_id,OLD.part_index,OLD.part_operation_id,OLD.payload,OLD.payload_digest)
           IS DISTINCT FROM
           (NEW.bot_id,NEW.intent_id,NEW.part_index,NEW.part_operation_id,NEW.payload,NEW.payload_digest) THEN
            RAISE EXCEPTION 'Delivery semantic part is immutable';
        END IF;
    ELSE
        IF (to_jsonb(OLD)-ARRAY['state','receipt','observed_at','started_at']) IS DISTINCT FROM
           (to_jsonb(NEW)-ARRAY['state','receipt','observed_at','started_at']) THEN
            RAISE EXCEPTION 'Delivery transport attempt is immutable';
        END IF;
        IF OLD.started_at IS NOT NULL AND NEW.started_at IS DISTINCT FROM OLD.started_at THEN
            RAISE EXCEPTION 'Delivery transport start is immutable';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER bot_send_intents_immutable BEFORE UPDATE OR DELETE ON public.bot_send_intents
    FOR EACH ROW EXECUTE FUNCTION public.bot_delivery_immutable();
CREATE TRIGGER bot_send_parts_immutable BEFORE UPDATE ON public.bot_send_parts
    FOR EACH ROW EXECUTE FUNCTION public.bot_delivery_immutable();
CREATE TRIGGER bot_send_attempts_immutable BEFORE UPDATE ON public.bot_send_attempts
    FOR EACH ROW EXECUTE FUNCTION public.bot_delivery_immutable();
CREATE TRIGGER bot_delivery_resolutions_immutable BEFORE UPDATE OR DELETE ON public.bot_delivery_resolutions
    FOR EACH ROW EXECUTE FUNCTION public.bot_delivery_immutable();
