-- Durable component facts and per-subscriber delivery state. Payload bodies live
-- in the removable payload store; these rows retain identities and digests.
CREATE TABLE public.bot_generation_event_definitions (
    bot_id text NOT NULL,
    generation_id text NOT NULL,
    component_id text NOT NULL,
    direction text NOT NULL CHECK (direction IN ('PUBLISH','SUBSCRIBE')),
    definition_id text NOT NULL,
    topic text NOT NULL,
    contract_major integer NOT NULL CHECK (contract_major > 0),
    contract_minor integer NOT NULL CHECK (contract_minor >= 0),
    schema_digest text NOT NULL,
    max_attempts integer CHECK (max_attempts BETWEEN 1 AND 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,generation_id,direction,definition_id),
    FOREIGN KEY (bot_id,generation_id) REFERENCES public.bot_generations(bot_id,generation_id)
);

CREATE TABLE public.bot_event_subscription_heads (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    subscriber_id text NOT NULL,
    contract_major integer NOT NULL CHECK (contract_major > 0),
    component_id text NOT NULL,
    generation_id text NOT NULL,
    topic text NOT NULL,
    contract_minor integer NOT NULL CHECK (contract_minor >= 0),
    schema_digest text NOT NULL,
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 32),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,subscriber_id,contract_major),
    FOREIGN KEY (bot_id,generation_id) REFERENCES public.bot_generations(bot_id,generation_id)
);

CREATE TABLE public.bot_event_clock (
    bot_id text PRIMARY KEY REFERENCES public.forest_bot_identity(bot_id),
    next_sequence bigint NOT NULL DEFAULT 0 CHECK (next_sequence >= 0)
);

CREATE TABLE public.bot_event_contracts (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    topic text NOT NULL,
    contract_major integer NOT NULL CHECK (contract_major > 0),
    contract_minor integer NOT NULL CHECK (contract_minor >= 0),
    schema_digest text NOT NULL,
    first_generation_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,topic,contract_major,contract_minor),
    FOREIGN KEY (bot_id,first_generation_id)
        REFERENCES public.bot_generations(bot_id,generation_id)
);

CREATE TABLE public.bot_component_events (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    event_id text NOT NULL,
    event_seq bigint NOT NULL CHECK (event_seq > 0),
    topic text NOT NULL,
    contract_major integer NOT NULL CHECK (contract_major > 0),
    contract_minor integer NOT NULL CHECK (contract_minor >= 0),
    schema_digest text NOT NULL,
    publisher_component_id text NOT NULL,
    publisher_generation_id text NOT NULL,
    publisher_call_id text NOT NULL,
    operation_id text NOT NULL,
    input_digest text NOT NULL,
    payload_id text NOT NULL,
    payload_digest text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,event_id),
    UNIQUE (bot_id,event_seq),
    UNIQUE (bot_id,event_id,event_seq),
    UNIQUE (bot_id,publisher_component_id,operation_id),
    FOREIGN KEY (bot_id,publisher_generation_id)
        REFERENCES public.bot_generations(bot_id,generation_id),
    FOREIGN KEY (bot_id,publisher_call_id) REFERENCES public.bot_calls(bot_id,call_id),
    FOREIGN KEY (bot_id,payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id)
);

CREATE TABLE public.bot_event_deliveries (
    bot_id text NOT NULL,
    delivery_id text NOT NULL,
    event_id text NOT NULL,
    event_seq bigint NOT NULL CHECK (event_seq > 0),
    subscriber_id text NOT NULL,
    contract_major integer NOT NULL CHECK (contract_major > 0),
    component_id text NOT NULL,
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 32),
    status text NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','INFLIGHT','RETRY_WAIT','ACKED','DEAD')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND 32),
    dispatch_count bigint NOT NULL DEFAULT 0 CHECK (dispatch_count >= attempt_count),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claim_token text,
    claim_host_incarnation text,
    claim_owner_epoch bigint,
    claim_until timestamptz,
    claim_new_attempt boolean,
    call_operation_id text,
    result_payload_id text,
    last_error_code text,
    acked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT bot_event_delivery_claim_shape CHECK (
        (status='INFLIGHT') =
        (claim_token IS NOT NULL AND claim_host_incarnation IS NOT NULL
         AND claim_owner_epoch IS NOT NULL AND claim_until IS NOT NULL
         AND claim_new_attempt IS NOT NULL)
    ),
    CONSTRAINT bot_event_delivery_ack_shape CHECK (
        (status='ACKED') =
        (result_payload_id IS NOT NULL AND acked_at IS NOT NULL)
    ),
    PRIMARY KEY (bot_id,delivery_id),
    UNIQUE (bot_id,event_id,subscriber_id,contract_major),
    FOREIGN KEY (bot_id,event_id,event_seq)
        REFERENCES public.bot_component_events(bot_id,event_id,event_seq),
    FOREIGN KEY (bot_id,result_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id)
);
CREATE INDEX bot_event_deliveries_ready
    ON public.bot_event_deliveries(bot_id,available_at,event_seq,delivery_id)
    WHERE status IN ('PENDING','RETRY_WAIT');
CREATE INDEX bot_event_deliveries_expired
    ON public.bot_event_deliveries(bot_id,claim_until,event_seq,delivery_id)
    WHERE status='INFLIGHT';
CREATE INDEX bot_event_deliveries_open_order
    ON public.bot_event_deliveries(bot_id,subscriber_id,contract_major,event_seq)
    WHERE status NOT IN ('ACKED','DEAD');

CREATE TRIGGER bot_generation_event_definitions_immutable
    BEFORE UPDATE OR DELETE ON public.bot_generation_event_definitions
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_event_contracts_immutable
    BEFORE UPDATE OR DELETE ON public.bot_event_contracts
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_component_events_immutable
    BEFORE UPDATE OR DELETE ON public.bot_component_events
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE FUNCTION public.forest_event_delivery_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(OLD.bot_id,OLD.delivery_id,OLD.event_id,OLD.event_seq,OLD.subscriber_id,
           OLD.contract_major,OLD.component_id,OLD.max_attempts,OLD.created_at)
       IS DISTINCT FROM
       ROW(NEW.bot_id,NEW.delivery_id,NEW.event_id,NEW.event_seq,NEW.subscriber_id,
           NEW.contract_major,NEW.component_id,NEW.max_attempts,NEW.created_at) THEN
        RAISE EXCEPTION 'Forest event delivery identity is immutable' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER bot_event_delivery_identity BEFORE UPDATE ON public.bot_event_deliveries
    FOR EACH ROW EXECUTE FUNCTION public.forest_event_delivery_identity();
