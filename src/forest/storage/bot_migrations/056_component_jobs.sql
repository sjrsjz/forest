-- Durable plugin-owned work. This domain is independent of conversation Agent
-- schedules: it invokes a declared component handler and carries no chat authority.
CREATE TABLE public.bot_generation_job_definitions (
    bot_id text NOT NULL,
    generation_id text NOT NULL,
    component_id text NOT NULL,
    handler_id text NOT NULL,
    schema_digest text NOT NULL,
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 32),
    timeout_seconds double precision NOT NULL CHECK (timeout_seconds > 0 AND timeout_seconds <= 120),
    max_concurrency integer NOT NULL CHECK (max_concurrency BETWEEN 1 AND 16),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,generation_id,handler_id),
    FOREIGN KEY (bot_id,generation_id)
        REFERENCES public.bot_generations(bot_id,generation_id)
);

CREATE TABLE public.bot_component_job_heads (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    component_id text NOT NULL,
    handler_id text NOT NULL,
    generation_id text NOT NULL,
    schema_digest text NOT NULL,
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 32),
    timeout_seconds double precision NOT NULL CHECK (timeout_seconds > 0 AND timeout_seconds <= 120),
    max_concurrency integer NOT NULL CHECK (max_concurrency BETWEEN 1 AND 16),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,component_id,handler_id),
    FOREIGN KEY (bot_id,generation_id)
        REFERENCES public.bot_generations(bot_id,generation_id)
);

CREATE TABLE public.bot_component_job_clock (
    bot_id text PRIMARY KEY REFERENCES public.forest_bot_identity(bot_id),
    next_sequence bigint NOT NULL DEFAULT 0 CHECK (next_sequence >= 0)
);

CREATE TABLE public.bot_component_jobs (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    job_id text NOT NULL,
    job_seq bigint NOT NULL CHECK (job_seq > 0),
    component_id text NOT NULL,
    handler_id text NOT NULL,
    source_generation_id text NOT NULL,
    source_call_id text NOT NULL,
    operation_id text NOT NULL,
    input_digest text NOT NULL,
    payload_id text NOT NULL,
    payload_digest text NOT NULL,
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 32),
    first_available_at timestamptz NOT NULL,
    status text NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','INFLIGHT','RETRY_WAIT','ACKED','DEAD')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND 32),
    dispatch_count bigint NOT NULL DEFAULT 0 CHECK (dispatch_count >= attempt_count),
    available_at timestamptz NOT NULL,
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
    CONSTRAINT bot_component_job_claim_shape CHECK (
        (status='INFLIGHT') =
        (claim_token IS NOT NULL AND claim_host_incarnation IS NOT NULL
         AND claim_owner_epoch IS NOT NULL AND claim_until IS NOT NULL
         AND claim_new_attempt IS NOT NULL)
    ),
    CONSTRAINT bot_component_job_ack_shape CHECK (
        (status='ACKED') = (result_payload_id IS NOT NULL AND acked_at IS NOT NULL)
    ),
    PRIMARY KEY (bot_id,job_id),
    UNIQUE (bot_id,job_seq),
    UNIQUE (bot_id,component_id,operation_id),
    FOREIGN KEY (bot_id,source_generation_id)
        REFERENCES public.bot_generations(bot_id,generation_id),
    FOREIGN KEY (bot_id,source_call_id) REFERENCES public.bot_calls(bot_id,call_id),
    FOREIGN KEY (bot_id,payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
    FOREIGN KEY (bot_id,result_payload_id)
        REFERENCES public.bot_payload_headers(bot_id,payload_id)
);

CREATE INDEX bot_component_jobs_ready
    ON public.bot_component_jobs(bot_id,available_at,job_seq,job_id)
    WHERE status IN ('PENDING','RETRY_WAIT');
CREATE INDEX bot_component_jobs_expired
    ON public.bot_component_jobs(bot_id,claim_until,job_seq,job_id)
    WHERE status='INFLIGHT';
CREATE INDEX bot_component_jobs_handler_open
    ON public.bot_component_jobs(bot_id,component_id,handler_id,job_seq)
    WHERE status NOT IN ('ACKED','DEAD');

CREATE TRIGGER bot_generation_job_definitions_immutable
    BEFORE UPDATE OR DELETE ON public.bot_generation_job_definitions
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE FUNCTION public.forest_component_job_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(OLD.bot_id,OLD.job_id,OLD.job_seq,OLD.component_id,OLD.handler_id,
           OLD.source_generation_id,OLD.source_call_id,OLD.operation_id,OLD.input_digest,
           OLD.payload_id,OLD.payload_digest,OLD.max_attempts,OLD.first_available_at,OLD.created_at)
       IS DISTINCT FROM
       ROW(NEW.bot_id,NEW.job_id,NEW.job_seq,NEW.component_id,NEW.handler_id,
           NEW.source_generation_id,NEW.source_call_id,NEW.operation_id,NEW.input_digest,
           NEW.payload_id,NEW.payload_digest,NEW.max_attempts,NEW.first_available_at,NEW.created_at) THEN
        RAISE EXCEPTION 'Forest component job identity is immutable' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER bot_component_job_identity BEFORE UPDATE ON public.bot_component_jobs
    FOR EACH ROW EXECUTE FUNCTION public.forest_component_job_identity();
