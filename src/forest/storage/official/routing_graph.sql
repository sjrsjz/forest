CREATE TABLE route_publications (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    graph_id text NOT NULL,
    routing_generation_id text NOT NULL,
    graph_digest text NOT NULL CHECK (graph_digest ~ '^sha256:[a-f0-9]{64}$'),
    directory_digest text NOT NULL CHECK (directory_digest ~ '^sha256:[a-f0-9]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,graph_id),
    UNIQUE (bot_id,routing_generation_id)
);

CREATE TABLE event_publication_routes (
    bot_id text NOT NULL,
    graph_id text NOT NULL,
    publisher_component_id text NOT NULL,
    publisher_generation_id text NOT NULL,
    topic text NOT NULL,
    contract text NOT NULL CHECK (contract ~ '^[1-9][0-9]*\.[0-9]+$'),
    event_schema_digest text NOT NULL CHECK (event_schema_digest ~ '^sha256:[a-f0-9]{64}$'),
    PRIMARY KEY (bot_id,graph_id,publisher_component_id,topic,contract),
    FOREIGN KEY (bot_id,graph_id) REFERENCES route_publications(bot_id,graph_id)
);

CREATE TABLE event_subscription_routes (
    bot_id text NOT NULL,
    graph_id text NOT NULL,
    subscriber_id text NOT NULL,
    contract_major integer NOT NULL CHECK (contract_major > 0),
    topic text NOT NULL,
    contract text NOT NULL CHECK (contract ~ '^[1-9][0-9]*\.[0-9]+$'),
    event_schema_digest text NOT NULL CHECK (event_schema_digest ~ '^sha256:[a-f0-9]{64}$'),
    owner_component_id text NOT NULL,
    owner_generation_id text NOT NULL,
    handler_service_id text NOT NULL,
    handler_service_version integer NOT NULL CHECK (handler_service_version > 0),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 32),
    PRIMARY KEY (bot_id,graph_id,subscriber_id,contract_major),
    FOREIGN KEY (bot_id,graph_id) REFERENCES route_publications(bot_id,graph_id),
    UNIQUE (bot_id,graph_id,topic,contract,subscriber_id)
);

CREATE INDEX event_subscription_routes_topic ON event_subscription_routes(
    bot_id,graph_id,topic,contract,subscriber_id
);

CREATE TABLE component_job_routes (
    bot_id text NOT NULL,
    graph_id text NOT NULL,
    owner_component_id text NOT NULL,
    owner_generation_id text NOT NULL,
    handler_id text NOT NULL,
    input_schema_digest text NOT NULL CHECK (input_schema_digest ~ '^sha256:[a-f0-9]{64}$'),
    handler_service_id text NOT NULL,
    handler_service_version integer NOT NULL CHECK (handler_service_version > 0),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 32),
    timeout_seconds double precision NOT NULL CHECK (
        timeout_seconds > 0 AND timeout_seconds <= 120
    ),
    max_concurrency integer NOT NULL CHECK (max_concurrency BETWEEN 1 AND 16),
    PRIMARY KEY (bot_id,graph_id,owner_component_id,handler_id),
    FOREIGN KEY (bot_id,graph_id) REFERENCES route_publications(bot_id,graph_id)
);

CREATE TABLE component_events (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    event_id text NOT NULL,
    operation_id text NOT NULL,
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    publisher_component_id text NOT NULL,
    publisher_generation_id text NOT NULL,
    graph_id text NOT NULL,
    topic text NOT NULL,
    contract text NOT NULL CHECK (contract ~ '^[1-9][0-9]*\.[0-9]+$'),
    event_schema_digest text NOT NULL CHECK (event_schema_digest ~ '^sha256:[a-f0-9]{64}$'),
    payload_id text NOT NULL,
    payload_digest text NOT NULL CHECK (payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    causal_root_kind text NOT NULL,
    causal_root_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,event_id),
    UNIQUE (bot_id,publisher_component_id,operation_id),
    FOREIGN KEY (bot_id,graph_id) REFERENCES route_publications(bot_id,graph_id)
);

CREATE INDEX component_events_causal_root ON component_events(
    bot_id,causal_root_kind,causal_root_id,created_at,event_id
);

CREATE TABLE event_deliveries (
    bot_id text NOT NULL,
    delivery_id text NOT NULL,
    event_id text NOT NULL,
    subscriber_id text NOT NULL,
    contract_major integer NOT NULL CHECK (contract_major > 0),
    topic text NOT NULL,
    contract text NOT NULL,
    event_schema_digest text NOT NULL,
    owner_component_id text NOT NULL,
    handler_service_id text NOT NULL,
    handler_service_version integer NOT NULL CHECK (handler_service_version > 0),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 32),
    state text NOT NULL DEFAULT 'QUEUED' CHECK (
        state IN ('QUEUED','CLAIMED','RETRY_WAIT','SUCCEEDED','DEAD_LETTER','UNKNOWN')
    ),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND max_attempts),
    claim_token uuid,
    target_generation_id text,
    active_attempt_id text,
    lease_until timestamptz,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    error_code text,
    completed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,delivery_id),
    UNIQUE (bot_id,event_id,subscriber_id,contract_major),
    FOREIGN KEY (bot_id,event_id) REFERENCES component_events(bot_id,event_id),
    CHECK (
        (state='CLAIMED' AND claim_token IS NOT NULL AND target_generation_id IS NOT NULL
            AND active_attempt_id IS NOT NULL AND lease_until IS NOT NULL
            AND completed_at IS NULL)
        OR (state<>'CLAIMED' AND claim_token IS NULL AND target_generation_id IS NULL
            AND active_attempt_id IS NULL AND lease_until IS NULL)
    ),
    CHECK (
        (state IN ('SUCCEEDED','DEAD_LETTER','UNKNOWN') AND completed_at IS NOT NULL)
        OR (state IN ('QUEUED','CLAIMED','RETRY_WAIT') AND completed_at IS NULL)
    )
);

CREATE INDEX event_deliveries_claim ON event_deliveries(
    bot_id,state,available_at,updated_at,delivery_id
) WHERE state IN ('QUEUED','RETRY_WAIT','CLAIMED');

CREATE TABLE event_delivery_attempts (
    bot_id text NOT NULL,
    attempt_id text NOT NULL,
    delivery_id text NOT NULL,
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    claim_token uuid NOT NULL,
    target_generation_id text NOT NULL,
    operation_id text NOT NULL,
    state text NOT NULL CHECK (state IN ('DISPATCHING','SUCCEEDED','FAILED','UNKNOWN')),
    error_code text,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (bot_id,attempt_id),
    UNIQUE (bot_id,delivery_id,attempt_no),
    FOREIGN KEY (bot_id,delivery_id) REFERENCES event_deliveries(bot_id,delivery_id),
    CHECK (
        (state='DISPATCHING' AND error_code IS NULL AND completed_at IS NULL)
        OR (state='SUCCEEDED' AND error_code IS NULL AND completed_at IS NOT NULL)
        OR (state IN ('FAILED','UNKNOWN') AND error_code IS NOT NULL
            AND completed_at IS NOT NULL)
    )
);

CREATE TABLE component_jobs (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    job_id text NOT NULL,
    operation_id text NOT NULL,
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    owner_component_id text NOT NULL,
    scheduled_by_generation_id text NOT NULL,
    graph_id text NOT NULL,
    handler_id text NOT NULL,
    input_schema_digest text NOT NULL CHECK (input_schema_digest ~ '^sha256:[a-f0-9]{64}$'),
    input_payload_id text NOT NULL,
    input_payload_digest text NOT NULL CHECK (input_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    causal_root_kind text NOT NULL,
    causal_root_id text NOT NULL,
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 32),
    timeout_seconds double precision NOT NULL CHECK (
        timeout_seconds > 0 AND timeout_seconds <= 120
    ),
    max_concurrency integer NOT NULL CHECK (max_concurrency BETWEEN 1 AND 16),
    state text NOT NULL DEFAULT 'WAITING' CHECK (
        state IN ('WAITING','CLAIMED','RETRY_WAIT','SUCCEEDED','FAILED','CANCELLED','UNKNOWN')
    ),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND max_attempts),
    claim_token uuid,
    target_generation_id text,
    handler_service_id text NOT NULL,
    handler_service_version integer NOT NULL CHECK (handler_service_version > 0),
    active_attempt_id text,
    lease_until timestamptz,
    available_at timestamptz NOT NULL,
    result_payload_id text,
    result_payload_digest text,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,job_id),
    UNIQUE (bot_id,owner_component_id,operation_id),
    FOREIGN KEY (bot_id,graph_id) REFERENCES route_publications(bot_id,graph_id),
    CHECK ((result_payload_id IS NULL) = (result_payload_digest IS NULL)),
    CHECK (
        (state='CLAIMED' AND claim_token IS NOT NULL AND target_generation_id IS NOT NULL
            AND active_attempt_id IS NOT NULL AND lease_until IS NOT NULL
            AND completed_at IS NULL)
        OR (state<>'CLAIMED' AND claim_token IS NULL AND target_generation_id IS NULL
            AND active_attempt_id IS NULL AND lease_until IS NULL)
    ),
    CHECK (
        (state='SUCCEEDED' AND result_payload_id IS NOT NULL AND completed_at IS NOT NULL)
        OR (state IN ('FAILED','CANCELLED','UNKNOWN') AND result_payload_id IS NULL
            AND completed_at IS NOT NULL)
        OR (state IN ('WAITING','CLAIMED','RETRY_WAIT') AND result_payload_id IS NULL
            AND completed_at IS NULL)
    )
);

CREATE INDEX component_jobs_claim ON component_jobs(
    bot_id,state,available_at,updated_at,job_id
) WHERE state IN ('WAITING','RETRY_WAIT','CLAIMED');

CREATE INDEX component_jobs_causal_root ON component_jobs(
    bot_id,causal_root_kind,causal_root_id,created_at,job_id
);

CREATE TABLE component_job_attempts (
    bot_id text NOT NULL,
    attempt_id text NOT NULL,
    job_id text NOT NULL,
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    claim_token uuid NOT NULL,
    target_generation_id text NOT NULL,
    operation_id text NOT NULL,
    state text NOT NULL CHECK (state IN ('DISPATCHING','SUCCEEDED','FAILED','UNKNOWN')),
    error_code text,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (bot_id,attempt_id),
    UNIQUE (bot_id,job_id,attempt_no),
    FOREIGN KEY (bot_id,job_id) REFERENCES component_jobs(bot_id,job_id),
    CHECK (
        (state='DISPATCHING' AND error_code IS NULL AND completed_at IS NULL)
        OR (state='SUCCEEDED' AND error_code IS NULL AND completed_at IS NOT NULL)
        OR (state IN ('FAILED','UNKNOWN') AND error_code IS NOT NULL
            AND completed_at IS NOT NULL)
    )
);

CREATE FUNCTION routing_immutable_fact() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    RAISE EXCEPTION 'Forest routing fact is immutable' USING ERRCODE='55000';
END;
$$;

CREATE TRIGGER route_publications_immutable BEFORE UPDATE OR DELETE ON route_publications
    FOR EACH ROW EXECUTE FUNCTION routing_immutable_fact();
CREATE TRIGGER event_publication_routes_immutable BEFORE UPDATE OR DELETE ON event_publication_routes
    FOR EACH ROW EXECUTE FUNCTION routing_immutable_fact();
CREATE TRIGGER event_subscription_routes_immutable BEFORE UPDATE OR DELETE ON event_subscription_routes
    FOR EACH ROW EXECUTE FUNCTION routing_immutable_fact();
CREATE TRIGGER component_job_routes_immutable BEFORE UPDATE OR DELETE ON component_job_routes
    FOR EACH ROW EXECUTE FUNCTION routing_immutable_fact();
CREATE TRIGGER component_events_immutable BEFORE UPDATE OR DELETE ON component_events
    FOR EACH ROW EXECUTE FUNCTION routing_immutable_fact();

CREATE FUNCTION event_delivery_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP='DELETE' OR ROW(
        OLD.bot_id,OLD.delivery_id,OLD.event_id,OLD.subscriber_id,OLD.contract_major,
        OLD.topic,OLD.contract,OLD.event_schema_digest,OLD.owner_component_id,
        OLD.handler_service_id,OLD.handler_service_version,OLD.max_attempts
    ) IS DISTINCT FROM ROW(
        NEW.bot_id,NEW.delivery_id,NEW.event_id,NEW.subscriber_id,NEW.contract_major,
        NEW.topic,NEW.contract,NEW.event_schema_digest,NEW.owner_component_id,
        NEW.handler_service_id,NEW.handler_service_version,NEW.max_attempts
    ) THEN
        RAISE EXCEPTION 'Forest event delivery identity is immutable' USING ERRCODE='55000';
    END IF;
    IF NEW.revision<>OLD.revision+1 OR NOT (
        (OLD.state IN ('QUEUED','RETRY_WAIT') AND NEW.state='CLAIMED')
        OR (OLD.state='CLAIMED' AND NEW.state IN
            ('RETRY_WAIT','SUCCEEDED','DEAD_LETTER','UNKNOWN'))
    ) THEN
        RAISE EXCEPTION 'Illegal Forest event delivery transition' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION component_job_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP='DELETE' OR ROW(
        OLD.bot_id,OLD.job_id,OLD.operation_id,OLD.input_digest,
        OLD.owner_component_id,OLD.scheduled_by_generation_id,OLD.graph_id,
        OLD.handler_id,OLD.input_schema_digest,OLD.input_payload_id,
        OLD.input_payload_digest,OLD.causal_root_kind,OLD.causal_root_id,
        OLD.max_attempts,OLD.timeout_seconds,OLD.max_concurrency,
        OLD.handler_service_id,OLD.handler_service_version,OLD.created_at
    ) IS DISTINCT FROM ROW(
        NEW.bot_id,NEW.job_id,NEW.operation_id,NEW.input_digest,
        NEW.owner_component_id,NEW.scheduled_by_generation_id,NEW.graph_id,
        NEW.handler_id,NEW.input_schema_digest,NEW.input_payload_id,
        NEW.input_payload_digest,NEW.causal_root_kind,NEW.causal_root_id,
        NEW.max_attempts,NEW.timeout_seconds,NEW.max_concurrency,
        NEW.handler_service_id,NEW.handler_service_version,NEW.created_at
    ) THEN
        RAISE EXCEPTION 'Forest ComponentJob identity is immutable' USING ERRCODE='55000';
    END IF;
    IF NEW.revision<>OLD.revision+1 OR NOT (
        (OLD.state IN ('WAITING','RETRY_WAIT') AND NEW.state IN ('CLAIMED','CANCELLED'))
        OR (OLD.state='CLAIMED' AND NEW.state IN
            ('RETRY_WAIT','SUCCEEDED','FAILED','CANCELLED','UNKNOWN'))
    ) THEN
        RAISE EXCEPTION 'Illegal Forest ComponentJob transition' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION event_delivery_attempt_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP='DELETE' OR ROW(
        OLD.bot_id,OLD.attempt_id,OLD.delivery_id,OLD.attempt_no,OLD.claim_token,
        OLD.target_generation_id,OLD.operation_id,OLD.started_at
    ) IS DISTINCT FROM ROW(
        NEW.bot_id,NEW.attempt_id,NEW.delivery_id,NEW.attempt_no,NEW.claim_token,
        NEW.target_generation_id,NEW.operation_id,NEW.started_at
    ) OR OLD.state<>'DISPATCHING'
      OR NEW.state NOT IN ('SUCCEEDED','FAILED','UNKNOWN') THEN
        RAISE EXCEPTION 'Illegal Forest event attempt transition' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION component_job_attempt_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP='DELETE' OR ROW(
        OLD.bot_id,OLD.attempt_id,OLD.job_id,OLD.attempt_no,OLD.claim_token,
        OLD.target_generation_id,OLD.operation_id,OLD.started_at
    ) IS DISTINCT FROM ROW(
        NEW.bot_id,NEW.attempt_id,NEW.job_id,NEW.attempt_no,NEW.claim_token,
        NEW.target_generation_id,NEW.operation_id,NEW.started_at
    ) OR OLD.state<>'DISPATCHING'
      OR NEW.state NOT IN ('SUCCEEDED','FAILED','UNKNOWN') THEN
        RAISE EXCEPTION 'Illegal Forest ComponentJob attempt transition' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER event_deliveries_guard BEFORE UPDATE OR DELETE ON event_deliveries
    FOR EACH ROW EXECUTE FUNCTION event_delivery_guard();
CREATE TRIGGER component_jobs_guard BEFORE UPDATE OR DELETE ON component_jobs
    FOR EACH ROW EXECUTE FUNCTION component_job_guard();
CREATE TRIGGER event_delivery_attempts_guard
    BEFORE UPDATE OR DELETE ON event_delivery_attempts
    FOR EACH ROW EXECUTE FUNCTION event_delivery_attempt_guard();
CREATE TRIGGER component_job_attempts_guard
    BEFORE UPDATE OR DELETE ON component_job_attempts
    FOR EACH ROW EXECUTE FUNCTION component_job_attempt_guard();
