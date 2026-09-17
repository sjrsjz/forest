CREATE TABLE effect_operations (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    effect_operation_id text NOT NULL CHECK (octet_length(effect_operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    effect_kind text NOT NULL CHECK (octet_length(effect_kind) BETWEEN 1 AND 128),
    target_kind text NOT NULL CHECK (octet_length(target_kind) BETWEEN 1 AND 64),
    target_id text NOT NULL CHECK (octet_length(target_id) BETWEEN 1 AND 512),
    producer_component_id text NOT NULL
        CHECK (octet_length(producer_component_id) BETWEEN 1 AND 128),
    producer_generation_id text NOT NULL
        CHECK (octet_length(producer_generation_id) BETWEEN 1 AND 256),
    root_operation_kind text NOT NULL
        CHECK (octet_length(root_operation_kind) BETWEEN 1 AND 64),
    root_operation_id text NOT NULL
        CHECK (octet_length(root_operation_id) BETWEEN 1 AND 256),
    input_payload_id text,
    input_payload_digest text CHECK (
        input_payload_digest IS NULL OR input_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    state text NOT NULL DEFAULT 'PENDING' CHECK (
        state IN ('PENDING','PREPARED','DISPATCHING','SUCCEEDED',
                  'NOT_APPLIED','UNKNOWN','CANCELLED')
    ),
    current_attempt_id text,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, effect_operation_id),
    CHECK ((input_payload_id IS NULL) = (input_payload_digest IS NULL)),
    CHECK (
        (state IN ('PENDING','CANCELLED') AND current_attempt_id IS NULL)
        OR (state NOT IN ('PENDING','CANCELLED') AND current_attempt_id IS NOT NULL)
    )
);

CREATE TABLE effect_attempts (
    bot_id text NOT NULL,
    attempt_id text NOT NULL CHECK (octet_length(attempt_id) BETWEEN 1 AND 256),
    effect_operation_id text NOT NULL,
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    permit_operation_id text NOT NULL
        CHECK (octet_length(permit_operation_id) BETWEEN 1 AND 256),
    wire_payload_id text NOT NULL CHECK (octet_length(wire_payload_id) BETWEEN 1 AND 256),
    wire_payload_digest text NOT NULL CHECK (wire_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    authorization_ticket_id text NOT NULL
        CHECK (octet_length(authorization_ticket_id) BETWEEN 1 AND 256),
    authorization_action text NOT NULL
        CHECK (octet_length(authorization_action) BETWEEN 1 AND 200),
    authorization_resource_kind text NOT NULL
        CHECK (octet_length(authorization_resource_kind) BETWEEN 1 AND 64),
    authorization_resource_id text NOT NULL
        CHECK (octet_length(authorization_resource_id) BETWEEN 1 AND 512),
    authorization_plan_digest text CHECK (
        authorization_plan_digest IS NULL
        OR authorization_plan_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    authorization_policy_revision bigint NOT NULL CHECK (authorization_policy_revision > 0),
    executor_component_id text NOT NULL
        CHECK (octet_length(executor_component_id) BETWEEN 1 AND 128),
    executor_generation_id text NOT NULL
        CHECK (octet_length(executor_generation_id) BETWEEN 1 AND 256),
    fence_kind text NOT NULL CHECK (octet_length(fence_kind) BETWEEN 1 AND 64),
    fence_id text NOT NULL CHECK (octet_length(fence_id) BETWEEN 1 AND 256),
    fence_epoch bigint NOT NULL CHECK (fence_epoch > 0),
    prepared_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    deadline_at timestamptz NOT NULL,
    state text NOT NULL DEFAULT 'PREPARED' CHECK (
        state IN ('PREPARED','DISPATCHING','SUCCEEDED','NOT_APPLIED','UNKNOWN','ABANDONED')
    ),
    handoff_operation_id text CHECK (
        handoff_operation_id IS NULL OR octet_length(handoff_operation_id) BETWEEN 1 AND 256
    ),
    started_at timestamptz,
    settled_at timestamptz,
    resolved_at timestamptz,
    PRIMARY KEY (bot_id, attempt_id),
    UNIQUE (bot_id, effect_operation_id, attempt_no),
    UNIQUE (bot_id, permit_operation_id),
    FOREIGN KEY (bot_id, effect_operation_id)
        REFERENCES effect_operations(bot_id, effect_operation_id),
    CHECK (deadline_at > prepared_at AND deadline_at <= prepared_at + interval '30 seconds'),
    CHECK (
        (state = 'PREPARED' AND handoff_operation_id IS NULL AND started_at IS NULL
            AND settled_at IS NULL AND resolved_at IS NULL)
        OR (state = 'DISPATCHING' AND handoff_operation_id IS NOT NULL AND started_at IS NOT NULL
            AND settled_at IS NULL AND resolved_at IS NULL)
        OR (state IN ('SUCCEEDED','NOT_APPLIED') AND handoff_operation_id IS NOT NULL
            AND started_at IS NOT NULL AND settled_at IS NOT NULL)
        OR (state = 'UNKNOWN' AND handoff_operation_id IS NOT NULL AND started_at IS NOT NULL
            AND settled_at IS NOT NULL AND resolved_at IS NULL)
        OR (state = 'ABANDONED' AND handoff_operation_id IS NULL AND started_at IS NULL
            AND settled_at IS NOT NULL AND resolved_at IS NULL)
    )
);

CREATE TABLE effect_observations (
    bot_id text NOT NULL,
    observation_operation_id text NOT NULL
        CHECK (octet_length(observation_operation_id) BETWEEN 1 AND 256),
    attempt_id text NOT NULL,
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    evidence_kind text NOT NULL CHECK (octet_length(evidence_kind) BETWEEN 1 AND 64),
    observed_status text NOT NULL CHECK (
        observed_status IN ('SUCCEEDED','NOT_APPLIED','UNKNOWN')
    ),
    evidence_payload_id text,
    evidence_payload_digest text CHECK (
        evidence_payload_digest IS NULL
        OR evidence_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    accepted boolean NOT NULL,
    previous_state text NOT NULL,
    resulting_state text NOT NULL,
    conflict_code text,
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, observation_operation_id),
    FOREIGN KEY (bot_id, attempt_id) REFERENCES effect_attempts(bot_id, attempt_id),
    CHECK (previous_state IN ('DISPATCHING','SUCCEEDED','NOT_APPLIED','UNKNOWN')),
    CHECK (resulting_state IN ('SUCCEEDED','NOT_APPLIED','UNKNOWN')),
    CHECK ((evidence_payload_id IS NULL) = (evidence_payload_digest IS NULL)),
    CHECK ((accepted AND conflict_code IS NULL) OR (NOT accepted AND conflict_code IS NOT NULL))
);

CREATE TABLE attempt_abandonments (
    bot_id text NOT NULL,
    abandonment_operation_id text NOT NULL
        CHECK (octet_length(abandonment_operation_id) BETWEEN 1 AND 256),
    attempt_id text NOT NULL,
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    reason_code text NOT NULL CHECK (octet_length(reason_code) BETWEEN 1 AND 128),
    cancel_effect boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, abandonment_operation_id),
    UNIQUE (bot_id, attempt_id),
    FOREIGN KEY (bot_id, attempt_id) REFERENCES effect_attempts(bot_id, attempt_id)
);

CREATE INDEX effect_operations_state_idx ON effect_operations(bot_id, state, updated_at);
CREATE INDEX effect_attempts_state_idx ON effect_attempts(bot_id, state, deadline_at);
CREATE INDEX effect_observations_attempt_idx ON effect_observations(bot_id, attempt_id, observed_at);

CREATE FUNCTION effect_operation_guard() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP = 'DELETE' OR (
        to_jsonb(NEW) - ARRAY['state','current_attempt_id','attempt_count','revision','updated_at']
    ) IS DISTINCT FROM (
        to_jsonb(OLD) - ARRAY['state','current_attempt_id','attempt_count','revision','updated_at']
    ) OR NEW.revision <> OLD.revision + 1 THEN
        RAISE EXCEPTION 'Effect operation identity is immutable' USING ERRCODE = '55000';
    END IF;
    IF NOT (
        (OLD.state = 'PENDING' AND NEW.state = 'PREPARED')
        OR (OLD.state = 'PREPARED' AND NEW.state IN ('DISPATCHING','PENDING','CANCELLED'))
        OR (OLD.state = 'DISPATCHING' AND NEW.state IN ('SUCCEEDED','NOT_APPLIED','UNKNOWN'))
        OR (OLD.state = 'NOT_APPLIED' AND NEW.state = 'PREPARED')
        OR (OLD.state = 'UNKNOWN' AND NEW.state IN ('SUCCEEDED','NOT_APPLIED'))
    ) THEN
        RAISE EXCEPTION 'Illegal effect operation transition' USING ERRCODE = '55000';
    END IF;
    IF NEW.state = 'PREPARED' THEN
        IF NEW.current_attempt_id IS NULL OR NEW.current_attempt_id IS NOT DISTINCT FROM OLD.current_attempt_id
           OR NEW.attempt_count <> OLD.attempt_count + 1 THEN
            RAISE EXCEPTION 'Effect attempt fence did not advance' USING ERRCODE = '55000';
        END IF;
    ELSIF NEW.attempt_count <> OLD.attempt_count THEN
        RAISE EXCEPTION 'Effect attempt count changed outside prepare' USING ERRCODE = '55000';
    END IF;
    IF NEW.state <> 'PREPARED'
       AND NOT (OLD.state='PREPARED' AND NEW.state IN ('PENDING','CANCELLED'))
       AND NEW.current_attempt_id IS DISTINCT FROM OLD.current_attempt_id THEN
        RAISE EXCEPTION 'Effect current attempt changed outside prepare or abandon'
            USING ERRCODE = '55000';
    END IF;
    IF NEW.state = 'PREPARED' AND NOT EXISTS (
        SELECT 1 FROM effect_attempts a WHERE a.bot_id=NEW.bot_id
          AND a.attempt_id=NEW.current_attempt_id AND a.effect_operation_id=NEW.effect_operation_id
          AND a.state='PREPARED' AND a.attempt_no=NEW.attempt_count
    ) THEN
        RAISE EXCEPTION 'Prepared effect has no matching attempt' USING ERRCODE = '55000';
    END IF;
    IF NEW.state = 'DISPATCHING' AND NOT EXISTS (
        SELECT 1 FROM effect_attempts a WHERE a.bot_id=NEW.bot_id
          AND a.attempt_id=NEW.current_attempt_id AND a.state='DISPATCHING'
    ) THEN
        RAISE EXCEPTION 'Dispatching effect has no matching attempt' USING ERRCODE = '55000';
    END IF;
    IF NEW.state IN ('SUCCEEDED','NOT_APPLIED','UNKNOWN') AND NOT EXISTS (
        SELECT 1 FROM effect_attempts a WHERE a.bot_id=NEW.bot_id
          AND a.attempt_id=NEW.current_attempt_id AND a.state=NEW.state
    ) THEN
        RAISE EXCEPTION 'Settled effect has no matching attempt' USING ERRCODE = '55000';
    END IF;
    IF OLD.state='PREPARED' AND NEW.state IN ('PENDING','CANCELLED') AND NOT EXISTS (
        SELECT 1 FROM effect_attempts a JOIN attempt_abandonments x
          ON x.bot_id=a.bot_id AND x.attempt_id=a.attempt_id
        WHERE a.bot_id=NEW.bot_id AND a.attempt_id=OLD.current_attempt_id
          AND a.state='ABANDONED' AND x.cancel_effect=(NEW.state='CANCELLED')
    ) THEN
        RAISE EXCEPTION 'Abandoned effect has no matching fact' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER effect_operations_one_way BEFORE UPDATE OR DELETE ON effect_operations
    FOR EACH ROW EXECUTE FUNCTION effect_operation_guard();

CREATE FUNCTION effect_attempt_guard() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP = 'DELETE' OR (
        to_jsonb(NEW) - ARRAY['state','handoff_operation_id','started_at','settled_at','resolved_at']
    ) IS DISTINCT FROM (
        to_jsonb(OLD) - ARRAY['state','handoff_operation_id','started_at','settled_at','resolved_at']
    ) THEN
        RAISE EXCEPTION 'Effect attempt identity is immutable' USING ERRCODE = '55000';
    END IF;
    IF OLD.state = 'PREPARED' AND NEW.state = 'DISPATCHING'
       AND NEW.handoff_operation_id IS NOT NULL AND NEW.started_at IS NOT NULL
       AND NEW.settled_at IS NULL AND NEW.resolved_at IS NULL THEN
        RETURN NEW;
    END IF;
    IF OLD.state = 'PREPARED' AND NEW.state = 'ABANDONED'
       AND NEW.handoff_operation_id IS NULL AND NEW.started_at IS NULL
       AND NEW.settled_at IS NOT NULL AND NEW.resolved_at IS NULL
       AND EXISTS (SELECT 1 FROM attempt_abandonments a
           WHERE a.bot_id=NEW.bot_id AND a.attempt_id=NEW.attempt_id) THEN
        RETURN NEW;
    END IF;
    IF OLD.state = 'DISPATCHING' AND NEW.state IN ('SUCCEEDED','NOT_APPLIED','UNKNOWN')
       AND NEW.handoff_operation_id=OLD.handoff_operation_id
       AND NEW.started_at=OLD.started_at AND NEW.settled_at IS NOT NULL
       AND NEW.resolved_at IS NULL
       AND EXISTS (SELECT 1 FROM effect_observations o
           WHERE o.bot_id=NEW.bot_id AND o.attempt_id=NEW.attempt_id
             AND o.accepted AND o.resulting_state=NEW.state) THEN
        RETURN NEW;
    END IF;
    IF OLD.state = 'UNKNOWN' AND NEW.state IN ('SUCCEEDED','NOT_APPLIED')
       AND NEW.handoff_operation_id=OLD.handoff_operation_id
       AND NEW.started_at=OLD.started_at AND NEW.settled_at=OLD.settled_at
       AND NEW.resolved_at IS NOT NULL
       AND EXISTS (SELECT 1 FROM effect_observations o
           WHERE o.bot_id=NEW.bot_id AND o.attempt_id=NEW.attempt_id
             AND o.accepted AND o.previous_state='UNKNOWN' AND o.resulting_state=NEW.state) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'Illegal effect attempt transition' USING ERRCODE = '55000';
END;
$$;
CREATE TRIGGER effect_attempts_one_way BEFORE UPDATE OR DELETE ON effect_attempts
    FOR EACH ROW EXECUTE FUNCTION effect_attempt_guard();

CREATE FUNCTION reject_immutable_effect_fact() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    RAISE EXCEPTION 'Effect evidence is immutable' USING ERRCODE = '55000';
END;
$$;
CREATE TRIGGER effect_observations_immutable BEFORE UPDATE OR DELETE ON effect_observations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_effect_fact();
CREATE TRIGGER attempt_abandonments_immutable BEFORE UPDATE OR DELETE ON attempt_abandonments
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_effect_fact();
