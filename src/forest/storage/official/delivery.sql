CREATE TABLE delivery_lanes (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    binding_ref text NOT NULL CHECK (octet_length(binding_ref) BETWEEN 1 AND 256),
    lane_kind text NOT NULL CHECK (lane_kind IN ('CHAT','CONTROL')),
    ordering_key text NOT NULL CHECK (octet_length(ordering_key) BETWEEN 1 AND 256),
    next_sequence bigint NOT NULL DEFAULT 1 CHECK (next_sequence > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,binding_ref,lane_kind,ordering_key)
);

CREATE TABLE send_intents (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    intent_id text NOT NULL CHECK (octet_length(intent_id) BETWEEN 1 AND 256),
    create_operation_id text NOT NULL CHECK (
        octet_length(create_operation_id) BETWEEN 1 AND 256
    ),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    binding_ref text NOT NULL CHECK (octet_length(binding_ref) BETWEEN 1 AND 256),
    lane_kind text NOT NULL CHECK (lane_kind IN ('CHAT','CONTROL')),
    ordering_key text NOT NULL CHECK (octet_length(ordering_key) BETWEEN 1 AND 256),
    lane_sequence bigint NOT NULL CHECK (lane_sequence > 0),
    conversation_ref text,
    channel_kind text NOT NULL CHECK (channel_kind IN ('GROUP','PRIVATE')),
    channel_ref text NOT NULL CHECK (octet_length(channel_ref) BETWEEN 1 AND 256),
    cause_component_id text NOT NULL CHECK (
        octet_length(cause_component_id) BETWEEN 1 AND 128
    ),
    cause_kind text NOT NULL CHECK (octet_length(cause_kind) BETWEEN 1 AND 128),
    cause_id text NOT NULL CHECK (octet_length(cause_id) BETWEEN 1 AND 512),
    delivery_generation_id text NOT NULL CHECK (
        octet_length(delivery_generation_id) BETWEEN 1 AND 256
    ),
    reply_ref text,
    part_count integer NOT NULL CHECK (part_count BETWEEN 1 AND 64),
    current_part_index integer NOT NULL DEFAULT 0 CHECK (current_part_index BETWEEN 0 AND 63),
    state text NOT NULL DEFAULT 'QUEUED' CHECK (
        state IN ('QUEUED','SENDING','SENT','FAILED','UNKNOWN','CANCELLED')
    ),
    error_code text CHECK (error_code IS NULL OR octet_length(error_code) BETWEEN 1 AND 128),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (bot_id,intent_id),
    UNIQUE (bot_id,create_operation_id),
    UNIQUE (bot_id,binding_ref,lane_kind,ordering_key,lane_sequence),
    FOREIGN KEY (bot_id,binding_ref,lane_kind,ordering_key)
        REFERENCES delivery_lanes(bot_id,binding_ref,lane_kind,ordering_key),
    CHECK (conversation_ref IS NULL OR octet_length(conversation_ref) BETWEEN 1 AND 256),
    CHECK (reply_ref IS NULL OR octet_length(reply_ref) BETWEEN 1 AND 512),
    CHECK (
        (lane_kind='CHAT' AND conversation_ref IS NOT NULL AND reply_ref IS NOT NULL)
        OR (lane_kind='CONTROL' AND reply_ref IS NULL)
    ),
    CHECK (current_part_index < part_count),
    CHECK (
        (state IN ('QUEUED','SENDING','UNKNOWN') AND completed_at IS NULL)
        OR (state IN ('SENT','FAILED','CANCELLED') AND completed_at IS NOT NULL)
    )
);

CREATE TABLE send_parts (
    bot_id text NOT NULL,
    intent_id text NOT NULL,
    part_index integer NOT NULL CHECK (part_index BETWEEN 0 AND 63),
    part_operation_id text NOT NULL CHECK (
        octet_length(part_operation_id) BETWEEN 1 AND 256
    ),
    effect_operation_ref text NOT NULL CHECK (
        octet_length(effect_operation_ref) BETWEEN 1 AND 256
    ),
    semantic_digest text NOT NULL CHECK (semantic_digest ~ '^sha256:[a-f0-9]{64}$'),
    payload_id text NOT NULL CHECK (octet_length(payload_id) BETWEEN 1 AND 256),
    payload_digest text NOT NULL CHECK (payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    payload_owner_component_id text NOT NULL CHECK (
        octet_length(payload_owner_component_id) BETWEEN 1 AND 128
    ),
    payload_owner_kind text NOT NULL CHECK (
        octet_length(payload_owner_kind) BETWEEN 1 AND 128
    ),
    payload_owner_id text NOT NULL CHECK (octet_length(payload_owner_id) BETWEEN 1 AND 512),
    state text NOT NULL DEFAULT 'QUEUED' CHECK (
        state IN ('QUEUED','CLAIMED','PREPARED','DISPATCHING','SENT','FAILED','UNKNOWN','CANCELLED')
    ),
    owner_id text,
    owner_epoch bigint NOT NULL DEFAULT 0 CHECK (owner_epoch >= 0),
    owner_expires_at timestamptz,
    current_attempt_id text,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,intent_id,part_index),
    UNIQUE (bot_id,part_operation_id),
    UNIQUE (bot_id,effect_operation_ref),
    FOREIGN KEY (bot_id,intent_id) REFERENCES send_intents(bot_id,intent_id),
    CHECK (owner_id IS NULL OR octet_length(owner_id) BETWEEN 1 AND 256),
    CHECK (
        (state IN ('CLAIMED','PREPARED','DISPATCHING') AND owner_id IS NOT NULL
            AND owner_expires_at IS NOT NULL)
        OR (state IN ('QUEUED','SENT','FAILED','UNKNOWN','CANCELLED') AND owner_id IS NULL
            AND owner_expires_at IS NULL)
    ),
    CHECK (
        (state IN ('PREPARED','DISPATCHING','SENT','FAILED','UNKNOWN')
            AND current_attempt_id IS NOT NULL)
        OR (state IN ('QUEUED','CLAIMED','CANCELLED') AND current_attempt_id IS NULL)
    )
);

CREATE TABLE claim_operations (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    worker_id text NOT NULL CHECK (octet_length(worker_id) BETWEEN 1 AND 256),
    selected_intent_id text,
    selected_part_index integer,
    selected_owner_epoch bigint,
    owner_expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    FOREIGN KEY (bot_id,selected_intent_id,selected_part_index)
        REFERENCES send_parts(bot_id,intent_id,part_index),
    CHECK (
        (selected_intent_id IS NULL AND selected_part_index IS NULL
            AND selected_owner_epoch IS NULL AND owner_expires_at IS NULL)
        OR (selected_intent_id IS NOT NULL AND selected_part_index IS NOT NULL
            AND selected_owner_epoch IS NOT NULL AND owner_expires_at IS NOT NULL)
    )
);

CREATE TABLE transport_attempts (
    bot_id text NOT NULL,
    attempt_id text NOT NULL CHECK (octet_length(attempt_id) BETWEEN 1 AND 256),
    intent_id text NOT NULL,
    part_index integer NOT NULL,
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    prepare_operation_id text NOT NULL CHECK (
        octet_length(prepare_operation_id) BETWEEN 1 AND 256
    ),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    effect_operation_ref text NOT NULL CHECK (
        octet_length(effect_operation_ref) BETWEEN 1 AND 256
    ),
    effect_attempt_ref text NOT NULL CHECK (
        octet_length(effect_attempt_ref) BETWEEN 1 AND 256
    ),
    effect_attempt_no integer NOT NULL CHECK (effect_attempt_no > 0),
    transport_action text NOT NULL CHECK (octet_length(transport_action) BETWEEN 1 AND 128),
    wire_payload_id text NOT NULL CHECK (octet_length(wire_payload_id) BETWEEN 1 AND 256),
    wire_payload_digest text NOT NULL CHECK (wire_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    binding_epoch bigint NOT NULL CHECK (binding_epoch > 0),
    connection_epoch bigint NOT NULL CHECK (connection_epoch > 0),
    client_incarnation text NOT NULL CHECK (
        octet_length(client_incarnation) BETWEEN 1 AND 256
    ),
    adapter_generation_id text NOT NULL CHECK (
        octet_length(adapter_generation_id) BETWEEN 1 AND 256
    ),
    executor_fence_digest text NOT NULL CHECK (
        executor_fence_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    authorization_ticket_ref text NOT NULL CHECK (
        octet_length(authorization_ticket_ref) BETWEEN 1 AND 256
    ),
    authorization_plan_digest text CHECK (
        authorization_plan_digest IS NULL
        OR authorization_plan_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    deadline_at timestamptz NOT NULL,
    state text NOT NULL DEFAULT 'PREPARED' CHECK (
        state IN ('PREPARED','DISPATCHING','SENT','NOT_APPLIED','REJECTED','UNKNOWN','ABANDONED')
    ),
    handoff_operation_id text,
    receipt_payload_id text,
    receipt_payload_digest text CHECK (
        receipt_payload_digest IS NULL OR receipt_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    error_code text CHECK (error_code IS NULL OR octet_length(error_code) BETWEEN 1 AND 128),
    prepared_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at timestamptz,
    settled_at timestamptz,
    resolved_at timestamptz,
    PRIMARY KEY (bot_id,attempt_id),
    UNIQUE (bot_id,prepare_operation_id),
    UNIQUE (bot_id,intent_id,part_index,attempt_no),
    UNIQUE (bot_id,effect_attempt_ref),
    FOREIGN KEY (bot_id,intent_id,part_index)
        REFERENCES send_parts(bot_id,intent_id,part_index) DEFERRABLE INITIALLY DEFERRED,
    CHECK ((receipt_payload_id IS NULL) = (receipt_payload_digest IS NULL)),
    CHECK (handoff_operation_id IS NULL OR octet_length(handoff_operation_id) BETWEEN 1 AND 256),
    CHECK (
        (state='PREPARED' AND handoff_operation_id IS NULL AND started_at IS NULL
            AND settled_at IS NULL)
        OR (state='DISPATCHING' AND handoff_operation_id IS NOT NULL AND started_at IS NOT NULL
            AND settled_at IS NULL)
        OR (state IN ('SENT','NOT_APPLIED','REJECTED','UNKNOWN')
            AND handoff_operation_id IS NOT NULL AND started_at IS NOT NULL
            AND settled_at IS NOT NULL)
        OR (state='ABANDONED' AND handoff_operation_id IS NULL AND started_at IS NULL
            AND settled_at IS NOT NULL)
    )
);

ALTER TABLE send_parts ADD CONSTRAINT send_parts_current_attempt_fk
    FOREIGN KEY (bot_id,current_attempt_id)
    REFERENCES transport_attempts(bot_id,attempt_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE delivery_observations (
    bot_id text NOT NULL,
    observation_operation_id text NOT NULL CHECK (
        octet_length(observation_operation_id) BETWEEN 1 AND 256
    ),
    effect_observation_operation_id text NOT NULL CHECK (
        octet_length(effect_observation_operation_id) BETWEEN 1 AND 256
    ),
    attempt_id text NOT NULL,
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    evidence_kind text NOT NULL CHECK (evidence_kind IN ('EXECUTOR','RECONCILIATION')),
    observed_status text NOT NULL CHECK (
        observed_status IN ('SENT','RETRYABLE_NOT_APPLIED','REJECTED_NOT_APPLIED','UNKNOWN')
    ),
    receipt_payload_id text,
    receipt_payload_digest text CHECK (
        receipt_payload_digest IS NULL OR receipt_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    error_code text CHECK (error_code IS NULL OR octet_length(error_code) BETWEEN 1 AND 128),
    accepted boolean NOT NULL,
    previous_state text NOT NULL CHECK (
        previous_state IN ('DISPATCHING','SENT','NOT_APPLIED','REJECTED','UNKNOWN')
    ),
    resulting_state text NOT NULL CHECK (
        resulting_state IN ('SENT','NOT_APPLIED','REJECTED','UNKNOWN')
    ),
    conflict_code text,
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,observation_operation_id),
    UNIQUE (bot_id,effect_observation_operation_id),
    FOREIGN KEY (bot_id,attempt_id) REFERENCES transport_attempts(bot_id,attempt_id),
    CHECK ((receipt_payload_id IS NULL) = (receipt_payload_digest IS NULL)),
    CHECK ((accepted AND conflict_code IS NULL) OR (NOT accepted AND conflict_code IS NOT NULL))
);

CREATE TABLE claim_releases (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    intent_id text NOT NULL,
    part_index integer NOT NULL,
    owner_epoch bigint NOT NULL CHECK (owner_epoch > 0),
    reason_code text NOT NULL CHECK (octet_length(reason_code) BETWEEN 1 AND 128),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    FOREIGN KEY (bot_id,intent_id,part_index)
        REFERENCES send_parts(bot_id,intent_id,part_index)
);

CREATE TABLE intent_cancellations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    intent_id text NOT NULL,
    reason_code text NOT NULL CHECK (octet_length(reason_code) BETWEEN 1 AND 128),
    resulting_state text NOT NULL CHECK (resulting_state IN ('CANCELLED','UNKNOWN')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    FOREIGN KEY (bot_id,intent_id) REFERENCES send_intents(bot_id,intent_id)
);

CREATE INDEX send_intents_queue_idx ON send_intents(
    bot_id,lane_kind,state,created_at,intent_id
);
CREATE INDEX send_parts_queue_idx ON send_parts(bot_id,state,owner_expires_at);
CREATE INDEX transport_attempts_state_idx ON transport_attempts(bot_id,state,settled_at);

CREATE FUNCTION reject_immutable_row() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'delivery immutable fact cannot change' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_lane() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.binding_ref,NEW.lane_kind,NEW.ordering_key,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.binding_ref,OLD.lane_kind,OLD.ordering_key,OLD.created_at) OR
       NEW.next_sequence < OLD.next_sequence THEN
        RAISE EXCEPTION 'Delivery lane cannot move backwards' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION guard_intent() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.intent_id,NEW.create_operation_id,NEW.input_digest,
           NEW.binding_ref,NEW.lane_kind,NEW.ordering_key,NEW.lane_sequence,
           NEW.conversation_ref,NEW.channel_kind,NEW.channel_ref,
           NEW.cause_component_id,NEW.cause_kind,NEW.cause_id,
           NEW.delivery_generation_id,NEW.reply_ref,
           NEW.part_count,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.intent_id,OLD.create_operation_id,OLD.input_digest,
           OLD.binding_ref,OLD.lane_kind,OLD.ordering_key,OLD.lane_sequence,
           OLD.conversation_ref,OLD.channel_kind,OLD.channel_ref,
           OLD.cause_component_id,OLD.cause_kind,OLD.cause_id,
           OLD.delivery_generation_id,OLD.reply_ref,
           OLD.part_count,OLD.created_at) OR
       NEW.current_part_index < OLD.current_part_index THEN
        RAISE EXCEPTION 'SendIntent identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='QUEUED' AND NEW.state IN ('SENDING','CANCELLED') THEN RETURN NEW; END IF;
    IF OLD.state='SENDING' AND NEW.state IN ('QUEUED','SENT','FAILED','UNKNOWN','CANCELLED')
        THEN RETURN NEW; END IF;
    IF OLD.state='UNKNOWN' AND NEW.state IN ('QUEUED','SENT','FAILED') THEN RETURN NEW; END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid SendIntent transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_part() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.intent_id,NEW.part_index,NEW.part_operation_id,
           NEW.effect_operation_ref,
           NEW.semantic_digest,NEW.payload_id,NEW.payload_digest,
           NEW.payload_owner_component_id,NEW.payload_owner_kind,NEW.payload_owner_id,
           NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.intent_id,OLD.part_index,OLD.part_operation_id,
           OLD.effect_operation_ref,
           OLD.semantic_digest,OLD.payload_id,OLD.payload_digest,
           OLD.payload_owner_component_id,OLD.payload_owner_kind,OLD.payload_owner_id,
           OLD.created_at) OR NEW.owner_epoch < OLD.owner_epoch OR
           NEW.attempt_count < OLD.attempt_count THEN
        RAISE EXCEPTION 'SendPart identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='QUEUED' AND NEW.state IN ('CLAIMED','CANCELLED') THEN RETURN NEW; END IF;
    IF OLD.state='CLAIMED' AND NEW.state IN ('QUEUED','PREPARED','CANCELLED') THEN RETURN NEW; END IF;
    IF OLD.state='PREPARED' AND NEW.state IN ('CLAIMED','DISPATCHING','CANCELLED') THEN RETURN NEW; END IF;
    IF OLD.state='DISPATCHING' AND NEW.state IN ('SENT','FAILED','UNKNOWN','QUEUED') THEN
        RETURN NEW;
    END IF;
    IF OLD.state='UNKNOWN' AND NEW.state IN ('SENT','FAILED','QUEUED') THEN RETURN NEW; END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid SendPart transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.attempt_id,NEW.intent_id,NEW.part_index,NEW.attempt_no,
           NEW.prepare_operation_id,NEW.input_digest,NEW.effect_operation_ref,
           NEW.effect_attempt_ref,NEW.effect_attempt_no,NEW.transport_action,NEW.wire_payload_id,
           NEW.wire_payload_digest,NEW.binding_epoch,NEW.connection_epoch,
           NEW.client_incarnation,NEW.adapter_generation_id,NEW.executor_fence_digest,
           NEW.authorization_ticket_ref,NEW.authorization_plan_digest,
           NEW.deadline_at,NEW.prepared_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.attempt_id,OLD.intent_id,OLD.part_index,OLD.attempt_no,
           OLD.prepare_operation_id,OLD.input_digest,OLD.effect_operation_ref,
           OLD.effect_attempt_ref,OLD.effect_attempt_no,OLD.transport_action,OLD.wire_payload_id,
           OLD.wire_payload_digest,OLD.binding_epoch,OLD.connection_epoch,
           OLD.client_incarnation,OLD.adapter_generation_id,OLD.executor_fence_digest,
           OLD.authorization_ticket_ref,OLD.authorization_plan_digest,
           OLD.deadline_at,OLD.prepared_at) THEN
        RAISE EXCEPTION 'Transport attempt identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='PREPARED' AND NEW.state IN ('DISPATCHING','ABANDONED') THEN RETURN NEW; END IF;
    IF OLD.state='DISPATCHING' AND NEW.state IN ('SENT','NOT_APPLIED','REJECTED','UNKNOWN')
        THEN RETURN NEW; END IF;
    IF OLD.state='UNKNOWN' AND NEW.state IN ('SENT','NOT_APPLIED','REJECTED') THEN RETURN NEW; END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid transport attempt transition' USING ERRCODE='55000';
END;
$$;

CREATE TRIGGER delivery_lanes_guard BEFORE UPDATE OR DELETE ON delivery_lanes
    FOR EACH ROW EXECUTE FUNCTION guard_lane();
CREATE TRIGGER send_intents_guard BEFORE UPDATE OR DELETE ON send_intents
    FOR EACH ROW EXECUTE FUNCTION guard_intent();
CREATE TRIGGER send_parts_guard BEFORE UPDATE OR DELETE ON send_parts
    FOR EACH ROW EXECUTE FUNCTION guard_part();
CREATE TRIGGER transport_attempts_guard BEFORE UPDATE OR DELETE ON transport_attempts
    FOR EACH ROW EXECUTE FUNCTION guard_attempt();
CREATE TRIGGER claim_operations_immutable BEFORE UPDATE OR DELETE ON claim_operations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER delivery_observations_immutable BEFORE UPDATE OR DELETE ON delivery_observations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER claim_releases_immutable BEFORE UPDATE OR DELETE ON claim_releases
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER intent_cancellations_immutable BEFORE UPDATE OR DELETE ON intent_cancellations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
