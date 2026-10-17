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
    retry_ready_at timestamptz NOT NULL DEFAULT clock_timestamp(),
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
    evidence_details jsonb CHECK (
        evidence_details IS NULL OR jsonb_typeof(evidence_details)='object'
    ),
    error_code text CHECK (error_code IS NULL OR octet_length(error_code) BETWEEN 1 AND 128),
    accepted boolean NOT NULL,
    previous_state text NOT NULL CHECK (
        previous_state IN ('DISPATCHING','SENT','NOT_APPLIED','REJECTED','UNKNOWN')
    ),
    resulting_state text NOT NULL CHECK (
        resulting_state IN ('DISPATCHING','SENT','NOT_APPLIED','REJECTED','UNKNOWN')
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
CREATE INDEX send_intents_unknown_page_idx ON send_intents(
    bot_id,updated_at DESC,intent_id DESC
) WHERE state='UNKNOWN';
CREATE INDEX send_intents_unknown_filter_idx ON send_intents(
    bot_id,lane_kind,channel_ref,updated_at DESC,intent_id DESC
) WHERE state='UNKNOWN';
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
    IF OLD.state IN ('PREPARED','DISPATCHING') AND NEW.state=OLD.state
       AND NEW.owner_epoch > OLD.owner_epoch THEN RETURN NEW; END IF;
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

CREATE FUNCTION delivery_create_intent(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'input_digest';
    requested_intent text := input->>'intent_id';
    requested_lane text := input->>'lane_kind';
    requested_generation text := input->>'delivery_generation_id';
    lane_row __forest_module_schema__.delivery_lanes%ROWTYPE;
    existing __forest_module_schema__.send_intents%ROWTYPE;
    created __forest_module_schema__.send_intents%ROWTYPE;
    part jsonb;
    part_number bigint;
    part_count integer := jsonb_array_length(input->'parts');
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.delivery-intent/1'
       OR requested_bot IS NULL
       OR requested_operation IS NULL OR octet_length(requested_operation) NOT BETWEEN 1 AND 256
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR requested_intent IS NULL OR octet_length(requested_intent) NOT BETWEEN 1 AND 256
       OR requested_lane NOT IN ('CHAT','CONTROL')
       OR input->>'channel_kind' NOT IN ('GROUP','PRIVATE')
       OR requested_generation IS DISTINCT FROM context->>'generation_id'
       OR jsonb_typeof(input->'parts') IS DISTINCT FROM 'array'
       OR part_count NOT BETWEEN 1 AND 64 THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','DELIVERY_INPUT_INVALID','message','Delivery intent input is invalid','status',422
            )
        );
    END IF;
    IF requested_lane='CHAT' AND (
        input->>'conversation_ref' IS NULL
        OR input->>'reply_ref' IS NULL
        OR input->>'ordering_key' IS DISTINCT FROM input->>'conversation_ref'
        OR input->>'cause_component_id' IS DISTINCT FROM 'conversation.scheduler'
        OR input->>'cause_kind' IS DISTINCT FROM 'context.reply'
        OR input->>'cause_id' IS DISTINCT FROM input->>'reply_ref'
    ) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','DELIVERY_CAUSE_INVALID','message','CHAT intent cause is invalid','status',422
            )
        );
    END IF;
    IF requested_lane='CONTROL' AND input->>'reply_ref' IS NOT NULL THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','DELIVERY_INPUT_INVALID','message','CONTROL intent cannot answer a reply','status',422
            )
        );
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(requested_bot || E'\\0delivery-intent\\0' || requested_operation,0));
    SELECT * INTO existing FROM __forest_module_schema__.send_intents
    WHERE bot_id=requested_bot AND create_operation_id=requested_operation;
    IF FOUND THEN
        IF existing.input_digest <> requested_digest OR existing.intent_id <> requested_intent THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','DELIVERY_INTENT_CONFLICT','message','Delivery operation input differs','status',409
                )
            );
        END IF;
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'intent_id',existing.intent_id,'state',existing.state,
                'lane_sequence',existing.lane_sequence,'part_count',existing.part_count
            )
        );
    END IF;
    IF EXISTS (
        SELECT 1 FROM __forest_module_schema__.send_intents
        WHERE bot_id=requested_bot AND intent_id=requested_intent
    ) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','DELIVERY_INTENT_CONFLICT','message','Delivery intent identity differs','status',409
            )
        );
    END IF;
    INSERT INTO __forest_module_schema__.delivery_lanes(
        bot_id,binding_ref,lane_kind,ordering_key
    ) VALUES (
        requested_bot,input->>'binding_ref',requested_lane,input->>'ordering_key'
    ) ON CONFLICT DO NOTHING;
    SELECT * INTO lane_row FROM __forest_module_schema__.delivery_lanes
    WHERE bot_id=requested_bot AND binding_ref=input->>'binding_ref'
      AND lane_kind=requested_lane AND ordering_key=input->>'ordering_key'
    FOR UPDATE;
    INSERT INTO __forest_module_schema__.send_intents(
        bot_id,intent_id,create_operation_id,input_digest,binding_ref,lane_kind,
        ordering_key,lane_sequence,conversation_ref,channel_kind,channel_ref,
        cause_component_id,cause_kind,cause_id,delivery_generation_id,reply_ref,part_count
    ) VALUES (
        requested_bot,requested_intent,requested_operation,requested_digest,
        input->>'binding_ref',requested_lane,input->>'ordering_key',lane_row.next_sequence,
        input->>'conversation_ref',input->>'channel_kind',input->>'channel_ref',
        input->>'cause_component_id',input->>'cause_kind',input->>'cause_id',
        requested_generation,input->>'reply_ref',part_count
    ) RETURNING * INTO created;
    FOR part, part_number IN
        SELECT value, ordinality FROM jsonb_array_elements(input->'parts') WITH ORDINALITY
    LOOP
        IF jsonb_typeof(part) IS DISTINCT FROM 'object'
           OR part->>'part_operation_id' IS NULL
           OR part->>'effect_operation_ref' IS NULL
           OR jsonb_typeof(part->'payload') IS DISTINCT FROM 'object'
           OR part->'payload'->>'digest' !~ '^sha256:[a-f0-9]{64}$' THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','DELIVERY_INPUT_INVALID','message','Delivery part input is invalid','status',422
                )
            );
        END IF;
        INSERT INTO __forest_module_schema__.send_parts(
            bot_id,intent_id,part_index,part_operation_id,effect_operation_ref,
            semantic_digest,payload_id,payload_digest,payload_owner_component_id,
            payload_owner_kind,payload_owner_id
        ) VALUES (
            requested_bot,requested_intent,part_number-1,
            part->>'part_operation_id',part->>'effect_operation_ref',part->'payload'->>'digest',
            part->'payload'->>'payload_id',part->'payload'->>'digest',
            part->'payload'->>'owner_component_id',part->'payload'->>'owner_kind',
            part->'payload'->>'owner_id'
        );
    END LOOP;
    UPDATE __forest_module_schema__.delivery_lanes
    SET next_sequence=next_sequence+1,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND binding_ref=input->>'binding_ref'
      AND lane_kind=requested_lane AND ordering_key=input->>'ordering_key';
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'intent_id',created.intent_id,'state',created.state,
            'lane_sequence',created.lane_sequence,'part_count',created.part_count
        )
    );
END;
$$;

CREATE FUNCTION delivery_create_chat_intent(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE sql VOLATILE SET search_path=pg_catalog
AS $$ SELECT __forest_module_schema__.delivery_create_intent(context,input) $$;

CREATE FUNCTION delivery_create_chat_intent_uow(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE sql VOLATILE SET search_path=pg_catalog
AS $$ SELECT __forest_module_schema__.delivery_create_intent(context,envelope->'input') $$;

CREATE FUNCTION delivery_create_control_intent(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE sql VOLATILE SET search_path=pg_catalog
AS $$ SELECT __forest_module_schema__.delivery_create_intent(context,input) $$;

CREATE FUNCTION delivery_create_control_intent_uow(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE sql VOLATILE SET search_path=pg_catalog
AS $$ SELECT __forest_module_schema__.delivery_create_intent(context,envelope->'input') $$;

CREATE TABLE owned_work_settlements (
    claim_token text PRIMARY KEY,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    settled_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER owned_work_settlements_immutable
    BEFORE UPDATE OR DELETE ON owned_work_settlements
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();

CREATE FUNCTION delivery_claim_work(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    selected record;
    selected_mode text;
    expires timestamptz;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_token IS NULL OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
            jsonb_build_object('code','DELIVERY_CLAIM_INPUT_INVALID','message','Delivery claim request is invalid','status',422));
    END IF;
    SELECT p.*,i.binding_ref,i.lane_kind,i.ordering_key,i.lane_sequence,
           i.channel_kind,i.channel_ref,a.attempt_id AS existing_attempt_id,a.state AS attempt_state
      INTO selected
      FROM __forest_module_schema__.send_parts p
      JOIN __forest_module_schema__.send_intents i
        ON i.bot_id=p.bot_id AND i.intent_id=p.intent_id
      LEFT JOIN __forest_module_schema__.transport_attempts a
        ON a.bot_id=p.bot_id AND a.attempt_id=p.current_attempt_id
     WHERE p.bot_id=requested_bot AND p.owner_id=requested_token
     FOR UPDATE OF p,i;
    IF FOUND THEN
        selected_mode := CASE selected.state WHEN 'PREPARED' THEN 'RECOVER_PREPARED'
                         WHEN 'DISPATCHING' THEN 'RECOVER_DISPATCHING' ELSE 'DISPATCH' END;
    ELSE
        SELECT p.*,i.binding_ref,i.lane_kind,i.ordering_key,i.lane_sequence,
               i.channel_kind,i.channel_ref,a.attempt_id AS existing_attempt_id,a.state AS attempt_state
          INTO selected
          FROM __forest_module_schema__.send_parts p
          JOIN __forest_module_schema__.send_intents i
            ON i.bot_id=p.bot_id AND i.intent_id=p.intent_id
          JOIN __forest_module_schema__.transport_attempts a
            ON a.bot_id=p.bot_id AND a.attempt_id=p.current_attempt_id
         WHERE p.bot_id=requested_bot AND p.state IN ('PREPARED','DISPATCHING')
           AND p.owner_expires_at<=clock_timestamp()
         ORDER BY p.owner_expires_at,p.intent_id,p.part_index
         FOR UPDATE OF p,i SKIP LOCKED LIMIT 1;
        IF FOUND THEN
            selected_mode := CASE selected.state WHEN 'PREPARED' THEN 'RECOVER_PREPARED'
                             ELSE 'RECOVER_DISPATCHING' END;
            expires := clock_timestamp() + requested_lease * interval '1 second';
            UPDATE __forest_module_schema__.send_parts
               SET owner_id=requested_token,owner_epoch=owner_epoch+1,owner_expires_at=expires,
                   updated_at=clock_timestamp()
             WHERE bot_id=requested_bot AND intent_id=selected.intent_id
               AND part_index=selected.part_index
             RETURNING owner_id,owner_epoch,owner_expires_at INTO selected.owner_id,selected.owner_epoch,selected.owner_expires_at;
        ELSE
            UPDATE __forest_module_schema__.send_parts p
               SET state='QUEUED',owner_id=NULL,owner_expires_at=NULL,updated_at=clock_timestamp()
             WHERE p.bot_id=requested_bot AND p.state='CLAIMED'
               AND p.owner_expires_at<=clock_timestamp();
            UPDATE __forest_module_schema__.send_intents i SET state='QUEUED',updated_at=clock_timestamp()
             WHERE i.bot_id=requested_bot AND i.state='SENDING'
               AND EXISTS (SELECT 1 FROM __forest_module_schema__.send_parts p
                            WHERE p.bot_id=i.bot_id AND p.intent_id=i.intent_id
                              AND p.part_index=i.current_part_index AND p.state='QUEUED');
            SELECT p.*,i.binding_ref,i.lane_kind,i.ordering_key,i.lane_sequence,
                   i.channel_kind,i.channel_ref,NULL::text AS existing_attempt_id,NULL::text AS attempt_state
              INTO selected
              FROM __forest_module_schema__.send_parts p
              JOIN __forest_module_schema__.send_intents i
                ON i.bot_id=p.bot_id AND i.intent_id=p.intent_id
             WHERE p.bot_id=requested_bot AND p.state='QUEUED' AND i.state='QUEUED'
               AND p.retry_ready_at<=clock_timestamp()
               AND p.part_index=i.current_part_index
               AND NOT EXISTS (
                   SELECT 1 FROM __forest_module_schema__.send_intents earlier
                    WHERE earlier.bot_id=i.bot_id AND earlier.binding_ref=i.binding_ref
                      AND earlier.lane_kind=i.lane_kind AND earlier.ordering_key=i.ordering_key
                      AND earlier.lane_sequence<i.lane_sequence
                      AND earlier.state IN ('QUEUED','SENDING','UNKNOWN'))
             ORDER BY CASE i.lane_kind WHEN 'CONTROL' THEN 0 ELSE 1 END,i.created_at,i.intent_id
             FOR UPDATE OF p,i SKIP LOCKED LIMIT 1;
            IF NOT FOUND THEN
                RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
            END IF;
            selected_mode := 'DISPATCH';
            expires := clock_timestamp() + requested_lease * interval '1 second';
            UPDATE __forest_module_schema__.send_parts
               SET state='CLAIMED',owner_id=requested_token,owner_epoch=owner_epoch+1,
                   owner_expires_at=expires,updated_at=clock_timestamp()
             WHERE bot_id=requested_bot AND intent_id=selected.intent_id
               AND part_index=selected.part_index
             RETURNING owner_id,owner_epoch,owner_expires_at INTO selected.owner_id,selected.owner_epoch,selected.owner_expires_at;
            UPDATE __forest_module_schema__.send_intents SET state='SENDING',updated_at=clock_timestamp()
             WHERE bot_id=requested_bot AND intent_id=selected.intent_id;
        END IF;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
      jsonb_build_object(
        'protocol','forest.owned-work-claim/1','work_id',selected.intent_id||':'||selected.part_index,
        'claim_token',requested_token,'attempt',selected.owner_epoch,'operation_id',requested_token,
        'causal_root_kind','delivery.intent','causal_root_id',selected.intent_id,
        'payload',jsonb_build_object(
          'mode',selected_mode,'intent_id',selected.intent_id,'part_index',selected.part_index,
          'part_operation_id',selected.part_operation_id,'effect_operation_ref',selected.effect_operation_ref,
          'attempt_id',COALESCE(selected.existing_attempt_id,requested_token),
          'semantic_digest',selected.semantic_digest,'binding_ref',selected.binding_ref,
          'lane_kind',selected.lane_kind,'ordering_key',selected.ordering_key,'lane_sequence',selected.lane_sequence,
          'channel_kind',selected.channel_kind,'channel_ref',selected.channel_ref,
          'owner_epoch',selected.owner_epoch,'owner_expires_at',selected.owner_expires_at,
          'payload',jsonb_build_object('payload_id',selected.payload_id,'digest',selected.payload_digest,
            'owner_component_id',selected.payload_owner_component_id,'owner_kind',selected.payload_owner_kind,
            'owner_id',selected.payload_owner_id))));
END;
$$;

CREATE FUNCTION delivery_settle_work(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE existing record; result_value jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR input->>'claim_token' IS NULL OR input->>'work_id' IS NULL
       OR input->'outcome'->>'status' NOT IN ('SUCCEEDED','FAILED') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
          jsonb_build_object('code','DELIVERY_SETTLEMENT_INPUT_INVALID','message','Delivery settlement is invalid','status',422));
    END IF;
    SELECT * INTO existing FROM __forest_module_schema__.owned_work_settlements
     WHERE claim_token=input->>'claim_token';
    IF FOUND THEN
        IF existing.input_payload IS DISTINCT FROM input THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
              jsonb_build_object('code','DELIVERY_SETTLEMENT_CONFLICT','message','Claim token has another settlement','status',409));
        END IF;
        RETURN existing.result_payload;
    END IF;
    IF input->'outcome'->>'status'='FAILED' THEN
        UPDATE __forest_module_schema__.send_parts p SET state='QUEUED',owner_id=NULL,
               owner_expires_at=NULL,updated_at=clock_timestamp()
         WHERE p.bot_id=context->>'bot_id' AND p.owner_id=input->>'claim_token' AND p.state='CLAIMED';
        UPDATE __forest_module_schema__.send_intents i SET state='QUEUED',updated_at=clock_timestamp()
         WHERE i.bot_id=context->>'bot_id' AND i.state='SENDING'
           AND EXISTS (SELECT 1 FROM __forest_module_schema__.send_parts p
             WHERE p.bot_id=i.bot_id AND p.intent_id=i.intent_id AND p.owner_id IS NULL
               AND p.part_index=i.current_part_index AND p.state='QUEUED');
    END IF;
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
      jsonb_build_object('protocol','forest.owned-work-settled/1','work_id',input->>'work_id'));
    INSERT INTO __forest_module_schema__.owned_work_settlements(claim_token,input_payload,result_payload)
      VALUES(input->>'claim_token',input,result_value);
    RETURN result_value;
END;
$$;

CREATE FUNCTION delivery_record_prepare(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE plan jsonb:=input->'input'; work jsonb:=plan->'work'; adapter jsonb:=plan->'adapter';
        wire jsonb:=plan->'wire'; permit jsonb:=input->'prior'->'effect'; auth jsonb:=input->'prior'->'authorization';
        part record; attempt_no_value integer; existing record;
BEGIN
    SELECT * INTO existing FROM __forest_module_schema__.transport_attempts
     WHERE bot_id=context->>'bot_id' AND attempt_id=work->>'attempt_id';
    IF FOUND THEN
      RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
        jsonb_build_object('attempt_id',existing.attempt_id,'state',existing.state,'permit',permit));
    END IF;
    SELECT * INTO part FROM __forest_module_schema__.send_parts
     WHERE bot_id=context->>'bot_id' AND intent_id=work->>'intent_id'
       AND part_index=(work->>'part_index')::integer FOR UPDATE;
    IF NOT FOUND OR part.state<>'CLAIMED' OR part.owner_id<>work->>'attempt_id'
       OR part.owner_epoch<>(work->>'owner_epoch')::bigint
       OR adapter->>'binding_ref' IS DISTINCT FROM work->>'binding_ref'
       OR auth->'ticket'->>'ticket_id' IS DISTINCT FROM plan->'authorization'->>'ticket_id'
       OR permit->>'effect_operation_id' IS DISTINCT FROM work->>'effect_operation_ref'
       OR permit->>'attempt_id' IS DISTINCT FROM work->>'attempt_id' THEN
      RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
        jsonb_build_object('code','DELIVERY_PREPARE_CONFLICT','message','Delivery claim or participant receipts differ','status',409));
    END IF;
    attempt_no_value := part.attempt_count+1;
    INSERT INTO __forest_module_schema__.transport_attempts(
      bot_id,attempt_id,intent_id,part_index,attempt_no,prepare_operation_id,input_digest,
      effect_operation_ref,effect_attempt_ref,effect_attempt_no,transport_action,
      wire_payload_id,wire_payload_digest,binding_epoch,connection_epoch,client_incarnation,
      adapter_generation_id,executor_fence_digest,authorization_ticket_ref,
      authorization_plan_digest,deadline_at)
    VALUES(context->>'bot_id',work->>'attempt_id',work->>'intent_id',(work->>'part_index')::integer,
      attempt_no_value,context->>'operation_id',context->>'input_digest',work->>'effect_operation_ref',
      permit->>'attempt_id',(permit->>'attempt_no')::integer,adapter->'prepared'->>'action',
      wire->>'payload_id',wire->>'digest',(adapter->>'binding_epoch')::bigint,
      (adapter->>'connection_epoch')::bigint,adapter->>'client_incarnation',
      adapter->>'adapter_generation_id',adapter->>'transport_digest',
      plan->'authorization'->>'ticket_id',plan->>'plan_digest',
      (plan->'effect'->>'deadline_at')::timestamptz);
    UPDATE __forest_module_schema__.send_parts SET state='PREPARED',current_attempt_id=work->>'attempt_id',
      attempt_count=attempt_no_value,updated_at=clock_timestamp()
     WHERE bot_id=context->>'bot_id' AND intent_id=work->>'intent_id'
       AND part_index=(work->>'part_index')::integer;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
      jsonb_build_object('attempt_id',work->>'attempt_id','state','PREPARED','permit',permit));
END;
$$;

CREATE FUNCTION delivery_record_handoff(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE plan jsonb:=input->'input'; work jsonb:=plan->'work'; attempt record; effect jsonb:=input->'prior'->'effect';
BEGIN
    SELECT * INTO attempt FROM __forest_module_schema__.transport_attempts
     WHERE bot_id=context->>'bot_id' AND attempt_id=work->>'attempt_id' FOR UPDATE;
    IF NOT FOUND OR (attempt.state<>'PREPARED' AND attempt.state<>'DISPATCHING')
       OR effect->>'attempt_id' IS DISTINCT FROM attempt.effect_attempt_ref
       OR effect->>'state' IS DISTINCT FROM 'DISPATCHING' THEN
      RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
        jsonb_build_object('code','DELIVERY_HANDOFF_CONFLICT','message','Delivery handoff receipt differs','status',409));
    END IF;
    IF attempt.state='PREPARED' THEN
      UPDATE __forest_module_schema__.transport_attempts SET state='DISPATCHING',
        handoff_operation_id=plan->'effect'->>'handoff_operation_id',
        started_at=(effect->>'started_at')::timestamptz
       WHERE bot_id=context->>'bot_id' AND attempt_id=attempt.attempt_id;
      UPDATE __forest_module_schema__.send_parts SET state='DISPATCHING',updated_at=clock_timestamp()
       WHERE bot_id=context->>'bot_id' AND intent_id=attempt.intent_id AND part_index=attempt.part_index;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
      jsonb_build_object('attempt_id',attempt.attempt_id,'state','DISPATCHING','permit',plan->'attempt'->'permit'));
END;
$$;

CREATE FUNCTION delivery_record_observe(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE plan jsonb:=input->'input'; work jsonb:=plan->'work'; attempt record; intent record;
        requested text:=plan->>'observed_status'; resulting text; receipt jsonb:=plan->'receipt';
        effect jsonb:=input->'prior'->'effect'; existing record; prior_state text;
        target_state text; expected_effect_state text; accepted boolean; conflict text;
        reconciliation boolean:=plan->'effect'->>'evidence_kind'='reconciliation';
        evidence_details jsonb;
BEGIN
    SELECT * INTO attempt FROM __forest_module_schema__.transport_attempts
     WHERE bot_id=context->>'bot_id' AND attempt_id=work->>'attempt_id' FOR UPDATE;
    SELECT * INTO intent FROM __forest_module_schema__.send_intents
     WHERE bot_id=context->>'bot_id' AND intent_id=work->>'intent_id' FOR UPDATE;
    IF attempt.attempt_id IS NULL OR intent.intent_id IS NULL
       OR attempt.intent_id<>intent.intent_id THEN
      RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
        jsonb_build_object('code','DELIVERY_OBSERVATION_CONFLICT','message','Delivery attempt is not observable','status',409));
    END IF;
    target_state:=CASE requested WHEN 'SENT' THEN 'SENT'
                  WHEN 'RETRYABLE_NOT_APPLIED' THEN 'NOT_APPLIED'
                  WHEN 'REJECTED_NOT_APPLIED' THEN 'REJECTED'
                  WHEN 'UNKNOWN' THEN 'UNKNOWN' END;
    expected_effect_state:=CASE requested WHEN 'SENT' THEN 'SUCCEEDED'
                           WHEN 'UNKNOWN' THEN 'UNKNOWN' ELSE 'NOT_APPLIED' END;
    IF target_state IS NULL OR jsonb_typeof(effect->'accepted')<>'boolean'
       OR effect->>'attempt_id' IS DISTINCT FROM attempt.effect_attempt_ref
       OR effect->>'observation_operation_id' IS DISTINCT FROM plan->'effect'->>'observation_operation_id'
       OR effect->>'observed_status' IS DISTINCT FROM expected_effect_state
       OR plan->'effect'->>'status' IS DISTINCT FROM expected_effect_state
       OR effect->>'evidence_kind' IS DISTINCT FROM plan->'effect'->>'evidence_kind' THEN
      RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
        jsonb_build_object('code','DELIVERY_OBSERVATION_CONFLICT','message','Effect observation differs from Delivery plan','status',409));
    END IF;
    IF plan->'effect'->>'effect_operation_id' IS DISTINCT FROM attempt.effect_operation_ref
       OR work->>'effect_operation_ref' IS DISTINCT FROM attempt.effect_operation_ref
       OR work->>'intent_id' IS DISTINCT FROM attempt.intent_id THEN
      RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
        jsonb_build_object('code','DELIVERY_OBSERVATION_CONFLICT','message','Effect or intent reference differs','status',409));
    END IF;
    IF reconciliation THEN
      IF requested='UNKNOWN' OR jsonb_typeof(plan->'receipt') IS DISTINCT FROM 'null'
         OR NULLIF(plan->>'requested_by','') IS NULL
         OR NULLIF(plan->'evidence'->>'kind','') IS NULL
         OR NULLIF(plan->'evidence'->>'reference','') IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
          jsonb_build_object('code','DELIVERY_RECONCILIATION_EVIDENCE_INVALID',
            'message','Reconciliation requires actor and verifiable evidence','status',422));
      END IF;
      evidence_details:=jsonb_build_object(
        'protocol','forest.delivery-reconciliation-evidence/1',
        'kind',plan->'evidence'->>'kind',
        'reference',plan->'evidence'->>'reference',
        'requested_by',plan->>'requested_by');
    ELSIF plan ? 'evidence' OR plan ? 'requested_by' THEN
      RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
        jsonb_build_object('code','DELIVERY_OBSERVATION_CONFLICT',
          'message','Executor observation cannot carry management evidence','status',409));
    END IF;
    SELECT * INTO existing FROM __forest_module_schema__.delivery_observations
     WHERE bot_id=context->>'bot_id' AND observation_operation_id=context->>'operation_id';
    IF FOUND THEN
      IF existing.input_digest IS DISTINCT FROM context->>'input_digest'
         OR existing.effect_observation_operation_id IS DISTINCT FROM effect->>'observation_operation_id' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
          jsonb_build_object('code','DELIVERY_OBSERVATION_CONFLICT','message','Observation operation differs','status',409));
      END IF;
      RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
        jsonb_build_object('intent_id',intent.intent_id,'state',intent.state,
          'accepted',existing.accepted,'conflict_code',existing.conflict_code));
    END IF;
    prior_state:=attempt.state;
    accepted:=(effect->>'accepted')::boolean AND (
      prior_state='DISPATCHING' OR prior_state=target_state
      OR (prior_state='UNKNOWN' AND upper(effect->>'evidence_kind')='RECONCILIATION'
          AND target_state<>'UNKNOWN')
    );
    conflict:=CASE WHEN accepted THEN NULL ELSE
              COALESCE(effect->>'conflict_code','DELIVERY_EVIDENCE_CONFLICT') END;
    INSERT INTO __forest_module_schema__.delivery_observations(
      bot_id,observation_operation_id,effect_observation_operation_id,attempt_id,
      input_digest,evidence_kind,observed_status,receipt_payload_id,receipt_payload_digest,
      evidence_details,error_code,accepted,previous_state,resulting_state,conflict_code)
    VALUES(context->>'bot_id',context->>'operation_id',effect->>'observation_operation_id',
      attempt.attempt_id,context->>'input_digest',upper(effect->>'evidence_kind'),requested,
      receipt->>'payload_id',receipt->>'digest',evidence_details,
      plan->>'error_code',accepted,
      prior_state,CASE WHEN accepted THEN target_state ELSE prior_state END,conflict);
    IF NOT accepted OR prior_state=target_state THEN
      RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
        jsonb_build_object('intent_id',intent.intent_id,'state',intent.state,
          'accepted',accepted,'conflict_code',conflict));
    END IF;
    IF requested='SENT' THEN
      UPDATE __forest_module_schema__.transport_attempts SET state='SENT',settled_at=clock_timestamp(),
        resolved_at=CASE WHEN prior_state='UNKNOWN' THEN clock_timestamp() ELSE resolved_at END,
        receipt_payload_id=receipt->>'payload_id',receipt_payload_digest=receipt->>'digest'
       WHERE bot_id=context->>'bot_id' AND attempt_id=attempt.attempt_id;
      UPDATE __forest_module_schema__.send_parts SET state='SENT',owner_id=NULL,owner_expires_at=NULL,
        updated_at=clock_timestamp() WHERE bot_id=context->>'bot_id'
        AND intent_id=attempt.intent_id AND part_index=attempt.part_index;
      IF attempt.part_index+1=intent.part_count THEN
        resulting:='SENT';
        UPDATE __forest_module_schema__.send_intents SET state='SENT',completed_at=clock_timestamp(),
          error_code=NULL,updated_at=clock_timestamp() WHERE bot_id=context->>'bot_id' AND intent_id=intent.intent_id;
      ELSE
        resulting:='QUEUED';
        UPDATE __forest_module_schema__.send_intents SET state='QUEUED',current_part_index=current_part_index+1,
          error_code=NULL,updated_at=clock_timestamp() WHERE bot_id=context->>'bot_id' AND intent_id=intent.intent_id;
      END IF;
    ELSIF requested='RETRYABLE_NOT_APPLIED' THEN
      resulting:='QUEUED';
      UPDATE __forest_module_schema__.transport_attempts SET state='NOT_APPLIED',settled_at=clock_timestamp(),
        resolved_at=CASE WHEN prior_state='UNKNOWN' THEN clock_timestamp() ELSE resolved_at END,
        error_code=plan->>'error_code' WHERE bot_id=context->>'bot_id' AND attempt_id=attempt.attempt_id;
      UPDATE __forest_module_schema__.send_parts SET state='QUEUED',owner_id=NULL,owner_expires_at=NULL,
        current_attempt_id=NULL,
        retry_ready_at=clock_timestamp()+make_interval(
          secs=>LEAST(60,1 << LEAST(attempt.attempt_no-1,6))),
        updated_at=clock_timestamp() WHERE bot_id=context->>'bot_id'
        AND intent_id=attempt.intent_id AND part_index=attempt.part_index;
      UPDATE __forest_module_schema__.send_intents SET state='QUEUED',error_code=plan->>'error_code',
        updated_at=clock_timestamp() WHERE bot_id=context->>'bot_id' AND intent_id=intent.intent_id;
    ELSIF requested='REJECTED_NOT_APPLIED' THEN
      resulting:='FAILED';
      UPDATE __forest_module_schema__.transport_attempts SET state='REJECTED',settled_at=clock_timestamp(),
        resolved_at=CASE WHEN prior_state='UNKNOWN' THEN clock_timestamp() ELSE resolved_at END,
        error_code=plan->>'error_code' WHERE bot_id=context->>'bot_id' AND attempt_id=attempt.attempt_id;
      UPDATE __forest_module_schema__.send_parts SET state='FAILED',owner_id=NULL,owner_expires_at=NULL,
        updated_at=clock_timestamp() WHERE bot_id=context->>'bot_id'
        AND intent_id=attempt.intent_id AND part_index=attempt.part_index;
      UPDATE __forest_module_schema__.send_intents SET state='FAILED',error_code=plan->>'error_code',
        completed_at=clock_timestamp(),updated_at=clock_timestamp()
        WHERE bot_id=context->>'bot_id' AND intent_id=intent.intent_id;
    ELSE
      resulting:='UNKNOWN';
      UPDATE __forest_module_schema__.transport_attempts SET state='UNKNOWN',settled_at=clock_timestamp(),
        error_code=plan->>'error_code' WHERE bot_id=context->>'bot_id' AND attempt_id=attempt.attempt_id;
      UPDATE __forest_module_schema__.send_parts SET state='UNKNOWN',owner_id=NULL,owner_expires_at=NULL,
        updated_at=clock_timestamp() WHERE bot_id=context->>'bot_id'
        AND intent_id=attempt.intent_id AND part_index=attempt.part_index;
      UPDATE __forest_module_schema__.send_intents SET state='UNKNOWN',error_code=plan->>'error_code',
        updated_at=clock_timestamp() WHERE bot_id=context->>'bot_id' AND intent_id=intent.intent_id;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
      jsonb_build_object('intent_id',intent.intent_id,'state',resulting,
        'accepted',accepted,'conflict_code',conflict));
END;
$$;

CREATE FUNCTION delivery_release_claim(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE work jsonb:=input->'input'->'work'; changed integer;
BEGIN
  UPDATE __forest_module_schema__.send_parts SET state='QUEUED',owner_id=NULL,owner_expires_at=NULL,
    updated_at=clock_timestamp() WHERE bot_id=context->>'bot_id' AND intent_id=work->>'intent_id'
    AND part_index=(work->>'part_index')::integer AND state='CLAIMED'
    AND owner_id=work->>'attempt_id' AND owner_epoch=(work->>'owner_epoch')::bigint;
  GET DIAGNOSTICS changed=ROW_COUNT;
  IF changed<>1 THEN RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
    jsonb_build_object('code','DELIVERY_RELEASE_CONFLICT','message','Delivery claim is stale','status',409)); END IF;
  UPDATE __forest_module_schema__.send_intents SET state='QUEUED',updated_at=clock_timestamp()
   WHERE bot_id=context->>'bot_id' AND intent_id=work->>'intent_id';
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('intent_id',work->>'intent_id','state','QUEUED'));
END;
$$;

CREATE FUNCTION delivery_record_abandon(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE work jsonb:=input->'input'->'work'; attempt record;
BEGIN
  SELECT * INTO attempt FROM __forest_module_schema__.transport_attempts
   WHERE bot_id=context->>'bot_id' AND attempt_id=work->>'attempt_id' FOR UPDATE;
  IF NOT FOUND OR attempt.state NOT IN ('PREPARED','ABANDONED') THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','DELIVERY_ABANDON_CONFLICT','message','Delivery attempt is not abandonable','status',409));
  END IF;
  IF attempt.state='PREPARED' THEN
    UPDATE __forest_module_schema__.transport_attempts SET state='ABANDONED',settled_at=clock_timestamp()
     WHERE bot_id=context->>'bot_id' AND attempt_id=attempt.attempt_id;
    UPDATE __forest_module_schema__.send_parts SET state='CLAIMED',current_attempt_id=NULL,updated_at=clock_timestamp()
     WHERE bot_id=context->>'bot_id' AND intent_id=attempt.intent_id AND part_index=attempt.part_index;
    UPDATE __forest_module_schema__.send_parts SET state='QUEUED',owner_id=NULL,owner_expires_at=NULL,
      updated_at=clock_timestamp() WHERE bot_id=context->>'bot_id'
      AND intent_id=attempt.intent_id AND part_index=attempt.part_index;
    UPDATE __forest_module_schema__.send_intents SET state='QUEUED',updated_at=clock_timestamp()
     WHERE bot_id=context->>'bot_id' AND intent_id=attempt.intent_id;
  END IF;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('intent_id',attempt.intent_id,'state','QUEUED'));
END;
$$;

CREATE FUNCTION delivery_list_unknown(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE snapshot_at timestamptz:=COALESCE((input->>'as_of')::timestamptz,statement_timestamp());
        page jsonb; requested_limit integer:=COALESCE((input->>'limit')::integer,50);
BEGIN
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'intent_id',ranked.intent_id,'lane_kind',ranked.lane_kind,
    'channel_kind',ranked.channel_kind,'channel_ref',ranked.channel_ref,
    'state','UNKNOWN','updated_at',ranked.updated_at,
    'attempt_id',ranked.current_attempt_id)
    ORDER BY ranked.updated_at DESC,ranked.intent_id DESC),'[]'::jsonb)
    INTO page
  FROM (
    SELECT i.intent_id,i.lane_kind,i.channel_kind,i.channel_ref,i.updated_at,
           p.current_attempt_id
    FROM __forest_module_schema__.send_intents i
    JOIN __forest_module_schema__.send_parts p
      ON p.bot_id=i.bot_id AND p.intent_id=i.intent_id
     AND p.part_index=i.current_part_index
    WHERE i.bot_id=context->>'bot_id' AND i.state='UNKNOWN'
      AND i.updated_at<=snapshot_at
      AND (input->>'lane_kind' IS NULL OR i.lane_kind=input->>'lane_kind')
      AND (input->>'channel_ref' IS NULL OR i.channel_ref=input->>'channel_ref')
      AND (input->>'before_updated_at' IS NULL OR
        (i.updated_at,i.intent_id)<((input->>'before_updated_at')::timestamptz,
                                    input->>'before_intent_id'))
    ORDER BY i.updated_at DESC,i.intent_id DESC LIMIT requested_limit
  ) ranked;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
    'value',jsonb_build_object('as_of',snapshot_at,'items',page));
END;
$$;

CREATE FUNCTION delivery_get_unknown(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE intent record; attempt record; part record; observed jsonb;
BEGIN
  SELECT * INTO intent FROM __forest_module_schema__.send_intents
   WHERE bot_id=context->>'bot_id' AND intent_id=input->>'intent_id';
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
      'error',jsonb_build_object('code','DELIVERY_INTENT_NOT_FOUND',
        'message','Delivery intent is unavailable','status',404));
  END IF;
  SELECT * INTO attempt FROM __forest_module_schema__.transport_attempts
   WHERE bot_id=intent.bot_id AND intent_id=intent.intent_id
   ORDER BY prepared_at DESC,attempt_id DESC LIMIT 1;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
      'error',jsonb_build_object('code','DELIVERY_ATTEMPT_NOT_FOUND',
        'message','Delivery intent has no transport attempt','status',404));
  END IF;
  SELECT * INTO part FROM __forest_module_schema__.send_parts
   WHERE bot_id=attempt.bot_id AND intent_id=attempt.intent_id
     AND part_index=attempt.part_index;
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'observation_operation_id',o.observation_operation_id,
    'effect_observation_operation_id',o.effect_observation_operation_id,
    'evidence_kind',o.evidence_kind,'observed_status',o.observed_status,
    'evidence_details',o.evidence_details,'accepted',o.accepted,
    'previous_state',o.previous_state,'resulting_state',o.resulting_state,
    'conflict_code',o.conflict_code,'observed_at',o.observed_at)
    ORDER BY o.observed_at,o.observation_operation_id),'[]'::jsonb)
    INTO observed
  FROM __forest_module_schema__.delivery_observations o
  WHERE o.bot_id=attempt.bot_id AND o.attempt_id=attempt.attempt_id;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object(
      'intent',jsonb_build_object('intent_id',intent.intent_id,'state',intent.state,
        'lane_kind',intent.lane_kind,'channel_kind',intent.channel_kind,
        'channel_ref',intent.channel_ref,'binding_ref',intent.binding_ref,
        'delivery_generation_id',intent.delivery_generation_id,
        'current_part_index',intent.current_part_index,'part_count',intent.part_count,
        'created_at',intent.created_at,'updated_at',intent.updated_at,
        'error_code',intent.error_code),
      'part',jsonb_build_object('part_index',part.part_index,'state',part.state,
        'effect_operation_ref',part.effect_operation_ref,
        'current_attempt_id',part.current_attempt_id),
      'attempt',jsonb_build_object('attempt_id',attempt.attempt_id,'state',attempt.state,
        'attempt_no',attempt.attempt_no,'effect_attempt_ref',attempt.effect_attempt_ref,
        'effect_operation_ref',attempt.effect_operation_ref,
        'adapter_generation_id',attempt.adapter_generation_id,
        'started_at',attempt.started_at,'settled_at',attempt.settled_at,
        'resolved_at',attempt.resolved_at,'error_code',attempt.error_code),
      'observations',observed));
END;
$$;
