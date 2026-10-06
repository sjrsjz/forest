CREATE TABLE conversation_lanes (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    conversation_id text NOT NULL CHECK (octet_length(conversation_id) BETWEEN 1 AND 256),
    conversation_key_digest text NOT NULL CHECK (conversation_key_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_component_id text NOT NULL CHECK (octet_length(source_component_id) BETWEEN 1 AND 128),
    binding_ref text NOT NULL CHECK (octet_length(binding_ref) BETWEEN 1 AND 256),
    platform text NOT NULL CHECK (octet_length(platform) BETWEEN 1 AND 64),
    channel_kind text NOT NULL CHECK (channel_kind IN ('GROUP','PRIVATE','SYSTEM')),
    channel_ref text NOT NULL CHECK (octet_length(channel_ref) BETWEEN 1 AND 256),
    thread_ref text CHECK (thread_ref IS NULL OR octet_length(thread_ref) BETWEEN 1 AND 256),
    next_ingress_seq bigint NOT NULL DEFAULT 1 CHECK (next_ingress_seq > 0),
    classified_through_seq bigint NOT NULL DEFAULT 0 CHECK (classified_through_seq >= 0),
    context_applied_seq bigint NOT NULL DEFAULT 0 CHECK (context_applied_seq >= 0),
    mailbox_revision bigint NOT NULL DEFAULT 0 CHECK (mailbox_revision >= 0),
    context_revision bigint NOT NULL DEFAULT 0 CHECK (context_revision >= 0),
    context_view_revision bigint NOT NULL DEFAULT 0 CHECK (context_view_revision >= 0),
    context_view_unit_count bigint NOT NULL DEFAULT 0 CHECK (context_view_unit_count >= 0),
    context_view_payload_bytes bigint NOT NULL DEFAULT 0
        CHECK (context_view_payload_bytes >= 0),
    context_view_media_bytes bigint NOT NULL DEFAULT 0
        CHECK (context_view_media_bytes >= 0),
    context_view_decoded_bytes bigint NOT NULL DEFAULT 0
        CHECK (context_view_decoded_bytes >= 0),
    media_cost_revision bigint NOT NULL DEFAULT 0 CHECK (media_cost_revision >= 0),
    maintenance_media_cost_revision bigint CHECK (
        maintenance_media_cost_revision IS NULL OR maintenance_media_cost_revision >= 0
    ),
    context_archived_unit_count bigint NOT NULL DEFAULT 0
        CHECK (context_archived_unit_count >= 0),
    maintenance_generation_id text CHECK (
        maintenance_generation_id IS NULL
        OR octet_length(maintenance_generation_id) BETWEEN 1 AND 256
    ),
    maintenance_context_revision bigint CHECK (
        maintenance_context_revision IS NULL OR maintenance_context_revision >= 0
    ),
    maintenance_view_revision bigint CHECK (
        maintenance_view_revision IS NULL OR maintenance_view_revision >= 0
    ),
    maintenance_retry_at timestamptz,
    actor_owner_id text CHECK (actor_owner_id IS NULL OR octet_length(actor_owner_id) BETWEEN 1 AND 256),
    actor_epoch bigint NOT NULL DEFAULT 0 CHECK (actor_epoch >= 0),
    actor_expires_at timestamptz,
    active_session_ref text CHECK (
        active_session_ref IS NULL OR octet_length(active_session_ref) BETWEEN 1 AND 256
    ),
    active_run_ref text CHECK (
        active_run_ref IS NULL OR octet_length(active_run_ref) BETWEEN 1 AND 256
    ),
    resume_not_before timestamptz,
    resume_reason_code text CHECK (
        resume_reason_code IS NULL OR octet_length(resume_reason_code) BETWEEN 1 AND 128
    ),
    resume_reason_ref text CHECK (
        resume_reason_ref IS NULL OR octet_length(resume_reason_ref) BETWEEN 1 AND 256
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, conversation_id),
    UNIQUE (bot_id, conversation_key_digest),
    CHECK (context_applied_seq <= classified_through_seq),
    CHECK (classified_through_seq < next_ingress_seq),
    CHECK (
        (actor_owner_id IS NULL AND actor_expires_at IS NULL)
        OR (actor_owner_id IS NOT NULL AND actor_expires_at IS NOT NULL AND actor_epoch > 0)
    ),
    CHECK (active_run_ref IS NULL OR active_session_ref IS NOT NULL),
    CHECK ((resume_not_before IS NULL) = (resume_reason_code IS NULL)),
    CHECK (resume_reason_ref IS NULL OR resume_reason_code IS NOT NULL),
    CHECK (resume_not_before IS NULL OR active_session_ref IS NOT NULL),
    CHECK (
        (maintenance_generation_id IS NULL AND maintenance_context_revision IS NULL
         AND maintenance_view_revision IS NULL AND maintenance_media_cost_revision IS NULL)
        OR (maintenance_generation_id IS NOT NULL
            AND maintenance_context_revision IS NOT NULL
            AND maintenance_view_revision IS NOT NULL
            AND maintenance_media_cost_revision IS NOT NULL)
    )
);

CREATE TABLE inbox_messages (
    bot_id text NOT NULL,
    conversation_id text NOT NULL,
    event_id text NOT NULL CHECK (octet_length(event_id) BETWEEN 1 AND 256),
    ingress_seq bigint NOT NULL CHECK (ingress_seq > 0),
    admission_operation_id text NOT NULL
        CHECK (octet_length(admission_operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_component_id text NOT NULL CHECK (octet_length(source_component_id) BETWEEN 1 AND 128),
    source_event_ref text NOT NULL CHECK (octet_length(source_event_ref) BETWEEN 1 AND 512),
    event_kind text NOT NULL CHECK (octet_length(event_kind) BETWEEN 1 AND 128),
    platform_message_ref text CHECK (
        platform_message_ref IS NULL OR octet_length(platform_message_ref) BETWEEN 1 AND 512
    ),
    author_subject_ref text NOT NULL
        CHECK (octet_length(author_subject_ref) BETWEEN 1 AND 256),
    content_payload_id text NOT NULL CHECK (octet_length(content_payload_id) BETWEEN 1 AND 256),
    content_payload_digest text NOT NULL CHECK (content_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    content_byte_length integer NOT NULL CHECK (content_byte_length BETWEEN 1 AND 1048576),
    content_owner_component_id text NOT NULL
        CHECK (octet_length(content_owner_component_id) BETWEEN 1 AND 128),
    content_owner_kind text NOT NULL
        CHECK (octet_length(content_owner_kind) BETWEEN 1 AND 128),
    content_owner_id text NOT NULL CHECK (octet_length(content_owner_id) BETWEEN 1 AND 512),
    author_payload_id text NOT NULL CHECK (octet_length(author_payload_id) BETWEEN 1 AND 256),
    author_payload_digest text NOT NULL CHECK (author_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    author_owner_component_id text NOT NULL
        CHECK (octet_length(author_owner_component_id) BETWEEN 1 AND 128),
    author_owner_kind text NOT NULL
        CHECK (octet_length(author_owner_kind) BETWEEN 1 AND 128),
    author_owner_id text NOT NULL CHECK (octet_length(author_owner_id) BETWEEN 1 AND 512),
    context_disposition text NOT NULL CHECK (
        context_disposition IN ('MATERIALIZE','PLACEHOLDER')
    ),
    classification_state text NOT NULL DEFAULT 'PENDING' CHECK (
        classification_state IN ('PENDING','FINAL')
    ),
    trigger_decision text CHECK (
        trigger_decision IS NULL OR trigger_decision IN (
            'NO_TRIGGER','TRIGGER','CONTROL_COMMAND','ROUTED','FAILED'
        )
    ),
    classification_operation_id text CHECK (
        classification_operation_id IS NULL
        OR octet_length(classification_operation_id) BETWEEN 1 AND 256
    ),
    classification_input_digest text CHECK (
        classification_input_digest IS NULL
        OR classification_input_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    classifier_component_id text CHECK (
        classifier_component_id IS NULL
        OR octet_length(classifier_component_id) BETWEEN 1 AND 128
    ),
    classifier_generation_id text CHECK (
        classifier_generation_id IS NULL
        OR octet_length(classifier_generation_id) BETWEEN 1 AND 256
    ),
    rule_revision text CHECK (
        rule_revision IS NULL OR octet_length(rule_revision) BETWEEN 1 AND 256
    ),
    reason_code text CHECK (reason_code IS NULL OR octet_length(reason_code) BETWEEN 1 AND 128),
    route_ref text CHECK (route_ref IS NULL OR octet_length(route_ref) BETWEEN 1 AND 512),
    sent_at timestamptz,
    received_at timestamptz NOT NULL,
    classification_deadline timestamptz NOT NULL,
    classified_at timestamptz,
    PRIMARY KEY (bot_id, conversation_id, event_id),
    UNIQUE (bot_id, conversation_id, ingress_seq),
    UNIQUE (bot_id, admission_operation_id),
    FOREIGN KEY (bot_id, conversation_id)
        REFERENCES conversation_lanes(bot_id, conversation_id),
    CHECK (classification_deadline > received_at),
    CHECK (
        (classification_state='PENDING' AND trigger_decision IS NULL
            AND classification_operation_id IS NULL AND classification_input_digest IS NULL
            AND classifier_component_id IS NULL AND classifier_generation_id IS NULL
            AND classified_at IS NULL)
        OR (classification_state='FINAL' AND trigger_decision IS NOT NULL
            AND classification_operation_id IS NOT NULL AND classification_input_digest IS NOT NULL
            AND classifier_component_id IS NOT NULL AND classifier_generation_id IS NOT NULL
            AND classified_at IS NOT NULL)
    )
);

CREATE TABLE conversation_triggers (
    bot_id text NOT NULL,
    conversation_id text NOT NULL,
    trigger_id text NOT NULL CHECK (octet_length(trigger_id) BETWEEN 1 AND 256),
    event_id text NOT NULL,
    ingress_seq bigint NOT NULL CHECK (ingress_seq > 0),
    principal_ref text NOT NULL CHECK (octet_length(principal_ref) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    status text NOT NULL DEFAULT 'PENDING' CHECK (
        status IN ('PENDING','CLAIMED','ANSWERED','FAILED','CANCELLED')
    ),
    claimed_session_ref text CHECK (
        claimed_session_ref IS NULL OR octet_length(claimed_session_ref) BETWEEN 1 AND 256
    ),
    claimed_run_ref text CHECK (
        claimed_run_ref IS NULL OR octet_length(claimed_run_ref) BETWEEN 1 AND 256
    ),
    input_batch_id text,
    answered_reply_ref text CHECK (
        answered_reply_ref IS NULL OR octet_length(answered_reply_ref) BETWEEN 1 AND 512
    ),
    terminal_reason_code text CHECK (
        terminal_reason_code IS NULL OR octet_length(terminal_reason_code) BETWEEN 1 AND 128
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, trigger_id),
    UNIQUE (bot_id, conversation_id, event_id),
    FOREIGN KEY (bot_id, conversation_id, event_id)
        REFERENCES inbox_messages(bot_id, conversation_id, event_id),
    CHECK (
        (status='PENDING' AND claimed_session_ref IS NULL AND claimed_run_ref IS NULL
            AND input_batch_id IS NULL AND answered_reply_ref IS NULL
            AND terminal_reason_code IS NULL)
        OR (status='CLAIMED' AND claimed_session_ref IS NOT NULL AND claimed_run_ref IS NOT NULL
            AND input_batch_id IS NOT NULL AND answered_reply_ref IS NULL
            AND terminal_reason_code IS NULL)
        OR (status='ANSWERED' AND claimed_session_ref IS NOT NULL AND claimed_run_ref IS NOT NULL
            AND input_batch_id IS NOT NULL AND answered_reply_ref IS NOT NULL
            AND terminal_reason_code IS NULL)
        OR (status IN ('FAILED','CANCELLED') AND answered_reply_ref IS NULL
            AND terminal_reason_code IS NOT NULL)
    )
);

CREATE TABLE input_batches (
    bot_id text NOT NULL,
    conversation_id text NOT NULL,
    input_batch_id text NOT NULL CHECK (octet_length(input_batch_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    boundary text NOT NULL CHECK (boundary IN ('IDLE','MODEL_SAFE_POINT','SESSION_END')),
    kind text NOT NULL CHECK (kind IN (
        'TRIGGER_INPUT','CONTEXT_PREFILL','SILENT_DRAIN',
        'RECOVERY_CLAIM','RECOVERY_PREFILL'
    )),
    start_seq bigint CHECK (start_seq IS NULL OR start_seq > 0),
    end_seq bigint CHECK (end_seq IS NULL OR end_seq > 0),
    trigger_cut_seq bigint CHECK (trigger_cut_seq IS NULL OR trigger_cut_seq > 0),
    selected_input_end_seq bigint NOT NULL CHECK (selected_input_end_seq >= 0),
    session_ref text CHECK (session_ref IS NULL OR octet_length(session_ref) BETWEEN 1 AND 256),
    run_ref text CHECK (run_ref IS NULL OR octet_length(run_ref) BETWEEN 1 AND 256),
    actor_owner_id text NOT NULL CHECK (octet_length(actor_owner_id) BETWEEN 1 AND 256),
    actor_epoch bigint NOT NULL CHECK (actor_epoch > 0),
    context_revision bigint NOT NULL CHECK (context_revision > 0),
    context_view_revision bigint NOT NULL CHECK (context_view_revision >= 0),
    trigger_count integer CHECK (trigger_count IS NULL OR trigger_count > 0),
    last_trigger_ref text CHECK (
        last_trigger_ref IS NULL OR octet_length(last_trigger_ref) BETWEEN 1 AND 256
    ),
    last_trigger_ingress_seq bigint CHECK (
        last_trigger_ingress_seq IS NULL OR last_trigger_ingress_seq > 0
    ),
    trigger_claim_digest text CHECK (
        trigger_claim_digest IS NULL OR trigger_claim_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    trigger_claim_ref text CHECK (
        trigger_claim_ref IS NULL OR trigger_claim_ref ~ '^claim:[a-f0-9]{64}$'
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, input_batch_id),
    UNIQUE (bot_id, operation_id),
    FOREIGN KEY (bot_id, conversation_id)
        REFERENCES conversation_lanes(bot_id, conversation_id),
    CHECK ((session_ref IS NULL) = (run_ref IS NULL)),
    CHECK (
        (kind IN ('TRIGGER_INPUT','RECOVERY_CLAIM','RECOVERY_PREFILL')
            AND trigger_count IS NOT NULL AND last_trigger_ref IS NOT NULL
            AND last_trigger_ingress_seq IS NOT NULL
            AND last_trigger_ingress_seq<=trigger_cut_seq
            AND trigger_claim_digest IS NOT NULL AND trigger_claim_ref IS NOT NULL)
        OR (kind='CONTEXT_PREFILL' AND (
            (trigger_count IS NULL AND last_trigger_ref IS NULL
             AND last_trigger_ingress_seq IS NULL AND trigger_claim_digest IS NULL
             AND trigger_claim_ref IS NULL)
            OR (trigger_count IS NOT NULL AND last_trigger_ref IS NOT NULL
                AND last_trigger_ingress_seq IS NOT NULL
                AND last_trigger_ingress_seq<=end_seq
                AND trigger_claim_digest IS NOT NULL AND trigger_claim_ref IS NOT NULL)
        ))
        OR (kind='SILENT_DRAIN'
            AND trigger_count IS NULL AND last_trigger_ref IS NULL
            AND last_trigger_ingress_seq IS NULL AND trigger_claim_digest IS NULL
            AND trigger_claim_ref IS NULL)
    ),
    CHECK (
        (kind='TRIGGER_INPUT' AND start_seq IS NOT NULL AND end_seq IS NOT NULL
            AND start_seq <= end_seq AND trigger_cut_seq=end_seq
            AND selected_input_end_seq=end_seq AND session_ref IS NOT NULL)
        OR (kind='CONTEXT_PREFILL' AND start_seq IS NOT NULL AND end_seq IS NOT NULL
            AND start_seq <= end_seq AND end_seq-start_seq < 64
            AND trigger_cut_seq IS NULL AND selected_input_end_seq=end_seq
            AND session_ref IS NOT NULL)
        OR (kind='SILENT_DRAIN' AND start_seq IS NOT NULL AND end_seq IS NOT NULL
            AND start_seq <= end_seq AND trigger_cut_seq IS NULL
            AND selected_input_end_seq=end_seq AND session_ref IS NULL)
        OR (kind IN ('RECOVERY_CLAIM','RECOVERY_PREFILL')
            AND start_seq IS NULL AND end_seq IS NULL
            AND trigger_cut_seq IS NOT NULL AND selected_input_end_seq >= trigger_cut_seq
            AND session_ref IS NOT NULL)
    )
);

CREATE TABLE selection_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    conversation_id text NOT NULL,
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    boundary text NOT NULL CHECK (boundary IN ('IDLE','MODEL_SAFE_POINT','SESSION_END')),
    result_status text NOT NULL CHECK (
        result_status IN ('BATCH','WAIT_INPUT','CONTINUE','IDLE')
    ),
    input_batch_id text,
    observed_mailbox_revision bigint NOT NULL CHECK (observed_mailbox_revision >= 0),
    observed_classified_through_seq bigint NOT NULL CHECK (observed_classified_through_seq >= 0),
    selected_input_end_seq bigint NOT NULL CHECK (selected_input_end_seq >= 0),
    actor_owner_id text NOT NULL CHECK (octet_length(actor_owner_id) BETWEEN 1 AND 256),
    actor_epoch bigint NOT NULL CHECK (actor_epoch > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, operation_id),
    FOREIGN KEY (bot_id, conversation_id)
        REFERENCES conversation_lanes(bot_id, conversation_id),
    FOREIGN KEY (bot_id, input_batch_id)
        REFERENCES input_batches(bot_id, input_batch_id),
    CHECK (
        (result_status='BATCH' AND input_batch_id IS NOT NULL)
        OR (result_status<>'BATCH' AND input_batch_id IS NULL)
    )
);

CREATE TABLE input_batch_events (
    bot_id text NOT NULL,
    input_batch_id text NOT NULL,
    event_index integer NOT NULL CHECK (event_index >= 0),
    conversation_id text NOT NULL,
    event_id text NOT NULL,
    ingress_seq bigint NOT NULL CHECK (ingress_seq > 0),
    PRIMARY KEY (bot_id, input_batch_id, event_index),
    UNIQUE (bot_id, conversation_id, event_id),
    FOREIGN KEY (bot_id, input_batch_id)
        REFERENCES input_batches(bot_id, input_batch_id),
    FOREIGN KEY (bot_id, conversation_id, event_id)
        REFERENCES inbox_messages(bot_id, conversation_id, event_id)
);

CREATE TABLE input_batch_triggers (
    bot_id text NOT NULL,
    input_batch_id text NOT NULL,
    trigger_index integer NOT NULL CHECK (trigger_index >= 0),
    trigger_id text NOT NULL,
    PRIMARY KEY (bot_id, input_batch_id, trigger_index),
    UNIQUE (bot_id, input_batch_id, trigger_id),
    FOREIGN KEY (bot_id, input_batch_id)
        REFERENCES input_batches(bot_id, input_batch_id),
    FOREIGN KEY (bot_id, trigger_id)
        REFERENCES conversation_triggers(bot_id, trigger_id)
);

CREATE TABLE trigger_claim_groups (
    bot_id text NOT NULL,
    conversation_id text NOT NULL,
    claim_ref text NOT NULL CHECK (claim_ref ~ '^claim:[a-f0-9]{64}$'),
    session_ref text NOT NULL CHECK (octet_length(session_ref) BETWEEN 1 AND 256),
    run_ref text NOT NULL CHECK (octet_length(run_ref) BETWEEN 1 AND 256),
    trigger_count integer NOT NULL CHECK (trigger_count>0),
    segment_count integer NOT NULL CHECK (segment_count>0),
    last_trigger_ref text NOT NULL CHECK (octet_length(last_trigger_ref) BETWEEN 1 AND 256),
    last_trigger_ingress_seq bigint NOT NULL CHECK (last_trigger_ingress_seq>0),
    claim_digest text NOT NULL CHECK (claim_digest ~ '^sha256:[a-f0-9]{64}$'),
    claim_state text NOT NULL DEFAULT 'ACTIVE' CHECK (claim_state IN ('ACTIVE','TERMINAL')),
    terminal_operation_id text CHECK (
        terminal_operation_id IS NULL OR octet_length(terminal_operation_id) BETWEEN 1 AND 256
    ),
    terminal_status text CHECK (
        terminal_status IS NULL OR terminal_status IN (
            'ANSWERED','FAILED','CANCELLED','REQUEUED'
        )
    ),
    PRIMARY KEY (bot_id,claim_ref),
    UNIQUE (bot_id,conversation_id,session_ref,run_ref),
    FOREIGN KEY (bot_id,conversation_id)
        REFERENCES conversation_lanes(bot_id,conversation_id),
    CHECK ((claim_state='ACTIVE')=(terminal_operation_id IS NULL)),
    CHECK ((claim_state='ACTIVE')=(terminal_status IS NULL))
);

CREATE TABLE trigger_claim_segments (
    bot_id text NOT NULL,
    claim_ref text NOT NULL,
    segment_index integer NOT NULL CHECK (segment_index>=0),
    input_batch_id text NOT NULL,
    trigger_count integer NOT NULL CHECK (trigger_count BETWEEN 1 AND 64),
    segment_digest text NOT NULL CHECK (segment_digest ~ '^sha256:[a-f0-9]{64}$'),
    PRIMARY KEY (bot_id,claim_ref,segment_index),
    UNIQUE (bot_id,input_batch_id),
    FOREIGN KEY (bot_id,claim_ref) REFERENCES trigger_claim_groups(bot_id,claim_ref),
    FOREIGN KEY (bot_id,input_batch_id) REFERENCES input_batches(bot_id,input_batch_id)
);

CREATE TABLE context_units (
    bot_id text NOT NULL,
    conversation_id text NOT NULL,
    unit_id text NOT NULL CHECK (octet_length(unit_id) BETWEEN 1 AND 256),
    context_revision bigint NOT NULL CHECK (context_revision > 0),
    unit_index integer NOT NULL CHECK (unit_index >= 0),
    kind text NOT NULL CHECK (kind IN ('INPUT','AGENT_STEP','REPLY','CONTROL','EFFECT')),
    source_component_id text NOT NULL CHECK (octet_length(source_component_id) BETWEEN 1 AND 128),
    source_kind text NOT NULL CHECK (octet_length(source_kind) BETWEEN 1 AND 128),
    source_id text NOT NULL CHECK (octet_length(source_id) BETWEEN 1 AND 512),
    input_batch_id text,
    event_id text,
    ingress_seq bigint CHECK (ingress_seq IS NULL OR ingress_seq > 0),
    payload_id text NOT NULL CHECK (octet_length(payload_id) BETWEEN 1 AND 256),
    payload_digest text NOT NULL CHECK (payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    payload_byte_length integer NOT NULL CHECK (payload_byte_length BETWEEN 1 AND 1048576),
    payload_owner_component_id text NOT NULL
        CHECK (octet_length(payload_owner_component_id) BETWEEN 1 AND 128),
    payload_owner_kind text NOT NULL
        CHECK (octet_length(payload_owner_kind) BETWEEN 1 AND 128),
    payload_owner_id text NOT NULL CHECK (octet_length(payload_owner_id) BETWEEN 1 AND 512),
    author_payload_id text,
    author_payload_digest text CHECK (
        author_payload_digest IS NULL OR author_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    author_owner_component_id text CHECK (
        author_owner_component_id IS NULL
        OR octet_length(author_owner_component_id) BETWEEN 1 AND 128
    ),
    author_owner_kind text CHECK (
        author_owner_kind IS NULL OR octet_length(author_owner_kind) BETWEEN 1 AND 128
    ),
    author_owner_id text CHECK (
        author_owner_id IS NULL OR octet_length(author_owner_id) BETWEEN 1 AND 512
    ),
    author_subject_ref text CHECK (
        author_subject_ref IS NULL OR octet_length(author_subject_ref) BETWEEN 1 AND 256
    ),
    trigger_decision text CHECK (
        trigger_decision IS NULL OR trigger_decision IN (
            'NO_TRIGGER','TRIGGER','CONTROL_COMMAND','ROUTED','FAILED'
        )
    ),
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, conversation_id, unit_id),
    UNIQUE (bot_id, conversation_id, context_revision, unit_index),
    FOREIGN KEY (bot_id, conversation_id)
        REFERENCES conversation_lanes(bot_id, conversation_id),
    FOREIGN KEY (bot_id, input_batch_id)
        REFERENCES input_batches(bot_id, input_batch_id),
    CHECK ((author_payload_id IS NULL) = (author_payload_digest IS NULL)),
    CHECK (
        (author_payload_id IS NULL AND author_owner_component_id IS NULL
            AND author_owner_kind IS NULL AND author_owner_id IS NULL)
        OR (author_payload_id IS NOT NULL AND author_owner_component_id IS NOT NULL
            AND author_owner_kind IS NOT NULL AND author_owner_id IS NOT NULL)
    ),
    CHECK (
        (kind='INPUT' AND input_batch_id IS NOT NULL AND event_id IS NOT NULL
            AND ingress_seq IS NOT NULL AND author_payload_id IS NOT NULL
            AND author_subject_ref IS NOT NULL AND trigger_decision IS NOT NULL)
        OR (kind<>'INPUT' AND event_id IS NULL AND ingress_seq IS NULL
            AND trigger_decision IS NULL)
    )
);

CREATE UNIQUE INDEX context_input_event_idx
    ON context_units(bot_id, conversation_id, event_id) WHERE kind='INPUT';

-- Media arrives after ingress.  The inbox identity is immutable; this fact
-- records the verified asset cost without changing the original message.
CREATE TABLE context_media_costs (
    bot_id text NOT NULL,
    conversation_id text NOT NULL,
    event_id text NOT NULL,
    part_index integer NOT NULL CHECK (part_index >= 0),
    media_ref text NOT NULL CHECK (media_ref ~ '^media:[a-f0-9]{64}$'),
    media_revision bigint NOT NULL CHECK (media_revision = 1),
    payload_id text NOT NULL CHECK (octet_length(payload_id) BETWEEN 1 AND 256),
    payload_digest text NOT NULL CHECK (payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    byte_length integer NOT NULL CHECK (byte_length BETWEEN 1 AND 8388608),
    width integer NOT NULL CHECK (width > 0),
    height integer NOT NULL CHECK (height > 0),
    decoded_pixels integer NOT NULL CHECK (
        decoded_pixels BETWEEN 1 AND 16777216
        AND decoded_pixels::bigint >= width::bigint * height::bigint
    ),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,conversation_id,event_id,part_index),
    UNIQUE (bot_id,media_ref,media_revision),
    FOREIGN KEY (bot_id,conversation_id,event_id)
        REFERENCES inbox_messages(bot_id,conversation_id,event_id)
);

CREATE INDEX context_media_costs_event_idx
    ON context_media_costs(bot_id,conversation_id,event_id);

CREATE FUNCTION conversation_record_media_cost(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_conversation text := input->>'conversation_id';
    requested_event text := input->>'event_id';
    requested_index integer := (input->>'part_index')::integer;
    requested_revision bigint := (input->>'media_revision')::bigint;
    message record;
    previous record;
    active_unit boolean;
    expected_ref text;
BEGIN
    IF context->>'component_id' IS DISTINCT FROM 'conversation.scheduler'
       OR context->>'port_name' IS DISTINCT FROM 'conversation.media-cost.write'
       OR context->>'port_kind' IS DISTINCT FROM 'command'
       OR requested_bot IS NULL OR requested_conversation IS NULL
       OR requested_event IS NULL OR requested_index IS NULL OR requested_index<0
       OR requested_revision<>1
       OR input->>'part_id' IS DISTINCT FROM 'part:image:'||requested_index::text
       OR input#>>'{payload,owner_component_id}' IS DISTINCT FROM 'media.capture'
       OR input#>>'{payload,owner_kind}' IS DISTINCT FROM 'media.asset'
       OR input#>>'{payload,owner_id}' IS DISTINCT FROM input->>'media_ref'
       OR (input->>'decoded_pixels')::bigint NOT BETWEEN 1 AND 16777216
       OR (input->>'decoded_pixels')::bigint <
            (input->>'width')::bigint * (input->>'height')::bigint THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','CONTEXT_MEDIA_COST_INVALID',
                'message','Media cost identity is invalid','status',422));
    END IF;
    PERFORM 1 FROM __forest_module_schema__.conversation_lanes
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','CONVERSATION_NOT_FOUND',
                'message','Conversation lane does not exist','status',404));
    END IF;
    SELECT source_component_id,source_event_ref,ingress_seq INTO message
    FROM __forest_module_schema__.inbox_messages
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
      AND event_id=requested_event;
    expected_ref := 'media:'||encode(sha256(
        convert_to(message.source_event_ref,'UTF8') || decode('00','hex') ||
        convert_to(requested_index::text,'UTF8')),'hex');
    IF message.source_component_id IS DISTINCT FROM 'adapter.onebot'
       OR message.ingress_seq IS DISTINCT FROM (input->>'ingress_seq')::bigint
       OR input->>'media_ref' IS DISTINCT FROM expected_ref THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','CONTEXT_MEDIA_SOURCE_MISMATCH',
                'message','Media asset does not belong to this message','status',409));
    END IF;
    SELECT * INTO previous FROM __forest_module_schema__.context_media_costs
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
      AND event_id=requested_event AND part_index=requested_index;
    IF FOUND THEN
        IF previous.media_ref IS DISTINCT FROM input->>'media_ref'
           OR previous.media_revision IS DISTINCT FROM requested_revision
           OR previous.payload_id IS DISTINCT FROM input#>>'{payload,payload_id}'
           OR previous.payload_digest IS DISTINCT FROM input#>>'{payload,digest}'
           OR previous.byte_length IS DISTINCT FROM (input->>'byte_length')::integer
           OR previous.width IS DISTINCT FROM (input->>'width')::integer
           OR previous.height IS DISTINCT FROM (input->>'height')::integer
           OR previous.decoded_pixels IS DISTINCT FROM
              (input->>'decoded_pixels')::integer THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','CONTEXT_MEDIA_COST_CONFLICT',
                    'message','Media cost fact differs from its first publication','status',409));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('recorded',true));
    END IF;
    INSERT INTO __forest_module_schema__.context_media_costs(
        bot_id,conversation_id,event_id,part_index,media_ref,media_revision,
        payload_id,payload_digest,byte_length,width,height,decoded_pixels
    ) VALUES (requested_bot,requested_conversation,requested_event,requested_index,
        input->>'media_ref',requested_revision,input#>>'{payload,payload_id}',
        input#>>'{payload,digest}',
        (input->>'byte_length')::integer,(input->>'width')::integer,
        (input->>'height')::integer,(input->>'decoded_pixels')::integer);
    SELECT EXISTS (
        SELECT 1 FROM __forest_module_schema__.active_context_units a
        JOIN __forest_module_schema__.context_units u
          ON u.bot_id=a.bot_id AND u.conversation_id=a.conversation_id
         AND u.unit_id=a.unit_id
        WHERE a.bot_id=requested_bot AND a.conversation_id=requested_conversation
          AND u.event_id=requested_event
    ) INTO active_unit;
    UPDATE __forest_module_schema__.conversation_lanes
    SET context_view_media_bytes=context_view_media_bytes+
            CASE WHEN active_unit THEN (input->>'byte_length')::integer ELSE 0 END,
        context_view_decoded_bytes=context_view_decoded_bytes+
            CASE WHEN active_unit THEN (input->>'decoded_pixels')::bigint*4 ELSE 0 END,
        media_cost_revision=media_cost_revision+1,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('recorded',true));
END;
$$;

CREATE TABLE context_append_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    conversation_id text NOT NULL,
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    append_kind text NOT NULL CHECK (append_kind IN ('FACTS','FINAL_REPLY')),
    session_ref text CHECK (session_ref IS NULL OR octet_length(session_ref) BETWEEN 1 AND 256),
    run_ref text CHECK (run_ref IS NULL OR octet_length(run_ref) BETWEEN 1 AND 256),
    context_revision bigint NOT NULL CHECK (context_revision > 0),
    unit_count integer NOT NULL CHECK (unit_count > 0),
    answered_trigger_count integer NOT NULL DEFAULT 0 CHECK (answered_trigger_count >= 0),
    reply_ref text CHECK (reply_ref IS NULL OR octet_length(reply_ref) BETWEEN 1 AND 512),
    actor_owner_id text NOT NULL CHECK (octet_length(actor_owner_id) BETWEEN 1 AND 256),
    actor_epoch bigint NOT NULL CHECK (actor_epoch > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, operation_id),
    UNIQUE (bot_id, conversation_id, context_revision),
    FOREIGN KEY (bot_id, conversation_id)
        REFERENCES conversation_lanes(bot_id, conversation_id),
    CHECK ((session_ref IS NULL) = (run_ref IS NULL)),
    CHECK (
        (append_kind='FACTS' AND answered_trigger_count=0 AND reply_ref IS NULL)
        OR (append_kind='FINAL_REPLY' AND answered_trigger_count>0 AND reply_ref IS NOT NULL
            AND session_ref IS NOT NULL)
    )
);

CREATE TABLE context_compactions (
    bot_id text NOT NULL,
    conversation_id text NOT NULL,
    compaction_id text NOT NULL CHECK (octet_length(compaction_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    base_view_revision bigint NOT NULL CHECK (base_view_revision >= 0),
    published_view_revision bigint NOT NULL CHECK (published_view_revision > base_view_revision),
    source_set_digest text NOT NULL CHECK (source_set_digest ~ '^sha256:[a-f0-9]{64}$'),
    policy_revision text NOT NULL CHECK (octet_length(policy_revision) BETWEEN 1 AND 256),
    archive_payload_id text NOT NULL CHECK (octet_length(archive_payload_id) BETWEEN 1 AND 256),
    archive_payload_digest text NOT NULL CHECK (archive_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    archived_unit_count bigint NOT NULL CHECK (archived_unit_count > 0),
    archived_payload_bytes bigint NOT NULL CHECK (archived_payload_bytes > 0),
    archived_media_bytes bigint NOT NULL CHECK (archived_media_bytes >= 0),
    archived_decoded_bytes bigint NOT NULL CHECK (archived_decoded_bytes >= 0),
    resulting_view_unit_count bigint NOT NULL CHECK (resulting_view_unit_count >= 0),
    resulting_view_payload_bytes bigint NOT NULL CHECK (resulting_view_payload_bytes >= 0),
    resulting_view_media_bytes bigint NOT NULL CHECK (resulting_view_media_bytes >= 0),
    resulting_view_decoded_bytes bigint NOT NULL CHECK (resulting_view_decoded_bytes >= 0),
    base_media_cost_revision bigint NOT NULL CHECK (base_media_cost_revision >= 0),
    cumulative_archived_unit_count bigint NOT NULL
        CHECK (cumulative_archived_unit_count >= archived_unit_count),
    first_context_revision bigint NOT NULL CHECK (first_context_revision > 0),
    last_context_revision bigint NOT NULL CHECK (last_context_revision >= first_context_revision),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, compaction_id),
    UNIQUE (bot_id, operation_id),
    UNIQUE (bot_id, conversation_id, published_view_revision),
    FOREIGN KEY (bot_id, conversation_id)
        REFERENCES conversation_lanes(bot_id, conversation_id)
);

CREATE TABLE context_compaction_units (
    bot_id text NOT NULL,
    conversation_id text NOT NULL,
    compaction_id text NOT NULL,
    archived_index bigint NOT NULL CHECK (archived_index >= 0),
    media_byte_length bigint NOT NULL DEFAULT 0 CHECK (media_byte_length >= 0),
    decoded_byte_length bigint NOT NULL DEFAULT 0 CHECK (decoded_byte_length >= 0),
    unit_id text NOT NULL,
    context_revision bigint NOT NULL CHECK (context_revision > 0),
    unit_index integer NOT NULL CHECK (unit_index >= 0),
    payload_digest text NOT NULL CHECK (payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    author_payload_digest text CHECK (
        author_payload_digest IS NULL OR author_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    PRIMARY KEY (bot_id, compaction_id, archived_index),
    UNIQUE (bot_id, conversation_id, unit_id),
    FOREIGN KEY (bot_id, compaction_id)
        REFERENCES context_compactions(bot_id, compaction_id),
    FOREIGN KEY (bot_id, conversation_id, unit_id)
        REFERENCES context_units(bot_id, conversation_id, unit_id)
);

-- A transactional current-view index. Immutable context_units and archive
-- membership remain the authority; this avoids scanning years of archived
-- units on every Context read or bounded compaction claim.
CREATE TABLE active_context_units (
    bot_id text NOT NULL,
    conversation_id text NOT NULL,
    unit_id text NOT NULL,
    context_revision bigint NOT NULL CHECK (context_revision>0),
    unit_index integer NOT NULL CHECK (unit_index>=0),
    PRIMARY KEY (bot_id,conversation_id,unit_id),
    UNIQUE (bot_id,conversation_id,context_revision,unit_index),
    FOREIGN KEY (bot_id,conversation_id,unit_id)
        REFERENCES context_units(bot_id,conversation_id,unit_id)
);

CREATE INDEX inbox_classification_idx
    ON inbox_messages(bot_id, conversation_id, ingress_seq, classification_state);
CREATE INDEX trigger_pending_idx
    ON conversation_triggers(bot_id, conversation_id, status, ingress_seq, trigger_id);
CREATE INDEX trigger_claimed_run_idx
    ON conversation_triggers(bot_id, conversation_id, claimed_run_ref, event_id)
    WHERE status='CLAIMED';
CREATE INDEX active_trigger_claim_group_idx
    ON trigger_claim_groups(bot_id,conversation_id,claim_ref)
    WHERE claim_state='ACTIVE';
CREATE INDEX unsettled_requeue_group_idx
    ON trigger_claim_groups(bot_id,conversation_id,claim_ref)
    WHERE terminal_status='REQUEUED';
CREATE INDEX context_order_idx
    ON context_units(bot_id, conversation_id, context_revision, unit_index);
CREATE INDEX context_compaction_order_idx
    ON context_compaction_units(bot_id,conversation_id,context_revision,unit_index);
CREATE INDEX lane_wakeup_idx ON conversation_lanes(
    bot_id,resume_not_before,mailbox_revision,conversation_id
);
CREATE INDEX lane_context_maintenance_idx ON conversation_lanes(
    bot_id,maintenance_retry_at,updated_at,conversation_id
) WHERE context_view_unit_count>1;

CREATE TABLE trigger_terminal_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    conversation_id text NOT NULL,
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    terminal_session_ref text NOT NULL
        CHECK (octet_length(terminal_session_ref) BETWEEN 1 AND 256),
    terminal_run_ref text NOT NULL CHECK (octet_length(terminal_run_ref) BETWEEN 1 AND 256),
    resulting_status text NOT NULL CHECK (resulting_status IN ('FAILED','CANCELLED')),
    reason_code text NOT NULL CHECK (octet_length(reason_code) BETWEEN 1 AND 128),
    actor_owner_id text NOT NULL CHECK (octet_length(actor_owner_id) BETWEEN 1 AND 256),
    actor_epoch bigint NOT NULL CHECK (actor_epoch > 0),
    trigger_count integer NOT NULL CHECK (trigger_count > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, operation_id),
    FOREIGN KEY (bot_id, conversation_id)
        REFERENCES conversation_lanes(bot_id, conversation_id)
);

CREATE TABLE trigger_requeue_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    request_id text NOT NULL CHECK (octet_length(request_id) BETWEEN 1 AND 256),
    conversation_id text NOT NULL,
    claim_ref text NOT NULL CHECK (claim_ref ~ '^claim:[a-f0-9]{64}$'),
    claim_digest text NOT NULL CHECK (claim_digest ~ '^sha256:[a-f0-9]{64}$'),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    abandoned_session_ref text NOT NULL
        CHECK (octet_length(abandoned_session_ref) BETWEEN 1 AND 256),
    abandoned_run_ref text NOT NULL
        CHECK (octet_length(abandoned_run_ref) BETWEEN 1 AND 256),
    reason_code text NOT NULL CHECK (octet_length(reason_code) BETWEEN 1 AND 128),
    actor_owner_id text NOT NULL CHECK (octet_length(actor_owner_id) BETWEEN 1 AND 256),
    actor_epoch bigint NOT NULL CHECK (actor_epoch > 0),
    trigger_count integer NOT NULL CHECK (trigger_count > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    UNIQUE (bot_id,claim_ref),
    FOREIGN KEY (bot_id,claim_ref) REFERENCES trigger_claim_groups(bot_id,claim_ref),
    FOREIGN KEY (bot_id,conversation_id)
        REFERENCES conversation_lanes(bot_id,conversation_id)
);

CREATE TABLE conversation_requeue_requests (
    bot_id text NOT NULL,
    request_id text NOT NULL CHECK (octet_length(request_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    conversation_id text NOT NULL,
    session_ref text NOT NULL CHECK (octet_length(session_ref) BETWEEN 1 AND 256),
    run_ref text NOT NULL CHECK (octet_length(run_ref) BETWEEN 1 AND 256),
    claim_ref text NOT NULL CHECK (claim_ref ~ '^claim:[a-f0-9]{64}$'),
    claim_digest text NOT NULL CHECK (claim_digest ~ '^sha256:[a-f0-9]{64}$'),
    reason_code text NOT NULL CHECK (octet_length(reason_code) BETWEEN 1 AND 128),
    requested_by text NOT NULL CHECK (octet_length(requested_by) BETWEEN 1 AND 256),
    state text NOT NULL DEFAULT 'PENDING'
        CHECK (state IN ('PENDING','APPLIED','REJECTED')),
    disposition_code text CHECK (
        disposition_code IS NULL OR octet_length(disposition_code) BETWEEN 1 AND 128
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    settled_at timestamptz,
    PRIMARY KEY (bot_id,request_id),
    UNIQUE (bot_id,claim_ref),
    UNIQUE (bot_id,conversation_id,run_ref),
    FOREIGN KEY (bot_id,conversation_id)
        REFERENCES conversation_lanes(bot_id,conversation_id),
    FOREIGN KEY (bot_id,claim_ref)
        REFERENCES trigger_claim_groups(bot_id,claim_ref),
    CHECK ((state='PENDING' AND disposition_code IS NULL AND settled_at IS NULL)
        OR (state<>'PENDING' AND disposition_code IS NOT NULL AND settled_at IS NOT NULL))
);

CREATE TABLE trigger_settlement_progress (
    bot_id text NOT NULL,
    claim_ref text NOT NULL,
    next_segment_index integer NOT NULL DEFAULT 0 CHECK (next_segment_index>=0),
    lease_token text CHECK (lease_token IS NULL OR octet_length(lease_token) BETWEEN 1 AND 256),
    lease_epoch integer NOT NULL DEFAULT 0 CHECK (lease_epoch>=0),
    lease_expires_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,claim_ref),
    FOREIGN KEY (bot_id,claim_ref) REFERENCES trigger_claim_groups(bot_id,claim_ref),
    CHECK ((lease_token IS NULL)=(lease_expires_at IS NULL)),
    CHECK (completed_at IS NULL OR lease_token IS NULL)
);
CREATE INDEX trigger_settlement_pending_idx
    ON trigger_settlement_progress(bot_id,updated_at,claim_ref)
    WHERE completed_at IS NULL;
CREATE UNIQUE INDEX trigger_settlement_lease_token_idx
    ON trigger_settlement_progress(bot_id,lease_token)
    WHERE lease_token IS NOT NULL;

CREATE TABLE trigger_segment_settlements (
    bot_id text NOT NULL,
    claim_ref text NOT NULL,
    segment_index integer NOT NULL CHECK (segment_index>=0),
    claim_token text NOT NULL CHECK (octet_length(claim_token) BETWEEN 1 AND 256),
    lease_epoch integer NOT NULL CHECK (lease_epoch>0),
    settled_count integer NOT NULL CHECK (settled_count BETWEEN 1 AND 64),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,claim_ref,segment_index),
    UNIQUE (bot_id,claim_token),
    FOREIGN KEY (bot_id,claim_ref) REFERENCES trigger_claim_groups(bot_id,claim_ref)
);

CREATE TABLE trigger_owned_work_settlements (
    claim_token text PRIMARY KEY CHECK (octet_length(claim_token) BETWEEN 1 AND 256),
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION guard_inbox_message() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'Inbox messages are immutable facts' USING ERRCODE='55000';
    END IF;
    IF ROW(NEW.bot_id,NEW.conversation_id,NEW.event_id,NEW.ingress_seq,
           NEW.admission_operation_id,NEW.input_digest,NEW.source_component_id,
           NEW.source_event_ref,NEW.event_kind,NEW.platform_message_ref,
           NEW.author_subject_ref,NEW.content_payload_id,NEW.content_payload_digest,
           NEW.content_owner_component_id,NEW.content_owner_kind,NEW.content_owner_id,
           NEW.author_payload_id,NEW.author_payload_digest,NEW.author_owner_component_id,
           NEW.author_owner_kind,NEW.author_owner_id,NEW.context_disposition,
           NEW.sent_at,NEW.received_at,NEW.classification_deadline)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.conversation_id,OLD.event_id,OLD.ingress_seq,
           OLD.admission_operation_id,OLD.input_digest,OLD.source_component_id,
           OLD.source_event_ref,OLD.event_kind,OLD.platform_message_ref,
           OLD.author_subject_ref,OLD.content_payload_id,OLD.content_payload_digest,
           OLD.content_owner_component_id,OLD.content_owner_kind,OLD.content_owner_id,
           OLD.author_payload_id,OLD.author_payload_digest,OLD.author_owner_component_id,
           OLD.author_owner_kind,OLD.author_owner_id,OLD.context_disposition,
           OLD.sent_at,OLD.received_at,OLD.classification_deadline)
       OR OLD.classification_state<>'PENDING' OR NEW.classification_state<>'FINAL' THEN
        RAISE EXCEPTION 'Illegal inbox message transition' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER inbox_message_one_way BEFORE UPDATE OR DELETE ON inbox_messages
    FOR EACH ROW EXECUTE FUNCTION guard_inbox_message();

CREATE FUNCTION guard_conversation_trigger() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
DECLARE
    terminal_group record;
BEGIN
    IF TG_OP='DELETE' OR
       ROW(NEW.bot_id,NEW.conversation_id,NEW.trigger_id,NEW.event_id,
           NEW.ingress_seq,NEW.principal_ref,NEW.input_digest,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.conversation_id,OLD.trigger_id,OLD.event_id,
           OLD.ingress_seq,OLD.principal_ref,OLD.input_digest,OLD.created_at) THEN
        RAISE EXCEPTION 'Trigger identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.status='PENDING' AND NEW.status='CLAIMED' THEN
        RETURN NEW;
    END IF;
    IF OLD.status='CLAIMED' AND NEW.status='PENDING'
       AND NEW.claimed_session_ref IS NULL AND NEW.claimed_run_ref IS NULL
       AND NEW.input_batch_id IS NULL
       AND NEW.answered_reply_ref IS NULL AND NEW.terminal_reason_code IS NULL
       AND EXISTS (
           SELECT 1 FROM trigger_claim_groups g
           JOIN trigger_claim_segments s
             ON s.bot_id=g.bot_id AND s.claim_ref=g.claim_ref
            AND s.input_batch_id=OLD.input_batch_id
           JOIN input_batch_triggers m
             ON m.bot_id=s.bot_id AND m.input_batch_id=s.input_batch_id
            AND m.trigger_id=OLD.trigger_id
           JOIN trigger_requeue_operations r
             ON r.bot_id=g.bot_id AND r.claim_ref=g.claim_ref
            AND r.operation_id=g.terminal_operation_id
           WHERE g.bot_id=OLD.bot_id AND g.conversation_id=OLD.conversation_id
             AND g.session_ref=OLD.claimed_session_ref
             AND g.run_ref=OLD.claimed_run_ref
             AND g.claim_state='TERMINAL' AND g.terminal_status='REQUEUED'
       ) THEN
        RETURN NEW;
    END IF;
    IF OLD.status='CLAIMED' AND NEW.status IN ('ANSWERED','FAILED','CANCELLED')
       AND ROW(NEW.claimed_session_ref,NEW.claimed_run_ref,NEW.input_batch_id)
           IS NOT DISTINCT FROM
           ROW(OLD.claimed_session_ref,OLD.claimed_run_ref,OLD.input_batch_id) THEN
        SELECT g.claim_ref,g.terminal_status,g.terminal_operation_id
        INTO terminal_group
        FROM trigger_claim_groups g
        JOIN trigger_claim_segments s
          ON s.bot_id=g.bot_id AND s.claim_ref=g.claim_ref
         AND s.input_batch_id=OLD.input_batch_id
        JOIN input_batch_triggers m
          ON m.bot_id=s.bot_id AND m.input_batch_id=s.input_batch_id
         AND m.trigger_id=OLD.trigger_id
        WHERE g.bot_id=OLD.bot_id AND g.conversation_id=OLD.conversation_id
          AND g.session_ref=OLD.claimed_session_ref
          AND g.run_ref=OLD.claimed_run_ref
          AND g.claim_state='TERMINAL' AND g.terminal_status=NEW.status;
        IF terminal_group IS NULL THEN
            RAISE EXCEPTION 'Terminal trigger has no matching claim group membership'
                USING ERRCODE='55000';
        END IF;
        IF NEW.status='ANSWERED' AND EXISTS (
            SELECT 1 FROM context_append_operations a
            WHERE a.bot_id=NEW.bot_id AND a.operation_id=terminal_group.terminal_operation_id
              AND a.conversation_id=NEW.conversation_id
              AND a.append_kind='FINAL_REPLY' AND a.reply_ref=NEW.answered_reply_ref
              AND a.session_ref=NEW.claimed_session_ref AND a.run_ref=NEW.claimed_run_ref
        ) THEN
            RETURN NEW;
        END IF;
        IF NEW.status IN ('FAILED','CANCELLED') AND EXISTS (
            SELECT 1 FROM trigger_terminal_operations t
            WHERE t.bot_id=NEW.bot_id AND t.operation_id=terminal_group.terminal_operation_id
              AND t.conversation_id=NEW.conversation_id
              AND t.terminal_session_ref=NEW.claimed_session_ref
              AND t.terminal_run_ref=NEW.claimed_run_ref
              AND t.resulting_status=NEW.status
              AND t.reason_code=NEW.terminal_reason_code
        ) THEN
            RETURN NEW;
        END IF;
    END IF;
    RAISE EXCEPTION 'Illegal trigger transition' USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER conversation_trigger_one_way BEFORE UPDATE OR DELETE ON conversation_triggers
    FOR EACH ROW EXECUTE FUNCTION guard_conversation_trigger();

CREATE FUNCTION reject_scheduler_fact_change() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    RAISE EXCEPTION 'Scheduler fact is immutable' USING ERRCODE='55000';
END;
$$;
CREATE FUNCTION guard_active_context_unit() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog AS $$
DECLARE
    source_row record;
BEGIN
    IF TG_OP='INSERT' THEN
        SELECT u.context_revision,u.unit_index INTO source_row
        FROM __forest_module_schema__.context_units u
        WHERE u.bot_id=NEW.bot_id AND u.conversation_id=NEW.conversation_id
          AND u.unit_id=NEW.unit_id;
        IF source_row IS NULL
           OR source_row.context_revision IS DISTINCT FROM NEW.context_revision
           OR source_row.unit_index IS DISTINCT FROM NEW.unit_index
           OR EXISTS (
               SELECT 1 FROM __forest_module_schema__.context_compaction_units cu
               WHERE cu.bot_id=NEW.bot_id AND cu.conversation_id=NEW.conversation_id
                 AND cu.unit_id=NEW.unit_id
           ) THEN
            RAISE EXCEPTION 'Active Context index differs from immutable source'
                USING ERRCODE='55000';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP='DELETE' AND EXISTS (
        SELECT 1 FROM __forest_module_schema__.context_compaction_units cu
        WHERE cu.bot_id=OLD.bot_id AND cu.conversation_id=OLD.conversation_id
          AND cu.unit_id=OLD.unit_id
    ) THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION 'Active Context index transition is invalid'
        USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER active_context_unit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON active_context_units
    FOR EACH ROW EXECUTE FUNCTION guard_active_context_unit();
CREATE TRIGGER input_batches_immutable BEFORE UPDATE OR DELETE ON input_batches
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();
CREATE TRIGGER selection_operations_immutable BEFORE UPDATE OR DELETE ON selection_operations
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();
CREATE TRIGGER input_batch_events_immutable BEFORE UPDATE OR DELETE ON input_batch_events
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();
CREATE TRIGGER input_batch_triggers_immutable BEFORE UPDATE OR DELETE ON input_batch_triggers
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();
CREATE TRIGGER trigger_claim_segments_immutable BEFORE UPDATE OR DELETE ON trigger_claim_segments
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();
CREATE FUNCTION guard_trigger_claim_group() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP='DELETE' OR ROW(
        NEW.bot_id,NEW.conversation_id,NEW.claim_ref,NEW.session_ref,NEW.run_ref
    ) IS DISTINCT FROM ROW(
        OLD.bot_id,OLD.conversation_id,OLD.claim_ref,OLD.session_ref,OLD.run_ref
    ) OR OLD.claim_state<>'ACTIVE' THEN
        RAISE EXCEPTION 'Trigger claim group identity or terminal state is immutable'
            USING ERRCODE='55000';
    END IF;
    IF NEW.claim_state='ACTIVE' AND NEW.terminal_operation_id IS NULL
       AND NEW.segment_count=OLD.segment_count+1
       AND NEW.trigger_count>OLD.trigger_count
       AND NEW.last_trigger_ingress_seq>OLD.last_trigger_ingress_seq
       AND NEW.claim_digest IS DISTINCT FROM OLD.claim_digest THEN
        RETURN NEW;
    END IF;
    IF NEW.claim_state='TERMINAL' AND NEW.terminal_operation_id IS NOT NULL
       AND NEW.terminal_status IS NOT NULL
       AND ROW(NEW.trigger_count,NEW.segment_count,NEW.last_trigger_ref,
               NEW.last_trigger_ingress_seq,NEW.claim_digest)
           IS NOT DISTINCT FROM ROW(OLD.trigger_count,OLD.segment_count,
               OLD.last_trigger_ref,OLD.last_trigger_ingress_seq,OLD.claim_digest)
       AND (
           (NEW.terminal_status='ANSWERED' AND EXISTS (
               SELECT 1 FROM context_append_operations a
               WHERE a.bot_id=NEW.bot_id AND a.operation_id=NEW.terminal_operation_id
                 AND a.conversation_id=NEW.conversation_id
                 AND a.session_ref=NEW.session_ref AND a.run_ref=NEW.run_ref
                 AND a.append_kind='FINAL_REPLY'
           )) OR (NEW.terminal_status IN ('FAILED','CANCELLED') AND EXISTS (
               SELECT 1 FROM trigger_terminal_operations t
               WHERE t.bot_id=NEW.bot_id AND t.operation_id=NEW.terminal_operation_id
                 AND t.conversation_id=NEW.conversation_id
                 AND t.terminal_session_ref=NEW.session_ref
                 AND t.terminal_run_ref=NEW.run_ref
                 AND t.resulting_status=NEW.terminal_status
           )) OR (NEW.terminal_status='REQUEUED' AND EXISTS (
               SELECT 1 FROM trigger_requeue_operations r
               WHERE r.bot_id=NEW.bot_id
                 AND r.operation_id=NEW.terminal_operation_id
                 AND r.claim_ref=NEW.claim_ref
                 AND r.claim_digest=NEW.claim_digest
                 AND r.conversation_id=NEW.conversation_id
                 AND r.abandoned_session_ref=NEW.session_ref
                 AND r.abandoned_run_ref=NEW.run_ref
           ))
       ) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'Illegal trigger claim group transition' USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER trigger_claim_group_one_way BEFORE UPDATE OR DELETE ON trigger_claim_groups
    FOR EACH ROW EXECUTE FUNCTION guard_trigger_claim_group();
CREATE TRIGGER trigger_terminal_operations_immutable BEFORE UPDATE OR DELETE ON trigger_terminal_operations
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();
CREATE TRIGGER trigger_requeue_operations_immutable BEFORE UPDATE OR DELETE ON trigger_requeue_operations
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();
CREATE FUNCTION guard_conversation_requeue_request() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP='DELETE' OR OLD.state<>'PENDING'
       OR ROW(NEW.bot_id,NEW.request_id,NEW.input_digest,NEW.conversation_id,
              NEW.session_ref,NEW.run_ref,NEW.claim_ref,NEW.claim_digest,
              NEW.reason_code,NEW.requested_by,NEW.created_at)
          IS DISTINCT FROM
          ROW(OLD.bot_id,OLD.request_id,OLD.input_digest,OLD.conversation_id,
              OLD.session_ref,OLD.run_ref,OLD.claim_ref,OLD.claim_digest,
              OLD.reason_code,OLD.requested_by,OLD.created_at)
       OR NOT EXISTS (
           SELECT 1 FROM trigger_claim_groups g
           WHERE g.bot_id=OLD.bot_id AND g.claim_ref=OLD.claim_ref
             AND g.claim_state='TERMINAL'
             AND ((NEW.state='APPLIED' AND g.terminal_status='REQUEUED')
               OR (NEW.state='REJECTED' AND g.terminal_status<>'REQUEUED'))
       ) THEN
        RAISE EXCEPTION 'Illegal Conversation requeue request transition'
            USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER conversation_requeue_request_one_way
    BEFORE UPDATE OR DELETE ON conversation_requeue_requests
    FOR EACH ROW EXECUTE FUNCTION guard_conversation_requeue_request();
CREATE TRIGGER trigger_segment_settlements_immutable
    BEFORE UPDATE OR DELETE ON trigger_segment_settlements
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();
CREATE TRIGGER trigger_owned_work_settlements_immutable
    BEFORE UPDATE OR DELETE ON trigger_owned_work_settlements
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();
CREATE TRIGGER context_units_immutable BEFORE UPDATE OR DELETE ON context_units
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();
CREATE TRIGGER context_append_operations_immutable
    BEFORE UPDATE OR DELETE ON context_append_operations
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();
CREATE TRIGGER context_compactions_immutable BEFORE UPDATE OR DELETE ON context_compactions
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();
CREATE TRIGGER context_compaction_units_immutable
    BEFORE UPDATE OR DELETE ON context_compaction_units
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();

CREATE FUNCTION conversation_admit_message(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    requested_conversation text := body->>'conversation_id';
    requested_event text := body->>'event_id';
    lane_digest text;
    existing record;
    lane record;
    allocated_seq bigint;
    received_at_value timestamptz;
    deadline_value timestamptz;
    sent_at_value timestamptz;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR body IS NULL
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM body->>'source_component_id'
       OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_ADMISSION_INVALID',
                'message','Conversation admission envelope is invalid','status',422
            )
        );
    END IF;
    received_at_value := (body->>'received_at')::timestamptz;
    deadline_value := (body->>'classification_deadline')::timestamptz;
    sent_at_value := NULLIF(body->>'sent_at','')::timestamptz;
    IF deadline_value <= received_at_value
       OR deadline_value > received_at_value + interval '15 minutes' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_ADMISSION_INVALID',
                'message','Classification deadline is outside its bound','status',422
            )
        );
    END IF;
    lane_digest := 'sha256:' || encode(
        sha256(convert_to(jsonb_build_array(
            body->>'source_component_id', body->>'binding_ref', body->>'platform',
            body->>'channel_kind', body->>'channel_ref', body->'thread_ref'
        )::text,'UTF8')),
        'hex'
    );
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.conversation.scheduler/admit/' || requested_bot || '/' || requested_operation,
        0
    ));
    SELECT * INTO existing
    FROM __forest_module_schema__.inbox_messages
    WHERE bot_id=requested_bot AND admission_operation_id=requested_operation;
    IF FOUND THEN
        IF existing.input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','MESSAGE_OPERATION_CONFLICT',
                    'message','Admission operation input differs','status',409
                )
            );
        END IF;
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'conversation_id',existing.conversation_id,'event_id',existing.event_id,
                'ingress_seq',existing.ingress_seq,'duplicate',true
            )
        );
    END IF;
    SELECT * INTO existing
    FROM __forest_module_schema__.inbox_messages
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
      AND event_id=requested_event;
    IF FOUND THEN
        IF existing.input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','MESSAGE_ID_CONFLICT',
                    'message','Event identity differs','status',409
                )
            );
        END IF;
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'conversation_id',existing.conversation_id,'event_id',existing.event_id,
                'ingress_seq',existing.ingress_seq,'duplicate',true
            )
        );
    END IF;
    INSERT INTO __forest_module_schema__.conversation_lanes(
        bot_id,conversation_id,conversation_key_digest,source_component_id,binding_ref,
        platform,channel_kind,channel_ref,thread_ref
    ) VALUES (
        requested_bot,requested_conversation,lane_digest,body->>'source_component_id',
        body->>'binding_ref',body->>'platform',body->>'channel_kind',body->>'channel_ref',
        body->>'thread_ref'
    ) ON CONFLICT DO NOTHING;
    SELECT * INTO lane
    FROM __forest_module_schema__.conversation_lanes
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
    FOR UPDATE;
    IF lane IS NULL
       OR lane.conversation_key_digest IS DISTINCT FROM lane_digest
       OR lane.source_component_id IS DISTINCT FROM body->>'source_component_id'
       OR lane.binding_ref IS DISTINCT FROM body->>'binding_ref'
       OR lane.platform IS DISTINCT FROM body->>'platform'
       OR lane.channel_kind IS DISTINCT FROM body->>'channel_kind'
       OR lane.channel_ref IS DISTINCT FROM body->>'channel_ref'
       OR lane.thread_ref IS DISTINCT FROM body->>'thread_ref' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_IDENTITY_CONFLICT',
                'message','Conversation identity differs','status',409
            )
        );
    END IF;
    allocated_seq := lane.next_ingress_seq;
    INSERT INTO __forest_module_schema__.inbox_messages(
        bot_id,conversation_id,event_id,ingress_seq,admission_operation_id,input_digest,
        source_component_id,source_event_ref,event_kind,platform_message_ref,
        author_subject_ref,content_payload_id,content_payload_digest,content_byte_length,
        content_owner_component_id,content_owner_kind,content_owner_id,
        author_payload_id,author_payload_digest,author_owner_component_id,
        author_owner_kind,author_owner_id,context_disposition,sent_at,received_at,
        classification_deadline
    ) VALUES (
        requested_bot,requested_conversation,requested_event,allocated_seq,
        requested_operation,requested_digest,body->>'source_component_id',
        body->>'source_event_ref',body->>'event_kind',body->>'platform_message_ref',
        body->>'author_subject_ref',body#>>'{content,payload_id}',
        body#>>'{content,digest}',(body->>'content_byte_length')::integer,
        body#>>'{content,owner_component_id}',
        body#>>'{content,owner_kind}',body#>>'{content,owner_id}',
        body#>>'{author,payload_id}',body#>>'{author,digest}',
        body#>>'{author,owner_component_id}',body#>>'{author,owner_kind}',
        body#>>'{author,owner_id}',body->>'context_disposition',sent_at_value,
        received_at_value,deadline_value
    );
    UPDATE __forest_module_schema__.conversation_lanes
    SET next_ingress_seq=next_ingress_seq+1,
        mailbox_revision=mailbox_revision+1,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'conversation_id',requested_conversation,'event_id',requested_event,
            'ingress_seq',allocated_seq,'duplicate',false
        )
    );
END;
$$;

CREATE FUNCTION conversation_complete_classification(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    requested_conversation text := body->>'conversation_id';
    requested_event text := body->>'event_id';
    requested_decision text := body->>'decision';
    lane record;
    message record;
    other record;
    stored_trigger record;
    trigger_digest text;
    classified bigint;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR body IS NULL
       OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_CLASSIFICATION_INVALID',
                'message','Conversation classification envelope is invalid','status',422
            )
        );
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.conversation.scheduler/classify/' || requested_bot || '/' || requested_operation,
        0
    ));
    SELECT *,clock_timestamp() AS database_now INTO lane
    FROM __forest_module_schema__.conversation_lanes
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
    FOR UPDATE;
    IF lane IS NULL THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_NOT_FOUND',
                'message','Conversation lane does not exist','status',404
            )
        );
    END IF;
    SELECT conversation_id,event_id,classification_input_digest INTO other
    FROM __forest_module_schema__.inbox_messages
    WHERE bot_id=requested_bot AND classification_operation_id=requested_operation;
    IF FOUND AND (
        other.conversation_id IS DISTINCT FROM requested_conversation
        OR other.event_id IS DISTINCT FROM requested_event
        OR other.classification_input_digest IS DISTINCT FROM requested_digest
    ) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CLASSIFICATION_OPERATION_CONFLICT',
                'message','Classification operation input differs','status',409
            )
        );
    END IF;
    SELECT * INTO message
    FROM __forest_module_schema__.inbox_messages
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
      AND event_id=requested_event
    FOR UPDATE;
    IF message IS NULL THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MESSAGE_NOT_FOUND','message','Inbox message does not exist','status',404
            )
        );
    END IF;
    IF message.classification_state='FINAL' THEN
        IF message.classification_input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','CLASSIFICATION_CONFLICT',
                    'message','Message classification differs','status',409
                )
            );
        END IF;
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'event_id',requested_event,'ingress_seq',message.ingress_seq,
                'decision',message.trigger_decision,
                'classified_through_seq',lane.classified_through_seq,'duplicate',true
            )
        );
    END IF;
    IF lane.database_now >= message.classification_deadline
       AND body->>'classifier_component_id' <> 'conversation.scheduler.deadline' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CLASSIFICATION_DEADLINE_EXCEEDED',
                'message','Classification deadline has passed','status',409
            )
        );
    END IF;
    UPDATE __forest_module_schema__.inbox_messages
    SET classification_state='FINAL',trigger_decision=requested_decision,
        classification_operation_id=requested_operation,
        classification_input_digest=requested_digest,
        classifier_component_id=body->>'classifier_component_id',
        classifier_generation_id=body->>'classifier_generation_id',
        rule_revision=body->>'rule_revision',reason_code=body->>'reason_code',
        route_ref=body->>'route_ref',classified_at=clock_timestamp()
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
      AND event_id=requested_event;
    IF requested_decision='TRIGGER' THEN
        trigger_digest := 'sha256:' || encode(sha256(convert_to(jsonb_build_object(
            'conversation_id',requested_conversation,
            'event_id',requested_event,
            'ingress_seq',message.ingress_seq,
            'principal_ref',body->>'principal_ref'
        )::text,'UTF8')),'hex');
        INSERT INTO __forest_module_schema__.conversation_triggers(
            bot_id,conversation_id,trigger_id,event_id,ingress_seq,principal_ref,input_digest
        ) VALUES (
            requested_bot,requested_conversation,body->>'trigger_id',requested_event,
            message.ingress_seq,body->>'principal_ref',trigger_digest
        ) ON CONFLICT DO NOTHING;
        SELECT * INTO stored_trigger
        FROM __forest_module_schema__.conversation_triggers
        WHERE bot_id=requested_bot AND trigger_id=body->>'trigger_id';
        IF stored_trigger IS NULL
           OR stored_trigger.conversation_id IS DISTINCT FROM requested_conversation
           OR stored_trigger.event_id IS DISTINCT FROM requested_event
           OR stored_trigger.input_digest IS DISTINCT FROM trigger_digest THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','TRIGGER_ID_CONFLICT',
                    'message','Trigger identity differs','status',409
                )
            );
        END IF;
    END IF;
    SELECT COALESCE(
        MIN(ingress_seq) FILTER (WHERE classification_state='PENDING') - 1,
        MAX(ingress_seq),
        lane.classified_through_seq
    ) INTO classified
    FROM __forest_module_schema__.inbox_messages
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
      AND ingress_seq > lane.classified_through_seq;
    UPDATE __forest_module_schema__.conversation_lanes
    SET classified_through_seq=classified,
        mailbox_revision=mailbox_revision+1,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'event_id',requested_event,'ingress_seq',message.ingress_seq,
            'decision',requested_decision,'classified_through_seq',classified,
            'duplicate',false
        )
    );
END;
$$;

CREATE FUNCTION conversation_classify_shell_ingress(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    prior jsonb := envelope->'prior';
    conversation_step text := body->>'conversation_step_id';
    shell_step text := body->>'shell_step_id';
    admitted jsonb := prior->conversation_step;
    shell_result jsonb := prior->shell_step;
    rejection_code text;
    projected jsonb;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'adapter.onebot'
       OR admitted IS NULL OR shell_result IS NULL
       OR admitted->>'conversation_id' IS DISTINCT FROM body->>'conversation_id'
       OR admitted->>'event_id' IS DISTINCT FROM body->>'event_id'
       OR shell_result->>'request_id' IS NULL
       OR shell_result->>'shell_generation_id' IS NULL
       OR shell_result->>'policy_revision' !~ '^[0-9]+$'
       OR shell_result->>'state' NOT IN ('QUEUED','REJECTION_PENDING','REJECTED') THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_SHELL_CLASSIFICATION_INVALID',
                'message','Shell admission evidence is invalid','status',422
            )
        );
    END IF;
    rejection_code := shell_result->>'rejection_code';
    projected := jsonb_build_object(
        'protocol','forest.uow-step/1',
        'caller',envelope->'caller',
        'input',jsonb_build_object(
            'conversation_id',body->>'conversation_id',
            'event_id',body->>'event_id',
            'decision','CONTROL_COMMAND',
            'classifier_component_id','shell.command',
            'classifier_generation_id',shell_result->>'shell_generation_id',
            'rule_revision','shell-policy:'||(shell_result->>'policy_revision'),
            'reason_code',COALESCE(rejection_code,'SHELL_ACCEPTED'),
            'route_ref',shell_result->>'request_id',
            'trigger_id',NULL,
            'principal_ref',NULL
        ),
        'prior',prior
    );
    RETURN __forest_module_schema__.conversation_complete_classification(context, projected);
END;
$$;

CREATE FUNCTION conversation_authorize_run_trigger(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_run text := envelope#>>'{input,run_ref}';
    requested_trigger text := envelope#>>'{input,trigger_ref}';
    match_row record;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime'
       OR requested_run IS NULL THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_TRIGGER_AUTHORITY_INVALID',
                'message','Run trigger authority request is invalid','status',422
            )
        );
    END IF;
    IF requested_trigger IS NULL THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'protocol','forest.conversation-run-trigger-authority/1',
                'run_ref',requested_run,'trigger_ref',NULL,
                'session_ref',NULL,'conversation_ref',NULL,'principal_ref',NULL
            )
        );
    END IF;
    SELECT t.conversation_id,t.claimed_session_ref,t.principal_ref
    INTO match_row
    FROM __forest_module_schema__.conversation_triggers t
    JOIN __forest_module_schema__.input_batch_triggers m
      ON m.bot_id=t.bot_id AND m.input_batch_id=t.input_batch_id
     AND m.trigger_id=t.trigger_id
    JOIN __forest_module_schema__.trigger_claim_segments s
      ON s.bot_id=m.bot_id AND s.input_batch_id=m.input_batch_id
    JOIN __forest_module_schema__.trigger_claim_groups g
      ON g.bot_id=s.bot_id AND g.claim_ref=s.claim_ref
     AND g.conversation_id=t.conversation_id
     AND g.session_ref=t.claimed_session_ref AND g.run_ref=t.claimed_run_ref
    WHERE t.bot_id=requested_bot AND t.trigger_id=requested_trigger
      AND t.status='CLAIMED' AND t.claimed_run_ref=requested_run
      AND g.claim_state='ACTIVE';
    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_TOOL_AUTHORITY_INVALID',
                'message','Tool trigger does not belong to this Run','status',403
            )
        );
    END IF;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.conversation-run-trigger-authority/1',
            'run_ref',requested_run,'trigger_ref',requested_trigger,
            'session_ref',match_row.claimed_session_ref,
            'conversation_ref',match_row.conversation_id,
            'principal_ref',match_row.principal_ref
        )
    );
END;
$$;

CREATE FUNCTION conversation_trigger_effective_status(
    requested_bot text, requested_trigger text
) RETURNS text LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
    SELECT CASE WHEN t.status='CLAIMED' AND g.terminal_status='REQUEUED'
                THEN 'PENDING'
                WHEN t.status='CLAIMED' AND g.claim_state='TERMINAL'
                THEN g.terminal_status ELSE t.status END
    FROM __forest_module_schema__.conversation_triggers t
    LEFT JOIN __forest_module_schema__.trigger_claim_groups g
      ON g.bot_id=t.bot_id AND g.conversation_id=t.conversation_id
     AND g.session_ref=t.claimed_session_ref AND g.run_ref=t.claimed_run_ref
    WHERE t.bot_id=requested_bot AND t.trigger_id=requested_trigger
$$;

CREATE FUNCTION conversation_pending_trigger_summary(
    requested_bot text, requested_conversation text,
    selected_from_seq bigint, selected_through_seq bigint
) RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    item record;
    count_value integer := 0;
    tail_ref text;
    tail_seq bigint;
    rolling_digest bytea := sha256(convert_to(jsonb_build_array(
        'forest.trigger-claim/1',requested_bot,requested_conversation
    )::text,'UTF8'));
BEGIN
    FOR item IN
        SELECT trigger_id,principal_ref,ingress_seq
        FROM __forest_module_schema__.conversation_triggers
        WHERE bot_id=requested_bot AND conversation_id=requested_conversation
          AND (selected_from_seq IS NULL OR ingress_seq>=selected_from_seq)
          AND status='PENDING' AND ingress_seq<=selected_through_seq
        ORDER BY ingress_seq,trigger_id
    LOOP
        count_value := count_value+1;
        IF count_value>64 THEN
            RAISE EXCEPTION 'Trigger claim segment exceeds 64 members'
                USING ERRCODE='54000';
        END IF;
        rolling_digest := sha256(rolling_digest || convert_to(jsonb_build_array(
            item.trigger_id,item.principal_ref,item.ingress_seq
        )::text,'UTF8'));
        tail_ref := item.trigger_id;
        tail_seq := item.ingress_seq;
    END LOOP;
    IF count_value=0 THEN
        RETURN NULL;
    END IF;
    RETURN jsonb_build_object(
        'count',count_value,'last_trigger_ref',tail_ref,
        'last_trigger_ingress_seq',tail_seq,
        'digest','sha256:' || encode(rolling_digest,'hex')
    );
END;
$$;

CREATE FUNCTION conversation_selection_result(requested_bot text, requested_operation text)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog
AS $$
DECLARE
    receipt record;
    batch record;
    event_ids jsonb;
BEGIN
    SELECT * INTO receipt
    FROM __forest_module_schema__.selection_operations
    WHERE bot_id=requested_bot AND operation_id=requested_operation;
    IF receipt IS NULL THEN
        RETURN NULL;
    END IF;
    IF receipt.input_batch_id IS NULL THEN
        RETURN jsonb_build_object(
            'status',receipt.result_status,
            'operation_id',receipt.operation_id,
            'conversation_id',receipt.conversation_id,
            'classified_through_seq',receipt.observed_classified_through_seq,
            'selected_input_end_seq',receipt.selected_input_end_seq,
            'mailbox_revision',receipt.observed_mailbox_revision
        );
    END IF;
    SELECT * INTO batch
    FROM __forest_module_schema__.input_batches
    WHERE bot_id=requested_bot AND input_batch_id=receipt.input_batch_id;
    SELECT COALESCE(jsonb_agg(event_id ORDER BY event_index),'[]'::jsonb)
    INTO event_ids
    FROM __forest_module_schema__.input_batch_events
    WHERE bot_id=requested_bot AND input_batch_id=receipt.input_batch_id;
    RETURN jsonb_build_object(
        'status','BATCH',
        'operation_id',receipt.operation_id,
        'conversation_id',receipt.conversation_id,
        'classified_through_seq',receipt.observed_classified_through_seq,
        'selected_input_end_seq',receipt.selected_input_end_seq,
        'mailbox_revision',receipt.observed_mailbox_revision,
        'input_batch_id',batch.input_batch_id,
        'boundary',batch.boundary,
        'kind',batch.kind,
        'start_seq',batch.start_seq,
        'end_seq',batch.end_seq,
        'trigger_cut_seq',batch.trigger_cut_seq,
        'session_ref',batch.session_ref,
        'run_ref',batch.run_ref,
        'context_revision',batch.context_revision,
        'context_view_revision',batch.context_view_revision,
        'event_ids',event_ids,
        'trigger_claim',CASE WHEN batch.trigger_count IS NULL THEN NULL
            ELSE jsonb_build_object(
                'ref',batch.trigger_claim_ref,'count',batch.trigger_count,
                'last_trigger_ref',batch.last_trigger_ref,
                'last_trigger_ingress_seq',batch.last_trigger_ingress_seq,
                'digest',batch.trigger_claim_digest
            ) END
    );
END;
$$;

CREATE FUNCTION conversation_select_input(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    actor jsonb := body->'actor';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    requested_conversation text := actor->>'conversation_id';
    requested_owner text := actor->>'owner_id';
    requested_epoch bigint := (actor->>'epoch')::bigint;
    requested_boundary text := body->>'boundary';
    requested_mode text := body->>'selection_mode';
    requested_session text := body->>'session_ref';
    requested_run text := body->>'run_ref';
    requested_next_run text := body->>'next_run_ref';
    selected_run text;
    selected_session text;
    lane record;
    replay record;
    trigger_cut bigint;
    recovery_cut bigint;
    pending_window_count integer;
    has_gap boolean;
    selection_status text;
    selection_mailbox_revision bigint;
    selection_kind text;
    selection_start bigint;
    selection_end bigint;
    selection_trigger_cut bigint;
    selection_context_revision bigint;
    batch_id text;
    claim_summary jsonb;
    segment_summary jsonb;
    claim_ref_value text;
    claim_digest_value text;
    claim_count_value integer;
    claim_segment_index integer;
    claim_prior_digest bytea;
    existing_claim record;
    claimed_count integer;
    event_count integer;
    final_event_count integer;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR body IS NULL OR actor IS NULL
       OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_SELECTION_INVALID',
                'message','Conversation input selection envelope is invalid','status',422
            )
        );
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.conversation.scheduler/select/' || requested_bot || '/' || requested_operation,
        0
    ));
    SELECT * INTO lane
    FROM __forest_module_schema__.conversation_lanes
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
    FOR UPDATE;
    IF lane IS NULL THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_NOT_FOUND',
                'message','Conversation lane does not exist','status',404
            )
        );
    END IF;
    IF lane.actor_owner_id IS DISTINCT FROM requested_owner
       OR lane.actor_epoch IS DISTINCT FROM requested_epoch
       OR lane.actor_expires_at IS NULL
       OR lane.actor_expires_at <= clock_timestamp() THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','STALE_CONVERSATION_ACTOR',
                'message','Conversation actor is stale','status',409
            )
        );
    END IF;
    SELECT * INTO replay
    FROM __forest_module_schema__.selection_operations
    WHERE bot_id=requested_bot AND operation_id=requested_operation;
    IF replay.operation_id IS NOT NULL THEN
        IF replay.input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','SELECTION_OPERATION_CONFLICT',
                    'message','Selection operation input differs','status',409
                )
            );
        END IF;
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',__forest_module_schema__.conversation_selection_result(
                requested_bot,requested_operation
            )
        );
    END IF;
    IF requested_boundary='IDLE' THEN
        IF lane.active_session_ref IS NOT NULL THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','CONVERSATION_SESSION_ACTIVE',
                    'message','Conversation already has an active Session','status',409
                )
            );
        END IF;
    ELSIF lane.active_session_ref IS DISTINCT FROM requested_session
       OR lane.active_run_ref IS DISTINCT FROM requested_run THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_SESSION_CONFLICT',
                'message','Session or Run fence differs','status',409
            )
        );
    END IF;
    IF requested_boundary='IDLE' AND EXISTS (
        SELECT 1 FROM __forest_module_schema__.trigger_claim_groups g
        WHERE g.bot_id=requested_bot AND g.conversation_id=requested_conversation
          AND g.claim_state='ACTIVE'
          AND (g.session_ref IS DISTINCT FROM requested_session
               OR g.run_ref IS DISTINCT FROM requested_run)
    ) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_PREFILL_TARGET_CONFLICT',
                'message','An unfinished prefill belongs to another Run','status',409
            )
        );
    END IF;
    IF EXISTS (
        SELECT 1 FROM __forest_module_schema__.trigger_claim_groups g
        JOIN __forest_module_schema__.trigger_settlement_progress p
          ON p.bot_id=g.bot_id AND p.claim_ref=g.claim_ref
        WHERE g.bot_id=requested_bot
          AND g.conversation_id=requested_conversation
          AND g.terminal_status='REQUEUED' AND p.completed_at IS NULL
    ) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_REQUEUE_SETTLEMENT_PENDING',
                'message','Requeued trigger segments have not settled','status',409
            )
        );
    END IF;
    -- Classification and every trigger-state transition hold this lane row
    -- first. The lane fence stabilizes the pending set without locking an
    -- arbitrarily large trigger backlog during each 64-event prefill.
    SELECT ingress_seq INTO trigger_cut
    FROM __forest_module_schema__.conversation_triggers
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
      AND status='PENDING' AND ingress_seq<=lane.classified_through_seq
    ORDER BY ingress_seq DESC,trigger_id DESC LIMIT 1;
    SELECT count(*)::integer INTO pending_window_count FROM (
        SELECT 1 FROM __forest_module_schema__.conversation_triggers
        WHERE bot_id=requested_bot AND conversation_id=requested_conversation
          AND status='PENDING' AND ingress_seq<=lane.classified_through_seq
        LIMIT 65
    ) bounded_pending;
    has_gap := lane.classified_through_seq < lane.next_ingress_seq - 1;
    IF requested_mode='PREFILL_ONLY' THEN
        IF pending_window_count>64 THEN
            SELECT max(ingress_seq) INTO recovery_cut FROM (
                SELECT ingress_seq
                FROM __forest_module_schema__.conversation_triggers
                WHERE bot_id=requested_bot
                  AND conversation_id=requested_conversation
                  AND status='PENDING'
                  AND ingress_seq<=lane.context_applied_seq
                ORDER BY ingress_seq,trigger_id LIMIT 64
            ) bounded_recovery;
        END IF;
        IF recovery_cut IS NOT NULL THEN
            -- Previously committed Context may still contain pending triggers
            -- after an explicit recovery. Claim at most 64 old triggers without
            -- appending duplicate Context units; leave the last pending trigger
            -- for the final input selection.
            selection_kind := 'RECOVERY_PREFILL';
            selection_start := NULL;
            selection_end := NULL;
            selection_trigger_cut := recovery_cut;
            selected_session := requested_session;
            selected_run := CASE WHEN requested_boundary='SESSION_END'
                THEN requested_next_run ELSE requested_run END;
            IF selected_session IS NULL OR selected_run IS NULL THEN
                RETURN jsonb_build_object(
                    'protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object(
                        'code','CONVERSATION_SESSION_REQUIRED',
                        'message','Recovery prefill requires its target Run','status',409
                    )
                );
            END IF;
        ELSIF trigger_cut IS NOT NULL AND trigger_cut-lane.context_applied_seq > 64 THEN
            -- Retain the last trigger in the final Agent InputBatch. Claim
            -- earlier triggers for the target Run as each Context prefix lands.
            selection_kind := 'CONTEXT_PREFILL';
            selection_start := lane.context_applied_seq+1;
            selection_end := LEAST(lane.context_applied_seq+64,trigger_cut-1);
            selection_trigger_cut := NULL;
            selected_session := requested_session;
            selected_run := CASE WHEN requested_boundary='SESSION_END'
                THEN requested_next_run ELSE requested_run END;
            IF selected_session IS NULL OR selected_run IS NULL THEN
                RETURN jsonb_build_object(
                    'protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object(
                        'code','CONVERSATION_SESSION_REQUIRED',
                        'message','Context prefill requires its target Run','status',409
                    )
                );
            END IF;
        ELSE
            INSERT INTO __forest_module_schema__.selection_operations(
                bot_id,operation_id,conversation_id,input_digest,boundary,result_status,
                input_batch_id,observed_mailbox_revision,observed_classified_through_seq,
                selected_input_end_seq,actor_owner_id,actor_epoch
            ) VALUES (
                requested_bot,requested_operation,requested_conversation,requested_digest,
                requested_boundary,'CONTINUE',NULL,lane.mailbox_revision,
                lane.classified_through_seq,lane.context_applied_seq,requested_owner,requested_epoch
            );
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',true,
                'value',__forest_module_schema__.conversation_selection_result(
                    requested_bot,requested_operation
                )
            );
        END IF;
    ELSIF trigger_cut IS NOT NULL THEN
        IF requested_mode='SILENT_ONLY' THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','CONVERSATION_TRIGGER_PENDING',
                    'message','Silent drain cannot claim a pending trigger','status',409
                )
            );
        END IF;
        IF trigger_cut-lane.context_applied_seq > 64
           OR pending_window_count>64 THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','CONVERSATION_PREFILL_REQUIRED',
                    'message','Trigger prefix requires bounded Context prefill','status',409
                )
            );
        END IF;
        IF requested_session IS NULL OR requested_run IS NULL THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','CONVERSATION_SESSION_REQUIRED',
                    'message','A trigger cut requires Session and Run references','status',409
                )
            );
        END IF;
        selected_session := requested_session;
        selected_run := requested_run;
        IF requested_boundary='SESSION_END' THEN
            IF requested_next_run IS NULL THEN
                RETURN jsonb_build_object(
                    'protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object(
                        'code','CONVERSATION_SUCCESSOR_RUN_REQUIRED',
                        'message','A pending trigger after a reply needs a successor Run','status',409
                    )
                );
            END IF;
            selected_run := requested_next_run;
        END IF;
        selection_trigger_cut := trigger_cut;
        IF trigger_cut > lane.context_applied_seq THEN
            selection_kind := 'TRIGGER_INPUT';
            selection_start := lane.context_applied_seq + 1;
            selection_end := trigger_cut;
        ELSE
            selection_kind := 'RECOVERY_CLAIM';
            selection_start := NULL;
            selection_end := NULL;
        END IF;
    ELSIF requested_boundary='MODEL_SAFE_POINT' THEN
        selection_status := CASE WHEN has_gap THEN 'WAIT_INPUT' ELSE 'CONTINUE' END;
        INSERT INTO __forest_module_schema__.selection_operations(
            bot_id,operation_id,conversation_id,input_digest,boundary,result_status,
            input_batch_id,observed_mailbox_revision,observed_classified_through_seq,
            selected_input_end_seq,actor_owner_id,actor_epoch
        ) VALUES (
            requested_bot,requested_operation,requested_conversation,requested_digest,
            requested_boundary,selection_status,NULL,lane.mailbox_revision,
            lane.classified_through_seq,lane.context_applied_seq,requested_owner,requested_epoch
        );
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',__forest_module_schema__.conversation_selection_result(
                requested_bot,requested_operation
            )
        );
    ELSIF lane.classified_through_seq > lane.context_applied_seq THEN
        selection_kind := 'SILENT_DRAIN';
        selection_start := lane.context_applied_seq + 1;
        -- A long silent backlog must never become one unbounded UoW frame.
        -- The settlement releases this actor; the next claim takes the next
        -- contiguous prefix while retaining the same per-group order.
        selection_end := LEAST(lane.classified_through_seq,
                               lane.context_applied_seq + 64);
        selection_trigger_cut := NULL;
        selected_session := NULL;
        selected_run := NULL;
    ELSE
        selection_status := CASE WHEN has_gap THEN 'WAIT_INPUT' ELSE 'IDLE' END;
        selection_mailbox_revision := lane.mailbox_revision;
        IF requested_boundary='SESSION_END' AND selection_status='IDLE' THEN
            UPDATE __forest_module_schema__.conversation_lanes
            SET active_session_ref=NULL,active_run_ref=NULL,resume_not_before=NULL,
                resume_reason_code=NULL,resume_reason_ref=NULL,
                mailbox_revision=mailbox_revision+1,updated_at=clock_timestamp()
            WHERE bot_id=requested_bot AND conversation_id=requested_conversation;
            selection_mailbox_revision := selection_mailbox_revision + 1;
        END IF;
        INSERT INTO __forest_module_schema__.selection_operations(
            bot_id,operation_id,conversation_id,input_digest,boundary,result_status,
            input_batch_id,observed_mailbox_revision,observed_classified_through_seq,
            selected_input_end_seq,actor_owner_id,actor_epoch
        ) VALUES (
            requested_bot,requested_operation,requested_conversation,requested_digest,
            requested_boundary,selection_status,NULL,selection_mailbox_revision,
            lane.classified_through_seq,lane.context_applied_seq,requested_owner,requested_epoch
        );
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',__forest_module_schema__.conversation_selection_result(
                requested_bot,requested_operation
            )
        );
    END IF;

    SELECT count(*)::integer,
           count(*) FILTER (WHERE classification_state='FINAL')::integer
    INTO event_count,final_event_count
    FROM __forest_module_schema__.inbox_messages
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
      AND selection_start IS NOT NULL
      AND ingress_seq BETWEEN selection_start AND selection_end;
    IF selection_start IS NOT NULL AND (
        event_count <> selection_end-selection_start+1
        OR final_event_count <> event_count
    ) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_SEQUENCE_CORRUPT',
                'message','Selected input is not a final prefix','status',500
            )
        );
    END IF;
    selection_context_revision := lane.context_revision + CASE
        WHEN selection_start IS NULL THEN 0 ELSE 1 END;
    IF selection_context_revision <= 0 THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_SEQUENCE_CORRUPT',
                'message','Recovery claim has no committed Context','status',500
            )
        );
    END IF;
    batch_id := 'batch:' || encode(sha256(convert_to(
        jsonb_build_array(requested_bot,requested_operation)::text,'UTF8'
    )),'hex');
    IF selection_kind IN (
        'TRIGGER_INPUT','RECOVERY_CLAIM','CONTEXT_PREFILL','RECOVERY_PREFILL'
    ) THEN
        segment_summary := __forest_module_schema__.conversation_pending_trigger_summary(
            requested_bot,requested_conversation,
            CASE WHEN selection_kind='CONTEXT_PREFILL' THEN selection_start ELSE NULL END,
            CASE WHEN selection_kind='CONTEXT_PREFILL' THEN selection_end
                 WHEN selection_kind='RECOVERY_PREFILL' THEN selection_trigger_cut
                 ELSE lane.classified_through_seq END
        );
        IF segment_summary IS NULL AND selection_kind<>'CONTEXT_PREFILL' THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','CONVERSATION_TRIGGER_STATE_CONFLICT',
                    'message','Selected trigger group has no pending members','status',409
                )
            );
        END IF;
        IF segment_summary IS NOT NULL THEN
            claim_ref_value := 'claim:' || encode(sha256(convert_to(
                jsonb_build_array(requested_bot,requested_conversation,
                                  selected_session,selected_run)::text,'UTF8'
            )),'hex');
            SELECT * INTO existing_claim
            FROM __forest_module_schema__.trigger_claim_groups
            WHERE bot_id=requested_bot AND claim_ref=claim_ref_value FOR UPDATE;
            IF existing_claim.claim_ref IS NOT NULL AND (
                existing_claim.conversation_id IS DISTINCT FROM requested_conversation
                OR existing_claim.session_ref IS DISTINCT FROM selected_session
                OR existing_claim.run_ref IS DISTINCT FROM selected_run
                OR existing_claim.claim_state IS DISTINCT FROM 'ACTIVE'
            ) THEN
                RAISE EXCEPTION 'Trigger claim identity collision' USING ERRCODE='55000';
            END IF;
            claim_prior_digest := CASE WHEN existing_claim.claim_ref IS NULL
                THEN sha256(convert_to(jsonb_build_array(
                    'forest.trigger-group/1',requested_bot,requested_conversation,
                    selected_session,selected_run
                )::text,'UTF8'))
                ELSE decode(substr(existing_claim.claim_digest,8),'hex') END;
            claim_count_value := COALESCE(existing_claim.trigger_count,0)
                                 +(segment_summary->>'count')::integer;
            claim_segment_index := COALESCE(existing_claim.segment_count,0);
            claim_digest_value := 'sha256:' || encode(sha256(
                claim_prior_digest || convert_to(jsonb_build_array(
                    batch_id,segment_summary->>'count',segment_summary->>'digest'
                )::text,'UTF8')
            ),'hex');
            claim_summary := jsonb_build_object(
                'count',claim_count_value,
                'last_trigger_ref',segment_summary->>'last_trigger_ref',
                'last_trigger_ingress_seq',segment_summary->>'last_trigger_ingress_seq',
                'digest',claim_digest_value
            );
        END IF;
    END IF;
    INSERT INTO __forest_module_schema__.input_batches(
        bot_id,conversation_id,input_batch_id,operation_id,input_digest,boundary,kind,
        start_seq,end_seq,trigger_cut_seq,selected_input_end_seq,session_ref,run_ref,
        actor_owner_id,actor_epoch,context_revision,context_view_revision,
        trigger_count,last_trigger_ref,last_trigger_ingress_seq,
        trigger_claim_digest,trigger_claim_ref
    ) VALUES (
        requested_bot,requested_conversation,batch_id,requested_operation,requested_digest,
        requested_boundary,selection_kind,selection_start,selection_end,
        selection_trigger_cut,COALESCE(selection_end,lane.context_applied_seq),
        selected_session,selected_run,requested_owner,requested_epoch,
        selection_context_revision,lane.context_view_revision,
        (claim_summary->>'count')::integer,claim_summary->>'last_trigger_ref',
        (claim_summary->>'last_trigger_ingress_seq')::bigint,
        claim_summary->>'digest',claim_ref_value
    );
    IF selection_start IS NOT NULL THEN
        INSERT INTO __forest_module_schema__.input_batch_events(
            bot_id,input_batch_id,event_index,conversation_id,event_id,ingress_seq
        )
        SELECT requested_bot,batch_id,
               row_number() OVER (ORDER BY ingress_seq)-1,
               requested_conversation,event_id,ingress_seq
        FROM __forest_module_schema__.inbox_messages
        WHERE bot_id=requested_bot AND conversation_id=requested_conversation
          AND ingress_seq BETWEEN selection_start AND selection_end
        ORDER BY ingress_seq;
        INSERT INTO __forest_module_schema__.context_units(
            bot_id,conversation_id,unit_id,context_revision,unit_index,kind,
            source_component_id,source_kind,source_id,input_batch_id,event_id,ingress_seq,
            payload_id,payload_digest,payload_byte_length,payload_owner_component_id,payload_owner_kind,
            payload_owner_id,author_payload_id,author_payload_digest,
            author_owner_component_id,author_owner_kind,author_owner_id,
            author_subject_ref,trigger_decision
        )
        SELECT requested_bot,requested_conversation,
               'input:' || encode(sha256(convert_to(jsonb_build_array(
                   requested_bot,requested_conversation,event_id
               )::text,'UTF8')),'hex'),
               selection_context_revision,
               row_number() OVER (ORDER BY ingress_seq)-1,
               'INPUT',source_component_id,'conversation.input',event_id,batch_id,
               event_id,ingress_seq,content_payload_id,content_payload_digest,content_byte_length,
               content_owner_component_id,content_owner_kind,content_owner_id,
               author_payload_id,author_payload_digest,author_owner_component_id,
               author_owner_kind,author_owner_id,author_subject_ref,trigger_decision
        FROM __forest_module_schema__.inbox_messages
        WHERE bot_id=requested_bot AND conversation_id=requested_conversation
          AND ingress_seq BETWEEN selection_start AND selection_end
        ORDER BY ingress_seq;
        INSERT INTO __forest_module_schema__.active_context_units(
            bot_id,conversation_id,unit_id,context_revision,unit_index
        )
        SELECT bot_id,conversation_id,unit_id,context_revision,unit_index
        FROM __forest_module_schema__.context_units
        WHERE bot_id=requested_bot AND conversation_id=requested_conversation
          AND context_revision=selection_context_revision;
    END IF;
    IF segment_summary IS NOT NULL THEN
        UPDATE __forest_module_schema__.conversation_triggers
        SET status='CLAIMED',claimed_session_ref=selected_session,
            claimed_run_ref=selected_run,input_batch_id=batch_id,
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND conversation_id=requested_conversation
          AND status='PENDING'
          AND ((selection_kind='CONTEXT_PREFILL'
                AND ingress_seq BETWEEN selection_start AND selection_end)
               OR (selection_kind='RECOVERY_PREFILL'
                   AND ingress_seq<=selection_trigger_cut)
               OR (selection_kind<>'CONTEXT_PREFILL'
                   AND selection_kind<>'RECOVERY_PREFILL'
                   AND ingress_seq<=lane.classified_through_seq));
        GET DIAGNOSTICS claimed_count = ROW_COUNT;
        IF claimed_count<>(segment_summary->>'count')::integer THEN
            RAISE EXCEPTION 'Trigger claim changed inside the lane fence'
                USING ERRCODE='55000';
        END IF;
        INSERT INTO __forest_module_schema__.input_batch_triggers(
            bot_id,input_batch_id,trigger_index,trigger_id
        )
        SELECT requested_bot,batch_id,
               row_number() OVER (ORDER BY ingress_seq,trigger_id)-1,trigger_id
        FROM __forest_module_schema__.conversation_triggers
        WHERE bot_id=requested_bot AND conversation_id=requested_conversation
          AND status='CLAIMED' AND input_batch_id=batch_id
        ORDER BY ingress_seq,trigger_id;
        IF existing_claim.claim_ref IS NULL THEN
            INSERT INTO __forest_module_schema__.trigger_claim_groups(
                bot_id,conversation_id,claim_ref,session_ref,run_ref,
                trigger_count,segment_count,last_trigger_ref,
                last_trigger_ingress_seq,claim_digest
            ) VALUES (
                requested_bot,requested_conversation,claim_ref_value,
                selected_session,selected_run,claim_count_value,1,
                segment_summary->>'last_trigger_ref',
                (segment_summary->>'last_trigger_ingress_seq')::bigint,
                claim_digest_value
            );
        ELSE
            UPDATE __forest_module_schema__.trigger_claim_groups
            SET trigger_count=claim_count_value,segment_count=claim_segment_index+1,
                last_trigger_ref=segment_summary->>'last_trigger_ref',
                last_trigger_ingress_seq=(segment_summary->>'last_trigger_ingress_seq')::bigint,
                claim_digest=claim_digest_value
            WHERE bot_id=requested_bot AND claim_ref=claim_ref_value;
        END IF;
        INSERT INTO __forest_module_schema__.trigger_claim_segments(
            bot_id,claim_ref,segment_index,input_batch_id,trigger_count,segment_digest
        ) VALUES (
            requested_bot,claim_ref_value,claim_segment_index,batch_id,
            (segment_summary->>'count')::integer,segment_summary->>'digest'
        );
    END IF;
    selection_mailbox_revision := lane.mailbox_revision + 1;
    UPDATE __forest_module_schema__.conversation_lanes
    SET context_applied_seq=COALESCE(selection_end,context_applied_seq),
        context_revision=selection_context_revision,
        context_view_unit_count=context_view_unit_count+COALESCE(event_count,0),
        context_view_payload_bytes=context_view_payload_bytes+COALESCE((
            SELECT sum(content_byte_length)
            FROM __forest_module_schema__.inbox_messages
            WHERE bot_id=requested_bot AND conversation_id=requested_conversation
              AND selection_start IS NOT NULL
              AND ingress_seq BETWEEN selection_start AND selection_end
        ),0),
        context_view_media_bytes=context_view_media_bytes+COALESCE((
            SELECT sum(m.byte_length)
            FROM __forest_module_schema__.context_media_costs m
            JOIN __forest_module_schema__.inbox_messages i
              ON i.bot_id=m.bot_id AND i.conversation_id=m.conversation_id
             AND i.event_id=m.event_id
            WHERE i.bot_id=requested_bot AND i.conversation_id=requested_conversation
              AND selection_start IS NOT NULL
              AND i.ingress_seq BETWEEN selection_start AND selection_end
        ),0),
        context_view_decoded_bytes=context_view_decoded_bytes+COALESCE((
            SELECT sum(m.decoded_pixels::bigint * 4)
            FROM __forest_module_schema__.context_media_costs m
            JOIN __forest_module_schema__.inbox_messages i
              ON i.bot_id=m.bot_id AND i.conversation_id=m.conversation_id
             AND i.event_id=m.event_id
            WHERE i.bot_id=requested_bot AND i.conversation_id=requested_conversation
              AND selection_start IS NOT NULL
              AND i.ingress_seq BETWEEN selection_start AND selection_end
        ),0),
        active_session_ref=CASE
            WHEN requested_boundary='SESSION_END' AND selection_kind='SILENT_DRAIN'
                THEN NULL
            WHEN selection_kind IN ('CONTEXT_PREFILL','RECOVERY_PREFILL')
                THEN active_session_ref
            ELSE COALESCE(selected_session,active_session_ref)
        END,
        active_run_ref=CASE
            WHEN requested_boundary='SESSION_END' AND selection_kind='SILENT_DRAIN'
                THEN NULL
            WHEN selection_kind IN ('CONTEXT_PREFILL','RECOVERY_PREFILL')
                THEN active_run_ref
            ELSE COALESCE(selected_run,active_run_ref)
        END,
        mailbox_revision=mailbox_revision+1,
        resume_not_before=NULL,resume_reason_code=NULL,resume_reason_ref=NULL,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation;
    INSERT INTO __forest_module_schema__.selection_operations(
        bot_id,operation_id,conversation_id,input_digest,boundary,result_status,
        input_batch_id,observed_mailbox_revision,observed_classified_through_seq,
        selected_input_end_seq,actor_owner_id,actor_epoch
    ) VALUES (
        requested_bot,requested_operation,requested_conversation,requested_digest,
        requested_boundary,'BATCH',batch_id,selection_mailbox_revision,
        lane.classified_through_seq,COALESCE(selection_end,lane.context_applied_seq),
        requested_owner,requested_epoch
    );
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',__forest_module_schema__.conversation_selection_result(
            requested_bot,requested_operation
        )
    );
END;
$$;

CREATE FUNCTION conversation_context_page(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog
AS $$
DECLARE
    actor jsonb := input->'actor';
    requested_bot text := context->>'bot_id';
    requested_conversation text := actor->>'conversation_id';
    requested_run text := input->>'run_ref';
    requested_owner text := actor->>'owner_id';
    requested_epoch bigint := (actor->>'epoch')::bigint;
    requested_offset integer := (input->>'offset')::integer;
    requested_limit integer := (input->>'limit')::integer;
    lane record;
    compaction record;
    units jsonb;
    archive_value jsonb := NULL;
    total_units bigint;
    returned_count integer;
    next_offset integer := NULL;
    claimed_trigger_count integer := 0;
BEGIN
    IF actor IS NULL OR requested_conversation IS NULL OR requested_owner IS NULL
       OR requested_epoch IS NULL OR requested_epoch<1
       OR requested_offset IS NULL OR requested_offset<0 OR requested_offset>1000000
       OR requested_limit IS NULL OR requested_limit<1 OR requested_limit>32
       OR (requested_run IS NOT NULL AND octet_length(requested_run) NOT BETWEEN 1 AND 256) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONTEXT_PAGE_INPUT_INVALID',
                'message','Context page request is invalid','status',422
            )
        );
    END IF;
    SELECT * INTO lane
    FROM __forest_module_schema__.conversation_lanes
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation;
    IF lane IS NULL THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_NOT_FOUND',
                'message','Conversation lane does not exist','status',404
            )
        );
    END IF;
    IF lane.actor_owner_id IS DISTINCT FROM requested_owner
       OR lane.actor_epoch IS DISTINCT FROM requested_epoch
       OR lane.actor_expires_at IS NULL
       OR lane.actor_expires_at<=CURRENT_TIMESTAMP THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','STALE_CONVERSATION_ACTOR',
                'message','Conversation actor is stale','status',409
            )
        );
    END IF;
    IF requested_run IS NOT NULL
       AND lane.active_run_ref IS DISTINCT FROM requested_run THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONTEXT_SNAPSHOT_RUN_FENCE_CHANGED',
                'message','Conversation Run differs from the requested snapshot','status',409
            )
        );
    END IF;
    IF requested_run IS NOT NULL THEN
        SELECT count(*) INTO claimed_trigger_count
        FROM __forest_module_schema__.conversation_triggers t
        WHERE t.bot_id=requested_bot AND t.conversation_id=requested_conversation
          AND t.claimed_session_ref=lane.active_session_ref
          AND t.claimed_run_ref=requested_run AND t.status='CLAIMED';
    END IF;
    SELECT * INTO compaction
    FROM __forest_module_schema__.context_compactions
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
    ORDER BY published_view_revision DESC LIMIT 1;
    total_units := lane.context_view_unit_count;
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'conversation_id',u.conversation_id,'unit_id',u.unit_id,
        'context_revision',u.context_revision,'unit_index',u.unit_index,
        'kind',u.kind,'source_component_id',u.source_component_id,
        'source_kind',u.source_kind,'source_id',u.source_id,
        'input_batch_id',u.input_batch_id,'event_id',u.event_id,
        'ingress_seq',u.ingress_seq,'payload_id',u.payload_id,
        'payload_digest',u.payload_digest,
        'payload_byte_length',u.payload_byte_length,
        'payload_owner_component_id',u.payload_owner_component_id,
        'payload_owner_kind',u.payload_owner_kind,
        'payload_owner_id',u.payload_owner_id,
        'author_payload_id',u.author_payload_id,
        'author_payload_digest',u.author_payload_digest,
        'author_owner_component_id',u.author_owner_component_id,
        'author_owner_kind',u.author_owner_kind,
        'author_owner_id',u.author_owner_id,
        'author_subject_ref',u.author_subject_ref,
        'trigger_decision',u.trigger_decision,
        'required_for_run',requested_run IS NOT NULL AND EXISTS (
            SELECT 1 FROM __forest_module_schema__.conversation_triggers t
            WHERE t.bot_id=u.bot_id AND t.conversation_id=u.conversation_id
              AND t.event_id=u.event_id AND t.status='CLAIMED'
              AND t.claimed_session_ref=lane.active_session_ref
              AND t.claimed_run_ref=requested_run
        ),
        'media_parts',COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'part_index',m.part_index,'media_ref',m.media_ref,
                'media_revision',m.media_revision,'byte_length',m.byte_length,
                'width',m.width,'height',m.height,
                'decoded_pixels',m.decoded_pixels,
                'payload',jsonb_build_object(
                    'payload_id',m.payload_id,'digest',m.payload_digest,
                    'owner_component_id','media.capture','owner_kind','media.asset',
                    'owner_id',m.media_ref)
            ) ORDER BY m.part_index)
            FROM __forest_module_schema__.context_media_costs m
            WHERE m.bot_id=u.bot_id AND m.conversation_id=u.conversation_id
              AND m.event_id=u.event_id
        ),'[]'::jsonb)
    ) ORDER BY u.context_revision,u.unit_index),'[]'::jsonb)
    INTO units
    FROM (
        SELECT u.*
        FROM __forest_module_schema__.active_context_units a
        JOIN __forest_module_schema__.context_units u
          ON u.bot_id=a.bot_id AND u.conversation_id=a.conversation_id
         AND u.unit_id=a.unit_id
        WHERE a.bot_id=requested_bot AND a.conversation_id=requested_conversation
        ORDER BY a.context_revision,a.unit_index
        OFFSET requested_offset LIMIT requested_limit
    ) u;
    returned_count := jsonb_array_length(units);
    IF (requested_offset<total_units AND returned_count=0)
       OR requested_offset+returned_count>total_units THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONTEXT_ACTIVE_INDEX_INVALID',
                'message','Active Context index does not cover the view','status',409
            )
        );
    END IF;
    IF requested_offset+returned_count<total_units THEN
        next_offset := requested_offset+returned_count;
    END IF;
    IF compaction IS NOT NULL THEN
        archive_value := jsonb_build_object(
            'compaction_id',compaction.compaction_id,
            'published_view_revision',compaction.published_view_revision,
            'source_set_digest',compaction.source_set_digest,
            'policy_revision',compaction.policy_revision,
            'archived_unit_count',compaction.archived_unit_count,
            'cumulative_archived_unit_count',compaction.cumulative_archived_unit_count,
            'first_context_revision',compaction.first_context_revision,
            'last_context_revision',compaction.last_context_revision,
            'payload',jsonb_build_object(
                'payload_id',compaction.archive_payload_id,
                'digest',compaction.archive_payload_digest,
                'owner_component_id','conversation.scheduler',
                'owner_kind','context.archive',
                'owner_id',compaction.compaction_id
            )
        );
    END IF;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'conversation_id',requested_conversation,
            'run_ref',requested_run,
            'claimed_trigger_count',claimed_trigger_count,
            'context_applied_seq',lane.context_applied_seq,
            'classified_through_seq',lane.classified_through_seq,
            'context_revision',lane.context_revision,
            'context_view_revision',lane.context_view_revision,
            'source_component_id',lane.source_component_id,
            'binding_ref',lane.binding_ref,'platform',lane.platform,
            'channel_kind',lane.channel_kind,'channel_ref',lane.channel_ref,
            'thread_ref',lane.thread_ref,
            'context_view_unit_count',lane.context_view_unit_count,
            'context_view_payload_bytes',lane.context_view_payload_bytes,
            'context_view_media_bytes',lane.context_view_media_bytes,
            'context_view_decoded_bytes',lane.context_view_decoded_bytes,
            'media_cost_revision',lane.media_cost_revision,
            'context_archived_unit_count',lane.context_archived_unit_count,
            'archive',archive_value,'offset',requested_offset,
            'next_offset',next_offset,'total_units',total_units,'units',units
        )
    );
END;
$$;

CREATE FUNCTION conversation_prepare_context_compaction(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    selected jsonb := envelope#>'{prior,select}';
    anchor jsonb := body->'anchor';
    pre_run boolean := anchor IS NOT NULL;
    handoff boolean := COALESCE(
        anchor->>'protocol'='forest.conversation-handoff-compaction-anchor/1',false);
    policy jsonb := body->'context_view';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    requested_conversation text := COALESCE(selected->>'conversation_id',anchor->>'conversation_id');
    high_watermark integer := (policy->>'high_watermark_units')::integer;
    retain_units integer := (policy->>'retain_units')::integer;
    high_watermark_bytes bigint := (policy->>'high_watermark_payload_bytes')::bigint;
    retain_bytes bigint := (policy->>'retain_payload_bytes')::bigint;
    high_watermark_media_bytes bigint := (policy->>'high_watermark_media_bytes')::bigint;
    retain_media_bytes bigint := (policy->>'retain_media_bytes')::bigint;
    high_watermark_decoded_bytes bigint := (policy->>'high_watermark_decoded_bytes')::bigint;
    retain_decoded_bytes bigint := (policy->>'retain_decoded_bytes')::bigint;
    high_watermark_estimated_tokens bigint := (policy->>'high_watermark_estimated_tokens')::bigint;
    retain_estimated_tokens bigint := (policy->>'retain_estimated_tokens')::bigint;
    utf8_bytes_per_estimated_token bigint := (policy->>'utf8_bytes_per_estimated_token')::bigint;
    unit_overhead_estimated_tokens bigint := (policy->>'unit_overhead_estimated_tokens')::bigint;
    policy_revision text := policy->>'policy_revision';
    lane record;
    replay record;
    archive_target bigint;
    units jsonb;
    archived_count bigint;
    archived_bytes bigint;
    archived_media_bytes bigint;
    archived_decoded_bytes bigint;
    first_revision bigint;
    last_revision bigint;
    source_set_digest text;
    published_view_revision bigint;
    cumulative_count bigint;
    resulting_count bigint;
    resulting_bytes bigint;
    resulting_media_bytes bigint;
    resulting_decoded_bytes bigint;
    compaction_id text;
    archive_value jsonb;
    commit_value jsonb;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR (selected IS NULL AND NOT pre_run) OR policy IS NULL
       OR (pre_run AND (selected IS NOT NULL
           OR anchor->>'protocol' NOT IN (
               'forest.conversation-pre-run-anchor/1',
               'forest.conversation-handoff-compaction-anchor/1')
           OR anchor->>'conversation_id' IS DISTINCT FROM anchor#>>'{actor,conversation_id}'
           OR anchor#>>'{actor,owner_id}' IS NULL
           OR anchor#>>'{actor,epoch}' !~ '^[1-9][0-9]*$'))
       OR (handoff AND (
           anchor->>'session_ref' IS NULL OR anchor->>'run_ref' IS NULL
           OR envelope#>>'{prior,agent,protocol}' IS DISTINCT FROM
              'forest.agent-completed-compaction/1'
           OR envelope#>>'{prior,agent,conversation_id}' IS DISTINCT FROM
              anchor->>'conversation_id'
           OR envelope#>>'{prior,agent,session_ref}' IS DISTINCT FROM
              anchor->>'session_ref'
           OR envelope#>>'{prior,agent,run_ref}' IS DISTINCT FROM
              anchor->>'run_ref'))
       OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR high_watermark NOT BETWEEN 2 AND 1000000
       OR retain_units NOT BETWEEN 1 AND high_watermark-1
       OR high_watermark_bytes NOT BETWEEN 1024 AND 134217728
       OR retain_bytes NOT BETWEEN 1 AND high_watermark_bytes-1
       OR high_watermark_media_bytes NOT BETWEEN 1024 AND 134217728
       OR retain_media_bytes NOT BETWEEN 1 AND high_watermark_media_bytes-1
       OR high_watermark_decoded_bytes NOT BETWEEN 1024 AND 536870912
       OR retain_decoded_bytes NOT BETWEEN 1 AND high_watermark_decoded_bytes-1
       OR high_watermark_estimated_tokens NOT BETWEEN 1024 AND 1048576
       OR retain_estimated_tokens NOT BETWEEN 1 AND high_watermark_estimated_tokens-1
       OR utf8_bytes_per_estimated_token NOT BETWEEN 1 AND 16
       OR unit_overhead_estimated_tokens NOT BETWEEN 0 AND 256
       OR policy_revision IS DISTINCT FROM 'context-view-multimodal-window-v5' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONTEXT_COMPACTION_INPUT_INVALID',
                'message','Context compaction input is invalid','status',422
            )
        );
    END IF;
    IF NOT pre_run AND selected->>'status' <> 'BATCH' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'protocol','forest.payload-json-plan/1','status','SKIP'
            )
        );
    END IF;
    IF pre_run THEN
        SELECT * INTO lane
        FROM __forest_module_schema__.conversation_lanes
        WHERE bot_id=requested_bot AND conversation_id=requested_conversation
        FOR UPDATE;
        IF lane IS NULL
           OR lane.actor_owner_id IS DISTINCT FROM anchor#>>'{actor,owner_id}'
           OR lane.actor_epoch IS DISTINCT FROM (anchor#>>'{actor,epoch}')::bigint
           OR lane.actor_expires_at IS NULL
           OR lane.actor_expires_at<=clock_timestamp() THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','STALE_CONVERSATION_ACTOR',
                    'message','Pre-run Context compaction requires the current Actor fence','status',409
                )
            );
        END IF;
    END IF;
    SELECT * INTO replay
    FROM __forest_module_schema__.context_compactions
    WHERE bot_id=requested_bot AND operation_id=requested_operation;
    IF replay IS NOT NULL THEN
        IF replay.input_digest IS DISTINCT FROM requested_digest
           OR replay.conversation_id IS DISTINCT FROM requested_conversation THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','CONTEXT_COMPACTION_OPERATION_CONFLICT',
                    'message','Context compaction operation differs','status',409
                )
            );
        END IF;
        SELECT COALESCE(jsonb_agg(jsonb_build_object(
            'unit_id',u.unit_id,
            'context_revision',u.context_revision,
            'unit_index',u.unit_index,
            'payload_digest',u.payload_digest,
            'payload_byte_length',u.payload_byte_length,
            'media_byte_length',cu.media_byte_length,
            'decoded_byte_length',cu.decoded_byte_length,
            'author_payload_digest',u.author_payload_digest,
            'event_id',u.event_id,
            'input_batch_id',u.input_batch_id
        ) ORDER BY cu.archived_index),'[]'::jsonb)
        INTO units
        FROM __forest_module_schema__.context_compaction_units cu
        JOIN __forest_module_schema__.context_units u
          ON u.bot_id=cu.bot_id AND u.conversation_id=cu.conversation_id
         AND u.unit_id=cu.unit_id
        WHERE cu.bot_id=requested_bot AND cu.compaction_id=replay.compaction_id;
        archive_value := jsonb_build_object(
            'protocol','forest.context-archive-reference/1',
            'conversation_id',replay.conversation_id,
            'compaction_id',replay.compaction_id,
            'published_view_revision',replay.published_view_revision,
            'policy_revision',replay.policy_revision,
            'archived_unit_count',replay.archived_unit_count,
            'cumulative_archived_unit_count',replay.cumulative_archived_unit_count,
            'source_set_digest',replay.source_set_digest,
            'coverage',jsonb_build_object(
                'first_context_revision',replay.first_context_revision,
                'last_context_revision',replay.last_context_revision
            ),
            'authority','conversation.scheduler/context_compaction_units'
        );
        commit_value := jsonb_build_object(
            'operation_id',replay.operation_id,'input_digest',replay.input_digest,
            'conversation_id',replay.conversation_id,'compaction_id',replay.compaction_id,
            'base_view_revision',replay.base_view_revision,
            'published_view_revision',replay.published_view_revision,
            'source_set_digest',replay.source_set_digest,
            'policy_revision',replay.policy_revision,
            'archived_unit_count',replay.archived_unit_count,
            'archived_payload_bytes',replay.archived_payload_bytes,
            'archived_media_bytes',replay.archived_media_bytes,
            'archived_decoded_bytes',replay.archived_decoded_bytes,
            'resulting_view_unit_count',replay.resulting_view_unit_count,
            'resulting_view_payload_bytes',replay.resulting_view_payload_bytes,
            'resulting_view_media_bytes',replay.resulting_view_media_bytes,
            'resulting_view_decoded_bytes',replay.resulting_view_decoded_bytes,
            'base_media_cost_revision',replay.base_media_cost_revision,
            'cumulative_archived_unit_count',replay.cumulative_archived_unit_count,
            'first_context_revision',replay.first_context_revision,
            'last_context_revision',replay.last_context_revision,'units',units
        );
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'protocol','forest.payload-json-plan/1','status','WRITE',
                'payload',jsonb_build_object(
                    'owner_kind','context.archive','owner_id',replay.compaction_id,
                    'value',archive_value,'parents','[]'::jsonb,
                    'sources',jsonb_build_array(jsonb_build_object(
                        'source_component_id','conversation.scheduler',
                        'source_kind','context.archive','source_id',replay.compaction_id,
                        'source_revision',replay.published_view_revision,'part_id','*',
                        'source_trunk_id',NULL,'observation_conversation_id',replay.conversation_id,
                        'observation_binding_id',NULL,'author_subject_id',NULL,
                        'occurred_at',NULL,'propagation_class','bot'
                    ))
                ),
                'commit',commit_value
            )
        );
    END IF;
    IF NOT pre_run THEN
        SELECT * INTO lane
        FROM __forest_module_schema__.conversation_lanes
        WHERE bot_id=requested_bot AND conversation_id=requested_conversation
        FOR UPDATE;
    END IF;
    IF lane IS NULL THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_NOT_FOUND',
                'message','Conversation lane does not exist','status',404
            )
        );
    END IF;
    IF handoff AND (
        lane.active_session_ref IS DISTINCT FROM anchor->>'session_ref'
        OR lane.active_run_ref IS DISTINCT FROM anchor->>'run_ref') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','CONTEXT_HANDOFF_BOUNDARY_CLOSED',
                'message','Conversation moved beyond the completed Run','status',409));
    END IF;
    IF pre_run AND NOT handoff AND lane.active_session_ref IS NOT NULL THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'protocol','forest.payload-json-plan/1','status','SKIP'
            )
        );
    END IF;
    IF lane.context_view_unit_count <= high_watermark
       AND lane.context_view_payload_bytes <= high_watermark_bytes
       AND lane.context_view_media_bytes <= high_watermark_media_bytes
       AND lane.context_view_decoded_bytes <= high_watermark_decoded_bytes
       AND (lane.context_view_payload_bytes + utf8_bytes_per_estimated_token-1)
           /utf8_bytes_per_estimated_token
           + lane.context_view_unit_count*unit_overhead_estimated_tokens
           <= high_watermark_estimated_tokens THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'protocol','forest.payload-json-plan/1','status','SKIP'
            )
        );
    END IF;
    -- Keep the archive manifest and its Memory eligibility proof bounded.
    -- A later maintenance claim will continue from the next active prefix.
    -- The unit retention level is a target, not a floor: byte pressure may
    -- archive below it while keeping at least the newest active unit.
    archive_target := LEAST(lane.context_view_unit_count-1,64);
    IF archive_target < 1 THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'protocol','forest.payload-json-plan/1','status','SKIP'
            )
        );
    END IF;
    WITH oldest AS MATERIALIZED (
        SELECT u.*,COALESCE((
            SELECT sum(m.byte_length)
            FROM __forest_module_schema__.context_media_costs m
            WHERE m.bot_id=u.bot_id AND m.conversation_id=u.conversation_id
              AND m.event_id=u.event_id
        ),0)::bigint AS media_byte_length,
        COALESCE((
            SELECT sum(m.decoded_pixels::bigint * 4)
            FROM __forest_module_schema__.context_media_costs m
            WHERE m.bot_id=u.bot_id AND m.conversation_id=u.conversation_id
              AND m.event_id=u.event_id
        ),0)::bigint AS decoded_byte_length
        FROM __forest_module_schema__.active_context_units a
        JOIN __forest_module_schema__.context_units u
          ON u.bot_id=a.bot_id AND u.conversation_id=a.conversation_id
         AND u.unit_id=a.unit_id
        WHERE a.bot_id=requested_bot AND a.conversation_id=requested_conversation
        ORDER BY a.context_revision,a.unit_index
        LIMIT archive_target
    ), marked AS (
        SELECT a.*,
            row_number() OVER (ORDER BY a.context_revision,a.unit_index) AS archive_order,
            (EXISTS (
              SELECT 1 FROM __forest_module_schema__.conversation_triggers t
              WHERE t.bot_id=a.bot_id AND t.conversation_id=a.conversation_id
                AND t.event_id=a.event_id AND (
                    t.status='PENDING' OR (t.status='CLAIMED' AND NOT EXISTS (
                        SELECT 1 FROM __forest_module_schema__.trigger_claim_groups g
                        WHERE g.bot_id=t.bot_id AND g.conversation_id=t.conversation_id
                          AND g.session_ref=t.claimed_session_ref
                          AND g.run_ref=t.claimed_run_ref
                          AND g.claim_state='TERMINAL'
                    ))
                )
            ) OR (
              lane.active_session_ref IS NOT NULL AND NOT handoff AND (
                  EXISTS (
                      SELECT 1 FROM __forest_module_schema__.input_batches b
                      WHERE b.bot_id=a.bot_id AND b.input_batch_id=a.input_batch_id
                        AND b.session_ref=lane.active_session_ref
                  ) OR EXISTS (
                      SELECT 1 FROM __forest_module_schema__.context_append_operations op
                      WHERE op.bot_id=a.bot_id AND op.conversation_id=a.conversation_id
                        AND op.context_revision=a.context_revision
                        AND op.session_ref=lane.active_session_ref
                  )
              )
            )) AS is_protected
        FROM oldest a
    ), eligible AS (
        -- An unresolved trigger or active Session unit pins the suffix: an
        -- archive may only remove the contiguous prefix before that unit.
        SELECT m.* FROM (
            SELECT marked.*,
                   min(archive_order) FILTER (WHERE is_protected) OVER () AS first_protected
            FROM marked
        ) m
        WHERE m.first_protected IS NULL OR m.archive_order<m.first_protected
        ORDER BY m.archive_order
    ), ranked AS (
        SELECT eligible.*,
               sum(payload_byte_length) OVER (ORDER BY archive_order) AS prefix_bytes,
               sum(media_byte_length) OVER (ORDER BY archive_order) AS prefix_media_bytes,
               sum(decoded_byte_length) OVER (ORDER BY archive_order) AS prefix_decoded_bytes
        FROM eligible
    ), cutoff AS (
        SELECT COALESCE(
            min(archive_order) FILTER (WHERE
                lane.context_view_unit_count-archive_order<=retain_units
                AND lane.context_view_payload_bytes-prefix_bytes<=retain_bytes
                AND lane.context_view_media_bytes-prefix_media_bytes<=retain_media_bytes
                AND lane.context_view_decoded_bytes-prefix_decoded_bytes<=retain_decoded_bytes
                AND (lane.context_view_payload_bytes-prefix_bytes
                     + utf8_bytes_per_estimated_token-1)
                    /utf8_bytes_per_estimated_token
                    + (lane.context_view_unit_count-archive_order)
                      *unit_overhead_estimated_tokens<=retain_estimated_tokens),
            max(archive_order)
        ) AS last_order FROM ranked
    ), candidates AS (
        SELECT ranked.* FROM ranked CROSS JOIN cutoff
        WHERE ranked.archive_order<=cutoff.last_order
        ORDER BY ranked.archive_order
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'unit_id',unit_id,'context_revision',context_revision,
               'unit_index',unit_index,'payload_digest',payload_digest,
               'payload_byte_length',payload_byte_length,
               'media_byte_length',media_byte_length,
               'decoded_byte_length',decoded_byte_length,
               'author_payload_digest',author_payload_digest,'event_id',event_id,
               'input_batch_id',input_batch_id
           ) ORDER BY context_revision,unit_index),'[]'::jsonb),
           count(*)::bigint,COALESCE(sum(payload_byte_length),0)::bigint,
           COALESCE(sum(media_byte_length),0)::bigint,
           COALESCE(sum(decoded_byte_length),0)::bigint,
           min(context_revision),max(context_revision)
    INTO units,archived_count,archived_bytes,archived_media_bytes,
         archived_decoded_bytes,
         first_revision,last_revision
    FROM candidates;
    IF archived_count=0 THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'protocol','forest.payload-json-plan/1','status','SKIP'
            )
        );
    END IF;
    source_set_digest := 'sha256:' || encode(
        sha256(convert_to(units::text,'UTF8')),'hex'
    );
    published_view_revision := lane.context_view_revision+1;
    resulting_count := lane.context_view_unit_count-archived_count;
    resulting_bytes := lane.context_view_payload_bytes-archived_bytes;
    resulting_media_bytes := lane.context_view_media_bytes-archived_media_bytes;
    resulting_decoded_bytes := lane.context_view_decoded_bytes-archived_decoded_bytes;
    cumulative_count := lane.context_archived_unit_count+archived_count;
    compaction_id := 'compaction:' || encode(sha256(convert_to(jsonb_build_array(
        requested_bot,requested_conversation,published_view_revision,
        source_set_digest,policy_revision
    )::text,'UTF8')),'hex');
    archive_value := jsonb_build_object(
        'protocol','forest.context-archive-reference/1',
        'conversation_id',requested_conversation,'compaction_id',compaction_id,
        'published_view_revision',published_view_revision,
        'policy_revision',policy_revision,'archived_unit_count',archived_count,
        'cumulative_archived_unit_count',cumulative_count,
        'source_set_digest',source_set_digest,
        'coverage',jsonb_build_object(
            'first_context_revision',first_revision,
            'last_context_revision',last_revision
        ),
        'authority','conversation.scheduler/context_compaction_units'
    );
    commit_value := jsonb_build_object(
        'operation_id',requested_operation,'input_digest',requested_digest,
        'conversation_id',requested_conversation,'compaction_id',compaction_id,
        'base_view_revision',lane.context_view_revision,
        'published_view_revision',published_view_revision,
        'source_set_digest',source_set_digest,'policy_revision',policy_revision,
        'archived_unit_count',archived_count,
        'archived_payload_bytes',archived_bytes,
        'archived_media_bytes',archived_media_bytes,
        'archived_decoded_bytes',archived_decoded_bytes,
        'resulting_view_unit_count',resulting_count,
        'resulting_view_payload_bytes',resulting_bytes,
        'resulting_view_media_bytes',resulting_media_bytes,
        'resulting_view_decoded_bytes',resulting_decoded_bytes,
        'base_media_cost_revision',lane.media_cost_revision,
        'cumulative_archived_unit_count',cumulative_count,
        'first_context_revision',first_revision,'last_context_revision',last_revision,
        'units',units
    );
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.payload-json-plan/1','status','WRITE',
            'payload',jsonb_build_object(
                'owner_kind','context.archive','owner_id',compaction_id,
                'value',archive_value,'parents','[]'::jsonb,
                'sources',jsonb_build_array(jsonb_build_object(
                    'source_component_id','conversation.scheduler',
                    'source_kind','context.archive','source_id',compaction_id,
                    'source_revision',published_view_revision,'part_id','*',
                    'source_trunk_id',NULL,
                    'observation_conversation_id',requested_conversation,
                    'observation_binding_id',NULL,'author_subject_id',NULL,
                    'occurred_at',NULL,'propagation_class','bot'
                ))
            ),
            'commit',commit_value
        )
    );
END;
$$;

CREATE FUNCTION conversation_commit_context_compaction(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    selected jsonb := envelope#>'{prior,select}';
    anchor jsonb := envelope#>'{input,anchor}';
    pre_run boolean := anchor IS NOT NULL;
    handoff boolean := COALESCE(
        anchor->>'protocol'='forest.conversation-handoff-compaction-anchor/1',false);
    plan jsonb := envelope#>'{prior,prepare}';
    payload_result jsonb := envelope#>'{prior,payload}';
    commit_value jsonb;
    payload_ref jsonb;
    requested_bot text := context->>'bot_id';
    lane record;
    existing record;
    unit_count integer;
    payload_bytes bigint;
    media_bytes bigint;
    decoded_bytes bigint;
    media_cost_mismatches integer;
    decoded_cost_mismatches integer;
    event_refs jsonb;
    result_value jsonb;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR (selected IS NULL AND NOT pre_run) OR plan IS NULL OR payload_result IS NULL
       OR (pre_run AND selected IS NOT NULL)
       OR (handoff AND (
           envelope#>>'{prior,agent,protocol}' IS DISTINCT FROM
              'forest.agent-completed-compaction/1'
           OR envelope#>>'{prior,agent,conversation_id}' IS DISTINCT FROM
              anchor->>'conversation_id'
           OR envelope#>>'{prior,agent,session_ref}' IS DISTINCT FROM
              anchor->>'session_ref'
           OR envelope#>>'{prior,agent,run_ref}' IS DISTINCT FROM
              anchor->>'run_ref'))
       OR plan->>'protocol' IS DISTINCT FROM 'forest.payload-json-plan/1'
       OR payload_result->>'protocol' IS DISTINCT FROM 'forest.payload-json-put/1' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONTEXT_COMPACTION_COMMIT_INVALID',
                'message','Context compaction commit is invalid','status',422
            )
        );
    END IF;
    IF plan->>'status'='SKIP' THEN
        IF payload_result->>'status' IS DISTINCT FROM 'SKIPPED' THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','CONTEXT_COMPACTION_COMMIT_INVALID',
                    'message','Skipped compaction wrote a Payload','status',409
                )
            );
        END IF;
        IF pre_run THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',true,
                'value',jsonb_build_object(
                    'protocol','forest.context-pre-run-compaction/1',
                    'status','SKIPPED','conversation_id',anchor->>'conversation_id'
                )
            );
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',selected);
    END IF;
    commit_value := plan->'commit';
    payload_ref := payload_result->'payload';
    IF plan->>'status' IS DISTINCT FROM 'WRITE'
       OR payload_result->>'status' IS DISTINCT FROM 'WRITTEN'
       OR commit_value IS NULL OR payload_ref IS NULL
       OR payload_ref->>'owner_component_id' IS DISTINCT FROM 'conversation.scheduler'
       OR payload_ref->>'owner_kind' IS DISTINCT FROM 'context.archive'
       OR payload_ref->>'owner_id' IS DISTINCT FROM commit_value->>'compaction_id'
       OR (pre_run AND commit_value->>'conversation_id' IS DISTINCT FROM anchor->>'conversation_id')
       OR payload_ref->>'digest' !~ '^sha256:[a-f0-9]{64}$' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONTEXT_COMPACTION_COMMIT_INVALID',
                'message','Context archive Payload differs from its plan','status',409
            )
        );
    END IF;
    SELECT * INTO existing
    FROM __forest_module_schema__.context_compactions
    WHERE bot_id=requested_bot AND operation_id=commit_value->>'operation_id';
    IF existing IS NULL THEN
        SELECT * INTO lane
        FROM __forest_module_schema__.conversation_lanes
        WHERE bot_id=requested_bot
          AND conversation_id=commit_value->>'conversation_id'
        FOR UPDATE;
        IF lane IS NULL
           OR (handoff AND (
               lane.active_session_ref IS DISTINCT FROM anchor->>'session_ref'
               OR lane.active_run_ref IS DISTINCT FROM anchor->>'run_ref'))
           OR lane.context_view_revision IS DISTINCT FROM
              (commit_value->>'base_view_revision')::bigint
           OR lane.context_view_unit_count <
              (commit_value->>'archived_unit_count')::bigint
           OR lane.context_view_payload_bytes <
              (commit_value->>'archived_payload_bytes')::bigint
           OR lane.media_cost_revision IS DISTINCT FROM
              (commit_value->>'base_media_cost_revision')::bigint
           OR lane.context_view_media_bytes <
              (commit_value->>'archived_media_bytes')::bigint
           OR lane.context_view_decoded_bytes <
              (commit_value->>'archived_decoded_bytes')::bigint THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','CONTEXT_COMPACTION_STALE',
                    'message','Context view changed before compaction commit','status',409
                )
            );
        END IF;
        SELECT count(*)::integer,COALESCE(sum(u.payload_byte_length),0)::bigint
        INTO unit_count,payload_bytes
        FROM jsonb_array_elements(commit_value->'units') item
        JOIN __forest_module_schema__.context_units u
          ON u.bot_id=requested_bot
         AND u.conversation_id=commit_value->>'conversation_id'
         AND u.unit_id=item->>'unit_id'
         AND u.payload_byte_length=(item->>'payload_byte_length')::integer;
        SELECT COALESCE(sum(actual.media_bytes),0)::bigint,
               COALESCE(sum(actual.decoded_bytes),0)::bigint,
               count(*) FILTER (WHERE actual.media_bytes IS DISTINCT FROM
                   (item->>'media_byte_length')::bigint)::integer,
               count(*) FILTER (WHERE actual.decoded_bytes IS DISTINCT FROM
                   (item->>'decoded_byte_length')::bigint)::integer
        INTO media_bytes,decoded_bytes,media_cost_mismatches,
             decoded_cost_mismatches
        FROM jsonb_array_elements(commit_value->'units') item
        JOIN __forest_module_schema__.context_units u
          ON u.bot_id=requested_bot
         AND u.conversation_id=commit_value->>'conversation_id'
         AND u.unit_id=item->>'unit_id'
        CROSS JOIN LATERAL (
            SELECT COALESCE(sum(m.byte_length),0)::bigint AS media_bytes,
                   COALESCE(sum(m.decoded_pixels::bigint * 4),0)::bigint AS decoded_bytes
            FROM __forest_module_schema__.context_media_costs m
            WHERE m.bot_id=u.bot_id AND m.conversation_id=u.conversation_id
              AND m.event_id=u.event_id
        ) actual;
        IF unit_count<>(commit_value->>'archived_unit_count')::bigint
           OR payload_bytes<>(commit_value->>'archived_payload_bytes')::bigint
           OR media_cost_mismatches<>0
           OR media_bytes<>(commit_value->>'archived_media_bytes')::bigint
           OR decoded_cost_mismatches<>0
           OR decoded_bytes<>(commit_value->>'archived_decoded_bytes')::bigint
           OR lane.context_view_payload_bytes-payload_bytes <>
              (commit_value->>'resulting_view_payload_bytes')::bigint
           OR lane.context_view_media_bytes-media_bytes <>
              (commit_value->>'resulting_view_media_bytes')::bigint
           OR lane.context_view_decoded_bytes-decoded_bytes <>
              (commit_value->>'resulting_view_decoded_bytes')::bigint THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','CONTEXT_COMPACTION_COMMIT_INVALID',
                    'message','Context compaction membership is incomplete','status',422
                )
            );
        END IF;
        INSERT INTO __forest_module_schema__.context_compactions(
            bot_id,conversation_id,compaction_id,operation_id,input_digest,
            base_view_revision,published_view_revision,source_set_digest,policy_revision,
            archive_payload_id,archive_payload_digest,archived_unit_count,
            archived_payload_bytes,archived_media_bytes,archived_decoded_bytes,
            resulting_view_unit_count,resulting_view_payload_bytes,
            resulting_view_media_bytes,resulting_view_decoded_bytes,
            base_media_cost_revision,cumulative_archived_unit_count,
            first_context_revision,last_context_revision
        ) VALUES (
            requested_bot,commit_value->>'conversation_id',commit_value->>'compaction_id',
            commit_value->>'operation_id',commit_value->>'input_digest',
            (commit_value->>'base_view_revision')::bigint,
            (commit_value->>'published_view_revision')::bigint,
            commit_value->>'source_set_digest',commit_value->>'policy_revision',
            payload_ref->>'payload_id',payload_ref->>'digest',
            (commit_value->>'archived_unit_count')::bigint,
            (commit_value->>'archived_payload_bytes')::bigint,
            (commit_value->>'archived_media_bytes')::bigint,
            (commit_value->>'archived_decoded_bytes')::bigint,
            (commit_value->>'resulting_view_unit_count')::bigint,
            (commit_value->>'resulting_view_payload_bytes')::bigint,
            (commit_value->>'resulting_view_media_bytes')::bigint,
            (commit_value->>'resulting_view_decoded_bytes')::bigint,
            (commit_value->>'base_media_cost_revision')::bigint,
            (commit_value->>'cumulative_archived_unit_count')::bigint,
            (commit_value->>'first_context_revision')::bigint,
            (commit_value->>'last_context_revision')::bigint
        );
        INSERT INTO __forest_module_schema__.context_compaction_units(
            bot_id,conversation_id,compaction_id,archived_index,unit_id,
            context_revision,unit_index,payload_digest,author_payload_digest,
            media_byte_length,decoded_byte_length
        )
        SELECT requested_bot,commit_value->>'conversation_id',
               commit_value->>'compaction_id',u.ordinality-1,u.item->>'unit_id',
               (u.item->>'context_revision')::bigint,
               (u.item->>'unit_index')::integer,u.item->>'payload_digest',
               u.item->>'author_payload_digest',
               (u.item->>'media_byte_length')::bigint,
               (u.item->>'decoded_byte_length')::bigint
        FROM jsonb_array_elements(commit_value->'units')
             WITH ORDINALITY AS u(item,ordinality);
        DELETE FROM __forest_module_schema__.active_context_units a
        USING __forest_module_schema__.context_compaction_units cu
        WHERE cu.bot_id=requested_bot
          AND cu.conversation_id=commit_value->>'conversation_id'
          AND cu.compaction_id=commit_value->>'compaction_id'
          AND a.bot_id=cu.bot_id AND a.conversation_id=cu.conversation_id
          AND a.unit_id=cu.unit_id;
        GET DIAGNOSTICS unit_count = ROW_COUNT;
        IF unit_count<>(commit_value->>'archived_unit_count')::bigint THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','CONTEXT_ACTIVE_INDEX_INVALID',
                    'message','Active Context index differs from archive membership',
                    'status',409
                )
            );
        END IF;
        UPDATE __forest_module_schema__.conversation_lanes
        SET context_view_revision=(commit_value->>'published_view_revision')::bigint,
            context_view_unit_count=(commit_value->>'resulting_view_unit_count')::bigint,
            context_view_payload_bytes=
                (commit_value->>'resulting_view_payload_bytes')::bigint,
            context_view_media_bytes=
                (commit_value->>'resulting_view_media_bytes')::bigint,
            context_view_decoded_bytes=
                (commit_value->>'resulting_view_decoded_bytes')::bigint,
            context_archived_unit_count=
                (commit_value->>'cumulative_archived_unit_count')::bigint,
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot
          AND conversation_id=commit_value->>'conversation_id';
    ELSIF existing.input_digest IS DISTINCT FROM commit_value->>'input_digest'
       OR existing.compaction_id IS DISTINCT FROM commit_value->>'compaction_id'
       OR existing.archive_payload_id IS DISTINCT FROM payload_ref->>'payload_id'
       OR existing.archive_payload_digest IS DISTINCT FROM payload_ref->>'digest' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONTEXT_COMPACTION_OPERATION_CONFLICT',
                'message','Context compaction replay differs','status',409
            )
        );
    END IF;
    SELECT COALESCE(jsonb_agg(item->>'event_id' ORDER BY ordinality)
                    FILTER (WHERE item->>'event_id' IS NOT NULL),'[]'::jsonb)
    INTO event_refs
    FROM jsonb_array_elements(commit_value->'units') WITH ORDINALITY AS u(item,ordinality);
    IF pre_run THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'protocol','forest.context-pre-run-compaction/1',
                'status','COMPACTED',
                'conversation_id',commit_value->>'conversation_id',
                'context_view_revision',(commit_value->>'published_view_revision')::bigint,
                'memory_archive',jsonb_build_object(
                    'compaction_ref',commit_value->>'compaction_id',
                    'event_refs',event_refs
                )
            )
        );
    END IF;
    result_value := jsonb_set(
        selected,'{context_view_revision}',
        to_jsonb((commit_value->>'published_view_revision')::bigint),true
    ) || jsonb_build_object(
        'memory_archive',jsonb_build_object(
            'compaction_ref',commit_value->>'compaction_id',
            'event_refs',event_refs
        )
    );
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,'value',result_value
    );
END;
$$;

CREATE TABLE conversation_silent_drain_settlements (
    claim_token text PRIMARY KEY CHECK (octet_length(claim_token) BETWEEN 1 AND 256),
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER conversation_silent_drain_settlements_immutable
    BEFORE UPDATE OR DELETE ON conversation_silent_drain_settlements
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();

CREATE FUNCTION conversation_claim_silent_drain(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    lane record;
    next_epoch bigint;
    operation_id text;
    identity_digest text;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 3600 THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_CLAIM_INPUT_INVALID',
                'message','Conversation silent-drain claim is invalid','status',422
            )
        );
    END IF;
    SELECT * INTO lane
    FROM __forest_module_schema__.conversation_lanes
    WHERE bot_id=requested_bot AND actor_owner_id=requested_token
      AND actor_expires_at>clock_timestamp()
    FOR UPDATE;
    IF lane IS NULL THEN
        SELECT l.* INTO lane
        FROM __forest_module_schema__.conversation_lanes l
        WHERE l.bot_id=requested_bot
          AND l.active_session_ref IS NULL
          AND (l.resume_not_before IS NULL OR l.resume_not_before<=clock_timestamp())
          AND l.context_applied_seq<l.classified_through_seq
          AND (l.actor_owner_id IS NULL OR l.actor_expires_at<=clock_timestamp())
          AND NOT EXISTS (
              SELECT 1 FROM __forest_module_schema__.conversation_triggers t
              WHERE t.bot_id=l.bot_id AND t.conversation_id=l.conversation_id
                AND t.status='PENDING' AND t.ingress_seq<=l.classified_through_seq
          )
        ORDER BY l.updated_at,l.conversation_id
        FOR UPDATE SKIP LOCKED
        LIMIT 1;
        IF lane IS NULL THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',true,'value',NULL
            );
        END IF;
        next_epoch := lane.actor_epoch+1;
        UPDATE __forest_module_schema__.conversation_lanes
        SET actor_owner_id=requested_token,actor_epoch=next_epoch,
            actor_expires_at=clock_timestamp()+(requested_lease*interval '1 second'),
            resume_not_before=NULL,resume_reason_code=NULL,resume_reason_ref=NULL,
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND conversation_id=lane.conversation_id
        RETURNING * INTO lane;
    END IF;
    identity_digest := encode(sha256(convert_to(jsonb_build_array(
        requested_bot,lane.conversation_id,lane.actor_epoch
    )::text,'UTF8')),'hex');
    operation_id := 'conversation-silent:' || identity_digest;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.owned-work-claim/1',
            'work_id',lane.conversation_id,
            'claim_token',requested_token,
            'attempt',lane.actor_epoch,
            'operation_id',operation_id,
            'causal_root_kind','conversation.silent-drain',
            'causal_root_id',lane.conversation_id,
            'payload',jsonb_build_object(
                'actor',jsonb_build_object(
                    'conversation_id',lane.conversation_id,
                    'owner_id',requested_token,
                    'epoch',lane.actor_epoch
                ),
                'boundary','IDLE',
                'selection_mode','SILENT_ONLY',
                'session_ref','silent-session:' || identity_digest,
                'run_ref','silent-run:' || identity_digest,
                'next_run_ref',NULL
            )
        )
    );
END;
$$;

CREATE FUNCTION conversation_settle_silent_drain(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_work text := input->>'work_id';
    requested_token text := input->>'claim_token';
    requested_attempt bigint := (input->>'attempt')::bigint;
    existing record;
    lane record;
    result_value jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR requested_work IS NULL OR requested_token IS NULL
       OR requested_attempt IS NULL OR requested_attempt<1
       OR input#>>'{outcome,status}' NOT IN ('SUCCEEDED','FAILED') THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_SETTLEMENT_INPUT_INVALID',
                'message','Conversation silent-drain settlement is invalid','status',422
            )
        );
    END IF;
    SELECT * INTO existing
    FROM __forest_module_schema__.conversation_silent_drain_settlements
    WHERE claim_token=requested_token;
    IF existing IS NOT NULL THEN
        IF existing.input_payload=input THEN
            RETURN existing.result_payload;
        END IF;
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_SETTLEMENT_CONFLICT',
                'message','Conversation claim was settled with another outcome','status',409
            )
        );
    END IF;
    SELECT * INTO lane
    FROM __forest_module_schema__.conversation_lanes
    WHERE bot_id=requested_bot AND conversation_id=requested_work
    FOR UPDATE;
    IF lane IS NULL
       OR lane.actor_owner_id IS DISTINCT FROM requested_token
       OR lane.actor_epoch IS DISTINCT FROM requested_attempt THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','STALE_CONVERSATION_ACTOR',
                'message','Conversation actor no longer owns this lane','status',409
            )
        );
    END IF;
    UPDATE __forest_module_schema__.conversation_lanes
    SET actor_owner_id=NULL,actor_expires_at=NULL,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND conversation_id=requested_work;
    result_value := jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.owned-work-settled/1','work_id',requested_work
        )
    );
    INSERT INTO __forest_module_schema__.conversation_silent_drain_settlements(
        claim_token,input_payload,result_payload
    ) VALUES (requested_token,input,result_value);
    RETURN result_value;
END;
$$;

CREATE TABLE conversation_context_maintenance_settlements (
    claim_token text PRIMARY KEY CHECK (octet_length(claim_token) BETWEEN 1 AND 256),
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    settled_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER conversation_context_maintenance_settlements_immutable
    BEFORE UPDATE OR DELETE ON conversation_context_maintenance_settlements
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();

CREATE FUNCTION conversation_claim_context_maintenance(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_generation text := context->>'generation_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    lane record;
    next_epoch bigint;
    identity_digest text;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_bot IS NULL OR requested_generation IS NULL
       OR octet_length(requested_generation) NOT BETWEEN 1 AND 256
       OR requested_token IS NULL OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 3600 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','CONVERSATION_MAINTENANCE_CLAIM_INVALID',
                'message','Context maintenance claim is invalid','status',422));
    END IF;
    SELECT * INTO lane FROM __forest_module_schema__.conversation_lanes
    WHERE bot_id=requested_bot AND actor_owner_id=requested_token
      AND actor_expires_at>clock_timestamp() FOR UPDATE;
    IF lane IS NULL THEN
        SELECT l.* INTO lane FROM __forest_module_schema__.conversation_lanes l
        WHERE l.bot_id=requested_bot AND l.context_view_unit_count>1
          AND l.active_session_ref IS NULL
          AND l.context_applied_seq=l.classified_through_seq
          AND (l.actor_owner_id IS NULL OR l.actor_expires_at<=clock_timestamp())
          AND (l.maintenance_retry_at IS NULL
               OR l.maintenance_retry_at<=clock_timestamp())
          AND (l.maintenance_generation_id IS DISTINCT FROM requested_generation
               OR l.maintenance_context_revision IS DISTINCT FROM l.context_revision
               OR l.maintenance_view_revision IS DISTINCT FROM l.context_view_revision
               OR l.maintenance_media_cost_revision IS DISTINCT FROM l.media_cost_revision)
          AND NOT EXISTS (
              SELECT 1 FROM __forest_module_schema__.conversation_triggers t
              WHERE t.bot_id=l.bot_id AND t.conversation_id=l.conversation_id
                AND (t.status='PENDING' OR (t.status='CLAIMED' AND NOT EXISTS (
                    SELECT 1 FROM __forest_module_schema__.trigger_claim_groups g
                    WHERE g.bot_id=t.bot_id AND g.conversation_id=t.conversation_id
                      AND g.session_ref=t.claimed_session_ref
                      AND g.run_ref=t.claimed_run_ref AND g.claim_state='TERMINAL'
                )))
          )
        ORDER BY l.updated_at,l.conversation_id
        FOR UPDATE SKIP LOCKED LIMIT 1;
        IF lane IS NULL THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1',
                'ok',true,'value',NULL);
        END IF;
        next_epoch := lane.actor_epoch+1;
        UPDATE __forest_module_schema__.conversation_lanes
        SET actor_owner_id=requested_token,actor_epoch=next_epoch,
            actor_expires_at=clock_timestamp()+(requested_lease*interval '1 second'),
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND conversation_id=lane.conversation_id
        RETURNING * INTO lane;
    END IF;
    identity_digest := encode(sha256(convert_to(jsonb_build_array(
        requested_bot,lane.conversation_id,lane.actor_epoch
    )::text,'UTF8')),'hex');
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.owned-work-claim/1',
            'work_id',lane.conversation_id,'claim_token',requested_token,
            'attempt',lane.actor_epoch,
            'operation_id','conversation-maintenance:'||identity_digest,
            'causal_root_kind','conversation.context-maintenance',
            'causal_root_id',lane.conversation_id,
            'payload',jsonb_build_object('actor',jsonb_build_object(
                'conversation_id',lane.conversation_id,
                'owner_id',requested_token,'epoch',lane.actor_epoch
            ))
        ));
END;
$$;

CREATE FUNCTION conversation_settle_context_maintenance(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_generation text := context->>'generation_id';
    requested_work text := input->>'work_id';
    requested_token text := input->>'claim_token';
    requested_attempt bigint := (input->>'attempt')::bigint;
    outcome_status text := input#>>'{outcome,status}';
    maintenance_result jsonb := input#>'{outcome,result}';
    prior record;
    lane record;
    result_value jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR requested_bot IS NULL OR requested_generation IS NULL
       OR requested_work IS NULL OR requested_token IS NULL
       OR requested_attempt IS NULL OR requested_attempt<1
       OR (outcome_status IS DISTINCT FROM 'SUCCEEDED'
           AND outcome_status IS DISTINCT FROM 'FAILED') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','CONVERSATION_MAINTENANCE_SETTLEMENT_INVALID',
                'message','Context maintenance settlement is invalid','status',422));
    END IF;
    SELECT * INTO prior
    FROM __forest_module_schema__.conversation_context_maintenance_settlements
    WHERE claim_token=requested_token;
    IF prior IS NOT NULL THEN
        IF prior.input_payload=input THEN RETURN prior.result_payload; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','CONVERSATION_MAINTENANCE_SETTLEMENT_CONFLICT',
                'message','Context maintenance settled differently','status',409));
    END IF;
    SELECT * INTO lane FROM __forest_module_schema__.conversation_lanes
    WHERE bot_id=requested_bot AND conversation_id=requested_work FOR UPDATE;
    IF lane IS NULL OR lane.actor_owner_id IS DISTINCT FROM requested_token
       OR lane.actor_epoch IS DISTINCT FROM requested_attempt THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','STALE_CONVERSATION_ACTOR',
                'message','Context maintenance actor is stale','status',409));
    END IF;
    IF outcome_status='SUCCEEDED' THEN
        IF maintenance_result->>'protocol' IS DISTINCT FROM
                'forest.context-pre-run-compaction/1'
           OR maintenance_result->>'conversation_id' IS DISTINCT FROM requested_work
           OR (maintenance_result->>'status' IS DISTINCT FROM 'COMPACTED'
               AND maintenance_result->>'status' IS DISTINCT FROM 'SKIPPED')
           OR (maintenance_result->>'status'='COMPACTED' AND (
               lane.context_view_revision=0 OR
               maintenance_result->>'context_view_revision' IS DISTINCT FROM
                   lane.context_view_revision::text)) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','CONVERSATION_MAINTENANCE_RESULT_INVALID',
                    'message','Context maintenance result differs from the lane','status',409));
        END IF;
        UPDATE __forest_module_schema__.conversation_lanes
        SET actor_owner_id=NULL,actor_expires_at=NULL,
            maintenance_generation_id=requested_generation,
            maintenance_context_revision=lane.context_revision,
            maintenance_view_revision=lane.context_view_revision-
                CASE WHEN maintenance_result->>'status'='COMPACTED' THEN 1 ELSE 0 END,
            maintenance_media_cost_revision=lane.media_cost_revision,
            maintenance_retry_at=NULL,updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND conversation_id=requested_work;
    ELSE
        UPDATE __forest_module_schema__.conversation_lanes
        SET actor_owner_id=NULL,actor_expires_at=NULL,
            maintenance_retry_at=clock_timestamp()+interval '5 seconds',
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND conversation_id=requested_work;
    END IF;
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.owned-work-settled/1',
            'work_id',requested_work));
    INSERT INTO __forest_module_schema__.conversation_context_maintenance_settlements(
        claim_token,input_payload,result_payload
    ) VALUES(requested_token,input,result_value);
    RETURN result_value;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','CONVERSATION_MAINTENANCE_SETTLEMENT_INVALID',
            'message','Context maintenance settlement is invalid','status',422));
END;
$$;

CREATE FUNCTION conversation_answered_proposal_events(
    requested_bot text, requested_conversation text, requested_session text,
    requested_run text, requested_reply text, proposals jsonb
) RETURNS jsonb LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'trigger_ref',t.trigger_id,'event_ref',t.event_id
    ) ORDER BY t.trigger_id),'[]'::jsonb)
    FROM (
        SELECT DISTINCT item->>'trigger_ref' AS trigger_id
        FROM jsonb_array_elements(proposals) item
    ) requested
    JOIN __forest_module_schema__.conversation_triggers t
      ON t.trigger_id=requested.trigger_id
    JOIN __forest_module_schema__.trigger_claim_groups g
      ON g.bot_id=t.bot_id AND g.conversation_id=t.conversation_id
     AND g.session_ref=t.claimed_session_ref AND g.run_ref=t.claimed_run_ref
    JOIN __forest_module_schema__.context_append_operations a
      ON a.bot_id=g.bot_id AND a.operation_id=g.terminal_operation_id
     AND a.conversation_id=g.conversation_id
     AND a.session_ref=g.session_ref AND a.run_ref=g.run_ref
    WHERE t.bot_id=requested_bot AND t.conversation_id=requested_conversation
      AND t.claimed_session_ref=requested_session AND t.claimed_run_ref=requested_run
      AND t.status IN ('CLAIMED','ANSWERED')
      AND g.claim_state='TERMINAL' AND g.terminal_status='ANSWERED'
      AND a.append_kind='FINAL_REPLY' AND a.reply_ref=requested_reply
$$;

CREATE FUNCTION conversation_append_final_reply(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    actor jsonb := body->'actor';
    reply_value jsonb := body->'reply';
    agent_value jsonb := envelope#>'{prior,agent}';
    learning_proposals jsonb := agent_value#>'{learning,proposals}';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    requested_conversation text := actor->>'conversation_id';
    requested_session text := body->>'session_ref';
    requested_run text := body->>'run_ref';
    requested_reply text := reply_value->>'owner_id';
    requested_unit text := body->>'unit_id';
    requested_owner text := actor->>'owner_id';
    requested_epoch bigint := (actor->>'epoch')::bigint;
    lane record;
    replay record;
    claim_group record;
    context_revision_value bigint;
    claimed_trigger_count integer;
    answered_events_value jsonb;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'conversation.scheduler'
       OR requested_operation IS NULL OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR jsonb_typeof(learning_proposals) IS DISTINCT FROM 'array'
       OR jsonb_array_length(learning_proposals)>128
       OR reply_value->>'owner_component_id' IS DISTINCT FROM 'agent.runtime'
       OR reply_value->>'owner_kind' IS DISTINCT FROM 'final.reply'
       OR agent_value->>'run_id' IS DISTINCT FROM requested_run
       OR agent_value->>'session_id' IS DISTINCT FROM requested_session
       OR agent_value->>'conversation_ref' IS DISTINCT FROM requested_conversation
       OR agent_value->>'state' IS DISTINCT FROM 'COMPLETED'
       OR agent_value#>>'{final_candidate,payload_id}' IS DISTINCT FROM reply_value->>'payload_id'
       OR agent_value#>>'{final_candidate,digest}' IS DISTINCT FROM reply_value->>'digest' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_FINAL_REPLY_INVALID',
                'message','Final reply envelope differs from the completed Agent Run','status',422
            )
        );
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.conversation.scheduler/final-reply/' || requested_bot || '/' || requested_operation,0
    ));
    SELECT * INTO lane
    FROM __forest_module_schema__.conversation_lanes
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation FOR UPDATE;
    IF lane IS NULL
       OR lane.actor_owner_id IS DISTINCT FROM requested_owner
       OR lane.actor_epoch IS DISTINCT FROM requested_epoch
       OR lane.actor_expires_at IS NULL OR lane.actor_expires_at<=clock_timestamp()
       OR lane.active_session_ref IS DISTINCT FROM requested_session
       OR lane.active_run_ref IS DISTINCT FROM requested_run THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','STALE_CONVERSATION_ACTOR',
                'message','Conversation actor or Session fence is stale','status',409
            )
        );
    END IF;
    SELECT * INTO replay
    FROM __forest_module_schema__.context_append_operations
    WHERE bot_id=requested_bot AND operation_id=requested_operation;
    IF replay IS NOT NULL THEN
        IF replay.input_digest IS DISTINCT FROM requested_digest
           OR replay.reply_ref IS DISTINCT FROM requested_reply
           OR replay.run_ref IS DISTINCT FROM requested_run THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','CONTEXT_APPEND_CONFLICT',
                    'message','Final reply append replay differs','status',409
                )
            );
        END IF;
        answered_events_value := __forest_module_schema__.conversation_answered_proposal_events(
            requested_bot,requested_conversation,requested_session,requested_run,
            requested_reply,learning_proposals
        );
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'operation_id',requested_operation,'context_revision',replay.context_revision,
                'unit_id',requested_unit,'answered_trigger_count',replay.answered_trigger_count,
                'answered_events',answered_events_value,
                'reply_ref',requested_reply
            )
        );
    END IF;
    SELECT * INTO claim_group
    FROM __forest_module_schema__.trigger_claim_groups
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
      AND session_ref=requested_session AND run_ref=requested_run;
    IF claim_group IS NULL OR claim_group.claim_state<>'ACTIVE' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','TRIGGER_CLAIM_GROUP_CONFLICT',
                'message','Final reply has no active claim group','status',409
            )
        );
    END IF;
    claimed_trigger_count := claim_group.trigger_count;
    context_revision_value := lane.context_revision+1;
    INSERT INTO __forest_module_schema__.context_append_operations(
        bot_id,operation_id,conversation_id,input_digest,append_kind,
        session_ref,run_ref,context_revision,unit_count,answered_trigger_count,
        reply_ref,actor_owner_id,actor_epoch
    ) VALUES (
        requested_bot,requested_operation,requested_conversation,requested_digest,
        'FINAL_REPLY',requested_session,requested_run,context_revision_value,1,
        claimed_trigger_count,requested_reply,requested_owner,requested_epoch
    );
    INSERT INTO __forest_module_schema__.context_units(
        bot_id,conversation_id,unit_id,context_revision,unit_index,kind,
        source_component_id,source_kind,source_id,input_batch_id,event_id,ingress_seq,
            payload_id,payload_digest,payload_byte_length,payload_owner_component_id,payload_owner_kind,
        payload_owner_id,author_payload_id,author_payload_digest,
        author_owner_component_id,author_owner_kind,author_owner_id,
        author_subject_ref,trigger_decision
    ) VALUES (
        requested_bot,requested_conversation,requested_unit,context_revision_value,0,'REPLY',
        'agent.runtime','final.reply',requested_reply,NULL,NULL,NULL,
        reply_value->>'payload_id',reply_value->>'digest',
        (body->>'reply_byte_length')::integer,
        reply_value->>'owner_component_id',reply_value->>'owner_kind',
        reply_value->>'owner_id',NULL,NULL,NULL,NULL,NULL,NULL,NULL
    );
    INSERT INTO __forest_module_schema__.active_context_units(
        bot_id,conversation_id,unit_id,context_revision,unit_index
    ) VALUES(requested_bot,requested_conversation,requested_unit,context_revision_value,0);
    UPDATE __forest_module_schema__.trigger_claim_groups
    SET claim_state='TERMINAL',terminal_operation_id=requested_operation,
        terminal_status='ANSWERED'
    WHERE bot_id=requested_bot AND claim_ref=claim_group.claim_ref;
    UPDATE __forest_module_schema__.conversation_requeue_requests
    SET state='REJECTED',disposition_code='RUN_ANSWERED',settled_at=clock_timestamp()
    WHERE bot_id=requested_bot AND claim_ref=claim_group.claim_ref AND state='PENDING';
    INSERT INTO __forest_module_schema__.trigger_settlement_progress(bot_id,claim_ref)
    VALUES(requested_bot,claim_group.claim_ref);
    UPDATE __forest_module_schema__.conversation_lanes
    SET context_revision=context_revision_value,
        context_view_unit_count=context_view_unit_count+1,
        context_view_payload_bytes=context_view_payload_bytes+
            (body->>'reply_byte_length')::integer,
        mailbox_revision=mailbox_revision+1,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation;
    answered_events_value := __forest_module_schema__.conversation_answered_proposal_events(
        requested_bot,requested_conversation,requested_session,requested_run,
        requested_reply,learning_proposals
    );
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'operation_id',requested_operation,'context_revision',context_revision_value,
            'unit_id',requested_unit,'answered_trigger_count',claimed_trigger_count,
            'answered_events',answered_events_value,
            'reply_ref',requested_reply
        )
    );
END;
$$;

CREATE FUNCTION conversation_terminalize_run(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    agent_proof jsonb := envelope#>'{prior,agent}';
    actor jsonb := body->'actor';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    requested_conversation text := actor->>'conversation_id';
    requested_owner text := actor->>'owner_id';
    requested_epoch bigint := (actor->>'epoch')::bigint;
    requested_session text := body->>'session_ref';
    requested_run text := body->>'run_ref';
    requested_status text := body->>'terminal_status';
    requested_reason text := body->>'reason_code';
    trigger_count_value integer;
    lane record;
    replay record;
    claim_group record;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'conversation.scheduler'
       OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR requested_conversation IS NULL OR requested_owner IS NULL
       OR requested_epoch IS NULL OR requested_epoch<1
       OR requested_session IS NULL OR requested_run IS NULL
       OR requested_status NOT IN ('FAILED','CANCELLED')
       OR requested_reason IS NULL OR octet_length(requested_reason) NOT BETWEEN 1 AND 128
       OR agent_proof->>'protocol' IS DISTINCT FROM 'forest.agent-terminal-proof/1'
       OR agent_proof->>'conversation_id' IS DISTINCT FROM requested_conversation
       OR agent_proof->>'session_ref' IS DISTINCT FROM requested_session
       OR agent_proof->>'run_ref' IS DISTINCT FROM requested_run
       OR agent_proof->>'terminal_status' IS DISTINCT FROM requested_status
       OR agent_proof->>'reason_code' IS DISTINCT FROM requested_reason THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_TERMINAL_INPUT_INVALID',
                'message','Conversation terminal Run envelope is invalid','status',422
            )
        );
    END IF;
    SELECT * INTO replay
    FROM __forest_module_schema__.trigger_terminal_operations
    WHERE bot_id=requested_bot AND operation_id=requested_operation;
    IF replay IS NOT NULL THEN
        IF replay.input_digest IS DISTINCT FROM requested_digest
           OR replay.conversation_id IS DISTINCT FROM requested_conversation
           OR replay.terminal_session_ref IS DISTINCT FROM requested_session
           OR replay.terminal_run_ref IS DISTINCT FROM requested_run
           OR replay.resulting_status IS DISTINCT FROM requested_status
           OR replay.reason_code IS DISTINCT FROM requested_reason
           OR replay.actor_owner_id IS DISTINCT FROM requested_owner
           OR replay.actor_epoch IS DISTINCT FROM requested_epoch THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','CONVERSATION_TERMINAL_OPERATION_CONFLICT',
                    'message','Conversation terminal operation differs','status',409
                )
            );
        END IF;
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'protocol','forest.conversation-run-terminal/1',
                'conversation_id',requested_conversation,'session_ref',requested_session,
                'run_ref',requested_run,'terminal_status',requested_status,
                'reason_code',requested_reason,'trigger_count',replay.trigger_count
            )
        );
    END IF;
    SELECT * INTO lane
    FROM __forest_module_schema__.conversation_lanes
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation FOR UPDATE;
    IF lane IS NULL OR lane.actor_owner_id IS DISTINCT FROM requested_owner
       OR lane.actor_epoch IS DISTINCT FROM requested_epoch
       OR lane.active_session_ref IS DISTINCT FROM requested_session
       OR lane.active_run_ref IS DISTINCT FROM requested_run THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','STALE_CONVERSATION_ACTOR',
                'message','Conversation actor no longer owns the terminal Run','status',409
            )
        );
    END IF;
    SELECT * INTO claim_group
    FROM __forest_module_schema__.trigger_claim_groups
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
      AND session_ref=requested_session AND run_ref=requested_run;
    IF claim_group IS NULL OR claim_group.claim_state<>'ACTIVE' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','TRIGGER_CLAIM_GROUP_CONFLICT',
                'message','Terminal Run has no active claim group','status',409
            )
        );
    END IF;
    trigger_count_value := claim_group.trigger_count;
    INSERT INTO __forest_module_schema__.trigger_terminal_operations(
        bot_id,operation_id,conversation_id,input_digest,terminal_session_ref,
        terminal_run_ref,resulting_status,reason_code,actor_owner_id,actor_epoch,
        trigger_count
    ) VALUES (
        requested_bot,requested_operation,requested_conversation,requested_digest,
        requested_session,requested_run,requested_status,requested_reason,
        requested_owner,requested_epoch,trigger_count_value
    );
    UPDATE __forest_module_schema__.trigger_claim_groups
    SET claim_state='TERMINAL',terminal_operation_id=requested_operation,
        terminal_status=requested_status
    WHERE bot_id=requested_bot AND claim_ref=claim_group.claim_ref;
    UPDATE __forest_module_schema__.conversation_requeue_requests
    SET state='REJECTED',disposition_code='RUN_TERMINALIZED',settled_at=clock_timestamp()
    WHERE bot_id=requested_bot AND claim_ref=claim_group.claim_ref AND state='PENDING';
    INSERT INTO __forest_module_schema__.trigger_settlement_progress(bot_id,claim_ref)
    VALUES(requested_bot,claim_group.claim_ref);
    UPDATE __forest_module_schema__.conversation_lanes
    SET active_session_ref=NULL,active_run_ref=NULL,resume_not_before=NULL,
        resume_reason_code=NULL,resume_reason_ref=NULL,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.conversation-run-terminal/1',
            'conversation_id',requested_conversation,'session_ref',requested_session,
            'run_ref',requested_run,'terminal_status',requested_status,
            'reason_code',requested_reason,'trigger_count',trigger_count_value
        )
    );
END;
$$;

CREATE FUNCTION conversation_request_requeue(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_id text := context->>'operation_id';
    requested_conversation text := input->>'conversation_id';
    requested_run text := input->>'run_ref';
    requested_reason text := input->>'reason_code';
    requested_by text := input->>'requested_by';
    requested_digest text := 'sha256:' ||
        encode(sha256(convert_to(input::text,'UTF8')),'hex');
    lane record;
    claim_group record;
    existing record;
BEGIN
    IF requested_id IS NULL OR octet_length(requested_id) NOT BETWEEN 1 AND 256
       OR requested_conversation IS NULL
       OR octet_length(requested_conversation) NOT BETWEEN 1 AND 256
       OR requested_run IS NULL OR octet_length(requested_run) NOT BETWEEN 1 AND 256
       OR requested_reason IS NULL OR octet_length(requested_reason) NOT BETWEEN 1 AND 128
       OR requested_by IS NULL OR octet_length(requested_by) NOT BETWEEN 1 AND 256 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','CONVERSATION_REQUEUE_REQUEST_INVALID',
                'message','Requeue request is invalid','status',422));
    END IF;
    SELECT * INTO existing FROM __forest_module_schema__.conversation_requeue_requests
    WHERE bot_id=requested_bot AND request_id=requested_id;
    IF existing.request_id IS NOT NULL THEN
        IF existing.input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','CONVERSATION_REQUEUE_REQUEST_CONFLICT',
                    'message','Requeue request differs','status',409));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('protocol','forest.conversation-requeue-requested/1',
                'request_id',requested_id));
    END IF;
    SELECT * INTO lane FROM __forest_module_schema__.conversation_lanes
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation FOR SHARE;
    SELECT * INTO claim_group FROM __forest_module_schema__.trigger_claim_groups
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
      AND run_ref=requested_run FOR SHARE;
    IF lane.conversation_id IS NULL OR claim_group.claim_ref IS NULL
       OR claim_group.claim_state<>'ACTIVE'
       OR (
           (lane.active_run_ref=requested_run
            AND lane.active_session_ref=claim_group.session_ref)
           OR (lane.active_run_ref IS NULL AND lane.active_session_ref IS NULL
               AND NOT EXISTS (
                   SELECT 1 FROM __forest_module_schema__.trigger_claim_groups other
                   WHERE other.bot_id=requested_bot
                     AND other.conversation_id=requested_conversation
                     AND other.claim_state='ACTIVE'
                     AND other.claim_ref<>claim_group.claim_ref
               ))
       ) IS NOT TRUE THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','CONVERSATION_REQUEUE_TARGET_STALE',
                'message','Run has no active Conversation claim','status',409));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.conversation.scheduler/requeue-request/' || requested_bot || '/' ||
        claim_group.claim_ref,0));
    SELECT * INTO existing FROM __forest_module_schema__.conversation_requeue_requests
    WHERE bot_id=requested_bot AND request_id=requested_id;
    IF existing.request_id IS NOT NULL THEN
        IF existing.input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','CONVERSATION_REQUEUE_REQUEST_CONFLICT',
                    'message','Requeue request differs','status',409));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('protocol','forest.conversation-requeue-requested/1',
                'request_id',requested_id));
    END IF;
    IF EXISTS (SELECT 1 FROM __forest_module_schema__.conversation_requeue_requests r
               WHERE r.bot_id=requested_bot AND r.claim_ref=claim_group.claim_ref) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','CONVERSATION_REQUEUE_ALREADY_REQUESTED',
                'message','Run already has a requeue request','status',409));
    END IF;
    INSERT INTO __forest_module_schema__.conversation_requeue_requests(
        bot_id,request_id,input_digest,conversation_id,session_ref,run_ref,
        claim_ref,claim_digest,reason_code,requested_by
    ) VALUES (
        requested_bot,requested_id,requested_digest,requested_conversation,
        claim_group.session_ref,requested_run,claim_group.claim_ref,
        claim_group.claim_digest,requested_reason,requested_by
    );
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.conversation-requeue-requested/1',
            'request_id',requested_id));
END;
$$;

CREATE FUNCTION conversation_read_requeue_request(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_conversation text := input->>'conversation_id';
    requested_run text := input->>'run_ref';
    request_row record;
BEGIN
    IF requested_conversation IS NULL OR requested_run IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','CONVERSATION_REQUEUE_QUERY_INVALID',
                'message','Requeue request query is invalid','status',422));
    END IF;
    SELECT * INTO request_row
    FROM __forest_module_schema__.conversation_requeue_requests
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
      AND run_ref=requested_run;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.conversation-requeue-request/1',
            'state',CASE WHEN request_row IS NULL THEN 'NONE' ELSE request_row.state END,
            'request_id',request_row.request_id,
            'conversation_id',requested_conversation,
            'session_ref',request_row.session_ref,'run_ref',requested_run,
            'claim_ref',request_row.claim_ref,'claim_digest',request_row.claim_digest,
            'reason_code',request_row.reason_code,'requested_by',request_row.requested_by,
            'disposition_code',request_row.disposition_code
        ));
END;
$$;

CREATE FUNCTION conversation_list_requeue_candidates(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_conversation text := input->>'conversation_id';
    after_conversation text := input->>'after_conversation_id';
    requested_limit integer := (input->>'limit')::integer;
    items_value jsonb;
    has_more_value boolean;
    next_cursor_value text;
BEGIN
    IF requested_limit IS NULL OR requested_limit NOT BETWEEN 1 AND 50
       OR (requested_conversation IS NOT NULL
           AND octet_length(requested_conversation) NOT BETWEEN 1 AND 256)
       OR (after_conversation IS NOT NULL
           AND octet_length(after_conversation) NOT BETWEEN 1 AND 256) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','CONVERSATION_REQUEUE_LIST_INVALID',
                'message','Requeue candidate filter is invalid','status',422));
    END IF;
    WITH page AS (
        SELECT l.conversation_id,l.channel_kind,l.channel_ref,
               g.session_ref,g.run_ref,g.claim_ref,g.trigger_count,
               r.request_id,r.state AS request_state,r.disposition_code,
               row_number() OVER (ORDER BY l.conversation_id) AS rn
        FROM __forest_module_schema__.conversation_lanes l
        JOIN __forest_module_schema__.trigger_claim_groups g
          ON g.bot_id=l.bot_id AND g.conversation_id=l.conversation_id
         AND g.claim_state='ACTIVE'
         AND (
             (g.session_ref=l.active_session_ref AND g.run_ref=l.active_run_ref)
             OR (l.active_session_ref IS NULL AND l.active_run_ref IS NULL
                 AND NOT EXISTS (
                     SELECT 1 FROM __forest_module_schema__.trigger_claim_groups other
                     WHERE other.bot_id=g.bot_id AND other.conversation_id=g.conversation_id
                       AND other.claim_state='ACTIVE' AND other.claim_ref<>g.claim_ref
                 ))
         )
        LEFT JOIN __forest_module_schema__.conversation_requeue_requests r
          ON r.bot_id=g.bot_id AND r.claim_ref=g.claim_ref
        WHERE l.bot_id=requested_bot
          AND (requested_conversation IS NULL OR l.conversation_id=requested_conversation)
          AND (after_conversation IS NULL OR l.conversation_id>after_conversation)
        ORDER BY l.conversation_id LIMIT requested_limit+1
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'conversation_id',conversation_id,'channel_kind',channel_kind,
               'channel_ref',channel_ref,'session_ref',session_ref,'run_ref',run_ref,
               'claim_ref',claim_ref,'trigger_count',trigger_count,
               'request_id',request_id,'request_state',request_state,
               'disposition_code',disposition_code
           ) ORDER BY conversation_id) FILTER (WHERE rn<=requested_limit),'[]'::jsonb),
           COALESCE(bool_or(rn>requested_limit),false),
           max(conversation_id) FILTER (WHERE rn<=requested_limit)
    INTO items_value,has_more_value,next_cursor_value FROM page;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.conversation-requeue-candidates/1',
            'items',items_value,'has_more',has_more_value,
            'next_cursor',CASE WHEN has_more_value THEN next_cursor_value ELSE NULL END
        ));
END;
$$;

CREATE FUNCTION conversation_list_claim_anomalies(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_conversation text := input->>'conversation_id';
    after_conversation text := input->>'after_conversation_id';
    after_claim text := input->>'after_claim_ref';
    requested_limit integer := (input->>'limit')::integer;
    items_value jsonb;
    has_more_value boolean;
    next_conversation text;
    next_claim text;
BEGIN
    IF requested_limit IS NULL OR requested_limit NOT BETWEEN 1 AND 50
       OR (requested_conversation IS NOT NULL
           AND octet_length(requested_conversation) NOT BETWEEN 1 AND 256)
       OR (after_conversation IS NULL) IS DISTINCT FROM (after_claim IS NULL)
       OR (after_conversation IS NOT NULL
           AND octet_length(after_conversation) NOT BETWEEN 1 AND 256)
       OR (after_claim IS NOT NULL AND after_claim !~ '^claim:[a-f0-9]{64}$') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','CONVERSATION_CLAIM_ANOMALY_LIST_INVALID',
                'message','Claim anomaly filter is invalid','status',422));
    END IF;
    WITH page AS (
        SELECT g.conversation_id,g.claim_ref,g.session_ref,g.run_ref,
               l.channel_kind,l.channel_ref,
               l.active_session_ref AS lane_session_ref,
               l.active_run_ref AS lane_run_ref,
               CASE WHEN l.active_session_ref IS NULL AND l.active_run_ref IS NULL
                    THEN 'MULTIPLE_ACTIVE_GROUPS'
                    ELSE 'LANE_CLAIM_MISMATCH' END AS anomaly_code
        FROM __forest_module_schema__.trigger_claim_groups g
        JOIN __forest_module_schema__.conversation_lanes l
          ON l.bot_id=g.bot_id AND l.conversation_id=g.conversation_id
        WHERE g.bot_id=requested_bot AND g.claim_state='ACTIVE'
          AND (requested_conversation IS NULL
               OR g.conversation_id=requested_conversation)
          AND (after_conversation IS NULL
               OR (g.conversation_id,g.claim_ref)>(after_conversation,after_claim))
          AND (
              (l.active_session_ref=g.session_ref AND l.active_run_ref=g.run_ref)
              OR (l.active_session_ref IS NULL AND l.active_run_ref IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM __forest_module_schema__.trigger_claim_groups other
                      WHERE other.bot_id=g.bot_id
                        AND other.conversation_id=g.conversation_id
                        AND other.claim_state='ACTIVE'
                        AND other.claim_ref<>g.claim_ref
                  ))
          ) IS NOT TRUE
        ORDER BY g.conversation_id,g.claim_ref LIMIT requested_limit+1
    ), numbered AS (
        SELECT *,row_number() OVER (ORDER BY conversation_id,claim_ref) AS rn
        FROM page
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'conversation_id',conversation_id,'channel_kind',channel_kind,
               'channel_ref',channel_ref,'claim_ref',claim_ref,
               'session_ref',session_ref,'run_ref',run_ref,
               'lane_session_ref',lane_session_ref,'lane_run_ref',lane_run_ref,
               'anomaly_code',anomaly_code
           ) ORDER BY conversation_id,claim_ref)
           FILTER (WHERE rn<=requested_limit),'[]'::jsonb),
           COALESCE(bool_or(rn>requested_limit),false),
           max(conversation_id) FILTER (WHERE rn=requested_limit),
           max(claim_ref) FILTER (WHERE rn=requested_limit)
    INTO items_value,has_more_value,next_conversation,next_claim FROM numbered;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.conversation-claim-anomalies/1',
            'items',items_value,'has_more',has_more_value,
            'next_cursor',CASE WHEN has_more_value THEN jsonb_build_object(
                'conversation_id',next_conversation,'claim_ref',next_claim
            ) ELSE NULL END
        ));
END;
$$;

CREATE FUNCTION conversation_requeue_run(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    actor jsonb := body->'actor';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    requested_conversation text := actor->>'conversation_id';
    requested_owner text := actor->>'owner_id';
    requested_epoch bigint := (actor->>'epoch')::bigint;
    requested_session text := body->>'session_ref';
    requested_run text := body->>'run_ref';
    requested_request text := body->>'request_id';
    requested_ref text := body->>'claim_ref';
    requested_claim_digest text := body->>'claim_digest';
    requested_reason text := body->>'reason_code';
    lane record;
    claim_group record;
    request_row record;
    replay record;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'conversation.scheduler'
       OR body->>'protocol' IS DISTINCT FROM 'forest.conversation-run-requeue/1'
       OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR requested_conversation IS NULL OR requested_owner IS NULL
       OR requested_epoch IS NULL OR requested_epoch<1
       OR requested_session IS NULL OR requested_run IS NULL
       OR requested_request IS NULL OR octet_length(requested_request) NOT BETWEEN 1 AND 256
       OR requested_ref IS NULL OR requested_ref !~ '^claim:[a-f0-9]{64}$'
       OR requested_claim_digest IS NULL
       OR requested_claim_digest !~ '^sha256:[a-f0-9]{64}$'
       OR requested_reason IS NULL
       OR octet_length(requested_reason) NOT BETWEEN 1 AND 128 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','CONVERSATION_REQUEUE_INPUT_INVALID',
                'message','Run requeue input is invalid','status',422));
    END IF;
    SELECT * INTO replay
    FROM __forest_module_schema__.trigger_requeue_operations
    WHERE bot_id=requested_bot AND operation_id=requested_operation;
    IF replay IS NOT NULL THEN
        IF ROW(replay.input_digest,replay.request_id,replay.conversation_id,replay.claim_ref,
               replay.claim_digest,replay.abandoned_session_ref,
               replay.abandoned_run_ref,replay.reason_code,replay.actor_owner_id,
               replay.actor_epoch)
           IS DISTINCT FROM ROW(requested_digest,requested_request,requested_conversation,
               requested_ref,requested_claim_digest,requested_session,
               requested_run,requested_reason,requested_owner,requested_epoch) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','CONVERSATION_REQUEUE_OPERATION_CONFLICT',
                    'message','Run requeue operation differs','status',409));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'protocol','forest.conversation-run-requeued/1',
                'conversation_id',requested_conversation,
                'session_ref',requested_session,'run_ref',requested_run,
                'claim_ref',requested_ref,'trigger_count',replay.trigger_count
            ));
    END IF;
    SELECT * INTO lane
    FROM __forest_module_schema__.conversation_lanes
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
    FOR UPDATE;
    IF lane.conversation_id IS NULL OR lane.actor_owner_id IS DISTINCT FROM requested_owner
       OR lane.actor_epoch IS DISTINCT FROM requested_epoch
       OR lane.actor_expires_at IS NULL
       OR lane.actor_expires_at<=clock_timestamp()
       OR (
           (lane.active_session_ref=requested_session
            AND lane.active_run_ref=requested_run)
           OR (lane.active_session_ref IS NULL AND lane.active_run_ref IS NULL
               AND NOT EXISTS (
                   SELECT 1 FROM __forest_module_schema__.trigger_claim_groups other
                   WHERE other.bot_id=requested_bot
                     AND other.conversation_id=requested_conversation
                     AND other.claim_state='ACTIVE'
                     AND other.claim_ref<>requested_ref
               ))
       ) IS NOT TRUE THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','STALE_CONVERSATION_ACTOR',
                'message','Actor no longer owns the requeued Run','status',409));
    END IF;
    SELECT * INTO claim_group
    FROM __forest_module_schema__.trigger_claim_groups
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
      AND session_ref=requested_session AND run_ref=requested_run FOR UPDATE;
    IF claim_group.claim_ref IS NULL OR claim_group.claim_state<>'ACTIVE'
       OR claim_group.claim_ref IS DISTINCT FROM requested_ref
       OR claim_group.claim_digest IS DISTINCT FROM requested_claim_digest THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','TRIGGER_CLAIM_GROUP_CONFLICT',
                'message','Run requeue claim differs from the active group','status',409));
    END IF;
    SELECT * INTO request_row
    FROM __forest_module_schema__.conversation_requeue_requests
    WHERE bot_id=requested_bot AND request_id=requested_request FOR UPDATE;
    IF request_row IS NULL OR request_row.state<>'PENDING'
       OR request_row.conversation_id IS DISTINCT FROM requested_conversation
       OR request_row.session_ref IS DISTINCT FROM requested_session
       OR request_row.run_ref IS DISTINCT FROM requested_run
       OR request_row.claim_ref IS DISTINCT FROM requested_ref
       OR request_row.claim_digest IS DISTINCT FROM requested_claim_digest
       OR request_row.reason_code IS DISTINCT FROM requested_reason THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','CONVERSATION_REQUEUE_REQUEST_STALE',
                'message','Requeue request no longer owns this claim','status',409));
    END IF;
    INSERT INTO __forest_module_schema__.trigger_requeue_operations(
        bot_id,operation_id,request_id,conversation_id,claim_ref,claim_digest,input_digest,
        abandoned_session_ref,abandoned_run_ref,reason_code,actor_owner_id,
        actor_epoch,trigger_count
    ) VALUES (
        requested_bot,requested_operation,requested_request,requested_conversation,requested_ref,
        requested_claim_digest,requested_digest,requested_session,requested_run,
        requested_reason,requested_owner,requested_epoch,claim_group.trigger_count
    );
    UPDATE __forest_module_schema__.trigger_claim_groups
    SET claim_state='TERMINAL',terminal_operation_id=requested_operation,
        terminal_status='REQUEUED'
    WHERE bot_id=requested_bot AND claim_ref=requested_ref;
    UPDATE __forest_module_schema__.conversation_requeue_requests
    SET state='APPLIED',disposition_code='REQUEUED',settled_at=clock_timestamp()
    WHERE bot_id=requested_bot AND request_id=requested_request;
    INSERT INTO __forest_module_schema__.trigger_settlement_progress(bot_id,claim_ref)
    VALUES(requested_bot,requested_ref);
    UPDATE __forest_module_schema__.conversation_lanes
    SET active_session_ref=NULL,active_run_ref=NULL,resume_not_before=NULL,
        resume_reason_code=NULL,resume_reason_ref=NULL,
        mailbox_revision=mailbox_revision+1,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.conversation-run-requeued/1',
            'conversation_id',requested_conversation,
            'session_ref',requested_session,'run_ref',requested_run,
            'claim_ref',requested_ref,'trigger_count',claim_group.trigger_count
        ));
END;
$$;

CREATE FUNCTION conversation_settle_trigger_segment(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    requested_bot text := context->>'bot_id';
    requested_ref text := body->>'claim_ref';
    requested_index integer := (body->>'segment_index')::integer;
    requested_token text := body->>'claim_token';
    requested_epoch integer := (body->>'lease_epoch')::integer;
    progress record;
    claim_group record;
    segment record;
    receipt record;
    reply_ref_value text;
    reason_value text;
    changed_count integer;
    result_value jsonb;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'conversation.scheduler'
       OR body->>'protocol' IS DISTINCT FROM 'forest.trigger-segment-settle/1'
       OR requested_ref IS NULL OR requested_ref !~ '^claim:[a-f0-9]{64}$'
       OR requested_index IS NULL OR requested_index<0
       OR requested_token IS NULL OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_epoch IS NULL OR requested_epoch<1 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','TRIGGER_SETTLEMENT_INVALID',
                'message','Trigger segment settlement input is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.conversation.scheduler/trigger-settle/' || requested_bot || '/' ||
        requested_ref || '/' || requested_index::text,0
    ));
    SELECT * INTO receipt
    FROM __forest_module_schema__.trigger_segment_settlements
    WHERE bot_id=requested_bot AND claim_ref=requested_ref
      AND segment_index=requested_index;
    IF receipt IS NOT NULL THEN
        IF receipt.claim_token IS DISTINCT FROM requested_token
           OR receipt.lease_epoch IS DISTINCT FROM requested_epoch THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','TRIGGER_SETTLEMENT_CONFLICT',
                    'message','Trigger segment was settled by another lease','status',409));
        END IF;
        SELECT g.segment_count INTO claim_group
        FROM __forest_module_schema__.trigger_claim_groups g
        WHERE g.bot_id=requested_bot AND g.claim_ref=requested_ref;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('protocol','forest.trigger-segment-settled/1',
                'claim_ref',requested_ref,'segment_index',requested_index,
                'settled_count',receipt.settled_count,
                'remaining_segments',claim_group.segment_count-requested_index-1));
    END IF;
    SELECT * INTO progress
    FROM __forest_module_schema__.trigger_settlement_progress
    WHERE bot_id=requested_bot AND claim_ref=requested_ref FOR UPDATE;
    IF progress IS NULL OR progress.next_segment_index IS DISTINCT FROM requested_index
       OR progress.lease_token IS DISTINCT FROM requested_token
       OR progress.lease_epoch IS DISTINCT FROM requested_epoch
       OR progress.lease_expires_at<=clock_timestamp() THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','STALE_TRIGGER_SETTLEMENT_LEASE',
                'message','Trigger segment lease is no longer current','status',409));
    END IF;
    SELECT * INTO claim_group
    FROM __forest_module_schema__.trigger_claim_groups
    WHERE bot_id=requested_bot AND claim_ref=requested_ref;
    SELECT * INTO segment
    FROM __forest_module_schema__.trigger_claim_segments
    WHERE bot_id=requested_bot AND claim_ref=requested_ref
      AND segment_index=requested_index;
    IF claim_group IS NULL OR claim_group.claim_state<>'TERMINAL'
       OR segment IS NULL OR requested_index>=claim_group.segment_count THEN
        RAISE EXCEPTION 'Trigger settlement cursor has no terminal segment'
            USING ERRCODE='55000';
    END IF;
    IF claim_group.terminal_status='ANSWERED' THEN
        SELECT reply_ref INTO reply_ref_value
        FROM __forest_module_schema__.context_append_operations
        WHERE bot_id=requested_bot
          AND operation_id=claim_group.terminal_operation_id
          AND conversation_id=claim_group.conversation_id
          AND session_ref=claim_group.session_ref AND run_ref=claim_group.run_ref
          AND append_kind='FINAL_REPLY';
        IF reply_ref_value IS NULL THEN
            RAISE EXCEPTION 'Answered trigger group has no final reply fact'
                USING ERRCODE='55000';
        END IF;
    ELSIF claim_group.terminal_status IN ('FAILED','CANCELLED') THEN
        SELECT reason_code INTO reason_value
        FROM __forest_module_schema__.trigger_terminal_operations
        WHERE bot_id=requested_bot
          AND operation_id=claim_group.terminal_operation_id
          AND resulting_status=claim_group.terminal_status;
        IF reason_value IS NULL THEN
            RAISE EXCEPTION 'Terminal trigger group has no reason fact'
                USING ERRCODE='55000';
        END IF;
    ELSIF claim_group.terminal_status='REQUEUED' THEN
        PERFORM 1 FROM __forest_module_schema__.trigger_requeue_operations r
        WHERE r.bot_id=requested_bot
          AND r.claim_ref=requested_ref
          AND r.operation_id=claim_group.terminal_operation_id
          AND r.claim_digest=claim_group.claim_digest;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Requeued trigger group has no disposition fact'
                USING ERRCODE='55000';
        END IF;
    ELSE
        RAISE EXCEPTION 'Trigger group has an unknown terminal status'
            USING ERRCODE='55000';
    END IF;
    UPDATE __forest_module_schema__.conversation_triggers t
    SET status=CASE WHEN claim_group.terminal_status='REQUEUED'
                    THEN 'PENDING' ELSE claim_group.terminal_status END,
        claimed_session_ref=CASE WHEN claim_group.terminal_status='REQUEUED'
                    THEN NULL ELSE t.claimed_session_ref END,
        claimed_run_ref=CASE WHEN claim_group.terminal_status='REQUEUED'
                    THEN NULL ELSE t.claimed_run_ref END,
        input_batch_id=CASE WHEN claim_group.terminal_status='REQUEUED'
                    THEN NULL ELSE t.input_batch_id END,
        answered_reply_ref=reply_ref_value,terminal_reason_code=reason_value,
        updated_at=clock_timestamp()
    FROM __forest_module_schema__.input_batch_triggers m
    WHERE m.bot_id=requested_bot AND m.input_batch_id=segment.input_batch_id
      AND t.bot_id=m.bot_id AND t.trigger_id=m.trigger_id
      AND t.conversation_id=claim_group.conversation_id
      AND t.claimed_session_ref=claim_group.session_ref
      AND t.claimed_run_ref=claim_group.run_ref
      AND t.input_batch_id=segment.input_batch_id AND t.status='CLAIMED';
    GET DIAGNOSTICS changed_count = ROW_COUNT;
    IF changed_count<>segment.trigger_count THEN
        RAISE EXCEPTION 'Trigger settlement segment membership changed'
            USING ERRCODE='55000';
    END IF;
    INSERT INTO __forest_module_schema__.trigger_segment_settlements(
        bot_id,claim_ref,segment_index,claim_token,lease_epoch,settled_count
    ) VALUES (
        requested_bot,requested_ref,requested_index,requested_token,
        requested_epoch,changed_count
    );
    UPDATE __forest_module_schema__.trigger_settlement_progress
    SET next_segment_index=requested_index+1,lease_token=NULL,lease_expires_at=NULL,
        completed_at=CASE WHEN requested_index+1=claim_group.segment_count
                          THEN clock_timestamp() ELSE NULL END,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND claim_ref=requested_ref;
    result_value := jsonb_build_object('protocol','forest.trigger-segment-settled/1',
        'claim_ref',requested_ref,'segment_index',requested_index,
        'settled_count',changed_count,
        'remaining_segments',claim_group.segment_count-requested_index-1);
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
                              'value',result_value);
END;
$$;

CREATE FUNCTION conversation_claim_trigger_settlement(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    progress record;
    next_epoch integer;
    identity_digest text;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_token IS NULL OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 3600 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','TRIGGER_SETTLEMENT_CLAIM_INVALID',
                'message','Trigger settlement claim is invalid','status',422));
    END IF;
    SELECT p.*,g.segment_count INTO progress
    FROM __forest_module_schema__.trigger_settlement_progress p
    JOIN __forest_module_schema__.trigger_claim_groups g
      ON g.bot_id=p.bot_id AND g.claim_ref=p.claim_ref
    WHERE p.bot_id=requested_bot AND p.lease_token=requested_token
      AND p.lease_expires_at>clock_timestamp()
    FOR UPDATE OF p;
    IF progress IS NULL THEN
        SELECT p.*,g.segment_count INTO progress
        FROM __forest_module_schema__.trigger_settlement_progress p
        JOIN __forest_module_schema__.trigger_claim_groups g
          ON g.bot_id=p.bot_id AND g.claim_ref=p.claim_ref
        WHERE p.bot_id=requested_bot AND p.completed_at IS NULL
          AND p.next_segment_index<g.segment_count
          AND (p.lease_token IS NULL OR p.lease_expires_at<=clock_timestamp())
        ORDER BY p.updated_at,p.claim_ref
        FOR UPDATE OF p SKIP LOCKED LIMIT 1;
        IF progress IS NULL THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1',
                                      'ok',true,'value',NULL);
        END IF;
        IF progress.lease_epoch>=2147483647 THEN
            RAISE EXCEPTION 'Trigger settlement lease epoch exhausted'
                USING ERRCODE='54000';
        END IF;
        next_epoch := progress.lease_epoch+1;
        UPDATE __forest_module_schema__.trigger_settlement_progress
        SET lease_token=requested_token,lease_epoch=next_epoch,
            lease_expires_at=clock_timestamp()+(requested_lease*interval '1 second'),
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND claim_ref=progress.claim_ref;
    ELSE
        next_epoch := progress.lease_epoch;
    END IF;
    identity_digest := encode(sha256(convert_to(jsonb_build_array(
        requested_bot,progress.claim_ref,progress.next_segment_index,next_epoch
    )::text,'UTF8')),'hex');
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.owned-work-claim/1',
            'work_id',progress.claim_ref,'claim_token',requested_token,
            'attempt',next_epoch,'operation_id','trigger-settlement:'||identity_digest,
            'causal_root_kind','conversation.trigger-settlement',
            'causal_root_id',progress.claim_ref,
            'payload',jsonb_build_object(
                'protocol','forest.trigger-segment-settle/1',
                'claim_ref',progress.claim_ref,
                'segment_index',progress.next_segment_index,
                'claim_token',requested_token,'lease_epoch',next_epoch
            )
        ));
END;
$$;

CREATE FUNCTION conversation_settle_trigger_owned_work(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_ref text := input->>'work_id';
    requested_token text := input->>'claim_token';
    requested_epoch integer := (input->>'attempt')::integer;
    outcome_status text := input#>>'{outcome,status}';
    outcome_result jsonb := input#>'{outcome,result}';
    prior record;
    receipt record;
    progress record;
    result_value jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR requested_ref IS NULL OR requested_ref !~ '^claim:[a-f0-9]{64}$'
       OR requested_token IS NULL OR requested_epoch IS NULL OR requested_epoch<1
       OR outcome_status NOT IN ('SUCCEEDED','FAILED') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','TRIGGER_OWNED_SETTLEMENT_INVALID',
                'message','Owned trigger settlement is invalid','status',422));
    END IF;
    SELECT * INTO prior FROM __forest_module_schema__.trigger_owned_work_settlements
    WHERE claim_token=requested_token;
    IF prior IS NOT NULL THEN
        IF prior.input_payload=input THEN RETURN prior.result_payload; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','TRIGGER_OWNED_SETTLEMENT_CONFLICT',
                'message','Trigger work claim has another settlement','status',409));
    END IF;
    SELECT s.*,g.segment_count INTO receipt
    FROM __forest_module_schema__.trigger_segment_settlements s
    JOIN __forest_module_schema__.trigger_claim_groups g
      ON g.bot_id=s.bot_id AND g.claim_ref=s.claim_ref
    WHERE s.bot_id=requested_bot AND s.claim_ref=requested_ref
      AND s.claim_token=requested_token AND s.lease_epoch=requested_epoch;
    IF receipt IS NOT NULL THEN
        IF outcome_status='SUCCEEDED' AND (
            outcome_result->>'protocol' IS DISTINCT FROM 'forest.trigger-segment-settled/1'
            OR outcome_result->>'claim_ref' IS DISTINCT FROM requested_ref
            OR (outcome_result->>'segment_index')::integer IS DISTINCT FROM receipt.segment_index
            OR (outcome_result->>'settled_count')::integer IS DISTINCT FROM receipt.settled_count
            OR (outcome_result->>'remaining_segments')::integer IS DISTINCT FROM
                receipt.segment_count-receipt.segment_index-1
        ) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','TRIGGER_OWNED_RESULT_INVALID',
                    'message','Trigger worker result differs from committed segment','status',409));
        END IF;
    ELSE
        IF outcome_status='SUCCEEDED' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','TRIGGER_OWNED_RESULT_MISSING',
                    'message','Worker reported success without a committed segment','status',409));
        END IF;
        SELECT * INTO progress
        FROM __forest_module_schema__.trigger_settlement_progress
        WHERE bot_id=requested_bot AND claim_ref=requested_ref FOR UPDATE;
        IF progress IS NULL OR progress.lease_token IS DISTINCT FROM requested_token
           OR progress.lease_epoch IS DISTINCT FROM requested_epoch THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','STALE_TRIGGER_SETTLEMENT_LEASE',
                    'message','Trigger work lease is stale','status',409));
        END IF;
        UPDATE __forest_module_schema__.trigger_settlement_progress
        SET lease_token=NULL,lease_expires_at=NULL,updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND claim_ref=requested_ref;
    END IF;
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.owned-work-settled/1',
                                   'work_id',requested_ref));
    INSERT INTO __forest_module_schema__.trigger_owned_work_settlements(
        claim_token,input_payload,result_payload
    ) VALUES(requested_token,input,result_value);
    RETURN result_value;
END;
$$;

CREATE TABLE conversation_agent_work_settlements (
    claim_token text PRIMARY KEY CHECK (octet_length(claim_token) BETWEEN 1 AND 256),
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER conversation_agent_work_settlements_immutable
    BEFORE UPDATE OR DELETE ON conversation_agent_work_settlements
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();

CREATE FUNCTION conversation_claim_agent_run(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    lane record;
    next_epoch bigint;
    identity_digest text;
    selected_session text;
    selected_run text;
    resume_value boolean;
    claimed_at_value timestamptz;
    unfinished_session text;
    unfinished_run text;
    unfinished_count integer;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_token IS NULL OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 3600 THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_AGENT_CLAIM_INVALID',
                'message','Conversation Agent claim is invalid','status',422
            )
        );
    END IF;
    SELECT * INTO lane
    FROM __forest_module_schema__.conversation_lanes
    WHERE bot_id=requested_bot AND actor_owner_id=requested_token
      AND actor_expires_at>clock_timestamp()
    FOR UPDATE;
    IF lane IS NULL THEN
        SELECT l.* INTO lane
        FROM __forest_module_schema__.conversation_lanes l
        WHERE l.bot_id=requested_bot
          AND l.channel_kind IN ('GROUP','PRIVATE')
          AND (l.actor_owner_id IS NULL OR l.actor_expires_at<=clock_timestamp())
          AND (l.resume_not_before IS NULL OR l.resume_not_before<=clock_timestamp())
          AND NOT EXISTS (
              SELECT 1 FROM __forest_module_schema__.trigger_claim_groups g
              JOIN __forest_module_schema__.trigger_settlement_progress p
                ON p.bot_id=g.bot_id AND p.claim_ref=g.claim_ref
              WHERE g.bot_id=l.bot_id AND g.conversation_id=l.conversation_id
                AND g.terminal_status='REQUEUED' AND p.completed_at IS NULL
          )
          AND (
              (l.active_session_ref IS NOT NULL AND l.active_run_ref IS NOT NULL)
              OR (
                  l.active_session_ref IS NULL
                  AND EXISTS (
                      SELECT 1 FROM __forest_module_schema__.conversation_triggers t
                      WHERE t.bot_id=l.bot_id AND t.conversation_id=l.conversation_id
                        AND t.status='PENDING' AND t.ingress_seq<=l.classified_through_seq
                  )
              )
          )
        ORDER BY CASE WHEN l.active_session_ref IS NOT NULL THEN 0 ELSE 1 END,
                 l.updated_at,l.conversation_id
        FOR UPDATE SKIP LOCKED
        LIMIT 1;
        IF lane IS NULL THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',true,'value',NULL
            );
        END IF;
        next_epoch := lane.actor_epoch+1;
        claimed_at_value := clock_timestamp();
        UPDATE __forest_module_schema__.conversation_lanes
        SET actor_owner_id=requested_token,actor_epoch=next_epoch,
            actor_expires_at=claimed_at_value+(requested_lease*interval '1 second'),
            resume_not_before=NULL,resume_reason_code=NULL,resume_reason_ref=NULL,
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND conversation_id=lane.conversation_id
        RETURNING * INTO lane;
    ELSE
        claimed_at_value := lane.actor_expires_at-(requested_lease*interval '1 second');
    END IF;
    identity_digest := encode(sha256(convert_to(jsonb_build_array(
        requested_bot,lane.conversation_id,lane.actor_epoch
    )::text,'UTF8')),'hex');
    IF lane.active_session_ref IS NULL THEN
        SELECT count(*)::integer INTO unfinished_count
        FROM __forest_module_schema__.trigger_claim_groups g
        WHERE g.bot_id=requested_bot AND g.conversation_id=lane.conversation_id
          AND g.claim_state='ACTIVE';
        IF unfinished_count>1 THEN
            RAISE EXCEPTION 'More than one unfinished Run owns prefilled triggers'
                USING ERRCODE='55000';
        END IF;
        IF unfinished_count=1 THEN
            SELECT g.session_ref,g.run_ref INTO unfinished_session,unfinished_run
            FROM __forest_module_schema__.trigger_claim_groups g
            WHERE g.bot_id=requested_bot AND g.conversation_id=lane.conversation_id
              AND g.claim_state='ACTIVE';
        END IF;
    END IF;
    resume_value := lane.active_session_ref IS NOT NULL;
    selected_session := COALESCE(lane.active_session_ref,unfinished_session,
                                 'session:' || identity_digest);
    selected_run := COALESCE(lane.active_run_ref,unfinished_run,
                             'run:' || identity_digest);
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.owned-work-claim/1','work_id',lane.conversation_id,
            'claim_token',requested_token,'attempt',lane.actor_epoch,
            'operation_id','conversation-agent:' || identity_digest,
            'causal_root_kind','conversation.agent-run',
            'causal_root_id',lane.conversation_id,
            'payload',jsonb_build_object(
                'actor',jsonb_build_object(
                    'conversation_id',lane.conversation_id,
                    'owner_id',requested_token,'epoch',lane.actor_epoch
                ),
                'conversation_id',lane.conversation_id,
                'session_ref',selected_session,'run_ref',selected_run,
                'resume',resume_value,'claimed_at',claimed_at_value,
                'binding_ref',lane.binding_ref,'channel_kind',lane.channel_kind,
                'channel_ref',lane.channel_ref
            )
        )
    );
END;
$$;

CREATE FUNCTION conversation_settle_agent_run(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_work text := input->>'work_id';
    requested_token text := input->>'claim_token';
    requested_attempt bigint := (input->>'attempt')::bigint;
    existing record;
    lane record;
    result_value jsonb;
    work_result jsonb := input#>'{outcome,result}';
    delay_value numeric;
    reason_value text;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR requested_work IS NULL OR requested_token IS NULL
       OR requested_attempt IS NULL OR requested_attempt<1
       OR input#>>'{outcome,status}' NOT IN ('SUCCEEDED','FAILED') THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_AGENT_SETTLEMENT_INVALID',
                'message','Conversation Agent settlement is invalid','status',422
            )
        );
    END IF;
    SELECT * INTO existing
    FROM __forest_module_schema__.conversation_agent_work_settlements
    WHERE claim_token=requested_token;
    IF existing IS NOT NULL THEN
        IF existing.input_payload=input THEN RETURN existing.result_payload; END IF;
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_AGENT_SETTLEMENT_CONFLICT',
                'message','Conversation Agent claim has another settlement','status',409
            )
        );
    END IF;
    SELECT * INTO lane
    FROM __forest_module_schema__.conversation_lanes
    WHERE bot_id=requested_bot AND conversation_id=requested_work FOR UPDATE;
    IF lane IS NULL OR lane.actor_owner_id IS DISTINCT FROM requested_token
       OR lane.actor_epoch IS DISTINCT FROM requested_attempt THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','STALE_CONVERSATION_ACTOR',
                'message','Conversation actor no longer owns this Agent claim','status',409
            )
        );
    END IF;
    IF input#>>'{outcome,status}'='SUCCEEDED' THEN
        IF work_result->>'protocol' IS DISTINCT FROM 'forest.conversation-agent-work/1'
           OR work_result->>'disposition' NOT IN ('RELEASE','DEFER') THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','CONVERSATION_AGENT_SETTLEMENT_INVALID',
                    'message','Conversation Agent work result is invalid','status',422
                )
            );
        END IF;
        IF work_result->>'disposition'='DEFER' THEN
            delay_value := (work_result->>'delay_seconds')::numeric;
            reason_value := work_result->>'reason_code';
            IF delay_value IS NULL OR delay_value NOT BETWEEN 0.1 AND 3600
               OR reason_value IS NULL OR octet_length(reason_value) NOT BETWEEN 1 AND 128 THEN
                RETURN jsonb_build_object(
                    'protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object(
                        'code','CONVERSATION_AGENT_SETTLEMENT_INVALID',
                        'message','Conversation Agent defer result is invalid','status',422
                    )
                );
            END IF;
            UPDATE __forest_module_schema__.conversation_lanes
            SET actor_owner_id=NULL,actor_expires_at=NULL,
                resume_not_before=clock_timestamp()+(delay_value*interval '1 second'),
                resume_reason_code=reason_value,resume_reason_ref=requested_token,
                updated_at=clock_timestamp()
            WHERE bot_id=requested_bot AND conversation_id=requested_work;
        ELSE
            IF lane.active_session_ref IS NOT NULL THEN
                RETURN jsonb_build_object(
                    'protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object(
                        'code','CONVERSATION_AGENT_RELEASE_CONFLICT',
                        'message','Agent work cannot release an active Session','status',409
                    )
                );
            END IF;
            UPDATE __forest_module_schema__.conversation_lanes
            SET actor_owner_id=NULL,actor_expires_at=NULL,
                resume_not_before=NULL,resume_reason_code=NULL,resume_reason_ref=NULL,
                updated_at=clock_timestamp()
            WHERE bot_id=requested_bot AND conversation_id=requested_work;
        END IF;
    ELSE
        reason_value := COALESCE(input#>>'{outcome,error,code}','agent_work_failed');
        UPDATE __forest_module_schema__.conversation_lanes
        SET actor_owner_id=NULL,actor_expires_at=NULL,
            resume_not_before=CASE WHEN active_session_ref IS NULL THEN NULL
                ELSE clock_timestamp()+interval '1 second' END,
            resume_reason_code=CASE WHEN active_session_ref IS NULL THEN NULL ELSE reason_value END,
            resume_reason_ref=CASE WHEN active_session_ref IS NULL THEN NULL ELSE requested_token END,
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND conversation_id=requested_work;
    END IF;
    result_value := jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.owned-work-settled/1','work_id',requested_work
        )
    );
    INSERT INTO __forest_module_schema__.conversation_agent_work_settlements(
        claim_token,input_payload,result_payload
    ) VALUES (requested_token,input,result_value);
    RETURN result_value;
END;
$$;
