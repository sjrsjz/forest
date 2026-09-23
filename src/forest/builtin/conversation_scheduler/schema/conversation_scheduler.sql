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
    context_archived_unit_count bigint NOT NULL DEFAULT 0
        CHECK (context_archived_unit_count >= 0),
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
    CHECK (resume_not_before IS NULL OR active_session_ref IS NOT NULL)
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
    kind text NOT NULL CHECK (kind IN ('TRIGGER_INPUT','SILENT_DRAIN','RECOVERY_CLAIM')),
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
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, input_batch_id),
    UNIQUE (bot_id, operation_id),
    FOREIGN KEY (bot_id, conversation_id)
        REFERENCES conversation_lanes(bot_id, conversation_id),
    CHECK ((session_ref IS NULL) = (run_ref IS NULL)),
    CHECK (
        (kind='TRIGGER_INPUT' AND start_seq IS NOT NULL AND end_seq IS NOT NULL
            AND start_seq <= end_seq AND trigger_cut_seq=end_seq
            AND selected_input_end_seq=end_seq AND session_ref IS NOT NULL)
        OR (kind='SILENT_DRAIN' AND start_seq IS NOT NULL AND end_seq IS NOT NULL
            AND start_seq <= end_seq AND trigger_cut_seq IS NULL
            AND selected_input_end_seq=end_seq AND session_ref IS NULL)
        OR (kind='RECOVERY_CLAIM' AND start_seq IS NULL AND end_seq IS NULL
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

CREATE TABLE trigger_requeues (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    conversation_id text NOT NULL,
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
    PRIMARY KEY (bot_id, operation_id),
    FOREIGN KEY (bot_id, conversation_id)
        REFERENCES conversation_lanes(bot_id, conversation_id)
);

CREATE TABLE trigger_requeue_items (
    bot_id text NOT NULL,
    operation_id text NOT NULL,
    item_index integer NOT NULL CHECK (item_index >= 0),
    trigger_id text NOT NULL,
    PRIMARY KEY (bot_id, operation_id, item_index),
    UNIQUE (bot_id, operation_id, trigger_id),
    FOREIGN KEY (bot_id, operation_id)
        REFERENCES trigger_requeues(bot_id, operation_id),
    FOREIGN KEY (bot_id, trigger_id)
        REFERENCES conversation_triggers(bot_id, trigger_id)
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
    resulting_view_unit_count bigint NOT NULL CHECK (resulting_view_unit_count >= 0),
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

CREATE INDEX inbox_classification_idx
    ON inbox_messages(bot_id, conversation_id, ingress_seq, classification_state);
CREATE INDEX trigger_pending_idx
    ON conversation_triggers(bot_id, conversation_id, status, ingress_seq, trigger_id);
CREATE INDEX context_order_idx
    ON context_units(bot_id, conversation_id, context_revision, unit_index);
CREATE INDEX context_compaction_order_idx
    ON context_compaction_units(bot_id,conversation_id,context_revision,unit_index);
CREATE INDEX lane_wakeup_idx ON conversation_lanes(
    bot_id,resume_not_before,mailbox_revision,conversation_id
);

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

CREATE TABLE trigger_terminal_items (
    bot_id text NOT NULL,
    operation_id text NOT NULL,
    item_index integer NOT NULL CHECK (item_index >= 0),
    trigger_id text NOT NULL,
    PRIMARY KEY (bot_id, operation_id, item_index),
    UNIQUE (bot_id, operation_id, trigger_id),
    FOREIGN KEY (bot_id, operation_id)
        REFERENCES trigger_terminal_operations(bot_id, operation_id),
    FOREIGN KEY (bot_id, trigger_id)
        REFERENCES conversation_triggers(bot_id, trigger_id)
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
BEGIN
    IF TG_OP='DELETE' OR
       ROW(NEW.bot_id,NEW.conversation_id,NEW.trigger_id,NEW.event_id,
           NEW.ingress_seq,NEW.principal_ref,NEW.input_digest,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.conversation_id,OLD.trigger_id,OLD.event_id,
           OLD.ingress_seq,OLD.principal_ref,OLD.input_digest,OLD.created_at) THEN
        RAISE EXCEPTION 'Trigger identity is immutable' USING ERRCODE='55000';
    END IF;
    IF (OLD.status='PENDING' AND NEW.status IN ('CLAIMED','FAILED','CANCELLED'))
       OR (OLD.status='CLAIMED' AND NEW.status IN ('ANSWERED','FAILED','CANCELLED','PENDING')) THEN
        IF NEW.status='ANSWERED' AND NOT EXISTS (
            SELECT 1 FROM context_append_operations a
            WHERE a.bot_id=NEW.bot_id AND a.conversation_id=NEW.conversation_id
              AND a.append_kind='FINAL_REPLY' AND a.reply_ref=NEW.answered_reply_ref
              AND a.session_ref=NEW.claimed_session_ref AND a.run_ref=NEW.claimed_run_ref
        ) THEN
            RAISE EXCEPTION 'Answered trigger has no final reply append operation'
                USING ERRCODE='55000';
        END IF;
        RETURN NEW;
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
CREATE TRIGGER input_batches_immutable BEFORE UPDATE OR DELETE ON input_batches
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();
CREATE TRIGGER selection_operations_immutable BEFORE UPDATE OR DELETE ON selection_operations
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();
CREATE TRIGGER input_batch_events_immutable BEFORE UPDATE OR DELETE ON input_batch_events
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();
CREATE TRIGGER input_batch_triggers_immutable BEFORE UPDATE OR DELETE ON input_batch_triggers
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();
CREATE TRIGGER trigger_requeues_immutable BEFORE UPDATE OR DELETE ON trigger_requeues
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();
CREATE TRIGGER trigger_requeue_items_immutable BEFORE UPDATE OR DELETE ON trigger_requeue_items
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();
CREATE TRIGGER trigger_terminal_operations_immutable BEFORE UPDATE OR DELETE ON trigger_terminal_operations
    FOR EACH ROW EXECUTE FUNCTION reject_scheduler_fact_change();
CREATE TRIGGER trigger_terminal_items_immutable BEFORE UPDATE OR DELETE ON trigger_terminal_items
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
        author_subject_ref,content_payload_id,content_payload_digest,
        content_owner_component_id,content_owner_kind,content_owner_id,
        author_payload_id,author_payload_digest,author_owner_component_id,
        author_owner_kind,author_owner_id,context_disposition,sent_at,received_at,
        classification_deadline
    ) VALUES (
        requested_bot,requested_conversation,requested_event,allocated_seq,
        requested_operation,requested_digest,body->>'source_component_id',
        body->>'source_event_ref',body->>'event_kind',body->>'platform_message_ref',
        body->>'author_subject_ref',body#>>'{content,payload_id}',
        body#>>'{content,digest}',body#>>'{content,owner_component_id}',
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
