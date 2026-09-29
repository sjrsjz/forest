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
         AND maintenance_view_revision IS NULL)
        OR (maintenance_generation_id IS NOT NULL
            AND maintenance_context_revision IS NOT NULL
            AND maintenance_view_revision IS NOT NULL)
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
    kind text NOT NULL CHECK (kind IN ('TRIGGER_INPUT','CONTEXT_PREFILL','SILENT_DRAIN','RECOVERY_CLAIM')),
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
        OR (kind='CONTEXT_PREFILL' AND start_seq IS NOT NULL AND end_seq IS NOT NULL
            AND start_seq <= end_seq AND end_seq-start_seq < 64
            AND trigger_cut_seq IS NULL AND selected_input_end_seq=end_seq
            AND session_ref IS NULL)
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
CREATE TRIGGER trigger_terminal_operations_immutable BEFORE UPDATE OR DELETE ON trigger_terminal_operations
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
    trigger_ids jsonb;
    triggers jsonb;
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
    SELECT
        COALESCE(jsonb_agg(m.trigger_id ORDER BY m.trigger_index),'[]'::jsonb),
        COALESCE(jsonb_agg(
            jsonb_build_object(
                'trigger_id',t.trigger_id,
                'principal_ref',t.principal_ref,
                'ingress_seq',t.ingress_seq
            ) ORDER BY m.trigger_index
        ),'[]'::jsonb)
    INTO trigger_ids,triggers
    FROM __forest_module_schema__.input_batch_triggers m
    JOIN __forest_module_schema__.conversation_triggers t
      ON t.bot_id=m.bot_id AND t.trigger_id=m.trigger_id
    WHERE m.bot_id=requested_bot AND m.input_batch_id=receipt.input_batch_id;
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
        'trigger_ids',trigger_ids,
        'triggers',triggers
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
    has_gap boolean;
    selection_status text;
    selection_mailbox_revision bigint;
    selection_kind text;
    selection_start bigint;
    selection_end bigint;
    selection_trigger_cut bigint;
    selection_context_revision bigint;
    batch_id text;
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
    IF replay IS NOT NULL THEN
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
    -- Classification and every trigger-state transition hold this lane row
    -- first. The lane fence stabilizes the pending set without locking an
    -- arbitrarily large trigger backlog during each 64-event prefill.
    SELECT ingress_seq INTO trigger_cut
    FROM __forest_module_schema__.conversation_triggers
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
      AND status='PENDING' AND ingress_seq<=lane.classified_through_seq
    ORDER BY ingress_seq DESC,trigger_id DESC LIMIT 1;
    has_gap := lane.classified_through_seq < lane.next_ingress_seq - 1;
    IF requested_mode='PREFILL_ONLY' THEN
        IF trigger_cut IS NOT NULL AND trigger_cut-lane.context_applied_seq > 64 THEN
            -- Retain the last trigger in the final Agent InputBatch. Earlier
            -- triggers stay PENDING while their source events enter Context.
            selection_kind := 'CONTEXT_PREFILL';
            selection_start := lane.context_applied_seq+1;
            selection_end := LEAST(lane.context_applied_seq+64,trigger_cut-1);
            selection_trigger_cut := NULL;
            selected_session := NULL;
            selected_run := NULL;
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
        IF trigger_cut-lane.context_applied_seq > 64 THEN
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
    INSERT INTO __forest_module_schema__.input_batches(
        bot_id,conversation_id,input_batch_id,operation_id,input_digest,boundary,kind,
        start_seq,end_seq,trigger_cut_seq,selected_input_end_seq,session_ref,run_ref,
        actor_owner_id,actor_epoch,context_revision,context_view_revision
    ) VALUES (
        requested_bot,requested_conversation,batch_id,requested_operation,requested_digest,
        requested_boundary,selection_kind,selection_start,selection_end,
        selection_trigger_cut,COALESCE(selection_end,lane.context_applied_seq),
        selected_session,selected_run,requested_owner,requested_epoch,
        selection_context_revision,lane.context_view_revision
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
            payload_id,payload_digest,payload_owner_component_id,payload_owner_kind,
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
               event_id,ingress_seq,content_payload_id,content_payload_digest,
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
    IF trigger_cut IS NOT NULL AND selection_kind <> 'CONTEXT_PREFILL' THEN
        UPDATE __forest_module_schema__.conversation_triggers
        SET status='CLAIMED',claimed_session_ref=selected_session,
            claimed_run_ref=selected_run,input_batch_id=batch_id,
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND conversation_id=requested_conversation
          AND status='PENDING' AND ingress_seq<=lane.classified_through_seq;
        INSERT INTO __forest_module_schema__.input_batch_triggers(
            bot_id,input_batch_id,trigger_index,trigger_id
        )
        SELECT requested_bot,batch_id,
               row_number() OVER (ORDER BY ingress_seq,trigger_id)-1,trigger_id
        FROM __forest_module_schema__.conversation_triggers
        WHERE bot_id=requested_bot AND conversation_id=requested_conversation
          AND status='CLAIMED' AND input_batch_id=batch_id
        ORDER BY ingress_seq,trigger_id;
    END IF;
    selection_mailbox_revision := lane.mailbox_revision + 1;
    UPDATE __forest_module_schema__.conversation_lanes
    SET context_applied_seq=COALESCE(selection_end,context_applied_seq),
        context_revision=selection_context_revision,
        context_view_unit_count=context_view_unit_count+COALESCE(event_count,0),
        active_session_ref=CASE
            WHEN requested_boundary='SESSION_END' AND selection_kind='SILENT_DRAIN'
                THEN NULL
            ELSE COALESCE(selected_session,active_session_ref)
        END,
        active_run_ref=CASE
            WHEN requested_boundary='SESSION_END' AND selection_kind='SILENT_DRAIN'
                THEN NULL
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
BEGIN
    IF actor IS NULL OR requested_conversation IS NULL OR requested_owner IS NULL
       OR requested_epoch IS NULL OR requested_epoch<1
       OR requested_offset IS NULL OR requested_offset<0 OR requested_offset>1000000
       OR requested_limit IS NULL OR requested_limit<1 OR requested_limit>32 THEN
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
        'payload_owner_component_id',u.payload_owner_component_id,
        'payload_owner_kind',u.payload_owner_kind,
        'payload_owner_id',u.payload_owner_id,
        'author_payload_id',u.author_payload_id,
        'author_payload_digest',u.author_payload_digest,
        'author_owner_component_id',u.author_owner_component_id,
        'author_owner_kind',u.author_owner_kind,
        'author_owner_id',u.author_owner_id,
        'author_subject_ref',u.author_subject_ref,
        'trigger_decision',u.trigger_decision
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
            'context_applied_seq',lane.context_applied_seq,
            'classified_through_seq',lane.classified_through_seq,
            'context_revision',lane.context_revision,
            'context_view_revision',lane.context_view_revision,
            'source_component_id',lane.source_component_id,
            'binding_ref',lane.binding_ref,'platform',lane.platform,
            'channel_kind',lane.channel_kind,'channel_ref',lane.channel_ref,
            'thread_ref',lane.thread_ref,
            'context_view_unit_count',lane.context_view_unit_count,
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
    policy jsonb := body->'context_view';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    requested_conversation text := COALESCE(selected->>'conversation_id',anchor->>'conversation_id');
    high_watermark integer := (policy->>'high_watermark_units')::integer;
    retain_units integer := (policy->>'retain_units')::integer;
    policy_revision text := policy->>'policy_revision';
    lane record;
    replay record;
    archive_target bigint;
    units jsonb;
    archived_count bigint;
    first_revision bigint;
    last_revision bigint;
    source_set_digest text;
    published_view_revision bigint;
    cumulative_count bigint;
    resulting_count bigint;
    compaction_id text;
    archive_value jsonb;
    commit_value jsonb;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR (selected IS NULL AND NOT pre_run) OR policy IS NULL
       OR (pre_run AND (selected IS NOT NULL
           OR anchor->>'protocol' IS DISTINCT FROM 'forest.conversation-pre-run-anchor/1'
           OR anchor->>'conversation_id' IS DISTINCT FROM anchor#>>'{actor,conversation_id}'
           OR anchor#>>'{actor,owner_id}' IS NULL
           OR anchor#>>'{actor,epoch}' !~ '^[1-9][0-9]*$'))
       OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR high_watermark NOT BETWEEN 2 AND 1000000
       OR retain_units NOT BETWEEN 1 AND high_watermark-1
       OR policy_revision IS DISTINCT FROM 'context-view-unit-window-v1' THEN
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
            'resulting_view_unit_count',replay.resulting_view_unit_count,
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
    IF pre_run AND lane.active_session_ref IS NOT NULL THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'protocol','forest.payload-json-plan/1','status','SKIP'
            )
        );
    END IF;
    IF lane.context_view_unit_count <= high_watermark THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'protocol','forest.payload-json-plan/1','status','SKIP'
            )
        );
    END IF;
    -- Keep the archive manifest and its Memory eligibility proof bounded.
    -- A later maintenance claim will continue from the next active prefix.
    archive_target := LEAST(lane.context_view_unit_count-retain_units,64);
    WITH oldest AS MATERIALIZED (
        SELECT u.*
        FROM __forest_module_schema__.active_context_units a
        JOIN __forest_module_schema__.context_units u
          ON u.bot_id=a.bot_id AND u.conversation_id=a.conversation_id
         AND u.unit_id=a.unit_id
        WHERE a.bot_id=requested_bot AND a.conversation_id=requested_conversation
        ORDER BY a.context_revision,a.unit_index
        LIMIT archive_target
    ), candidates AS (
        SELECT a.* FROM oldest a
        WHERE NOT EXISTS (
              SELECT 1 FROM __forest_module_schema__.conversation_triggers t
              WHERE t.bot_id=a.bot_id AND t.conversation_id=a.conversation_id
                AND t.event_id=a.event_id AND t.status IN ('PENDING','CLAIMED')
          )
          AND NOT (
              lane.active_session_ref IS NOT NULL AND (
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
          )
        ORDER BY a.context_revision,a.unit_index
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'unit_id',unit_id,'context_revision',context_revision,
               'unit_index',unit_index,'payload_digest',payload_digest,
               'author_payload_digest',author_payload_digest,'event_id',event_id,
               'input_batch_id',input_batch_id
           ) ORDER BY context_revision,unit_index),'[]'::jsonb),
           count(*)::bigint,min(context_revision),max(context_revision)
    INTO units,archived_count,first_revision,last_revision
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
        'resulting_view_unit_count',resulting_count,
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
    plan jsonb := envelope#>'{prior,prepare}';
    payload_result jsonb := envelope#>'{prior,payload}';
    commit_value jsonb;
    payload_ref jsonb;
    requested_bot text := context->>'bot_id';
    lane record;
    existing record;
    unit_count integer;
    event_refs jsonb;
    result_value jsonb;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR (selected IS NULL AND NOT pre_run) OR plan IS NULL OR payload_result IS NULL
       OR (pre_run AND selected IS NOT NULL)
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
           OR lane.context_view_revision IS DISTINCT FROM
              (commit_value->>'base_view_revision')::bigint
           OR lane.context_view_unit_count <
              (commit_value->>'archived_unit_count')::bigint THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','CONTEXT_COMPACTION_STALE',
                    'message','Context view changed before compaction commit','status',409
                )
            );
        END IF;
        SELECT count(*)::integer INTO unit_count
        FROM jsonb_array_elements(commit_value->'units');
        IF unit_count<>(commit_value->>'archived_unit_count')::bigint THEN
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
            resulting_view_unit_count,cumulative_archived_unit_count,
            first_context_revision,last_context_revision
        ) VALUES (
            requested_bot,commit_value->>'conversation_id',commit_value->>'compaction_id',
            commit_value->>'operation_id',commit_value->>'input_digest',
            (commit_value->>'base_view_revision')::bigint,
            (commit_value->>'published_view_revision')::bigint,
            commit_value->>'source_set_digest',commit_value->>'policy_revision',
            payload_ref->>'payload_id',payload_ref->>'digest',
            (commit_value->>'archived_unit_count')::bigint,
            (commit_value->>'resulting_view_unit_count')::bigint,
            (commit_value->>'cumulative_archived_unit_count')::bigint,
            (commit_value->>'first_context_revision')::bigint,
            (commit_value->>'last_context_revision')::bigint
        );
        INSERT INTO __forest_module_schema__.context_compaction_units(
            bot_id,conversation_id,compaction_id,archived_index,unit_id,
            context_revision,unit_index,payload_digest,author_payload_digest
        )
        SELECT requested_bot,commit_value->>'conversation_id',
               commit_value->>'compaction_id',u.ordinality-1,u.item->>'unit_id',
               (u.item->>'context_revision')::bigint,
               (u.item->>'unit_index')::integer,u.item->>'payload_digest',
               u.item->>'author_payload_digest'
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
               OR l.maintenance_view_revision IS DISTINCT FROM l.context_view_revision)
          AND NOT EXISTS (
              SELECT 1 FROM __forest_module_schema__.conversation_triggers t
              WHERE t.bot_id=l.bot_id AND t.conversation_id=l.conversation_id
                AND t.status IN ('PENDING','CLAIMED')
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
    WHERE t.bot_id=requested_bot AND t.conversation_id=requested_conversation
      AND t.claimed_session_ref=requested_session AND t.claimed_run_ref=requested_run
      AND t.status='ANSWERED' AND t.answered_reply_ref=requested_reply
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
    SELECT count(*)::integer INTO claimed_trigger_count
    FROM __forest_module_schema__.conversation_triggers t
    WHERE t.bot_id=requested_bot AND t.conversation_id=requested_conversation
      AND t.status='CLAIMED' AND t.claimed_session_ref=requested_session
      AND t.claimed_run_ref=requested_run;
    IF claimed_trigger_count=0 THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','TRIGGER_STATE_CONFLICT',
                'message','Final reply Run has no claimed triggers','status',409
            )
        );
    END IF;
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
        payload_id,payload_digest,payload_owner_component_id,payload_owner_kind,
        payload_owner_id,author_payload_id,author_payload_digest,
        author_owner_component_id,author_owner_kind,author_owner_id,
        author_subject_ref,trigger_decision
    ) VALUES (
        requested_bot,requested_conversation,requested_unit,context_revision_value,0,'REPLY',
        'agent.runtime','final.reply',requested_reply,NULL,NULL,NULL,
        reply_value->>'payload_id',reply_value->>'digest',
        reply_value->>'owner_component_id',reply_value->>'owner_kind',
        reply_value->>'owner_id',NULL,NULL,NULL,NULL,NULL,NULL,NULL
    );
    INSERT INTO __forest_module_schema__.active_context_units(
        bot_id,conversation_id,unit_id,context_revision,unit_index
    ) VALUES(requested_bot,requested_conversation,requested_unit,context_revision_value,0);
    UPDATE __forest_module_schema__.conversation_triggers t
    SET status='ANSWERED',answered_reply_ref=requested_reply,updated_at=clock_timestamp()
    WHERE t.bot_id=requested_bot AND t.conversation_id=requested_conversation
      AND t.status='CLAIMED' AND t.claimed_session_ref=requested_session
      AND t.claimed_run_ref=requested_run;
    UPDATE __forest_module_schema__.conversation_lanes
    SET context_revision=context_revision_value,
        context_view_unit_count=context_view_unit_count+1,
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
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'conversation.scheduler'
       OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR requested_conversation IS NULL OR requested_owner IS NULL
       OR requested_epoch IS NULL OR requested_epoch<1
       OR requested_session IS NULL OR requested_run IS NULL
       OR requested_status NOT IN ('FAILED','CANCELLED')
       OR requested_reason IS NULL OR octet_length(requested_reason) NOT BETWEEN 1 AND 128 THEN
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
    SELECT count(*)::integer INTO trigger_count_value
    FROM __forest_module_schema__.conversation_triggers
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
      AND claimed_session_ref=requested_session
      AND claimed_run_ref=requested_run AND status='CLAIMED';
    IF trigger_count_value=0 THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','CONVERSATION_TERMINAL_TRIGGER_CONFLICT',
                'message','Terminal Run has no claimed triggers','status',409
            )
        );
    END IF;
    INSERT INTO __forest_module_schema__.trigger_terminal_operations(
        bot_id,operation_id,conversation_id,input_digest,terminal_session_ref,
        terminal_run_ref,resulting_status,reason_code,actor_owner_id,actor_epoch,
        trigger_count
    ) VALUES (
        requested_bot,requested_operation,requested_conversation,requested_digest,
        requested_session,requested_run,requested_status,requested_reason,
        requested_owner,requested_epoch,trigger_count_value
    );
    UPDATE __forest_module_schema__.conversation_triggers
    SET status=requested_status,terminal_reason_code=requested_reason,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND conversation_id=requested_conversation
      AND claimed_session_ref=requested_session
      AND claimed_run_ref=requested_run AND status='CLAIMED';
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
    resume_value := lane.active_session_ref IS NOT NULL;
    selected_session := COALESCE(lane.active_session_ref,'session:' || identity_digest);
    selected_run := COALESCE(lane.active_run_ref,'run:' || identity_digest);
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
