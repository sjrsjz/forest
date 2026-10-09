CREATE TABLE memory_domains (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    initialization_operation_id text NOT NULL
        CHECK (octet_length(initialization_operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    space_id text NOT NULL CHECK (octet_length(space_id) BETWEEN 1 AND 256),
    embedding_model_kind text NOT NULL
        CHECK (octet_length(embedding_model_kind) BETWEEN 1 AND 256),
    model_revision_digest text NOT NULL CHECK (
        model_revision_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    dimensions integer NOT NULL CHECK (dimensions BETWEEN 1 AND 4096),
    max_batch_items integer NOT NULL CHECK (max_batch_items BETWEEN 1 AND 32),
    normalization text NOT NULL CHECK (normalization IN ('L2','NONE')),
    fusion_policy_revision text NOT NULL
        CHECK (octet_length(fusion_policy_revision) BETWEEN 1 AND 256),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (bot_id,initialization_operation_id),
    UNIQUE (bot_id,space_id)
);

CREATE TABLE memory_domain_bind_work (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    state text NOT NULL CHECK (state IN ('PENDING','CLAIMED','READY')),
    claim_token text UNIQUE,
    lease_deadline timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_error_code text,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((state='CLAIMED') = (claim_token IS NOT NULL AND lease_deadline IS NOT NULL))
);

CREATE TABLE memory_domain_bind_settlements (
    claim_token text PRIMARY KEY,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    settled_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE memory_sources (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    source_id text NOT NULL CHECK (octet_length(source_id) BETWEEN 1 AND 256),
    registration_operation_id text NOT NULL
        CHECK (octet_length(registration_operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_kind text NOT NULL CHECK (
        source_kind IN ('GROUP','DIRECT','DOCUMENT','SYSTEM','UNKNOWN')
    ),
    platform text CHECK (platform IS NULL OR octet_length(platform) BETWEEN 1 AND 64),
    realm_id text CHECK (realm_id IS NULL OR octet_length(realm_id) BETWEEN 1 AND 256),
    external_group_id text CHECK (
        external_group_id IS NULL OR octet_length(external_group_id) BETWEEN 1 AND 256
    ),
    identity_incarnation bigint NOT NULL CHECK (identity_incarnation > 0),
    identity_digest text NOT NULL CHECK (identity_digest ~ '^sha256:[a-f0-9]{64}$'),
    trunk_id text CHECK (trunk_id IS NULL OR octet_length(trunk_id) BETWEEN 1 AND 256),
    status text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','RETIRED')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,source_id),
    UNIQUE (bot_id,registration_operation_id),
    UNIQUE (bot_id,identity_digest),
    CHECK (
        (source_kind='GROUP' AND platform IS NOT NULL AND realm_id IS NOT NULL
            AND external_group_id IS NOT NULL AND trunk_id IS NOT NULL)
        OR (source_kind<>'GROUP' AND external_group_id IS NULL AND trunk_id IS NULL)
    )
);

CREATE TABLE memory_trunks (
    bot_id text NOT NULL,
    trunk_id text NOT NULL CHECK (octet_length(trunk_id) BETWEEN 1 AND 256),
    source_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,trunk_id),
    UNIQUE (bot_id,source_id),
    FOREIGN KEY (bot_id,source_id) REFERENCES memory_sources(bot_id,source_id)
);

CREATE TABLE source_registration_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_id text NOT NULL,
    duplicate_source boolean NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    FOREIGN KEY (bot_id,source_id) REFERENCES memory_sources(bot_id,source_id)
);

CREATE TABLE source_clocks (
    bot_id text NOT NULL,
    source_id text NOT NULL,
    next_source_order_seq bigint NOT NULL DEFAULT 1 CHECK (next_source_order_seq > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,source_id),
    FOREIGN KEY (bot_id,source_id) REFERENCES memory_sources(bot_id,source_id)
);

CREATE TABLE source_position_reservations (
    bot_id text NOT NULL,
    source_unit_id text NOT NULL CHECK (octet_length(source_unit_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_id text NOT NULL,
    identity_digest text NOT NULL CHECK (identity_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_was_existing boolean NOT NULL,
    source_order_seq bigint NOT NULL CHECK (source_order_seq > 0),
    conversation_ref text NOT NULL CHECK (octet_length(conversation_ref) BETWEEN 1 AND 256),
    event_ref text NOT NULL CHECK (octet_length(event_ref) BETWEEN 1 AND 256),
    ingress_seq bigint NOT NULL CHECK (ingress_seq > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,source_unit_id),
    UNIQUE (bot_id,operation_id),
    UNIQUE (bot_id,source_id,source_order_seq),
    UNIQUE (bot_id,conversation_ref,event_ref),
    FOREIGN KEY (bot_id,source_id) REFERENCES memory_sources(bot_id,source_id)
);

CREATE TABLE source_units (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    source_unit_id text NOT NULL CHECK (octet_length(source_unit_id) BETWEEN 1 AND 256),
    capture_operation_id text NOT NULL
        CHECK (octet_length(capture_operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_id text NOT NULL,
    trunk_id text,
    source_order_seq bigint NOT NULL CHECK (source_order_seq > 0),
    source_revision bigint NOT NULL CHECK (source_revision > 0),
    unit_kind text NOT NULL CHECK (octet_length(unit_kind) BETWEEN 1 AND 128),
    modality text NOT NULL CHECK (modality IN ('TEXT','IMAGE','MIXED','CONTROL')),
    part_count integer NOT NULL CHECK (part_count BETWEEN 1 AND 4096),
    content_manifest_digest text NOT NULL CHECK (
        content_manifest_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    author_subject_ref text NOT NULL
        CHECK (octet_length(author_subject_ref) BETWEEN 1 AND 256),
    author_payload_id text NOT NULL CHECK (octet_length(author_payload_id) BETWEEN 1 AND 256),
    author_payload_digest text NOT NULL CHECK (
        author_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    author_owner_component_id text NOT NULL CHECK (
        octet_length(author_owner_component_id) BETWEEN 1 AND 128
    ),
    author_owner_kind text NOT NULL
        CHECK (octet_length(author_owner_kind) BETWEEN 1 AND 128),
    author_owner_id text NOT NULL CHECK (octet_length(author_owner_id) BETWEEN 1 AND 512),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch >= 0),
    eligibility_state text NOT NULL DEFAULT 'REGISTERED' CHECK (
        eligibility_state IN ('REGISTERED','ELIGIBLE','SUPPRESSED')
    ),
    eligibility_revision bigint NOT NULL DEFAULT 0 CHECK (eligibility_revision >= 0),
    eligible_at timestamptz,
    eligible_xid xid8,
    occurred_at timestamptz NOT NULL,
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    accepted_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,source_unit_id),
    UNIQUE (bot_id,source_unit_id,source_revision),
    UNIQUE (bot_id,capture_operation_id),
    UNIQUE (bot_id,source_id,source_order_seq),
    FOREIGN KEY (bot_id,source_id) REFERENCES memory_sources(bot_id,source_id),
    FOREIGN KEY (bot_id,trunk_id) REFERENCES memory_trunks(bot_id,trunk_id),
    CHECK (
        (eligibility_state='REGISTERED' AND eligible_at IS NULL)
        OR eligibility_state='SUPPRESSED'
        OR (eligibility_state='ELIGIBLE' AND eligible_at IS NOT NULL)
    )
);

CREATE TABLE source_parts (
    bot_id text NOT NULL,
    source_unit_id text NOT NULL,
    part_id text NOT NULL CHECK (octet_length(part_id) BETWEEN 1 AND 256),
    part_index integer NOT NULL CHECK (part_index BETWEEN 0 AND 4095),
    part_kind text NOT NULL CHECK (part_kind IN ('TEXT','IMAGE')),
    media_type text NOT NULL CHECK (octet_length(media_type) BETWEEN 1 AND 128),
    payload_id text NOT NULL CHECK (octet_length(payload_id) BETWEEN 1 AND 256),
    payload_digest text NOT NULL CHECK (payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    payload_owner_component_id text NOT NULL CHECK (
        octet_length(payload_owner_component_id) BETWEEN 1 AND 128
    ),
    payload_owner_kind text NOT NULL CHECK (
        octet_length(payload_owner_kind) BETWEEN 1 AND 128
    ),
    payload_owner_id text NOT NULL CHECK (octet_length(payload_owner_id) BETWEEN 1 AND 512),
    width integer CHECK (width IS NULL OR width BETWEEN 1 AND 100000),
    height integer CHECK (height IS NULL OR height BETWEEN 1 AND 100000),
    byte_length bigint CHECK (byte_length IS NULL OR byte_length BETWEEN 0 AND 33554432),
    PRIMARY KEY (bot_id,source_unit_id,part_id),
    UNIQUE (bot_id,source_unit_id,part_index),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    CHECK (
        (part_kind='TEXT' AND width IS NULL AND height IS NULL)
        OR (part_kind='IMAGE' AND width IS NOT NULL AND height IS NOT NULL)
    )
);

CREATE TABLE source_occurrences (
    bot_id text NOT NULL,
    occurrence_id text NOT NULL CHECK (octet_length(occurrence_id) BETWEEN 1 AND 256),
    source_unit_id text NOT NULL,
    binding_ref text NOT NULL CHECK (octet_length(binding_ref) BETWEEN 1 AND 256),
    conversation_ref text NOT NULL
        CHECK (octet_length(conversation_ref) BETWEEN 1 AND 256),
    event_ref text NOT NULL CHECK (octet_length(event_ref) BETWEEN 1 AND 256),
    ingress_seq bigint CHECK (ingress_seq IS NULL OR ingress_seq > 0),
    input_batch_ref text CHECK (
        input_batch_ref IS NULL OR octet_length(input_batch_ref) BETWEEN 1 AND 256
    ),
    context_revision bigint CHECK (context_revision IS NULL OR context_revision > 0),
    eligibility_state text NOT NULL DEFAULT 'REGISTERED' CHECK (
        eligibility_state IN ('REGISTERED','ADMITTED','ELIGIBLE','SUPPRESSED')
    ),
    observed_at timestamptz NOT NULL,
    qualified_at timestamptz,
    PRIMARY KEY (bot_id,occurrence_id),
    UNIQUE (bot_id,binding_ref,event_ref),
    UNIQUE (bot_id,conversation_ref,event_ref),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    CHECK ((input_batch_ref IS NULL) = (context_revision IS NULL)),
    CHECK (
        (eligibility_state IN ('ADMITTED','ELIGIBLE') AND input_batch_ref IS NOT NULL)
        OR (eligibility_state IN ('REGISTERED','SUPPRESSED') AND input_batch_ref IS NULL)
    )
);

CREATE TABLE capture_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_unit_id text NOT NULL,
    occurrence_id text NOT NULL,
    duplicate_source boolean NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    FOREIGN KEY (bot_id,occurrence_id) REFERENCES source_occurrences(bot_id,occurrence_id)
);

CREATE TABLE eligibility_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_unit_id text NOT NULL,
    occurrence_id text NOT NULL,
    input_batch_ref text NOT NULL CHECK (octet_length(input_batch_ref) BETWEEN 1 AND 256),
    context_revision bigint NOT NULL CHECK (context_revision > 0),
    resulting_eligibility_revision bigint NOT NULL CHECK (resulting_eligibility_revision >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    FOREIGN KEY (bot_id,occurrence_id) REFERENCES source_occurrences(bot_id,occurrence_id)
);

CREATE TABLE pending_input_events (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    conversation_ref text NOT NULL
        CHECK (octet_length(conversation_ref) BETWEEN 1 AND 256),
    event_ref text NOT NULL CHECK (octet_length(event_ref) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    input_batch_ref text NOT NULL CHECK (octet_length(input_batch_ref) BETWEEN 1 AND 256),
    context_revision bigint NOT NULL CHECK (context_revision > 0),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch >= 0),
    state text NOT NULL DEFAULT 'PENDING_CAPTURE'
        CHECK (state IN ('PENDING_CAPTURE','RESOLVED','UNAVAILABLE')),
    source_unit_id text,
    occurrence_id text,
    error_code text CHECK (error_code IS NULL OR error_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    resolved_at timestamptz,
    PRIMARY KEY (bot_id,conversation_ref,event_ref),
    UNIQUE (bot_id,operation_id,event_ref),
    CHECK (
        (state='PENDING_CAPTURE' AND source_unit_id IS NULL AND occurrence_id IS NULL
            AND error_code IS NULL AND resolved_at IS NULL)
        OR (state='RESOLVED' AND source_unit_id IS NOT NULL AND occurrence_id IS NOT NULL
            AND error_code IS NULL AND resolved_at IS NOT NULL)
        OR (state='UNAVAILABLE' AND source_unit_id IS NULL AND occurrence_id IS NULL
            AND error_code IS NOT NULL AND resolved_at IS NOT NULL)
    ),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    FOREIGN KEY (bot_id,occurrence_id) REFERENCES source_occurrences(bot_id,occurrence_id)
);

CREATE TABLE unavailable_source_events (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    conversation_ref text NOT NULL CHECK (octet_length(conversation_ref) BETWEEN 1 AND 256),
    event_ref text NOT NULL CHECK (octet_length(event_ref) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    error_code text NOT NULL CHECK (error_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,conversation_ref,event_ref),
    UNIQUE (bot_id,operation_id)
);

CREATE TABLE pending_archive_events (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    conversation_ref text NOT NULL CHECK (octet_length(conversation_ref) BETWEEN 1 AND 256),
    event_ref text NOT NULL CHECK (octet_length(event_ref) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    compaction_ref text NOT NULL CHECK (octet_length(compaction_ref) BETWEEN 1 AND 256),
    input_batch_ref text NOT NULL CHECK (octet_length(input_batch_ref) BETWEEN 1 AND 256),
    context_revision bigint NOT NULL CHECK (context_revision > 0),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch >= 0),
    state text NOT NULL DEFAULT 'PENDING_CAPTURE' CHECK (
        state IN ('PENDING_CAPTURE','RESOLVED','UNAVAILABLE')
    ),
    source_unit_id text,
    occurrence_id text,
    error_code text,
    resolved_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,conversation_ref,event_ref),
    UNIQUE (bot_id,operation_id,event_ref),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    FOREIGN KEY (bot_id,occurrence_id) REFERENCES source_occurrences(bot_id,occurrence_id),
    CHECK (
        (state='PENDING_CAPTURE' AND source_unit_id IS NULL AND occurrence_id IS NULL
            AND error_code IS NULL AND resolved_at IS NULL)
        OR (state='RESOLVED' AND source_unit_id IS NOT NULL AND occurrence_id IS NOT NULL
            AND error_code IS NULL AND resolved_at IS NOT NULL)
        OR (state='UNAVAILABLE' AND source_unit_id IS NULL AND occurrence_id IS NULL
            AND error_code IS NOT NULL AND resolved_at IS NOT NULL)
    )
);

CREATE TABLE archive_qualification_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    compaction_ref text NOT NULL CHECK (octet_length(compaction_ref) BETWEEN 1 AND 256),
    conversation_ref text NOT NULL CHECK (octet_length(conversation_ref) BETWEEN 1 AND 256),
    event_ref text NOT NULL CHECK (octet_length(event_ref) BETWEEN 1 AND 256),
    source_unit_id text NOT NULL,
    occurrence_id text NOT NULL,
    resulting_eligibility_revision bigint NOT NULL CHECK (resulting_eligibility_revision > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id,event_ref),
    UNIQUE (bot_id,compaction_ref,event_ref),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    FOREIGN KEY (bot_id,occurrence_id) REFERENCES source_occurrences(bot_id,occurrence_id)
);

CREATE TABLE source_suppressions (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_unit_id text NOT NULL,
    deletion_epoch bigint NOT NULL CHECK (deletion_epoch > 0),
    reason_code text NOT NULL CHECK (octet_length(reason_code) BETWEEN 1 AND 128),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id,source_unit_id),
    UNIQUE (bot_id,source_unit_id),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id)
);

CREATE TABLE source_forget_operations (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_unit_id text NOT NULL,
    selection_digest text NOT NULL CHECK (selection_digest ~ '^sha256:[a-f0-9]{64}$'),
    deletion_epoch bigint NOT NULL CHECK (deletion_epoch > 0),
    result_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id)
);

CREATE TABLE batch_forget_operations (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    selection_id text NOT NULL,
    requested_by text NOT NULL CHECK (octet_length(requested_by) BETWEEN 1 AND 256),
    first_ordinal integer NOT NULL CHECK (first_ordinal BETWEEN 1 AND 1000000),
    last_ordinal integer NOT NULL CHECK (last_ordinal BETWEEN first_ordinal AND 1000000),
    member_digest text NOT NULL CHECK (member_digest ~ '^sha256:[a-f0-9]{64}$'),
    root_member_digest text NOT NULL CHECK (root_member_digest ~ '^sha256:[a-f0-9]{64}$'),
    selection_digest text NOT NULL CHECK (selection_digest ~ '^sha256:[a-f0-9]{64}$'),
    deletion_epoch bigint NOT NULL CHECK (deletion_epoch > 0),
    result_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id)
);
CREATE INDEX batch_forget_selection_progress_idx
    ON batch_forget_operations(bot_id,selection_id,first_ordinal,last_ordinal);

CREATE TABLE management_forget_work (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    request_id text NOT NULL CHECK (octet_length(request_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    work_kind text NOT NULL DEFAULT 'SOURCE' CHECK (work_kind IN ('SOURCE','BATCH')),
    source_unit_id text,
    selection_id text,
    after_ordinal integer CHECK (after_ordinal IS NULL OR after_ordinal BETWEEN 0 AND 999999),
    batch_size integer CHECK (batch_size IS NULL OR batch_size BETWEEN 1 AND 64),
    requested_by text NOT NULL CHECK (octet_length(requested_by) BETWEEN 1 AND 256),
    plan jsonb NOT NULL,
    state text NOT NULL DEFAULT 'QUEUED' CHECK (
        state IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','UNKNOWN')),
    claim_token text CHECK (claim_token IS NULL OR octet_length(claim_token) BETWEEN 1 AND 256),
    lease_deadline timestamptz,
    reconcile_checked_at timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND 3),
    result_payload jsonb,
    error_code text CHECK (error_code IS NULL OR octet_length(error_code) BETWEEN 1 AND 128),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,request_id),
    UNIQUE (bot_id,claim_token),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    CHECK (
        (work_kind='SOURCE' AND source_unit_id IS NOT NULL AND selection_id IS NULL
         AND after_ordinal IS NULL AND batch_size IS NULL)
        OR (work_kind='BATCH' AND source_unit_id IS NULL AND selection_id IS NOT NULL
            AND after_ordinal IS NOT NULL AND batch_size IS NOT NULL)
    ),
    CHECK ((state='RUNNING') = (claim_token IS NOT NULL AND lease_deadline IS NOT NULL)),
    CHECK ((state='SUCCEEDED') = (result_payload IS NOT NULL))
);
CREATE INDEX management_forget_claim_idx
    ON management_forget_work(bot_id,state,created_at,request_id);
CREATE INDEX management_forget_expired_idx
    ON management_forget_work(bot_id,lease_deadline,request_id)
    WHERE state='RUNNING';
CREATE INDEX management_forget_unknown_reconcile_idx
    ON management_forget_work(bot_id,reconcile_checked_at,request_id)
    WHERE state='UNKNOWN';

CREATE TABLE management_forget_settlements (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    claim_token text NOT NULL CHECK (octet_length(claim_token) BETWEEN 1 AND 256),
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    PRIMARY KEY (bot_id,claim_token)
);

CREATE TABLE forget_selections (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    selection_id text NOT NULL CHECK (octet_length(selection_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    requested_by text NOT NULL CHECK (octet_length(requested_by) BETWEEN 1 AND 256),
    filters jsonb NOT NULL CHECK (jsonb_typeof(filters)='object'),
    snapshot_xid pg_snapshot NOT NULL,
    snapshot_accepted_at timestamptz NOT NULL,
    member_count integer NOT NULL CHECK (member_count BETWEEN 1 AND 1000000),
    member_digest text NOT NULL CHECK (member_digest ~ '^sha256:[a-f0-9]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,selection_id),
    UNIQUE (bot_id,operation_id)
);
ALTER TABLE batch_forget_operations ADD CONSTRAINT batch_forget_selection_fk
    FOREIGN KEY (bot_id,selection_id) REFERENCES forget_selections(bot_id,selection_id);
ALTER TABLE management_forget_work ADD CONSTRAINT management_forget_selection_fk
    FOREIGN KEY (bot_id,selection_id) REFERENCES forget_selections(bot_id,selection_id);

CREATE TABLE forget_selection_members (
    bot_id text NOT NULL,
    selection_id text NOT NULL,
    ordinal integer NOT NULL CHECK (ordinal BETWEEN 1 AND 1000000),
    source_unit_id text NOT NULL,
    source_revision bigint NOT NULL CHECK (source_revision > 0),
    anchor_payload_id text NOT NULL CHECK (octet_length(anchor_payload_id) BETWEEN 1 AND 256),
    PRIMARY KEY (bot_id,selection_id,ordinal),
    UNIQUE (bot_id,selection_id,source_unit_id),
    FOREIGN KEY (bot_id,selection_id) REFERENCES forget_selections(bot_id,selection_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bot_id,source_unit_id,source_revision)
        REFERENCES source_units(bot_id,source_unit_id,source_revision)
);
CREATE INDEX forget_selection_members_source_idx
    ON forget_selection_members(bot_id,source_unit_id);

CREATE TABLE relation_contributions (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    contribution_id text NOT NULL CHECK (octet_length(contribution_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    relation_id text NOT NULL CHECK (octet_length(relation_id) BETWEEN 1 AND 256),
    relation_kind text NOT NULL CHECK (
        relation_kind IN ('ASSOCIATES_WITH','EVIDENCE','CONTRADICTS','SUBJECT_OF')
    ),
    left_kind text NOT NULL CHECK (left_kind IN ('SOURCE_UNIT','ASSERTION','ENTITY','ARTIFACT')),
    left_id text NOT NULL CHECK (octet_length(left_id) BETWEEN 1 AND 256),
    right_kind text NOT NULL CHECK (right_kind IN ('SOURCE_UNIT','ASSERTION','ENTITY','ARTIFACT')),
    right_id text NOT NULL CHECK (octet_length(right_id) BETWEEN 1 AND 256),
    relation_key text NOT NULL DEFAULT '' CHECK (octet_length(relation_key) <= 256),
    origin_kind text NOT NULL CHECK (origin_kind IN ('MANUAL','VECTOR','RULE','MODEL')),
    weight_micros bigint NOT NULL CHECK (weight_micros BETWEEN 0 AND 1000000000000),
    admitted_deletion_epoch bigint NOT NULL CHECK (admitted_deletion_epoch >= 0),
    active boolean NOT NULL DEFAULT true,
    cross_trunk boolean NOT NULL,
    sample_key text COLLATE "C" GENERATED ALWAYS AS (md5(contribution_id)) STORED,
    created_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,contribution_id),
    UNIQUE (bot_id,contribution_id,created_xid),
    UNIQUE (bot_id,operation_id),
    UNIQUE (bot_id,relation_id,contribution_id),
    CHECK (ROW(left_kind,left_id) IS DISTINCT FROM ROW(right_kind,right_id))
);

CREATE TABLE relation_evidence (
    bot_id text NOT NULL,
    contribution_id text NOT NULL,
    evidence_index integer NOT NULL CHECK (evidence_index BETWEEN 0 AND 63),
    source_unit_id text NOT NULL,
    source_revision bigint NOT NULL CHECK (source_revision > 0),
    part_id text NOT NULL CHECK (octet_length(part_id) BETWEEN 1 AND 256),
    created_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
    PRIMARY KEY (bot_id,contribution_id,evidence_index),
    UNIQUE (bot_id,contribution_id,source_unit_id,source_revision,part_id),
    FOREIGN KEY (bot_id,contribution_id,created_xid)
        REFERENCES relation_contributions(bot_id,contribution_id,created_xid),
    FOREIGN KEY (bot_id,source_unit_id,source_revision)
        REFERENCES source_units(bot_id,source_unit_id,source_revision),
    FOREIGN KEY (bot_id,source_unit_id,part_id)
        REFERENCES source_parts(bot_id,source_unit_id,part_id)
);

CREATE TABLE relation_retractions (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    contribution_id text NOT NULL,
    reason_payload_id text NOT NULL CHECK (octet_length(reason_payload_id) BETWEEN 1 AND 256),
    reason_payload_digest text NOT NULL CHECK (
        reason_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    reason_owner_component_id text NOT NULL CHECK (
        octet_length(reason_owner_component_id) BETWEEN 1 AND 128
    ),
    reason_owner_kind text NOT NULL CHECK (
        octet_length(reason_owner_kind) BETWEEN 1 AND 128
    ),
    reason_owner_id text NOT NULL CHECK (octet_length(reason_owner_id) BETWEEN 1 AND 512),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    UNIQUE (bot_id,contribution_id),
    FOREIGN KEY (bot_id,contribution_id)
        REFERENCES relation_contributions(bot_id,contribution_id)
);

CREATE TABLE memory_queries (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    query_id text NOT NULL CHECK (octet_length(query_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    input_kind text NOT NULL DEFAULT 'SOURCE_UNIT'
        CHECK (input_kind IN ('SOURCE_UNIT','ADMIN_TRIAL')),
    current_source_id text,
    input_batch_ref text CHECK (octet_length(input_batch_ref) BETWEEN 1 AND 256),
    context_revision bigint CHECK (context_revision > 0),
    query_mode text NOT NULL CHECK (query_mode IN ('TEXT','IMAGE','MIXED')),
    fusion_policy_revision text NOT NULL
        CHECK (octet_length(fusion_policy_revision) BETWEEN 1 AND 256),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch >= 0),
    graph_snapshot pg_snapshot NOT NULL DEFAULT pg_current_snapshot(),
    state text NOT NULL DEFAULT 'OPEN' CHECK (state IN ('OPEN','FROZEN')),
    bundle_id text CHECK (bundle_id IS NULL OR octet_length(bundle_id) BETWEEN 1 AND 256),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    frozen_at timestamptz,
    PRIMARY KEY (bot_id,query_id),
    UNIQUE (bot_id,query_id,input_kind),
    UNIQUE (bot_id,operation_id),
    UNIQUE (bot_id,bundle_id),
    FOREIGN KEY (bot_id,current_source_id) REFERENCES memory_sources(bot_id,source_id),
    CHECK ((input_kind='SOURCE_UNIT' AND current_source_id IS NOT NULL
            AND input_batch_ref IS NOT NULL AND context_revision IS NOT NULL)
        OR (input_kind='ADMIN_TRIAL' AND current_source_id IS NULL
            AND input_batch_ref IS NULL AND context_revision IS NULL)),
    CHECK ((state='FROZEN') = (bundle_id IS NOT NULL)),
    CHECK ((state='FROZEN') = (frozen_at IS NOT NULL))
);

CREATE TABLE memory_query_parts (
    bot_id text NOT NULL,
    query_id text NOT NULL,
    query_part_index integer NOT NULL CHECK (query_part_index BETWEEN 0 AND 255),
    input_kind text NOT NULL DEFAULT 'SOURCE_UNIT' CHECK (input_kind='SOURCE_UNIT'),
    source_unit_id text NOT NULL,
    source_revision bigint NOT NULL CHECK (source_revision > 0),
    occurrence_id text NOT NULL,
    part_id text NOT NULL CHECK (octet_length(part_id) BETWEEN 1 AND 256),
    modality text NOT NULL CHECK (modality IN ('TEXT','IMAGE')),
    PRIMARY KEY (bot_id,query_id,query_part_index),
    UNIQUE (bot_id,query_id,source_unit_id,part_id),
    FOREIGN KEY (bot_id,query_id,input_kind)
        REFERENCES memory_queries(bot_id,query_id,input_kind),
    FOREIGN KEY (bot_id,source_unit_id,part_id)
        REFERENCES source_parts(bot_id,source_unit_id,part_id),
    FOREIGN KEY (bot_id,occurrence_id) REFERENCES source_occurrences(bot_id,occurrence_id)
);

CREATE TABLE memory_trial_inputs (
    bot_id text NOT NULL,
    query_id text NOT NULL CHECK (octet_length(query_id) BETWEEN 1 AND 256),
    input_kind text NOT NULL DEFAULT 'ADMIN_TRIAL' CHECK (input_kind='ADMIN_TRIAL'),
    input_revision bigint NOT NULL DEFAULT 1 CHECK (input_revision>0),
    actor_subject_ref text NOT NULL CHECK (octet_length(actor_subject_ref) BETWEEN 1 AND 256),
    generation_id text NOT NULL CHECK (octet_length(generation_id) BETWEEN 1 AND 256),
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    cancelled_at timestamptz,
    PRIMARY KEY (bot_id,query_id),
    FOREIGN KEY (bot_id,query_id,input_kind)
        REFERENCES memory_queries(bot_id,query_id,input_kind),
    CHECK (expires_at>created_at AND expires_at<=created_at+interval '1 hour'),
    CHECK (cancelled_at IS NULL OR cancelled_at>=created_at)
);

CREATE TABLE memory_trial_parts (
    bot_id text NOT NULL,
    query_id text NOT NULL,
    query_part_index integer NOT NULL CHECK (query_part_index BETWEEN 0 AND 255),
    part_id text NOT NULL CHECK (octet_length(part_id) BETWEEN 1 AND 256),
    modality text NOT NULL CHECK (modality IN ('TEXT','IMAGE')),
    media_type text NOT NULL CHECK (octet_length(media_type) BETWEEN 1 AND 128),
    byte_length integer NOT NULL CHECK (byte_length BETWEEN 1 AND 8388608),
    width integer CHECK (width IS NULL OR width BETWEEN 1 AND 100000),
    height integer CHECK (height IS NULL OR height BETWEEN 1 AND 100000),
    payload_id text NOT NULL CHECK (octet_length(payload_id) BETWEEN 1 AND 256),
    payload_digest text NOT NULL CHECK (payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    payload_owner_component_id text NOT NULL
        CHECK (octet_length(payload_owner_component_id) BETWEEN 1 AND 128),
    payload_owner_kind text NOT NULL
        CHECK (octet_length(payload_owner_kind) BETWEEN 1 AND 128),
    payload_owner_id text NOT NULL
        CHECK (payload_owner_id = 'trial:' || encode(sha256(
            convert_to(query_id, 'UTF8') || decode('00', 'hex') ||
            convert_to(part_id, 'UTF8')), 'hex')),
    PRIMARY KEY (bot_id,query_id,query_part_index),
    UNIQUE (bot_id,query_id,part_id),
    FOREIGN KEY (bot_id,query_id) REFERENCES memory_trial_inputs(bot_id,query_id),
    CHECK ((modality='TEXT' AND width IS NULL AND height IS NULL)
        OR (modality='IMAGE' AND width IS NOT NULL AND height IS NOT NULL))
);

CREATE INDEX memory_trial_inputs_expiry_idx ON memory_trial_inputs(bot_id,expires_at,query_id);

CREATE TABLE memory_trial_gc_cursors (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    last_expires_at timestamptz,
    last_query_id text,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((last_expires_at IS NULL) = (last_query_id IS NULL))
);

CREATE TABLE facet_preparation_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    target_kind text NOT NULL CHECK (target_kind IN ('SOURCE_UNIT','QUERY')),
    target_id text NOT NULL CHECK (octet_length(target_id) BETWEEN 1 AND 256),
    batch_index integer NOT NULL CHECK (batch_index BETWEEN 0 AND 65535),
    facet_count integer NOT NULL CHECK (
        facet_count BETWEEN 0 AND 64 AND (target_kind='QUERY' OR facet_count>0)
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    UNIQUE (bot_id,target_kind,target_id,batch_index)
);

CREATE TABLE query_facet_preparations (
    bot_id text NOT NULL,
    query_id text NOT NULL,
    facet_count integer NOT NULL CHECK (facet_count BETWEEN 0 AND 256),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch>=0),
    completed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,query_id),
    FOREIGN KEY (bot_id,query_id) REFERENCES memory_queries(bot_id,query_id)
);

CREATE TABLE query_recall_work (
    bot_id text NOT NULL,
    query_id text NOT NULL,
    deadline_at timestamptz NOT NULL,
    state text NOT NULL DEFAULT 'PENDING'
        CHECK (state IN ('PENDING','CLAIMED','BLOCKED_UNKNOWN','READY','FAILED','CANCELLED')),
    cancel_requested boolean NOT NULL DEFAULT false,
    cancel_reason text CHECK (cancel_reason IS NULL OR cancel_reason IN ('SUPERSEDED','ADMIN_CANCELLED')),
    claim_token text UNIQUE,
    claim_payload jsonb,
    lease_deadline timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count>=0),
    failure_count integer NOT NULL DEFAULT 0 CHECK (failure_count BETWEEN 0 AND 3),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,query_id),
    FOREIGN KEY (bot_id,query_id) REFERENCES memory_queries(bot_id,query_id),
    CHECK ((state='CLAIMED') = (claim_token IS NOT NULL)),
    CHECK ((state='CLAIMED') = (claim_payload IS NOT NULL)),
    CHECK ((state='CLAIMED') = (lease_deadline IS NOT NULL)),
    CHECK ((state='FAILED') = (last_error_code IS NOT NULL)),
    CHECK (cancel_requested = (cancel_reason IS NOT NULL)),
    CHECK (state<>'CANCELLED' OR cancel_requested)
);

CREATE INDEX query_recall_work_due_idx ON query_recall_work(bot_id,next_attempt_at,query_id)
    WHERE state IN ('PENDING','CLAIMED','BLOCKED_UNKNOWN');

CREATE TABLE query_recall_settlements (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    claim_token text NOT NULL,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    settled_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,claim_token)
);

CREATE TABLE facet_preparation_work (
    bot_id text NOT NULL,
    source_unit_id text NOT NULL,
    state text NOT NULL DEFAULT 'PENDING'
        CHECK (state IN ('PENDING','CLAIMED','READY','SUPPRESSED','FAILED')),
    claim_token text UNIQUE,
    lease_deadline timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    prepared_facet_count integer CHECK (
        prepared_facet_count IS NULL OR prepared_facet_count BETWEEN 0 AND 4096
    ),
    last_error_code text,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,source_unit_id),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    CHECK ((state='CLAIMED') = (claim_token IS NOT NULL AND lease_deadline IS NOT NULL)),
    CHECK ((state='READY') = (prepared_facet_count IS NOT NULL))
);

CREATE TABLE facet_preparation_settlements (
    bot_id text NOT NULL,
    claim_token text NOT NULL,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    settled_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,claim_token)
);

CREATE TABLE embedding_facets (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    facet_id text NOT NULL CHECK (octet_length(facet_id) BETWEEN 1 AND 256),
    purpose text NOT NULL CHECK (purpose IN ('DOCUMENT','QUERY')),
    query_id text,
    input_kind text NOT NULL CHECK (input_kind IN ('SOURCE_UNIT','ADMIN_TRIAL')),
    input_ref text NOT NULL CHECK (octet_length(input_ref) BETWEEN 1 AND 256),
    input_revision bigint NOT NULL CHECK (input_revision > 0),
    source_unit_id text,
    source_revision bigint CHECK (source_revision > 0),
    trial_query_id text,
    part_id text NOT NULL CHECK (octet_length(part_id) BETWEEN 1 AND 256),
    modality text NOT NULL CHECK (modality IN ('TEXT','IMAGE')),
    selector_start_char integer,
    selector_end_char integer,
    input_payload_id text NOT NULL CHECK (octet_length(input_payload_id) BETWEEN 1 AND 256),
    input_payload_digest text NOT NULL CHECK (
        input_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    facet_content_digest text NOT NULL CHECK (
        facet_content_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    input_media_type text NOT NULL CHECK (octet_length(input_media_type) BETWEEN 1 AND 128),
    input_byte_length bigint NOT NULL CHECK (input_byte_length BETWEEN 1 AND 33554432),
    space_id text NOT NULL CHECK (octet_length(space_id) BETWEEN 1 AND 256),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch >= 0),
    state text NOT NULL DEFAULT 'PENDING' CHECK (
        state IN ('PENDING','PREPARED','DISPATCHED','EMBEDDED','READY','UNKNOWN','FAILED','SUPPRESSED')
    ),
    failure_code text CHECK (failure_code IS NULL OR
        failure_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    current_attempt_id text,
    index_ref_id text CHECK (index_ref_id IS NULL OR octet_length(index_ref_id) BETWEEN 1 AND 256),
    vector_digest text CHECK (vector_digest IS NULL OR vector_digest ~ '^sha256:[a-f0-9]{64}$'),
    result_payload_id text,
    result_payload_digest text CHECK (
        result_payload_digest IS NULL OR result_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    published_at timestamptz,
    published_xid xid8,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,facet_id),
    UNIQUE (bot_id,query_id,facet_id),
    FOREIGN KEY (bot_id,query_id) REFERENCES memory_queries(bot_id,query_id),
    FOREIGN KEY (bot_id,source_unit_id,part_id)
        REFERENCES source_parts(bot_id,source_unit_id,part_id),
    FOREIGN KEY (bot_id,trial_query_id,part_id)
        REFERENCES memory_trial_parts(bot_id,query_id,part_id),
    CHECK ((purpose='QUERY') = (query_id IS NOT NULL)),
    CHECK ((input_kind='SOURCE_UNIT' AND source_unit_id IS NOT NULL
            AND source_revision IS NOT NULL AND trial_query_id IS NULL
            AND input_ref=source_unit_id AND input_revision=source_revision)
        OR (input_kind='ADMIN_TRIAL' AND purpose='QUERY'
            AND source_unit_id IS NULL AND source_revision IS NULL
            AND trial_query_id=query_id AND input_ref=query_id)),
    CHECK (
        (modality='TEXT' AND selector_start_char IS NOT NULL
            AND selector_end_char IS NOT NULL AND selector_start_char < selector_end_char)
        OR (modality='IMAGE' AND selector_start_char IS NULL AND selector_end_char IS NULL)
    ),
    CHECK ((state='READY') = (index_ref_id IS NOT NULL)),
    CHECK ((state IN ('EMBEDDED','READY')) = (vector_digest IS NOT NULL)),
    CHECK ((state IN ('EMBEDDED','READY')) = (result_payload_id IS NOT NULL)),
    CHECK ((state IN ('EMBEDDED','READY')) = (result_payload_digest IS NOT NULL)),
    CHECK ((state='READY') = (published_at IS NOT NULL)),
    CHECK ((state='READY') = (published_xid IS NOT NULL)),
    CHECK (failure_code IS NULL OR state IN ('FAILED','SUPPRESSED')),
    UNIQUE (bot_id,index_ref_id)
);

CREATE TABLE query_match_preparations (
    bot_id text NOT NULL,
    query_id text NOT NULL,
    operation_id text NOT NULL,
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    space_id text NOT NULL,
    policy jsonb NOT NULL CHECK (jsonb_typeof(policy)='object'),
    facet_count integer NOT NULL CHECK (facet_count BETWEEN 0 AND 256),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch>=0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,query_id),
    FOREIGN KEY (bot_id,query_id) REFERENCES memory_queries(bot_id,query_id),
    FOREIGN KEY (bot_id,space_id) REFERENCES memory_domains(bot_id,space_id)
);

CREATE TABLE query_match_receipts (
    bot_id text NOT NULL,
    query_id text NOT NULL,
    query_facet_id text NOT NULL,
    operation_id text NOT NULL,
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch>=0),
    match_count integer NOT NULL CHECK (match_count BETWEEN 0 AND 256),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,query_id,query_facet_id),
    FOREIGN KEY (bot_id,query_id) REFERENCES query_match_preparations(bot_id,query_id),
    FOREIGN KEY (bot_id,query_id,query_facet_id)
        REFERENCES embedding_facets(bot_id,query_id,facet_id)
);

CREATE TABLE query_seed_matches (
    bot_id text NOT NULL,
    query_id text NOT NULL,
    query_facet_id text NOT NULL,
    source_unit_id text NOT NULL,
    source_revision bigint NOT NULL,
    document_facet_id text NOT NULL,
    candidate_rank integer NOT NULL CHECK (candidate_rank BETWEEN 1 AND 256),
    score double precision NOT NULL CHECK (score>'-Infinity'::double precision
        AND score<'Infinity'::double precision),
    PRIMARY KEY (bot_id,query_id,query_facet_id,source_unit_id),
    FOREIGN KEY (bot_id,query_id,query_facet_id)
        REFERENCES query_match_receipts(bot_id,query_id,query_facet_id),
    FOREIGN KEY (bot_id,document_facet_id) REFERENCES embedding_facets(bot_id,facet_id),
    FOREIGN KEY (bot_id,source_unit_id,source_revision)
        REFERENCES source_units(bot_id,source_unit_id,source_revision)
);

CREATE TABLE embedding_claims (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    claim_id text NOT NULL CHECK (octet_length(claim_id) BETWEEN 1 AND 256),
    purpose text NOT NULL CHECK (purpose IN ('DOCUMENT','QUERY')),
    space_id text NOT NULL CHECK (octet_length(space_id) BETWEEN 1 AND 256),
    source_epoch_high_watermark bigint NOT NULL CHECK (source_epoch_high_watermark >= 0),
    lease_seconds integer NOT NULL CHECK (lease_seconds BETWEEN 3 AND 86400),
    lease_deadline timestamptz NOT NULL,
    item_limit integer NOT NULL CHECK (item_limit BETWEEN 1 AND 32),
    item_count integer NOT NULL CHECK (item_count BETWEEN 0 AND item_limit),
    claim_payload jsonb NOT NULL,
    state text NOT NULL DEFAULT 'CLAIMED' CHECK (
        state IN ('CLAIMED','ATTEMPT_BOUND','EXPIRED','SETTLED')
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    settled_at timestamptz,
    PRIMARY KEY (bot_id,claim_id),
    CHECK ((state='SETTLED') = (settled_at IS NOT NULL))
);

CREATE TABLE embedding_claim_items (
    bot_id text NOT NULL,
    claim_id text NOT NULL,
    item_index integer NOT NULL CHECK (item_index BETWEEN 0 AND 31),
    facet_id text NOT NULL,
    PRIMARY KEY (bot_id,claim_id,item_index),
    UNIQUE (bot_id,claim_id,facet_id),
    FOREIGN KEY (bot_id,claim_id) REFERENCES embedding_claims(bot_id,claim_id),
    FOREIGN KEY (bot_id,facet_id) REFERENCES embedding_facets(bot_id,facet_id)
);

CREATE TABLE embedding_batch_attempts (
    bot_id text NOT NULL,
    attempt_id text NOT NULL CHECK (octet_length(attempt_id) BETWEEN 1 AND 256),
    claim_id text NOT NULL,
    effect_operation_id text NOT NULL CHECK (octet_length(effect_operation_id) BETWEEN 1 AND 256),
    executor_component_id text NOT NULL
        CHECK (octet_length(executor_component_id) BETWEEN 1 AND 128),
    executor_generation_id text NOT NULL
        CHECK (octet_length(executor_generation_id) BETWEEN 1 AND 256),
    fence_graph_id text NOT NULL CHECK (octet_length(fence_graph_id) BETWEEN 1 AND 256),
    fence_graph_revision bigint NOT NULL CHECK (fence_graph_revision > 0),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch >= 0),
    effect_wire_payload_id text NOT NULL
        CHECK (octet_length(effect_wire_payload_id) BETWEEN 1 AND 256),
    effect_wire_payload_digest text NOT NULL CHECK (
        effect_wire_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    reservation_id text NOT NULL CHECK (octet_length(reservation_id) BETWEEN 1 AND 256),
    deadline_at timestamptz NOT NULL,
    state text NOT NULL DEFAULT 'PREPARED' CHECK (
        state IN ('PREPARED','DISPATCHED','SUCCEEDED','NOT_SENT','UNKNOWN','FAILED')
    ),
    handoff_operation_id text CHECK (
        handoff_operation_id IS NULL OR octet_length(handoff_operation_id) BETWEEN 1 AND 256
    ),
    settlement_operation_id text CHECK (
        settlement_operation_id IS NULL
        OR octet_length(settlement_operation_id) BETWEEN 1 AND 256
    ),
    settlement_input_digest text CHECK (
        settlement_input_digest IS NULL
        OR settlement_input_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    result_payload_id text CHECK (
        result_payload_id IS NULL OR octet_length(result_payload_id) BETWEEN 1 AND 256
    ),
    result_payload_digest text CHECK (
        result_payload_digest IS NULL OR result_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    error_code text CHECK (error_code IS NULL OR octet_length(error_code) BETWEEN 1 AND 128),
    prepared_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    dispatched_at timestamptz,
    settled_at timestamptz,
    PRIMARY KEY (bot_id,attempt_id),
    UNIQUE (bot_id,claim_id),
    UNIQUE (bot_id,effect_operation_id),
    FOREIGN KEY (bot_id,claim_id) REFERENCES embedding_claims(bot_id,claim_id),
    CHECK (state<>'PREPARED' OR handoff_operation_id IS NULL),
    CHECK (state<>'PREPARED' OR dispatched_at IS NULL),
    CHECK (
        state NOT IN ('DISPATCHED','SUCCEEDED','UNKNOWN','FAILED')
        OR (handoff_operation_id IS NOT NULL AND dispatched_at IS NOT NULL)
    ),
    CHECK ((state IN ('PREPARED','DISPATCHED')) = (settlement_operation_id IS NULL)),
    CHECK ((state IN ('PREPARED','DISPATCHED')) = (settlement_input_digest IS NULL)),
    CHECK ((state IN ('PREPARED','DISPATCHED')) = (settled_at IS NULL)),
    CHECK ((state='SUCCEEDED') = (result_payload_id IS NOT NULL)),
    CHECK ((state='SUCCEEDED') = (result_payload_digest IS NOT NULL))
);

CREATE TABLE embedding_recovery_work (
    bot_id text NOT NULL,
    claim_id text NOT NULL,
    state text NOT NULL DEFAULT 'PENDING'
        CHECK (state IN ('PENDING','CLAIMED','SETTLED')),
    claim_token text CHECK (claim_token IS NULL OR octet_length(claim_token) BETWEEN 1 AND 256),
    claim_payload jsonb,
    lease_deadline timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,claim_id),
    UNIQUE (bot_id,claim_token),
    FOREIGN KEY (bot_id,claim_id) REFERENCES embedding_claims(bot_id,claim_id),
    CHECK ((state='CLAIMED') =
        (claim_token IS NOT NULL AND claim_payload IS NOT NULL AND lease_deadline IS NOT NULL))
);

CREATE TABLE embedding_work_settlements (
    bot_id text NOT NULL,
    claim_token text NOT NULL,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    PRIMARY KEY (bot_id,claim_token)
);

ALTER TABLE embedding_facets ADD CONSTRAINT embedding_facet_current_attempt_fk
    FOREIGN KEY (bot_id,current_attempt_id)
    REFERENCES embedding_batch_attempts(bot_id,attempt_id);

CREATE TABLE embedding_budget_days (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    budget_day date NOT NULL,
    reserved_requests integer NOT NULL DEFAULT 0 CHECK (reserved_requests >= 0),
    settled_requests bigint NOT NULL DEFAULT 0 CHECK (settled_requests >= 0),
    unknown_requests bigint NOT NULL DEFAULT 0 CHECK (unknown_requests >= 0),
    reserved_tokens bigint NOT NULL DEFAULT 0 CHECK (reserved_tokens >= 0),
    charged_tokens bigint NOT NULL DEFAULT 0 CHECK (charged_tokens >= 0),
    unknown_tokens bigint NOT NULL DEFAULT 0 CHECK (unknown_tokens >= 0),
    revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,budget_day)
);

CREATE TABLE embedding_budget_reservations (
    bot_id text NOT NULL,
    reservation_id text NOT NULL CHECK (octet_length(reservation_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    batch_ref text NOT NULL CHECK (octet_length(batch_ref) BETWEEN 1 AND 256),
    policy_revision text NOT NULL CHECK (octet_length(policy_revision) BETWEEN 1 AND 256),
    budget_day date NOT NULL,
    request_limit integer NOT NULL CHECK (request_limit BETWEEN 1 AND 1000000),
    token_limit bigint NOT NULL CHECK (token_limit BETWEEN 1 AND 1000000000000),
    reserved_tokens bigint NOT NULL CHECK (reserved_tokens BETWEEN 1 AND 1000000000),
    deadline_at timestamptz NOT NULL,
    state text NOT NULL DEFAULT 'RESERVED' CHECK (
        state IN ('RESERVED','RELEASED','CHARGED','UNKNOWN')
    ),
    settlement_operation_id text,
    settlement_input_digest text CHECK (
        settlement_input_digest IS NULL OR settlement_input_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    charged_tokens bigint CHECK (charged_tokens IS NULL OR charged_tokens >= 0),
    error_code text CHECK (error_code IS NULL OR octet_length(error_code) BETWEEN 1 AND 128),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    settled_at timestamptz,
    PRIMARY KEY (bot_id,reservation_id),
    UNIQUE (bot_id,operation_id),
    UNIQUE (bot_id,batch_ref),
    UNIQUE (bot_id,reservation_id,batch_ref),
    UNIQUE (bot_id,settlement_operation_id),
    FOREIGN KEY (bot_id,budget_day) REFERENCES embedding_budget_days(bot_id,budget_day),
    CHECK ((state='RESERVED') = (settled_at IS NULL)),
    CHECK ((state='RESERVED') = (settlement_operation_id IS NULL)),
    CHECK ((state='RESERVED') = (settlement_input_digest IS NULL)),
    CHECK ((state='CHARGED') = (charged_tokens IS NOT NULL))
);

ALTER TABLE embedding_batch_attempts ADD CONSTRAINT embedding_batch_reservation_fk
    FOREIGN KEY (bot_id,reservation_id,claim_id)
    REFERENCES embedding_budget_reservations(bot_id,reservation_id,batch_ref);

CREATE TABLE embedding_unknown_retry_operations (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    attempt_id text NOT NULL,
    requested_by text NOT NULL CHECK (octet_length(requested_by) BETWEEN 1 AND 256),
    result_payload jsonb NOT NULL CHECK (jsonb_typeof(result_payload)='object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    UNIQUE (bot_id,attempt_id),
    FOREIGN KEY (bot_id,attempt_id) REFERENCES embedding_batch_attempts(bot_id,attempt_id)
);

CREATE TABLE retrieval_bundles (
    bot_id text NOT NULL,
    bundle_id text NOT NULL CHECK (octet_length(bundle_id) BETWEEN 1 AND 256),
    query_id text NOT NULL,
    operation_id text NOT NULL,
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch>=0),
    read_count integer NOT NULL CHECK (read_count BETWEEN 0 AND 64),
    truncated boolean NOT NULL,
    association_sampled boolean NOT NULL,
    seed_count integer NOT NULL CHECK (seed_count BETWEEN 0 AND 64),
    result_count integer NOT NULL CHECK (result_count BETWEEN seed_count AND 64),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,bundle_id),
    UNIQUE (bot_id,query_id),
    FOREIGN KEY (bot_id,query_id) REFERENCES query_match_preparations(bot_id,query_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (bot_id,query_id,bundle_id)
);

CREATE TABLE retrieval_items (
    bot_id text NOT NULL,
    bundle_id text NOT NULL,
    item_index integer NOT NULL CHECK (item_index BETWEEN 0 AND 63),
    source_unit_id text NOT NULL,
    source_revision bigint NOT NULL,
    rank_score double precision NOT NULL CHECK (rank_score>=0 AND rank_score<=256),
    activation double precision NOT NULL CHECK (activation>0 AND activation<=1),
    is_seed boolean NOT NULL,
    path_parent_source_unit_id text,
    path_edge_id text,
    PRIMARY KEY (bot_id,bundle_id,item_index),
    UNIQUE (bot_id,bundle_id,source_unit_id),
    FOREIGN KEY (bot_id,bundle_id) REFERENCES retrieval_bundles(bot_id,bundle_id),
    FOREIGN KEY (bot_id,source_unit_id,source_revision)
        REFERENCES source_units(bot_id,source_unit_id,source_revision),
    FOREIGN KEY (bot_id,bundle_id,path_parent_source_unit_id)
        REFERENCES retrieval_items(bot_id,bundle_id,source_unit_id),
    CHECK ((path_parent_source_unit_id IS NULL) = (path_edge_id IS NULL)),
    CHECK (is_seed = (path_parent_source_unit_id IS NULL))
);

ALTER TABLE memory_queries ADD CONSTRAINT frozen_query_bundle_fk
    FOREIGN KEY (bot_id,query_id,bundle_id) REFERENCES retrieval_bundles(bot_id,query_id,bundle_id);

CREATE TABLE retrieval_materializations (
    bot_id text NOT NULL,
    bundle_id text NOT NULL,
    evidence_ref jsonb NOT NULL CHECK (jsonb_typeof(evidence_ref)='object'),
    item_indexes integer[] NOT NULL CHECK (cardinality(item_indexes)<=64),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch>=0),
    content_truncated boolean NOT NULL,
    missing_count integer NOT NULL CHECK (missing_count BETWEEN 0 AND 64),
    projected_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,bundle_id),
    FOREIGN KEY (bot_id,bundle_id) REFERENCES retrieval_bundles(bot_id,bundle_id)
);

CREATE TABLE retrieval_relations (
    bot_id text NOT NULL,
    bundle_id text NOT NULL,
    relation_index integer NOT NULL CHECK (relation_index BETWEEN 0 AND 63),
    edge_id text NOT NULL,
    relation_kind text NOT NULL CHECK (relation_kind IN ('SEQUENCE','ASSOCIATED')),
    source_unit_id text NOT NULL,
    target_unit_id text NOT NULL,
    weight double precision NOT NULL CHECK (weight>0 AND weight<=1),
    contribution_id text,
    PRIMARY KEY (bot_id,bundle_id,relation_index),
    UNIQUE (bot_id,bundle_id,edge_id,source_unit_id,target_unit_id),
    FOREIGN KEY (bot_id,bundle_id,source_unit_id)
        REFERENCES retrieval_items(bot_id,bundle_id,source_unit_id),
    FOREIGN KEY (bot_id,bundle_id,target_unit_id)
        REFERENCES retrieval_items(bot_id,bundle_id,source_unit_id),
    FOREIGN KEY (bot_id,contribution_id) REFERENCES relation_contributions(bot_id,contribution_id),
    CHECK (source_unit_id<>target_unit_id),
    CHECK ((relation_kind='ASSOCIATED') = (contribution_id IS NOT NULL))
);

CREATE TABLE association_journal (
    bot_id text NOT NULL,
    journal_id text NOT NULL CHECK (octet_length(journal_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    reply_ref text NOT NULL CHECK (octet_length(reply_ref) BETWEEN 1 AND 512),
    bundle_id text NOT NULL,
    query_id text NOT NULL,
    trigger_ref text NOT NULL,
    trigger_source_unit_id text NOT NULL,
    trigger_source_revision bigint NOT NULL CHECK (trigger_source_revision>0),
    trigger_part_id text NOT NULL,
    seed_source_unit_id text NOT NULL,
    seed_source_revision bigint NOT NULL CHECK (seed_source_revision>0),
    seed_part_id text NOT NULL,
    seed_rank_score double precision NOT NULL CHECK (seed_rank_score >= 0 AND seed_rank_score <= 256),
    promotion_ready boolean NOT NULL DEFAULT false,
    claim_token text CHECK (claim_token IS NULL OR octet_length(claim_token) BETWEEN 1 AND 256),
    lease_deadline timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    state text NOT NULL DEFAULT 'PENDING_ARCHIVE' CHECK (
        state IN ('PENDING_ARCHIVE','PROMOTED','SUPPRESSED')
    ),
    contribution_id text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    promoted_at timestamptz,
    PRIMARY KEY (bot_id,journal_id),
    UNIQUE (bot_id,operation_id,bundle_id,trigger_source_unit_id,seed_source_unit_id),
    FOREIGN KEY (bot_id,bundle_id) REFERENCES retrieval_bundles(bot_id,bundle_id),
    FOREIGN KEY (bot_id,trigger_source_unit_id)
        REFERENCES source_units(bot_id,source_unit_id),
    FOREIGN KEY (bot_id,seed_source_unit_id)
        REFERENCES source_units(bot_id,source_unit_id),
    FOREIGN KEY (bot_id,trigger_source_unit_id,trigger_source_revision)
        REFERENCES source_units(bot_id,source_unit_id,source_revision),
    FOREIGN KEY (bot_id,seed_source_unit_id,seed_source_revision)
        REFERENCES source_units(bot_id,source_unit_id,source_revision),
    FOREIGN KEY (bot_id,trigger_source_unit_id,trigger_part_id)
        REFERENCES source_parts(bot_id,source_unit_id,part_id),
    FOREIGN KEY (bot_id,seed_source_unit_id,seed_part_id)
        REFERENCES source_parts(bot_id,source_unit_id,part_id),
    CHECK (trigger_source_unit_id<>seed_source_unit_id),
    CHECK ((state='PROMOTED') = (contribution_id IS NOT NULL)),
    CHECK ((state='PROMOTED') = (promoted_at IS NOT NULL)),
    CHECK ((claim_token IS NULL) = (lease_deadline IS NULL))
);

CREATE TABLE association_promotion_settlements (
    bot_id text NOT NULL,
    claim_token text NOT NULL CHECK (octet_length(claim_token) BETWEEN 1 AND 256),
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    settled_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,claim_token)
);

CREATE UNIQUE INDEX association_journal_claim_token_uniq ON association_journal(
    bot_id,claim_token
) WHERE claim_token IS NOT NULL;

CREATE INDEX association_journal_ready_idx ON association_journal(
    bot_id,next_attempt_at,journal_id
) WHERE state='PENDING_ARCHIVE' AND promotion_ready;

CREATE INDEX association_journal_pending_trigger_idx ON association_journal(
    bot_id,trigger_source_unit_id,journal_id
) WHERE state='PENDING_ARCHIVE';

CREATE INDEX sources_external_group_idx ON memory_sources(
    bot_id,platform,realm_id,external_group_id,identity_incarnation
);
CREATE INDEX sources_group_browse_idx ON memory_sources(
    bot_id,external_group_id,source_id
) WHERE external_group_id IS NOT NULL;
CREATE UNIQUE INDEX sources_group_identity_uniq ON memory_sources(
    bot_id,platform,realm_id,external_group_id,identity_incarnation
) WHERE source_kind='GROUP';
CREATE INDEX source_units_browse_idx ON source_units(
    bot_id,eligibility_state,occurred_at DESC,source_unit_id DESC
);
CREATE INDEX source_units_trunk_idx ON source_units(
    bot_id,trunk_id,eligibility_state,occurred_at DESC,source_unit_id DESC
);

CREATE TABLE vector_index_deletions (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    index_ref_id text NOT NULL CHECK (octet_length(index_ref_id) BETWEEN 1 AND 256),
    space_id text NOT NULL CHECK (octet_length(space_id) BETWEEN 1 AND 256),
    deletion_epoch bigint NOT NULL CHECK (deletion_epoch >= 0),
    state text NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING','DELETED')),
    claim_token text CHECK (claim_token IS NULL OR octet_length(claim_token) BETWEEN 1 AND 256),
    claim_payload jsonb,
    lease_deadline timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    deleted_at timestamptz,
    PRIMARY KEY (bot_id,index_ref_id),
    UNIQUE (bot_id,claim_token),
    CHECK ((state='DELETED') = (deleted_at IS NOT NULL)),
    CHECK ((claim_token IS NULL) =
        (claim_payload IS NULL AND lease_deadline IS NULL))
);
CREATE TABLE vector_publish_batches (
    bot_id text NOT NULL,
    attempt_id text NOT NULL,
    state text NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING','CLAIMED','SETTLED')),
    claim_token text CHECK (claim_token IS NULL OR octet_length(claim_token) BETWEEN 1 AND 256),
    claim_payload jsonb,
    lease_deadline timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,attempt_id),
    UNIQUE (bot_id,claim_token),
    FOREIGN KEY (bot_id,attempt_id) REFERENCES embedding_batch_attempts(bot_id,attempt_id),
    CHECK ((state='CLAIMED') =
        (claim_token IS NOT NULL AND claim_payload IS NOT NULL AND lease_deadline IS NOT NULL))
);
CREATE TABLE vector_publish_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    result_payload jsonb NOT NULL,
    PRIMARY KEY (bot_id,operation_id)
);
CREATE TABLE vector_work_settlements (
    bot_id text NOT NULL,
    claim_token text NOT NULL,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    PRIMARY KEY (bot_id,claim_token)
);
CREATE INDEX source_units_author_idx ON source_units(
    bot_id,author_subject_ref,eligibility_state,occurred_at DESC,source_unit_id DESC
);
CREATE INDEX source_units_modality_idx ON source_units(
    bot_id,modality,eligibility_state,occurred_at DESC,source_unit_id DESC
);
CREATE INDEX source_units_kind_idx ON source_units(
    bot_id,unit_kind,eligibility_state,occurred_at DESC,source_unit_id DESC
);
CREATE INDEX eligibility_operations_batch_idx ON eligibility_operations(
    bot_id,input_batch_ref,context_revision,occurrence_id
);
CREATE INDEX source_occurrences_unit_idx ON source_occurrences(
    bot_id,source_unit_id,conversation_ref,ingress_seq
) INCLUDE (eligibility_state,input_batch_ref,context_revision);
CREATE INDEX source_occurrences_binding_browse_idx ON source_occurrences(
    bot_id,binding_ref,source_unit_id
);
CREATE INDEX pending_input_events_batch_idx ON pending_input_events(
    bot_id,input_batch_ref,context_revision,state,conversation_ref
);
CREATE INDEX relation_left_idx ON relation_contributions(
    bot_id,left_kind,left_id,active,relation_id,contribution_id
);
CREATE INDEX relation_right_idx ON relation_contributions(
    bot_id,right_kind,right_id,active,relation_id,contribution_id
);
CREATE INDEX relation_browse_idx ON relation_contributions(
    bot_id,relation_id,contribution_id
) WHERE active;
CREATE INDEX relation_cross_trunk_browse_idx ON relation_contributions(
    bot_id,cross_trunk,relation_id,contribution_id
) WHERE active;
CREATE INDEX relation_evidence_source_idx ON relation_evidence(
    bot_id,source_unit_id,contribution_id
);
CREATE INDEX relation_association_left_sample_idx ON relation_contributions(
    bot_id,left_id,sample_key,contribution_id
) WHERE left_kind='SOURCE_UNIT' AND right_kind='SOURCE_UNIT'
    AND relation_kind='ASSOCIATES_WITH' AND active;
CREATE INDEX relation_association_right_sample_idx ON relation_contributions(
    bot_id,right_id,sample_key,contribution_id
) WHERE left_kind='SOURCE_UNIT' AND right_kind='SOURCE_UNIT'
    AND relation_kind='ASSOCIATES_WITH' AND active;
CREATE INDEX embedding_facets_work_idx ON embedding_facets(
    bot_id,state,purpose,next_attempt_at,created_at,facet_id
);
CREATE INDEX facet_preparation_work_idx ON facet_preparation_work(
    bot_id,state,next_attempt_at,source_unit_id
);
CREATE INDEX facet_preparation_expired_idx ON facet_preparation_work(
    bot_id,lease_deadline,source_unit_id
) WHERE state='CLAIMED';
CREATE INDEX embedding_facets_source_idx ON embedding_facets(
    bot_id,source_unit_id,source_revision,part_id,purpose,state
);
CREATE INDEX embedding_facets_ready_source_idx ON embedding_facets(
    bot_id,source_unit_id
) WHERE purpose='DOCUMENT' AND state='READY';
CREATE INDEX embedding_facets_unknown_source_idx ON embedding_facets(
    bot_id,source_unit_id
) WHERE purpose='DOCUMENT' AND state='UNKNOWN';
CREATE INDEX embedding_facets_source_page_idx ON embedding_facets(
    bot_id,source_unit_id,source_revision,facet_id
) WHERE purpose='DOCUMENT';
CREATE INDEX embedding_facets_query_idx ON embedding_facets(
    bot_id,query_id,purpose,state,facet_id
);
CREATE INDEX embedding_facets_trial_expiry_idx ON embedding_facets(
    bot_id,state,trial_query_id,facet_id
) WHERE input_kind='ADMIN_TRIAL'
    AND state IN ('PENDING','PREPARED','DISPATCHED','EMBEDDED','READY','UNKNOWN','FAILED');
CREATE INDEX embedding_facets_snapshot_idx ON embedding_facets(
    bot_id,space_id,purpose,state,published_at,facet_id
);
CREATE INDEX embedding_batch_attempts_recovery_idx ON embedding_batch_attempts(
    bot_id,state,deadline_at,attempt_id
);
CREATE INDEX embedding_recovery_work_poll_idx ON embedding_recovery_work(
    bot_id,state,next_attempt_at,claim_id
);
CREATE INDEX vector_index_deletions_work_idx ON vector_index_deletions(
    bot_id,state,next_attempt_at,index_ref_id
);
CREATE INDEX vector_publish_batches_poll_idx ON vector_publish_batches(
    bot_id,state,next_attempt_at,attempt_id
);
CREATE INDEX embedding_claims_created_idx ON embedding_claims(
    bot_id,purpose,created_at,claim_id
);
CREATE INDEX embedding_claims_active_idx ON embedding_claims(
    bot_id,state,lease_deadline,claim_id
);
CREATE INDEX embedding_claim_items_facet_idx ON embedding_claim_items(
    bot_id,facet_id,claim_id
);
CREATE INDEX embedding_budget_reservations_state_idx ON embedding_budget_reservations(
    bot_id,state,budget_day,reservation_id
);

CREATE FUNCTION memory_trial_gc_context() RETURNS jsonb LANGUAGE sql STABLE AS $$
    SELECT coalesce(nullif(current_setting('forest.memory_trial_gc',true),'')::jsonb,'{}'::jsonb)
$$;

CREATE FUNCTION reject_immutable_row() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE gc jsonb := __forest_module_schema__.memory_trial_gc_context();
        fact jsonb := to_jsonb(OLD);
BEGIN
    IF TG_OP='DELETE' AND fact->>'bot_id'=gc->>'bot_id' AND (
        (TG_TABLE_NAME IN ('query_facet_preparations','query_match_preparations',
            'query_match_receipts','query_seed_matches') AND fact->>'query_id'=gc->>'query_id')
        OR (TG_TABLE_NAME='facet_preparation_operations'
            AND fact->>'target_kind'='QUERY' AND fact->>'target_id'=gc->>'query_id')
        OR (TG_TABLE_NAME='embedding_claim_items'
            AND gc->'claim_ids' ? (fact->>'claim_id') AND EXISTS (
                SELECT 1 FROM __forest_module_schema__.embedding_facets f
                WHERE f.bot_id=fact->>'bot_id' AND f.facet_id=fact->>'facet_id'
                  AND f.input_kind='ADMIN_TRIAL' AND f.query_id=gc->>'query_id'
            ))
        OR (TG_TABLE_NAME='embedding_work_settlements'
            AND gc->'claim_ids' ? (fact->>'claim_token'))
        OR (TG_TABLE_NAME='query_recall_settlements'
            AND fact#>>'{input_payload,payload,query_id}'=gc->>'query_id')
        OR (TG_TABLE_NAME='retrieval_bundles'
            AND fact->>'query_id'=gc->>'query_id' AND fact->>'bundle_id'=gc->>'bundle_id')
        OR (TG_TABLE_NAME IN ('retrieval_items','retrieval_materializations',
            'retrieval_relations') AND fact->>'bundle_id'=gc->>'bundle_id')
    ) THEN RETURN OLD; END IF;
    RAISE EXCEPTION 'memory.graph immutable fact cannot change' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_forget_selection_member() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM __forest_module_schema__.forget_selections
        WHERE bot_id=OLD.bot_id AND selection_id=OLD.selection_id
    ) THEN
        RAISE EXCEPTION 'memory.graph frozen selection member cannot change'
            USING ERRCODE='55000';
    END IF;
    RETURN OLD;
END;
$$;

CREATE FUNCTION memory_start_management_trial(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_query text := input->>'query_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'input_digest';
    requested_actor text := input->>'requested_by';
    domain_row __forest_module_schema__.memory_domains%ROWTYPE;
    query_row __forest_module_schema__.memory_queries%ROWTYPE;
    trial_row __forest_module_schema__.memory_trial_inputs%ROWTYPE;
    part_value jsonb;
    part_index bigint;
    text_count integer := 0;
    image_count integer := 0;
    total_bytes bigint := 0;
    expiry_value timestamptz;
    deadline_value timestamptz;
BEGIN
    IF requested_query IS NULL OR octet_length(requested_query) NOT BETWEEN 1 AND 256
       OR requested_operation IS NULL OR octet_length(requested_operation) NOT BETWEEN 1 AND 256
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR requested_actor IS NULL OR octet_length(requested_actor) NOT BETWEEN 1 AND 256
       OR context->>'generation_id' IS NULL
       OR jsonb_typeof(input->'parts') IS DISTINCT FROM 'array' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_TRIAL_INPUT_INVALID',
                'message','Management trial input is invalid','status',422));
    END IF;
    IF jsonb_array_length(input->'parts') NOT BETWEEN 1 AND 8 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_TRIAL_INPUT_INVALID',
                'message','Management trial requires one to eight parts','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/query/'||requested_bot||'/'||requested_query,0));
    SELECT * INTO query_row FROM __forest_module_schema__.memory_queries
    WHERE bot_id=requested_bot AND query_id=requested_query;
    IF FOUND THEN
        SELECT * INTO trial_row FROM __forest_module_schema__.memory_trial_inputs
        WHERE bot_id=requested_bot AND query_id=requested_query;
        IF query_row.input_kind<>'ADMIN_TRIAL'
           OR query_row.operation_id IS DISTINCT FROM requested_operation
           OR query_row.input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_TRIAL_IDENTITY_CONFLICT',
                    'message','Trial identity was bound to another request','status',409));
        END IF;
        IF trial_row.generation_id IS DISTINCT FROM context->>'generation_id'
           OR trial_row.expires_at<=clock_timestamp() THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_TRIAL_INPUT_UNAVAILABLE',
                    'message','Trial input has expired or its Generation was replaced','status',410));
        END IF;
    ELSE
        IF EXISTS (SELECT 1 FROM __forest_module_schema__.memory_queries
            WHERE bot_id=requested_bot AND operation_id=requested_operation) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_TRIAL_IDENTITY_CONFLICT',
                    'message','Trial operation was bound to another query','status',409));
        END IF;
        SELECT * INTO domain_row FROM __forest_module_schema__.memory_domains
        WHERE bot_id=requested_bot FOR SHARE;
        IF NOT FOUND THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_DOMAIN_UNBOUND',
                    'message','Trial requires a bound embedding domain','status',409));
        END IF;
        IF EXISTS (
            SELECT 1 FROM jsonb_array_elements(input->'parts') p
            WHERE jsonb_typeof(p) IS DISTINCT FROM 'object'
               OR p->>'part_id' IS NULL OR octet_length(p->>'part_id') NOT BETWEEN 1 AND 256
               OR p->>'modality' NOT IN ('TEXT','IMAGE')
               OR p->>'byte_length' !~ '^[1-9][0-9]*$'
               OR (p->>'byte_length')::bigint>8388608
               OR p#>>'{payload,owner_component_id}' IS DISTINCT FROM 'memory.graph'
               OR p#>>'{payload,owner_kind}' IS DISTINCT FROM 'trial.input'
               OR p#>>'{payload,owner_id}' IS DISTINCT FROM
                  'trial:' || encode(sha256(
                      convert_to(requested_query, 'UTF8') || decode('00', 'hex') ||
                      convert_to(p->>'part_id', 'UTF8')), 'hex')
               OR (p->>'modality'='TEXT' AND (
                   p->>'media_type'<>'text/plain' OR p->>'width' IS NOT NULL
                   OR p->>'height' IS NOT NULL OR (p->>'byte_length')::bigint>65536))
               OR (p->>'modality'='IMAGE' AND (
                   p->>'media_type' NOT IN ('image/png','image/jpeg','image/webp')
                   OR p->>'width' !~ '^[1-9][0-9]*$'
                   OR p->>'height' !~ '^[1-9][0-9]*$'
                   OR (p->>'width')::integer>100000 OR (p->>'height')::integer>100000))
        ) OR EXISTS (
            SELECT 1 FROM jsonb_array_elements(input->'parts') p
            GROUP BY p->>'part_id' HAVING count(*)>1
        ) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_TRIAL_INPUT_INVALID',
                    'message','Trial parts violate modality, size or PayloadRef bounds','status',422));
        END IF;
        SELECT count(*) FILTER (WHERE p->>'modality'='TEXT'),
               count(*) FILTER (WHERE p->>'modality'='IMAGE'),
               sum((p->>'byte_length')::bigint)
        INTO text_count,image_count,total_bytes
        FROM jsonb_array_elements(input->'parts') p;
        IF total_bytes>33554432 THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_TRIAL_INPUT_INVALID',
                    'message','Trial parts exceed the aggregate byte budget','status',422));
        END IF;
        expiry_value := clock_timestamp()+interval '10 minutes';
        deadline_value := clock_timestamp()+interval '8 minutes';
        INSERT INTO __forest_module_schema__.memory_queries(
            bot_id,query_id,operation_id,input_digest,input_kind,query_mode,
            fusion_policy_revision,accepted_deletion_epoch)
        VALUES(requested_bot,requested_query,requested_operation,requested_digest,
            'ADMIN_TRIAL',CASE WHEN text_count=0 THEN 'IMAGE'
                WHEN image_count=0 THEN 'TEXT' ELSE 'MIXED' END,
            domain_row.fusion_policy_revision,0);
        INSERT INTO __forest_module_schema__.memory_trial_inputs(
            bot_id,query_id,actor_subject_ref,generation_id,expires_at)
        VALUES(requested_bot,requested_query,requested_actor,context->>'generation_id',expiry_value);
        FOR part_value,part_index IN
            SELECT value,ordinality FROM jsonb_array_elements(input->'parts') WITH ORDINALITY
        LOOP
            INSERT INTO __forest_module_schema__.memory_trial_parts(
                bot_id,query_id,query_part_index,part_id,modality,media_type,byte_length,
                width,height,payload_id,payload_digest,payload_owner_component_id,
                payload_owner_kind,payload_owner_id)
            VALUES(requested_bot,requested_query,(part_index-1)::integer,
                part_value->>'part_id',part_value->>'modality',part_value->>'media_type',
                (part_value->>'byte_length')::integer,(part_value->>'width')::integer,
                (part_value->>'height')::integer,part_value#>>'{payload,payload_id}',
                part_value#>>'{payload,digest}',part_value#>>'{payload,owner_component_id}',
                part_value#>>'{payload,owner_kind}',part_value#>>'{payload,owner_id}');
        END LOOP;
        INSERT INTO __forest_module_schema__.query_recall_work(bot_id,query_id,deadline_at)
        VALUES(requested_bot,requested_query,deadline_value);
        SELECT * INTO trial_row FROM __forest_module_schema__.memory_trial_inputs
        WHERE bot_id=requested_bot AND query_id=requested_query;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-trial-started/1',
            'query_id',requested_query,'expires_at',trial_row.expires_at,
            'deadline_at',(SELECT deadline_at FROM __forest_module_schema__.query_recall_work
                WHERE bot_id=requested_bot AND query_id=requested_query)));
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_TRIAL_INPUT_INVALID',
            'message','Management trial input is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_read_management_trial(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    query_row __forest_module_schema__.memory_queries%ROWTYPE;
    trial_row __forest_module_schema__.memory_trial_inputs%ROWTYPE;
    work_row __forest_module_schema__.query_recall_work%ROWTYPE;
    evidence_reference jsonb;
    work_value jsonb;
BEGIN
    SELECT * INTO query_row FROM __forest_module_schema__.memory_queries
    WHERE bot_id=context->>'bot_id' AND query_id=input->>'query_id'
      AND input_kind='ADMIN_TRIAL';
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_TRIAL_NOT_FOUND',
                'message','Management trial does not exist','status',404));
    END IF;
    SELECT * INTO trial_row FROM __forest_module_schema__.memory_trial_inputs
    WHERE bot_id=query_row.bot_id AND query_id=query_row.query_id;
    IF NOT FOUND OR trial_row.actor_subject_ref IS DISTINCT FROM input->>'requested_by' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_TRIAL_NOT_FOUND',
                'message','Management trial does not exist','status',404));
    END IF;
    IF trial_row.generation_id IS DISTINCT FROM context->>'generation_id'
       OR trial_row.expires_at<=clock_timestamp() THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_TRIAL_INPUT_UNAVAILABLE',
                'message','Trial input has expired or its Generation was replaced','status',410));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.query_recall_work
    WHERE bot_id=query_row.bot_id AND query_id=query_row.query_id;
    IF FOUND AND work_row.state='READY' THEN
        -- Management pages revalidate each displayed item. The Agent's
        -- all-or-nothing evidence gate remains in memory_read_recall_work.
        IF query_row.state<>'FROZEN' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_EVIDENCE_UNAVAILABLE',
                    'message','Trial evidence is not frozen','status',409));
        END IF;
        SELECT m.evidence_ref INTO evidence_reference
        FROM __forest_module_schema__.retrieval_bundles b
        JOIN __forest_module_schema__.retrieval_materializations m
          ON m.bot_id=b.bot_id AND m.bundle_id=b.bundle_id
        WHERE b.bot_id=query_row.bot_id AND b.query_id=query_row.query_id;
        IF NOT FOUND THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_EVIDENCE_UNAVAILABLE',
                    'message','Trial evidence is not published','status',409));
        END IF;
        work_value := jsonb_build_object('value',jsonb_build_object(
            'state','READY','deadline_at',work_row.deadline_at,
            'error_code',NULL,'evidence_ref',evidence_reference));
    ELSE
        work_value := __forest_module_schema__.memory_read_recall_work(context,input);
        IF work_value->>'ok' IS DISTINCT FROM 'true' THEN RETURN work_value; END IF;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-trial/1',
            'query_id',query_row.query_id,'query_mode',query_row.query_mode,
            'state',work_value#>>'{value,state}',
            'deadline_at',work_value#>>'{value,deadline_at}',
            'expires_at',trial_row.expires_at,
            'error_code',work_value#>'{value,error_code}',
            'evidence_ref',work_value#>'{value,evidence_ref}'));
END;
$$;

CREATE FUNCTION memory_cancel_management_trial(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    query_row __forest_module_schema__.memory_queries%ROWTYPE;
    trial_row __forest_module_schema__.memory_trial_inputs%ROWTYPE;
    work_row __forest_module_schema__.query_recall_work%ROWTYPE;
    status_value text;
BEGIN
    SELECT * INTO query_row FROM __forest_module_schema__.memory_queries
    WHERE bot_id=context->>'bot_id' AND query_id=input->>'query_id'
      AND input_kind='ADMIN_TRIAL';
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_TRIAL_NOT_FOUND',
                'message','Management trial does not exist','status',404));
    END IF;
    SELECT * INTO trial_row FROM __forest_module_schema__.memory_trial_inputs
    WHERE bot_id=query_row.bot_id AND query_id=query_row.query_id FOR SHARE;
    IF NOT FOUND OR trial_row.actor_subject_ref IS DISTINCT FROM input->>'requested_by' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_TRIAL_NOT_FOUND',
                'message','Management trial does not exist','status',404));
    END IF;
    IF trial_row.generation_id IS DISTINCT FROM context->>'generation_id'
       OR trial_row.expires_at<=clock_timestamp() THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_TRIAL_INPUT_UNAVAILABLE',
                'message','Trial input is unavailable to this administrator','status',410));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.query_recall_work
    WHERE bot_id=query_row.bot_id AND query_id=query_row.query_id FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_RECALL_NOT_REQUESTED',
                'message','Trial has no recall work','status',404));
    END IF;
    IF work_row.state<>'READY' THEN
        UPDATE __forest_module_schema__.memory_trial_inputs
        SET cancelled_at=clock_timestamp()
        WHERE bot_id=query_row.bot_id AND query_id=query_row.query_id
          AND cancelled_at IS NULL;
    END IF;
    IF work_row.cancel_requested THEN
        status_value := CASE WHEN work_row.state='CANCELLED' THEN 'CANCELLED'
            ELSE 'CANCELLING' END;
    ELSIF work_row.state IN ('READY','FAILED') THEN
        status_value := 'TERMINAL';
    ELSIF work_row.state='CLAIMED' THEN
        UPDATE __forest_module_schema__.query_recall_work
        SET cancel_requested=true,cancel_reason='ADMIN_CANCELLED',updated_at=clock_timestamp()
        WHERE bot_id=work_row.bot_id AND query_id=work_row.query_id;
        status_value := 'CANCELLING';
    ELSE
        UPDATE __forest_module_schema__.query_recall_work
        SET state='CANCELLED',cancel_requested=true,cancel_reason='ADMIN_CANCELLED',
            updated_at=clock_timestamp()
        WHERE bot_id=work_row.bot_id AND query_id=work_row.query_id;
        status_value := 'CANCELLED';
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-trial-cancelled/1',
            'query_id',query_row.query_id,'status',status_value));
END;
$$;

CREATE FUNCTION memory_assert_public_query(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_queries
        WHERE bot_id=context->>'bot_id' AND query_id=input->>'query_id'
          AND input_kind='ADMIN_TRIAL'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_NOT_FOUND',
                'message','Public query does not exist','status',404));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-public-query/1',
            'query_id',input->>'query_id'));
END;
$$;

CREATE TRIGGER recall_settlements_immutable BEFORE UPDATE OR DELETE ON query_recall_settlements
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();

CREATE FUNCTION memory_begin_query(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    requested_bot text := context->>'bot_id';
    requested_query text := body->>'query_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    epoch_value jsonb := envelope->'prior'->(body->>'deletion_epoch_step_id');
    domain_row __forest_module_schema__.memory_domains%ROWTYPE;
    query_row __forest_module_schema__.memory_queries%ROWTYPE;
    selected_parts jsonb;
    selected_source text;
    source_count integer;
    text_count integer;
    image_count integer;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-query-begin/1'
       OR epoch_value->>'protocol' IS DISTINCT FROM 'forest.payload-epoch/1'
       OR epoch_value->>'deletion_epoch' IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_INPUT_INVALID',
                'message','Query preparation requires a current Payload epoch','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/query/'||requested_bot||'/'||requested_query,0));
    SELECT * INTO domain_row FROM __forest_module_schema__.memory_domains
    WHERE bot_id=requested_bot FOR SHARE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_DOMAIN_UNBOUND',
                'message','Query requires a bound embedding domain','status',409));
    END IF;
    SELECT * INTO query_row FROM __forest_module_schema__.memory_queries
    WHERE bot_id=requested_bot AND query_id=requested_query;
    IF FOUND THEN
        IF query_row.input_kind<>'SOURCE_UNIT'
           OR query_row.operation_id IS DISTINCT FROM requested_operation
           OR query_row.input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_QUERY_IDENTITY_CONFLICT',
                    'message','Query identity was bound to a different input','status',409));
        END IF;
    ELSE
        IF EXISTS (
            SELECT 1 FROM __forest_module_schema__.pending_input_events
            WHERE bot_id=requested_bot AND conversation_ref=body->>'conversation_ref'
              AND input_batch_ref=body->>'input_batch_ref'
              AND context_revision=(body->>'context_revision')::bigint
              AND state='PENDING_CAPTURE'
        ) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_QUERY_CAPTURE_PENDING',
                    'message','Query input still has unresolved source capture','status',409));
        END IF;
        PERFORM o.occurrence_id
        FROM __forest_module_schema__.eligibility_operations e
        JOIN __forest_module_schema__.source_occurrences o USING(bot_id,occurrence_id,source_unit_id)
        JOIN __forest_module_schema__.source_units u USING(bot_id,source_unit_id)
        JOIN __forest_module_schema__.memory_sources s USING(bot_id,source_id)
        WHERE o.bot_id=requested_bot AND o.conversation_ref=body->>'conversation_ref'
          AND e.input_batch_ref=body->>'input_batch_ref'
          AND e.context_revision=(body->>'context_revision')::bigint
        ORDER BY u.source_unit_id,o.occurrence_id FOR SHARE OF o,u,s;
        IF EXISTS (
            SELECT 1 FROM __forest_module_schema__.eligibility_operations e
            JOIN __forest_module_schema__.source_occurrences o USING(bot_id,occurrence_id,source_unit_id)
            JOIN __forest_module_schema__.source_units u USING(bot_id,source_unit_id)
            JOIN __forest_module_schema__.memory_sources s USING(bot_id,source_id)
            WHERE e.bot_id=requested_bot AND o.conversation_ref=body->>'conversation_ref'
              AND e.input_batch_ref=body->>'input_batch_ref'
              AND e.context_revision=(body->>'context_revision')::bigint
              AND (o.eligibility_state NOT IN ('ADMITTED','ELIGIBLE')
                   OR u.eligibility_state NOT IN ('REGISTERED','ELIGIBLE') OR s.status<>'ACTIVE'
                   OR o.input_batch_ref IS DISTINCT FROM e.input_batch_ref
                   OR o.context_revision IS DISTINCT FROM e.context_revision)
        ) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_QUERY_INPUT_UNAVAILABLE',
                    'message','Admitted query membership was revoked','status',409));
        END IF;
        WITH members AS (
            SELECT DISTINCT ON (u.source_unit_id)
                u.source_id,u.source_unit_id,u.source_revision,u.accepted_deletion_epoch,
                o.occurrence_id,o.ingress_seq
            FROM __forest_module_schema__.eligibility_operations e
            JOIN __forest_module_schema__.source_occurrences o
              USING(bot_id,occurrence_id,source_unit_id)
            JOIN __forest_module_schema__.source_units u USING(bot_id,source_unit_id)
            JOIN __forest_module_schema__.memory_sources s USING(bot_id,source_id)
            WHERE o.bot_id=requested_bot AND o.conversation_ref=body->>'conversation_ref'
              AND e.input_batch_ref=body->>'input_batch_ref'
              AND e.context_revision=(body->>'context_revision')::bigint
              AND o.eligibility_state IN ('ADMITTED','ELIGIBLE')
              AND u.eligibility_state IN ('REGISTERED','ELIGIBLE') AND s.status='ACTIVE'
              AND u.accepted_deletion_epoch<=(epoch_value->>'deletion_epoch')::bigint
            ORDER BY u.source_unit_id,o.ingress_seq,o.occurrence_id
        ), parts AS (
            SELECT m.*,p.part_id,p.part_index,p.part_kind
            FROM members m JOIN __forest_module_schema__.source_parts p
              ON p.bot_id=requested_bot AND p.source_unit_id=m.source_unit_id
            ORDER BY m.ingress_seq,m.occurrence_id,p.part_index LIMIT 257
        )
        SELECT jsonb_agg(to_jsonb(parts) ORDER BY ingress_seq,occurrence_id,part_index),
               min(source_id),count(DISTINCT source_id),
               count(*) FILTER (WHERE part_kind='TEXT'),
               count(*) FILTER (WHERE part_kind='IMAGE')
        INTO selected_parts,selected_source,source_count,text_count,image_count FROM parts;
        IF selected_parts IS NULL OR source_count<>1 THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_QUERY_INPUT_UNAVAILABLE',
                    'message','Query does not identify one admitted conversation source','status',409));
        END IF;
        IF jsonb_array_length(selected_parts)>256 THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_QUERY_PART_LIMIT',
                    'message','Query input exceeds 256 source parts','status',422));
        END IF;
        INSERT INTO __forest_module_schema__.memory_queries(
            bot_id,query_id,operation_id,input_digest,current_source_id,input_batch_ref,
            context_revision,query_mode,fusion_policy_revision,accepted_deletion_epoch)
        VALUES(requested_bot,requested_query,requested_operation,requested_digest,
            selected_source,body->>'input_batch_ref',(body->>'context_revision')::bigint,
            CASE WHEN text_count=0 THEN 'IMAGE' WHEN image_count=0 THEN 'TEXT' ELSE 'MIXED' END,
            domain_row.fusion_policy_revision,(epoch_value->>'deletion_epoch')::bigint)
        RETURNING * INTO query_row;
        INSERT INTO __forest_module_schema__.memory_query_parts(
            bot_id,query_id,query_part_index,source_unit_id,source_revision,
            occurrence_id,part_id,modality)
        SELECT requested_bot,requested_query,(ordinality-1)::integer,
               value->>'source_unit_id',(value->>'source_revision')::bigint,
               value->>'occurrence_id',value->>'part_id',value->>'part_kind'
        FROM jsonb_array_elements(selected_parts) WITH ORDINALITY;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-query/1',
            'query_id',query_row.query_id,'space_id',domain_row.space_id,
            'input_batch_ref',query_row.input_batch_ref,'context_revision',query_row.context_revision,
            'query_mode',query_row.query_mode,'part_count',(
                SELECT count(*) FROM __forest_module_schema__.memory_query_parts
                WHERE bot_id=requested_bot AND query_id=requested_query)));
END;
$$;

CREATE FUNCTION memory_read_query_parts(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_query text := input->>'query_id';
    query_row __forest_module_schema__.memory_queries%ROWTYPE;
    trial_row __forest_module_schema__.memory_trial_inputs%ROWTYPE;
    page_value jsonb;
    last_index integer;
    has_more boolean;
BEGIN
    SELECT * INTO query_row FROM __forest_module_schema__.memory_queries
    WHERE bot_id=requested_bot AND query_id=requested_query AND state IN ('OPEN','FROZEN');
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_INPUT_UNAVAILABLE',
                'message','Query input is unavailable','status',409));
    END IF;
    IF query_row.input_kind='ADMIN_TRIAL' THEN
        SELECT * INTO trial_row FROM __forest_module_schema__.memory_trial_inputs
        WHERE bot_id=requested_bot AND query_id=requested_query;
        IF NOT FOUND OR trial_row.generation_id IS DISTINCT FROM context->>'generation_id'
           OR trial_row.expires_at<=clock_timestamp()
           OR trial_row.cancelled_at IS NOT NULL THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_TRIAL_INPUT_UNAVAILABLE',
                    'message','Trial input has expired or its Generation was replaced','status',410));
        END IF;
        SELECT COALESCE(jsonb_agg(item ORDER BY query_part_index),'[]'::jsonb),
               max(query_part_index)
        INTO page_value,last_index FROM (
            SELECT p.query_part_index,jsonb_build_object(
                'query_part_index',p.query_part_index,'part_id',p.part_id,
                'part_index',p.query_part_index,'part_kind',p.modality,
                'media_type',p.media_type,'byte_length',p.byte_length,
                'input_identity',jsonb_build_object('kind','ADMIN_TRIAL',
                    'ref',trial_row.query_id,'revision',trial_row.input_revision),
                'payload',jsonb_build_object('payload_id',p.payload_id,
                    'digest',p.payload_digest,
                    'owner_component_id',p.payload_owner_component_id,
                    'owner_kind',p.payload_owner_kind,'owner_id',p.payload_owner_id)) AS item
            FROM __forest_module_schema__.memory_trial_parts p
            WHERE p.bot_id=requested_bot AND p.query_id=requested_query
              AND p.query_part_index>(input->>'after_part_index')::integer
            ORDER BY p.query_part_index LIMIT (input->>'limit')::integer
        ) page;
        SELECT EXISTS (SELECT 1 FROM __forest_module_schema__.memory_trial_parts p
            WHERE p.bot_id=requested_bot AND p.query_id=requested_query
              AND p.query_part_index>COALESCE(last_index,(input->>'after_part_index')::integer))
        INTO has_more;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('parts',page_value,'next_part_index',last_index,
                'has_more',has_more,'space_id',(SELECT space_id
                    FROM __forest_module_schema__.memory_domains WHERE bot_id=requested_bot)));
    END IF;
    IF EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_query_parts qp
        JOIN __forest_module_schema__.source_units u USING(bot_id,source_unit_id)
        JOIN __forest_module_schema__.memory_sources s USING(bot_id,source_id)
        JOIN __forest_module_schema__.source_occurrences o
          ON o.bot_id=qp.bot_id AND o.occurrence_id=qp.occurrence_id
        WHERE qp.bot_id=requested_bot AND qp.query_id=requested_query
          AND (u.source_revision<>qp.source_revision
               OR u.eligibility_state NOT IN ('REGISTERED','ELIGIBLE') OR s.status<>'ACTIVE'
               OR o.eligibility_state NOT IN ('ADMITTED','ELIGIBLE')
               OR o.input_batch_ref IS DISTINCT FROM query_row.input_batch_ref
               OR o.context_revision IS DISTINCT FROM query_row.context_revision)
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_INPUT_UNAVAILABLE',
                'message','Frozen query source membership is unavailable','status',409));
    END IF;
    SELECT COALESCE(jsonb_agg(item ORDER BY query_part_index),'[]'::jsonb),max(query_part_index)
    INTO page_value,last_index FROM (
        SELECT qp.query_part_index,jsonb_build_object(
            'query_part_index',qp.query_part_index,'source_unit_id',qp.source_unit_id,
            'source_revision',qp.source_revision,'occurrence_id',qp.occurrence_id,
            'input_identity',jsonb_build_object('kind','SOURCE_UNIT',
                'ref',qp.source_unit_id,'revision',qp.source_revision),
            'part_id',p.part_id,'part_index',p.part_index,'part_kind',p.part_kind,
            'media_type',p.media_type,'byte_length',p.byte_length,
            'payload',jsonb_build_object('payload_id',p.payload_id,'digest',p.payload_digest,
                'owner_component_id',p.payload_owner_component_id,
                'owner_kind',p.payload_owner_kind,'owner_id',p.payload_owner_id)) AS item
        FROM __forest_module_schema__.memory_query_parts qp
        JOIN __forest_module_schema__.source_parts p USING(bot_id,source_unit_id,part_id)
        WHERE qp.bot_id=requested_bot AND qp.query_id=requested_query
          AND qp.query_part_index>(input->>'after_part_index')::integer
        ORDER BY qp.query_part_index LIMIT (input->>'limit')::integer
    ) page;
    SELECT EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_query_parts
        WHERE bot_id=requested_bot AND query_id=requested_query AND query_part_index>last_index
    ) INTO has_more;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('parts',page_value,'next_part_index',last_index,'has_more',has_more,
            'space_id',(SELECT space_id FROM __forest_module_schema__.memory_domains
                WHERE bot_id=requested_bot)));
END;
$$;

CREATE FUNCTION memory_read_query_vectors(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_query text := input->>'query_id';
    page_check jsonb;
    facets_value jsonb;
    last_id text;
    has_more boolean;
    prepared_row __forest_module_schema__.query_facet_preparations%ROWTYPE;
BEGIN
    page_check := __forest_module_schema__.memory_read_query_parts(context,
        jsonb_build_object('query_id',requested_query,'after_part_index',-1,'limit',1));
    IF page_check->>'ok' IS DISTINCT FROM 'true' THEN RETURN page_check; END IF;
    SELECT * INTO prepared_row FROM __forest_module_schema__.query_facet_preparations
    WHERE bot_id=requested_bot AND query_id=requested_query;
    IF NOT FOUND OR prepared_row.facet_count<>(
        SELECT count(*) FROM __forest_module_schema__.embedding_facets
        WHERE bot_id=requested_bot AND query_id=requested_query AND purpose='QUERY' AND state='READY'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_VECTORS_PENDING',
                'message','Query vectors are not all published','status',409));
    END IF;
    SELECT COALESCE(jsonb_agg(item ORDER BY facet_id),'[]'::jsonb),max(facet_id)
    INTO facets_value,last_id FROM (
        SELECT facet_id,jsonb_build_object('ref_id',index_ref_id,'modality',modality) AS item
        FROM __forest_module_schema__.embedding_facets
        WHERE bot_id=requested_bot AND query_id=requested_query AND purpose='QUERY' AND state='READY'
          AND (input->>'after_facet_id' IS NULL OR facet_id>input->>'after_facet_id')
        ORDER BY facet_id LIMIT (input->>'limit')::integer
    ) page;
    SELECT EXISTS (
        SELECT 1 FROM __forest_module_schema__.embedding_facets
        WHERE bot_id=requested_bot AND query_id=requested_query AND purpose='QUERY' AND facet_id>last_id
    ) INTO has_more;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('space_id',page_check#>>'{value,space_id}',
            'index_snapshot_at',(SELECT created_at FROM __forest_module_schema__.memory_queries
                WHERE bot_id=requested_bot AND query_id=requested_query),
            'facets',facets_value,'next_facet_id',last_id,'has_more',has_more));
END;
$$;

CREATE FUNCTION memory_graph_unit_visible(
    requested_bot text, requested_unit text, graph_snapshot pg_snapshot,
    excluded_query text, current_conversation text, cut_seq bigint
) RETURNS boolean LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT EXISTS (
        SELECT 1 FROM __forest_module_schema__.source_units u
        JOIN __forest_module_schema__.memory_sources s USING (bot_id,source_id)
        WHERE u.bot_id=requested_bot AND u.source_unit_id=requested_unit
          AND u.eligibility_state='ELIGIBLE' AND s.status='ACTIVE'
          AND pg_visible_in_snapshot(u.eligible_xid,graph_snapshot)
          AND EXISTS (
              SELECT 1 FROM __forest_module_schema__.source_occurrences o
              WHERE o.bot_id=u.bot_id AND o.source_unit_id=u.source_unit_id
                AND o.eligibility_state='ELIGIBLE'
          ) AND NOT EXISTS (
              SELECT 1 FROM __forest_module_schema__.memory_query_parts qp
              WHERE qp.bot_id=u.bot_id AND qp.query_id=excluded_query
                AND qp.source_unit_id=u.source_unit_id
          ) AND NOT EXISTS (
              SELECT 1 FROM __forest_module_schema__.source_occurrences o
              WHERE o.bot_id=u.bot_id AND o.source_unit_id=u.source_unit_id
                AND o.conversation_ref=current_conversation
                AND (cut_seq IS NULL OR o.ingress_seq IS NULL OR o.ingress_seq>cut_seq
                     OR o.eligibility_state='REGISTERED')
          )
    );
$$;

CREATE FUNCTION memory_qualify_vector_candidates(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_query text := input->>'query_id';
    query_row __forest_module_schema__.memory_queries%ROWTYPE;
    page_check jsonb;
    candidates_value jsonb;
    conversation_value text;
    cut_value bigint;
BEGIN
    page_check := __forest_module_schema__.memory_read_query_vectors(context,
        jsonb_build_object('query_id',requested_query,'after_facet_id',NULL,'limit',1));
    IF page_check->>'ok' IS DISTINCT FROM 'true' THEN RETURN page_check; END IF;
    SELECT * INTO query_row FROM __forest_module_schema__.memory_queries
    WHERE bot_id=requested_bot AND query_id=requested_query;
    IF NOT EXISTS (
        SELECT 1 FROM __forest_module_schema__.embedding_facets
        WHERE bot_id=requested_bot AND query_id=requested_query AND purpose='QUERY'
          AND index_ref_id=input->>'query_facet_id' AND state='READY'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_FACET_MISMATCH',
                'message','Candidate qualification refers to another query facet','status',409));
    END IF;
    SELECT min(o.conversation_ref),max(o.ingress_seq) INTO conversation_value,cut_value
    FROM __forest_module_schema__.memory_query_parts qp
    JOIN __forest_module_schema__.source_occurrences o
      ON o.bot_id=qp.bot_id AND o.occurrence_id=qp.occurrence_id
    WHERE qp.bot_id=requested_bot AND qp.query_id=requested_query;
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'ref_id',f.index_ref_id,'candidate_rank',(hit->>'candidate_rank')::integer,
        'score',(hit->>'score')::double precision,'source_unit_id',u.source_unit_id,
        'source_revision',u.source_revision,'source_id',s.source_id,'trunk_id',s.trunk_id,
        'source_kind',s.source_kind,'platform',s.platform,'realm_id',s.realm_id,
        'external_group_id',s.external_group_id,'author_subject_ref',u.author_subject_ref
    ) ORDER BY (hit->>'candidate_rank')::integer),'[]'::jsonb)
    INTO candidates_value
    FROM jsonb_array_elements(input->'candidates') hit
    JOIN __forest_module_schema__.embedding_facets f
      ON f.bot_id=requested_bot AND f.index_ref_id=hit->>'ref_id'
     AND f.purpose='DOCUMENT' AND f.state='READY'
     AND f.space_id=page_check#>>'{value,space_id}'
     AND pg_visible_in_snapshot(f.published_xid,query_row.graph_snapshot)
    JOIN __forest_module_schema__.source_units u
     ON u.bot_id=f.bot_id AND u.source_unit_id=f.source_unit_id
     AND u.source_revision=f.source_revision
    JOIN __forest_module_schema__.memory_sources s
      ON s.bot_id=u.bot_id AND s.source_id=u.source_id
    WHERE __forest_module_schema__.memory_graph_unit_visible(
        requested_bot,u.source_unit_id,query_row.graph_snapshot,
        requested_query,conversation_value,cut_value);
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('candidates',candidates_value));
END;
$$;

CREATE FUNCTION memory_graph_association_visible(
    requested_bot text, contribution_ref text, contribution_xid xid8,
    target_ref text, graph_snapshot_value pg_snapshot, requested_query text,
    conversation_value text, cut_value bigint
) RETURNS boolean LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT pg_visible_in_snapshot(contribution_xid,graph_snapshot_value)
       AND __forest_module_schema__.memory_graph_unit_visible(
           requested_bot,target_ref,graph_snapshot_value,
           requested_query,conversation_value,cut_value)
       AND EXISTS (
           SELECT 1 FROM __forest_module_schema__.relation_evidence e
           WHERE e.bot_id=requested_bot AND e.contribution_id=contribution_ref
       ) AND NOT EXISTS (
           SELECT 1 FROM __forest_module_schema__.relation_evidence e
           WHERE e.bot_id=requested_bot AND e.contribution_id=contribution_ref
             AND NOT __forest_module_schema__.memory_graph_unit_visible(
                 requested_bot,e.source_unit_id,graph_snapshot_value,
                 requested_query,conversation_value,cut_value)
       )
$$;

CREATE FUNCTION memory_read_graph_neighbours(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_query text := input->>'query_id';
    requested_unit text := input->>'source_unit_id';
    association_limit integer := (input->>'association_limit')::integer;
    query_row __forest_module_schema__.memory_queries%ROWTYPE;
    unit_row __forest_module_schema__.source_units%ROWTYPE;
    check_value jsonb;
    conversation_value text;
    cut_value bigint;
    node_value jsonb;
    sequence_edges jsonb;
    association_edges jsonb;
    association_sampled boolean;
    sample_pivot text;
BEGIN
    IF association_limit IS NULL OR association_limit NOT BETWEEN 1 AND 128
       OR requested_unit IS NULL OR octet_length(requested_unit) NOT BETWEEN 1 AND 256 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_GRAPH_REQUEST_INVALID',
                'message','Graph neighbour request exceeds its bounds','status',422));
    END IF;
    check_value := __forest_module_schema__.memory_read_query_parts(context,
        jsonb_build_object('query_id',requested_query,'after_part_index',-1,'limit',1));
    IF check_value->>'ok' IS DISTINCT FROM 'true' THEN RETURN check_value; END IF;
    SELECT * INTO query_row FROM __forest_module_schema__.memory_queries
    WHERE bot_id=requested_bot AND query_id=requested_query;
    SELECT min(o.conversation_ref),max(o.ingress_seq) INTO conversation_value,cut_value
    FROM __forest_module_schema__.memory_query_parts qp
    JOIN __forest_module_schema__.source_occurrences o
      ON o.bot_id=qp.bot_id AND o.occurrence_id=qp.occurrence_id
    WHERE qp.bot_id=requested_bot AND qp.query_id=requested_query;
    IF NOT __forest_module_schema__.memory_graph_unit_visible(
        requested_bot,requested_unit,query_row.graph_snapshot,
        requested_query,conversation_value,cut_value) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('node',NULL,'edges','[]'::jsonb,'association_sampled',false));
    END IF;
    SELECT * INTO unit_row FROM __forest_module_schema__.source_units
    WHERE bot_id=requested_bot AND source_unit_id=requested_unit;
    SELECT jsonb_build_object(
        'source_unit_id',unit_row.source_unit_id,'source_revision',unit_row.source_revision,
        'source_id',s.source_id,'trunk_id',s.trunk_id,'source_kind',s.source_kind,
        'platform',s.platform,'realm_id',s.realm_id,'external_group_id',s.external_group_id,
        'author_subject_ref',unit_row.author_subject_ref)
    INTO node_value FROM __forest_module_schema__.memory_sources s
    WHERE s.bot_id=requested_bot AND s.source_id=unit_row.source_id;
    sample_pivot := md5(requested_query || chr(31) || requested_unit);
    -- Two indexed directional seeks. Hidden units are skipped without exposing
    -- their count; this is adjacency in the authorised archived view.
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'target_id',source_unit_id,'relation','SEQUENCE',
        'edge_id','sequence:'||encode(sha256(convert_to(jsonb_build_array(
            unit_row.source_id,requested_unit,source_unit_id)::text,'UTF8')),'hex'),
        'relation_id',NULL,'contribution_id',NULL) ORDER BY source_order_seq),'[]'::jsonb)
    INTO sequence_edges FROM (
        (SELECT u.source_unit_id,u.source_order_seq
         FROM __forest_module_schema__.source_units u
         WHERE u.bot_id=requested_bot AND u.source_id=unit_row.source_id
           AND u.source_order_seq<unit_row.source_order_seq
           AND __forest_module_schema__.memory_graph_unit_visible(
               requested_bot,u.source_unit_id,query_row.graph_snapshot,
               requested_query,conversation_value,cut_value)
         ORDER BY u.source_order_seq DESC LIMIT 1)
        UNION ALL
        (SELECT u.source_unit_id,u.source_order_seq
         FROM __forest_module_schema__.source_units u
         WHERE u.bot_id=requested_bot AND u.source_id=unit_row.source_id
           AND u.source_order_seq>unit_row.source_order_seq
           AND __forest_module_schema__.memory_graph_unit_visible(
               requested_bot,u.source_unit_id,query_row.graph_snapshot,
               requested_query,conversation_value,cut_value)
         ORDER BY u.source_order_seq LIMIT 1)
    ) neighbours;
    -- Four bounded index seeks cover both edge orientations and both sides of
    -- the query's random pivot. Only the small merged set is sorted; hidden
    -- candidates are filtered before each seek reaches its limit.
    WITH candidates AS MATERIALIZED (
        (SELECT c.contribution_id,c.relation_id,c.right_id AS target_id,c.sample_key
         FROM __forest_module_schema__.relation_contributions c
         WHERE c.bot_id=requested_bot AND c.left_kind='SOURCE_UNIT'
           AND c.left_id=requested_unit AND c.right_kind='SOURCE_UNIT'
           AND c.relation_kind='ASSOCIATES_WITH' AND c.active
           AND c.sample_key>=sample_pivot
           AND __forest_module_schema__.memory_graph_association_visible(
               requested_bot,c.contribution_id,c.created_xid,c.right_id,
               query_row.graph_snapshot,requested_query,conversation_value,cut_value)
         ORDER BY c.sample_key,c.contribution_id LIMIT association_limit+1)
        UNION ALL
        (SELECT c.contribution_id,c.relation_id,c.right_id AS target_id,c.sample_key
         FROM __forest_module_schema__.relation_contributions c
         WHERE c.bot_id=requested_bot AND c.left_kind='SOURCE_UNIT'
           AND c.left_id=requested_unit AND c.right_kind='SOURCE_UNIT'
           AND c.relation_kind='ASSOCIATES_WITH' AND c.active
           AND c.sample_key<sample_pivot
           AND __forest_module_schema__.memory_graph_association_visible(
               requested_bot,c.contribution_id,c.created_xid,c.right_id,
               query_row.graph_snapshot,requested_query,conversation_value,cut_value)
         ORDER BY c.sample_key,c.contribution_id LIMIT association_limit+1)
        UNION ALL
        (SELECT c.contribution_id,c.relation_id,c.left_id AS target_id,c.sample_key
         FROM __forest_module_schema__.relation_contributions c
         WHERE c.bot_id=requested_bot AND c.right_kind='SOURCE_UNIT'
           AND c.right_id=requested_unit AND c.left_kind='SOURCE_UNIT'
           AND c.relation_kind='ASSOCIATES_WITH' AND c.active
           AND c.sample_key>=sample_pivot
           AND __forest_module_schema__.memory_graph_association_visible(
               requested_bot,c.contribution_id,c.created_xid,c.left_id,
               query_row.graph_snapshot,requested_query,conversation_value,cut_value)
         ORDER BY c.sample_key,c.contribution_id LIMIT association_limit+1)
        UNION ALL
        (SELECT c.contribution_id,c.relation_id,c.left_id AS target_id,c.sample_key
         FROM __forest_module_schema__.relation_contributions c
         WHERE c.bot_id=requested_bot AND c.right_kind='SOURCE_UNIT'
           AND c.right_id=requested_unit AND c.left_kind='SOURCE_UNIT'
           AND c.relation_kind='ASSOCIATES_WITH' AND c.active
           AND c.sample_key<sample_pivot
           AND __forest_module_schema__.memory_graph_association_visible(
               requested_bot,c.contribution_id,c.created_xid,c.left_id,
               query_row.graph_snapshot,requested_query,conversation_value,cut_value)
         ORDER BY c.sample_key,c.contribution_id LIMIT association_limit+1)
    ), sampled AS MATERIALIZED (
        SELECT * FROM candidates
        ORDER BY (sample_key<sample_pivot),sample_key,contribution_id,target_id
        LIMIT association_limit+1
    ), ranked AS (
        SELECT *,row_number() OVER (
            ORDER BY (sample_key<sample_pivot),sample_key,contribution_id,target_id
        ) AS rank FROM sampled
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'target_id',target_id,'relation','ASSOCIATED','edge_id',contribution_id,
        'relation_id',relation_id,'contribution_id',contribution_id) ORDER BY rank)
        FILTER (WHERE rank<=association_limit),'[]'::jsonb),count(*)>association_limit
    INTO association_edges,association_sampled FROM ranked;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('node',node_value,'edges',sequence_edges||association_edges,
            'association_sampled',association_sampled));
END;
$$;

CREATE FUNCTION memory_read_match_preparation(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    preparation __forest_module_schema__.query_match_preparations%ROWTYPE;
    check_value jsonb;
    refs jsonb;
    receipt_count integer;
    matches_count integer;
BEGIN
    check_value := __forest_module_schema__.memory_read_query_parts(context,
        jsonb_build_object('query_id',input->>'query_id','after_part_index',-1,'limit',1));
    IF check_value->>'ok' IS DISTINCT FROM 'true' THEN RETURN check_value; END IF;
    SELECT * INTO preparation FROM __forest_module_schema__.query_match_preparations
    WHERE bot_id=context->>'bot_id' AND query_id=input->>'query_id';
    SELECT COALESCE(jsonb_agg(f.index_ref_id ORDER BY f.facet_id),'[]'::jsonb),
           count(*),COALESCE(sum(r.match_count),0)
    INTO refs,receipt_count,matches_count
    FROM __forest_module_schema__.query_match_receipts r
    JOIN __forest_module_schema__.embedding_facets f
      ON f.bot_id=r.bot_id AND f.facet_id=r.query_facet_id
    WHERE r.bot_id=context->>'bot_id' AND r.query_id=input->>'query_id';
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('prepared',preparation.query_id IS NOT NULL,
            'complete',preparation.query_id IS NOT NULL AND receipt_count=preparation.facet_count,
            'policy',preparation.policy,'space_id',preparation.space_id,
            'facet_count',preparation.facet_count,'completed_facet_refs',refs,
            'match_count',matches_count));
END;
$$;

CREATE FUNCTION memory_begin_query_matching(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    epoch_value jsonb := envelope->'prior'->(envelope#>>'{input,deletion_epoch_step_id}');
    check_value jsonb;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR epoch_value->>'protocol' IS DISTINCT FROM 'forest.payload-epoch/1' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_MATCH_AUTHORITY_MISSING',
                'message','Matching requires the Payload deletion barrier','status',422));
    END IF;
    PERFORM 1 FROM __forest_module_schema__.memory_queries
    WHERE bot_id=context->>'bot_id' AND query_id=body->>'query_id' FOR UPDATE;
    check_value := __forest_module_schema__.memory_read_query_vectors(context,
        jsonb_build_object('query_id',body->>'query_id','after_facet_id',NULL,'limit',1));
    IF check_value->>'ok' IS DISTINCT FROM 'true' THEN RETURN check_value; END IF;
    INSERT INTO __forest_module_schema__.query_match_preparations(
        bot_id,query_id,operation_id,input_digest,space_id,policy,facet_count,accepted_deletion_epoch)
    SELECT context->>'bot_id',body->>'query_id',context->>'operation_id',
        context->>'participant_input_digest',check_value#>>'{value,space_id}',body->'policy',
        p.facet_count,(epoch_value->>'deletion_epoch')::bigint
    FROM __forest_module_schema__.query_facet_preparations p
    WHERE p.bot_id=context->>'bot_id' AND p.query_id=body->>'query_id'
    ON CONFLICT (bot_id,query_id) DO NOTHING;
    RETURN __forest_module_schema__.memory_read_match_preparation(context,body);
END;
$$;

CREATE FUNCTION memory_record_query_matches(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    epoch_value jsonb := envelope->'prior'->(envelope#>>'{input,deletion_epoch_step_id}');
    preparation __forest_module_schema__.query_match_preparations%ROWTYPE;
    query_facet text;
    check_value jsonb;
    candidates jsonb;
    qualified jsonb;
    selected jsonb;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR epoch_value->>'protocol' IS DISTINCT FROM 'forest.payload-epoch/1' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_MATCH_AUTHORITY_MISSING',
                'message','Matching requires the Payload deletion barrier','status',422));
    END IF;
    SELECT * INTO preparation FROM __forest_module_schema__.query_match_preparations
    WHERE bot_id=context->>'bot_id' AND query_id=body->>'query_id' FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_MATCH_NOT_PREPARED',
                'message','Query matching is not prepared','status',409));
    END IF;
    check_value := __forest_module_schema__.memory_read_query_vectors(context,
        jsonb_build_object('query_id',body->>'query_id','after_facet_id',NULL,'limit',1));
    IF check_value->>'ok' IS DISTINCT FROM 'true' THEN RETURN check_value; END IF;
    SELECT facet_id INTO query_facet FROM __forest_module_schema__.embedding_facets
    WHERE bot_id=context->>'bot_id' AND query_id=body->>'query_id'
      AND purpose='QUERY' AND state='READY' AND index_ref_id=body->>'query_ref_id';
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_FACET_MISMATCH',
                'message','Match receipt refers to another query facet','status',409));
    END IF;
    IF EXISTS (SELECT 1 FROM __forest_module_schema__.query_match_receipts
        WHERE bot_id=context->>'bot_id' AND query_id=body->>'query_id'
          AND query_facet_id=query_facet) THEN
        RETURN __forest_module_schema__.memory_read_match_preparation(context,body);
    END IF;
    SELECT COALESCE(jsonb_agg(hit ORDER BY (hit->>'candidate_rank')::integer),'[]'::jsonb)
    INTO candidates FROM jsonb_array_elements(body->'candidates') hit
    WHERE (hit->>'score')::double precision >= (preparation.policy->>'min_similarity')::double precision
      AND (hit->>'candidate_rank')::integer <= (preparation.policy->>'candidate_limit_per_facet')::integer;
    qualified := __forest_module_schema__.memory_qualify_vector_candidates(context,
        jsonb_build_object('query_id',body->>'query_id','query_facet_id',body->>'query_ref_id',
            'candidates',candidates));
    IF qualified->>'ok' IS DISTINCT FROM 'true' THEN RETURN qualified; END IF;
    SELECT COALESCE(jsonb_agg(hit ORDER BY hit->>'source_unit_id'),'[]'::jsonb)
    INTO selected FROM (
        SELECT DISTINCT ON (hit->>'source_unit_id') hit
        FROM jsonb_array_elements(qualified#>'{value,candidates}') hit
        ORDER BY hit->>'source_unit_id',(hit->>'candidate_rank')::integer
    ) first_hits;
    INSERT INTO __forest_module_schema__.query_match_receipts(
        bot_id,query_id,query_facet_id,operation_id,input_digest,accepted_deletion_epoch,match_count)
    VALUES(context->>'bot_id',body->>'query_id',query_facet,context->>'operation_id',
        context->>'participant_input_digest',(epoch_value->>'deletion_epoch')::bigint,
        jsonb_array_length(selected));
    INSERT INTO __forest_module_schema__.query_seed_matches(
        bot_id,query_id,query_facet_id,source_unit_id,source_revision,
        document_facet_id,candidate_rank,score)
    SELECT context->>'bot_id',body->>'query_id',query_facet,hit->>'source_unit_id',
        (hit->>'source_revision')::bigint,f.facet_id,(hit->>'candidate_rank')::integer,
        (hit->>'score')::double precision
    FROM jsonb_array_elements(selected) hit
    JOIN __forest_module_schema__.embedding_facets f
      ON f.bot_id=context->>'bot_id' AND f.index_ref_id=hit->>'ref_id';
    RETURN __forest_module_schema__.memory_read_match_preparation(context,body);
END;
$$;

CREATE FUNCTION memory_read_matched_seeds(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    progress jsonb;
    preparation __forest_module_schema__.query_match_preparations%ROWTYPE;
    query_row __forest_module_schema__.memory_queries%ROWTYPE;
    conversation_value text;
    cut_value bigint;
    seeds jsonb;
BEGIN
    progress := __forest_module_schema__.memory_read_match_preparation(context,input);
    IF progress->>'ok' IS DISTINCT FROM 'true' THEN RETURN progress; END IF;
    IF progress#>>'{value,complete}' IS DISTINCT FROM 'true' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_MATCHES_PENDING',
                'message','Query match receipts are not complete','status',409));
    END IF;
    SELECT * INTO preparation FROM __forest_module_schema__.query_match_preparations
    WHERE bot_id=context->>'bot_id' AND query_id=input->>'query_id';
    SELECT * INTO query_row FROM __forest_module_schema__.memory_queries
    WHERE bot_id=preparation.bot_id AND query_id=preparation.query_id;
    SELECT min(o.conversation_ref),max(o.ingress_seq) INTO conversation_value,cut_value
    FROM __forest_module_schema__.memory_query_parts qp
    JOIN __forest_module_schema__.source_occurrences o
      ON o.bot_id=qp.bot_id AND o.occurrence_id=qp.occurrence_id
    WHERE qp.bot_id=preparation.bot_id AND qp.query_id=preparation.query_id;
    WITH ranked AS (
        SELECT m.source_unit_id,m.source_revision,
               sum(1.0::double precision/(60+m.candidate_rank)) AS rank_score,
               jsonb_agg(DISTINCT qf.modality ORDER BY qf.modality) AS modalities
        FROM __forest_module_schema__.query_seed_matches m
        JOIN __forest_module_schema__.embedding_facets f
          ON f.bot_id=m.bot_id AND f.facet_id=m.document_facet_id AND f.state='READY'
        JOIN __forest_module_schema__.embedding_facets qf
          ON qf.bot_id=m.bot_id AND qf.facet_id=m.query_facet_id AND qf.state='READY'
        WHERE m.bot_id=preparation.bot_id AND m.query_id=preparation.query_id
          AND __forest_module_schema__.memory_graph_unit_visible(
              preparation.bot_id,m.source_unit_id,query_row.graph_snapshot,
              preparation.query_id,conversation_value,cut_value)
        GROUP BY m.source_unit_id,m.source_revision
        ORDER BY rank_score DESC,m.source_unit_id LIMIT (preparation.policy->>'seed_limit')::integer
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'source_unit_id',u.source_unit_id,'source_revision',u.source_revision,
        'source_id',s.source_id,'trunk_id',s.trunk_id,'source_kind',s.source_kind,
        'platform',s.platform,'realm_id',s.realm_id,'external_group_id',s.external_group_id,
        'author_subject_ref',u.author_subject_ref,'rank_score',r.rank_score,
        'activation',1.0,'query_modalities',r.modalities
    ) ORDER BY r.rank_score DESC,r.source_unit_id),'[]'::jsonb) INTO seeds
    FROM ranked r JOIN __forest_module_schema__.source_units u
      ON u.bot_id=preparation.bot_id AND u.source_unit_id=r.source_unit_id
    JOIN __forest_module_schema__.memory_sources s USING(bot_id,source_id);
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('policy',preparation.policy,'result',jsonb_build_object(
            'protocol','forest.memory-query-seeds/1','query_id',query_row.query_id,
            'space_id',preparation.space_id,'index_snapshot_at',query_row.created_at,'seeds',seeds)));
END;
$$;

CREATE FUNCTION memory_read_retrieval_record(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
        (SELECT jsonb_build_object('protocol','forest.memory-retrieval-record/1',
            'query_id',b.query_id,'bundle_id',b.bundle_id,'result_count',b.result_count,
            'seed_count',b.seed_count,'read_count',b.read_count,
            'deletion_epoch',b.accepted_deletion_epoch,'truncated',b.truncated,
            'association_sampled',b.association_sampled)
         FROM __forest_module_schema__.retrieval_bundles b
         WHERE b.bot_id=context->>'bot_id' AND b.query_id=input->>'query_id'));
$$;

CREATE FUNCTION memory_freeze_retrieval(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    epoch_value jsonb := envelope->'prior'->(envelope#>>'{input,deletion_epoch_step_id}');
    query_row __forest_module_schema__.memory_queries%ROWTYPE;
    seed_view jsonb;
    policy jsonb;
    seed_set jsonb;
    cache jsonb := '{}'::jsonb;
    nodes jsonb := body->'nodes';
    edges jsonb := '[]'::jsonb;
    item jsonb;
    seed jsonb;
    parent jsonb;
    page jsonb;
    edge jsonb;
    weight_value double precision;
    index_value integer := 0;
    seed_count integer := 0;
    bundle_value text;
    seen text[] := ARRAY[]::text[];
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR epoch_value->>'protocol' IS DISTINCT FROM 'forest.payload-epoch/1' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_RETRIEVAL_AUTHORITY_MISSING',
                'message','Retrieval freeze requires the deletion barrier','status',422));
    END IF;
    SELECT * INTO query_row FROM __forest_module_schema__.memory_queries
    WHERE bot_id=context->>'bot_id' AND query_id=body->>'query_id' FOR UPDATE;
    IF query_row.state='FROZEN' THEN
        RETURN __forest_module_schema__.memory_read_retrieval_record(context,body);
    END IF;
    seed_view := __forest_module_schema__.memory_read_matched_seeds(context,body);
    IF seed_view->>'ok' IS DISTINCT FROM 'true' THEN RETURN seed_view; END IF;
    policy := seed_view#>'{value,policy}';
    SELECT COALESCE(jsonb_agg(value ORDER BY ordinality),'[]'::jsonb) INTO seed_set
    FROM jsonb_array_elements(seed_view#>'{value,result,seeds}') WITH ORDINALITY
    WHERE ordinality<=(policy->>'max_graph_nodes')::integer;
    IF jsonb_array_length(nodes)>(policy->>'max_graph_nodes')::integer
       OR (body->>'read_count')::integer NOT BETWEEN jsonb_array_length(nodes)
            AND (policy->>'max_graph_nodes')::integer
       OR jsonb_array_length(nodes)<>(SELECT count(DISTINCT value->>'source_unit_id')
            FROM jsonb_array_elements(nodes)) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_RETRIEVAL_INVALID',
                'message','Retrieval region violates its node bounds','status',422));
    END IF;
    FOR item IN SELECT value FROM jsonb_array_elements(nodes) LOOP
        page := __forest_module_schema__.memory_read_graph_neighbours(context,
            jsonb_build_object('query_id',body->>'query_id',
                'source_unit_id',item->>'source_unit_id',
                'association_limit',(policy->>'max_association_edges_per_node')::integer));
        IF page->>'ok' IS DISTINCT FROM 'true' THEN RETURN page; END IF;
        IF page#>'{value,node}'='null'::jsonb
           OR page#>>'{value,node,source_revision}' IS DISTINCT FROM item->>'source_revision' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_RETRIEVAL_STALE',
                    'message','Retrieval source is no longer eligible','status',409));
        END IF;
        cache := cache||jsonb_build_object(item->>'source_unit_id',page->'value');
        SELECT value INTO seed FROM jsonb_array_elements(seed_set)
        WHERE value->>'source_unit_id'=item->>'source_unit_id';
        IF (item->>'is_seed')::boolean THEN
            IF seed IS NULL OR item->>'path_parent_source_unit_id' IS NOT NULL
               OR item->>'path_edge_id' IS NOT NULL
               OR (item->>'activation')::double precision<>1.0
               OR (item->>'rank_score')::double precision<>(seed->>'rank_score')::double precision THEN
                RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object('code','MEMORY_RETRIEVAL_STALE',
                        'message','Retrieval seed does not match fixed receipts','status',409));
            END IF;
            seed_count := seed_count+1;
        ELSE
            SELECT value INTO parent FROM jsonb_array_elements(nodes)
            WHERE value->>'source_unit_id'=item->>'path_parent_source_unit_id';
            SELECT value INTO edge FROM jsonb_array_elements(
                cache#>ARRAY[item->>'path_parent_source_unit_id','edges'])
            WHERE value->>'target_id'=item->>'source_unit_id'
              AND value->>'edge_id'=item->>'path_edge_id';
            IF seed IS NOT NULL OR parent IS NULL OR edge IS NULL
               OR NOT (item->>'path_parent_source_unit_id'=ANY(seen)) THEN
                RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object('code','MEMORY_RETRIEVAL_STALE',
                        'message','Retrieval path is not an eligible rooted path','status',409));
            END IF;
            weight_value := (policy->>CASE edge->>'relation'
                WHEN 'SEQUENCE' THEN 'sequence_weight' ELSE 'association_weight' END)::double precision;
            IF (item->>'rank_score')::double precision<>0.0
               OR (item->>'activation')::double precision < (policy->>'min_graph_relevance')::double precision
               OR (item->>'activation')::double precision <>
                  (parent->>'activation')::double precision*weight_value THEN
                RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object('code','MEMORY_RETRIEVAL_INVALID',
                        'message','Retrieval activation does not match its path','status',422));
            END IF;
            edges := edges||jsonb_build_array(jsonb_build_object(
                'source',parent->>'source_unit_id','target',item->>'source_unit_id',
                'edge_id',edge->>'edge_id','relation',edge->>'relation',
                'contribution_id',edge->>'contribution_id','weight',weight_value));
        END IF;
        seen := array_append(seen,item->>'source_unit_id');
    END LOOP;
    IF seed_count<>jsonb_array_length(seed_set) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_RETRIEVAL_STALE',
                'message','Retrieval is missing a qualified seed','status',409));
    END IF;
    bundle_value := 'retrieval:'||encode(sha256(convert_to(
        jsonb_build_array(context->>'bot_id',query_row.query_id)::text,'UTF8')),'hex');
    INSERT INTO __forest_module_schema__.retrieval_bundles(
        bot_id,bundle_id,query_id,operation_id,input_digest,accepted_deletion_epoch,
        read_count,truncated,association_sampled,seed_count,result_count)
    VALUES(context->>'bot_id',bundle_value,query_row.query_id,context->>'operation_id',
        context->>'participant_input_digest',(epoch_value->>'deletion_epoch')::bigint,
        (body->>'read_count')::integer,(body->>'truncated')::boolean,
        (body->>'association_sampled')::boolean,seed_count,jsonb_array_length(nodes));
    FOR item IN SELECT value FROM jsonb_array_elements(nodes) LOOP
        INSERT INTO __forest_module_schema__.retrieval_items(
            bot_id,bundle_id,item_index,source_unit_id,source_revision,rank_score,
            activation,is_seed,path_parent_source_unit_id,path_edge_id)
        VALUES(context->>'bot_id',bundle_value,index_value,item->>'source_unit_id',
            (item->>'source_revision')::bigint,(item->>'rank_score')::double precision,
            (item->>'activation')::double precision,(item->>'is_seed')::boolean,
            item->>'path_parent_source_unit_id',item->>'path_edge_id');
        index_value := index_value+1;
    END LOOP;
    INSERT INTO __forest_module_schema__.retrieval_relations(
        bot_id,bundle_id,relation_index,source_unit_id,target_unit_id,
        edge_id,relation_kind,contribution_id,weight)
    SELECT context->>'bot_id',bundle_value,(ordinality-1)::integer,
        value->>'source',value->>'target',value->>'edge_id',value->>'relation',
        value->>'contribution_id',(value->>'weight')::double precision
    FROM jsonb_array_elements(edges) WITH ORDINALITY;
    UPDATE __forest_module_schema__.memory_queries SET state='FROZEN',bundle_id=bundle_value,
        frozen_at=clock_timestamp() WHERE bot_id=context->>'bot_id' AND query_id=query_row.query_id;
    RETURN __forest_module_schema__.memory_read_retrieval_record(context,body);
END;
$$;

CREATE FUNCTION memory_read_retrieval_parts(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_query text := input->>'query_id';
    requested_index integer := (input->>'item_index')::integer;
    requested_after integer := (input->>'after_part_index')::integer;
    requested_limit integer := (input->>'limit')::integer;
    bundle_row __forest_module_schema__.retrieval_bundles%ROWTYPE;
    item_row __forest_module_schema__.retrieval_items%ROWTYPE;
    cursor_row __forest_module_schema__.retrieval_items%ROWTYPE;
    seed_view jsonb;
    policy jsonb;
    page jsonb;
    node_value jsonb;
    parts_value jsonb;
    matched_parts jsonb;
    child_unit text;
    child_edge text;
    path_length integer := 0;
    valid_path boolean := true;
    last_index integer;
    more_value boolean := false;
BEGIN
    IF requested_index IS NULL OR requested_index NOT BETWEEN 0 AND 63
       OR requested_after IS NULL OR requested_after NOT BETWEEN -1 AND 4095
       OR requested_limit IS NULL OR requested_limit NOT BETWEEN 1 AND 32 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_RETRIEVAL_REQUEST_INVALID',
                'message','Retrieval parts request exceeds its bounds','status',422));
    END IF;
    SELECT * INTO bundle_row FROM __forest_module_schema__.retrieval_bundles
    WHERE bot_id=requested_bot AND query_id=requested_query;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_RETRIEVAL_NOT_FROZEN',
                'message','Query has no frozen retrieval result','status',409));
    END IF;
    seed_view := __forest_module_schema__.memory_read_matched_seeds(context,input);
    IF seed_view->>'ok' IS DISTINCT FROM 'true' THEN RETURN seed_view; END IF;
    policy := seed_view#>'{value,policy}';
    SELECT * INTO item_row FROM __forest_module_schema__.retrieval_items
    WHERE bot_id=requested_bot AND bundle_id=bundle_row.bundle_id AND item_index=requested_index;
    IF NOT FOUND THEN valid_path := false; END IF;
    cursor_row := item_row;
    WHILE valid_path LOOP
        path_length := path_length+1;
        IF path_length>64 OR cursor_row.source_unit_id IS NULL THEN
            valid_path := false; EXIT;
        END IF;
        page := __forest_module_schema__.memory_read_graph_neighbours(context,
            jsonb_build_object('query_id',requested_query,'source_unit_id',cursor_row.source_unit_id,
                'association_limit',(policy->>'max_association_edges_per_node')::integer));
        IF page->>'ok' IS DISTINCT FROM 'true' THEN RETURN page; END IF;
        IF page#>'{value,node}'='null'::jsonb
           OR page#>>'{value,node,source_revision}' IS DISTINCT FROM cursor_row.source_revision::text
           OR (child_unit IS NOT NULL AND NOT EXISTS (
               SELECT 1 FROM jsonb_array_elements(page#>'{value,edges}') e
               WHERE e->>'target_id'=child_unit AND e->>'edge_id'=child_edge)) THEN
            valid_path := false; EXIT;
        END IF;
        IF path_length=1 THEN node_value := page#>'{value,node}'; END IF;
        IF cursor_row.is_seed THEN
            valid_path := EXISTS (
                SELECT 1 FROM jsonb_array_elements(seed_view#>'{value,result,seeds}') s
                WHERE s->>'source_unit_id'=cursor_row.source_unit_id);
            EXIT;
        END IF;
        child_unit := cursor_row.source_unit_id;
        child_edge := cursor_row.path_edge_id;
        SELECT * INTO cursor_row FROM __forest_module_schema__.retrieval_items
        WHERE bot_id=requested_bot AND bundle_id=bundle_row.bundle_id
          AND source_unit_id=cursor_row.path_parent_source_unit_id;
    END LOOP;
    IF valid_path THEN
        SELECT node_value||jsonb_build_object(
            'occurred_at',u.occurred_at,'source_order_seq',u.source_order_seq,'unit_kind',u.unit_kind,
            'part_count',u.part_count,'is_current_source',
            COALESCE(u.source_id=q.current_source_id,false),
            'author_payload',jsonb_build_object('payload_id',u.author_payload_id,
                'digest',u.author_payload_digest,'owner_component_id',u.author_owner_component_id,
                'owner_kind',u.author_owner_kind,'owner_id',u.author_owner_id),
            'activation',item_row.activation,'rank_score',item_row.rank_score,'is_seed',item_row.is_seed,
            'path_parent_source_unit_id',item_row.path_parent_source_unit_id,
            'path_edge_id',item_row.path_edge_id)
        INTO node_value FROM __forest_module_schema__.source_units u
        JOIN __forest_module_schema__.memory_queries q ON q.bot_id=u.bot_id AND q.query_id=requested_query
        WHERE u.bot_id=requested_bot AND u.source_unit_id=item_row.source_unit_id;
        SELECT COALESCE(jsonb_agg(part_id ORDER BY part_id),'[]'::jsonb) INTO matched_parts
        FROM (
            SELECT DISTINCT f.part_id FROM __forest_module_schema__.query_seed_matches m
            JOIN __forest_module_schema__.embedding_facets f
              ON f.bot_id=m.bot_id AND f.facet_id=m.document_facet_id AND f.state='READY'
            JOIN __forest_module_schema__.embedding_facets qf
              ON qf.bot_id=m.bot_id AND qf.facet_id=m.query_facet_id AND qf.state='READY'
            WHERE m.bot_id=requested_bot AND m.query_id=requested_query
              AND m.source_unit_id=item_row.source_unit_id
        ) matches;
        node_value := node_value||jsonb_build_object('matched_part_ids',matched_parts);
        SELECT COALESCE(jsonb_agg(jsonb_build_object(
            'part_id',p.part_id,'part_index',p.part_index,'part_kind',p.part_kind,'media_type',p.media_type,
            'byte_length',p.byte_length,
            'width',p.width,'height',p.height,'payload',jsonb_build_object(
                'payload_id',p.payload_id,'digest',p.payload_digest,
                'owner_component_id',p.payload_owner_component_id,'owner_kind',p.payload_owner_kind,
                'owner_id',p.payload_owner_id)) ORDER BY p.part_index),'[]'::jsonb),max(p.part_index)
        INTO parts_value,last_index FROM (
            SELECT * FROM __forest_module_schema__.source_parts
            WHERE bot_id=requested_bot AND source_unit_id=item_row.source_unit_id
              AND part_index>requested_after ORDER BY part_index LIMIT requested_limit
        ) p;
        more_value := EXISTS (
            SELECT 1 FROM __forest_module_schema__.source_parts
            WHERE bot_id=requested_bot AND source_unit_id=item_row.source_unit_id
              AND part_index>COALESCE(last_index,requested_after));
    ELSE
        node_value := NULL; parts_value := '[]'::jsonb;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('query_id',requested_query,'bundle_id',bundle_row.bundle_id,
            'item_index',requested_index,'result_count',bundle_row.result_count,'node',node_value,
            'parts',parts_value,'last_part_index',last_index,'has_more',more_value));
END;
$$;

CREATE FUNCTION memory_read_management_trial_visibility(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    access_value jsonb;
    bundle_row __forest_module_schema__.retrieval_bundles%ROWTYPE;
    projection_row __forest_module_schema__.retrieval_materializations%ROWTYPE;
    requested_source text := input->>'source_unit_id';
    item_index_value integer;
    evidence_index_value integer := -1;
    source_unit_value text;
    check_value jsonb;
    visible_items jsonb := '[]'::jsonb;
BEGIN
    access_value := __forest_module_schema__.memory_read_management_trial(context,input);
    IF access_value->>'ok' IS DISTINCT FROM 'true' THEN RETURN access_value; END IF;
    IF access_value#>>'{value,state}' IS DISTINCT FROM 'READY' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_TRIAL_NOT_READY',
                'message','Trial evidence is not ready','status',409));
    END IF;
    SELECT * INTO bundle_row FROM __forest_module_schema__.retrieval_bundles
    WHERE bot_id=context->>'bot_id' AND query_id=input->>'query_id';
    SELECT * INTO projection_row FROM __forest_module_schema__.retrieval_materializations
    WHERE bot_id=bundle_row.bot_id AND bundle_id=bundle_row.bundle_id;
    IF NOT FOUND OR projection_row.evidence_ref IS DISTINCT FROM
       access_value#>'{value,evidence_ref}' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EVIDENCE_UNAVAILABLE',
                'message','Trial evidence changed','status',409));
    END IF;
    FOREACH item_index_value IN ARRAY projection_row.item_indexes LOOP
        evidence_index_value := evidence_index_value+1;
        SELECT source_unit_id INTO source_unit_value
        FROM __forest_module_schema__.retrieval_items
        WHERE bot_id=bundle_row.bot_id AND bundle_id=bundle_row.bundle_id
          AND item_index=item_index_value;
        IF NOT FOUND THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_EVIDENCE_INVALID',
                    'message','Trial evidence item is missing','status',409));
        END IF;
        IF requested_source IS NOT NULL AND source_unit_value<>requested_source THEN
            CONTINUE;
        END IF;
        check_value := __forest_module_schema__.memory_read_retrieval_parts(context,
            jsonb_build_object('query_id',bundle_row.query_id,
                'item_index',item_index_value,'after_part_index',-1,'limit',1));
        IF check_value->>'ok' IS DISTINCT FROM 'true' THEN RETURN check_value; END IF;
        visible_items := visible_items||jsonb_build_array(jsonb_build_object(
            'evidence_index',evidence_index_value,'item_index',item_index_value,
            'source_unit_id',source_unit_value,
            'visible',COALESCE(
                check_value#>>'{value,node,source_unit_id}'=source_unit_value,false)));
    END LOOP;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-trial-visibility/1',
            'query_id',bundle_row.query_id,'bundle_id',bundle_row.bundle_id,
            'evidence_ref',projection_row.evidence_ref,'items',visible_items));
END;
$$;

CREATE FUNCTION memory_read_retrieval_dependencies(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    check_value jsonb;
    refs jsonb;
    next_unit text;
    more_value boolean;
BEGIN
    check_value := __forest_module_schema__.memory_read_retrieval_parts(context,
        input||jsonb_build_object('after_part_index',-1,'limit',1));
    IF check_value->>'ok' IS DISTINCT FROM 'true' THEN RETURN check_value; END IF;
    IF check_value#>'{value,node}'='null'::jsonb THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('available',false,'refs','[]'::jsonb,
                'next_source_unit_id',NULL,'has_more',false));
    END IF;
    WITH RECURSIVE path AS (
        SELECT i.source_unit_id,i.path_parent_source_unit_id,0 AS depth FROM __forest_module_schema__.retrieval_items i
        WHERE i.bot_id=context->>'bot_id' AND i.bundle_id=check_value#>>'{value,bundle_id}'
          AND i.item_index=(input->>'item_index')::integer
        UNION ALL
        SELECT i.source_unit_id,i.path_parent_source_unit_id,p.depth+1
        FROM path p JOIN __forest_module_schema__.retrieval_items i
          ON i.bot_id=context->>'bot_id' AND i.bundle_id=check_value#>>'{value,bundle_id}'
         AND i.source_unit_id=p.path_parent_source_unit_id WHERE p.depth<63
    ), dependencies AS (
        SELECT source_unit_id FROM path
        UNION
        SELECT e.source_unit_id FROM path p
        JOIN __forest_module_schema__.retrieval_relations r
          ON r.bot_id=context->>'bot_id' AND r.bundle_id=check_value#>>'{value,bundle_id}'
         AND r.target_unit_id=p.source_unit_id
        JOIN __forest_module_schema__.relation_evidence e
          ON e.bot_id=r.bot_id AND e.contribution_id=r.contribution_id
    ), page AS (
        SELECT u.* FROM dependencies d JOIN __forest_module_schema__.source_units u
          ON u.bot_id=context->>'bot_id' AND u.source_unit_id=d.source_unit_id
        WHERE input->>'after_source_unit_id' IS NULL
           OR u.source_unit_id>input->>'after_source_unit_id'
        ORDER BY u.source_unit_id LIMIT 33
    ), numbered AS (
        SELECT *,row_number() OVER (ORDER BY source_unit_id) AS n FROM page
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object('source_unit_id',source_unit_id,'payload',
        jsonb_build_object('payload_id',author_payload_id,'digest',author_payload_digest,
            'owner_component_id',author_owner_component_id,'owner_kind',author_owner_kind,
            'owner_id',author_owner_id)) ORDER BY source_unit_id) FILTER (WHERE n<=32),'[]'::jsonb),
        max(source_unit_id) FILTER (WHERE n<=32),count(*)>32
    INTO refs,next_unit,more_value FROM numbered;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('available',true,'refs',refs,
            'next_source_unit_id',next_unit,'has_more',more_value));
END;
$$;

CREATE FUNCTION memory_read_retrieval_materialization(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    bundle_row __forest_module_schema__.retrieval_bundles%ROWTYPE;
    projection_row __forest_module_schema__.retrieval_materializations%ROWTYPE;
    check_value jsonb;
    index_value integer;
    current_source jsonb;
    max_chars integer;
    max_media_bytes integer;
BEGIN
    SELECT * INTO bundle_row FROM __forest_module_schema__.retrieval_bundles
    WHERE bot_id=context->>'bot_id' AND query_id=input->>'query_id';
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_RETRIEVAL_NOT_FROZEN',
                'message','Query has no frozen retrieval result','status',409));
    END IF;
    check_value := __forest_module_schema__.memory_read_matched_seeds(context,input);
    IF check_value->>'ok' IS DISTINCT FROM 'true' THEN RETURN check_value; END IF;
    max_chars := (check_value#>>'{value,policy,max_retrieval_chars}')::integer;
    max_media_bytes := (check_value#>>'{value,policy,max_retrieval_media_bytes}')::integer;
    SELECT jsonb_build_object('source_id',s.source_id,'source_kind',s.source_kind,
        'platform',s.platform,'realm_id',s.realm_id,'external_group_id',s.external_group_id,
        'trunk_id',s.trunk_id) INTO current_source
    FROM __forest_module_schema__.memory_queries q
    JOIN __forest_module_schema__.memory_sources s ON s.bot_id=q.bot_id AND s.source_id=q.current_source_id
    WHERE q.bot_id=bundle_row.bot_id AND q.query_id=bundle_row.query_id;
    SELECT * INTO projection_row FROM __forest_module_schema__.retrieval_materializations
    WHERE bot_id=bundle_row.bot_id AND bundle_id=bundle_row.bundle_id;
    IF FOUND THEN
        FOREACH index_value IN ARRAY projection_row.item_indexes LOOP
            check_value := __forest_module_schema__.memory_read_retrieval_parts(context,
                jsonb_build_object('query_id',bundle_row.query_id,'item_index',index_value,
                    'after_part_index',-1,'limit',1));
            IF check_value->>'ok' IS DISTINCT FROM 'true' THEN RETURN check_value; END IF;
            IF check_value#>'{value,node}'='null'::jsonb THEN
                RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object('code','MEMORY_EVIDENCE_UNAVAILABLE',
                        'message','Frozen evidence has a revoked source or path','status',409));
            END IF;
        END LOOP;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
        'bundle_id',bundle_row.bundle_id,'result_count',bundle_row.result_count,
        'input_kind',(SELECT input_kind FROM __forest_module_schema__.memory_queries
            WHERE bot_id=bundle_row.bot_id AND query_id=bundle_row.query_id),
        'current_source',current_source,'max_retrieval_chars',max_chars,
        'max_retrieval_media_bytes',max_media_bytes,
        'truncated',bundle_row.truncated,'association_sampled',bundle_row.association_sampled,
        'read_count',bundle_row.read_count,'evidence_ref',projection_row.evidence_ref,
        'item_indexes',COALESCE(to_jsonb(projection_row.item_indexes),'[]'::jsonb),
        'content_truncated',projection_row.content_truncated,'missing_count',projection_row.missing_count));
END;
$$;

CREATE FUNCTION memory_read_query_progress(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_query text := input->>'query_id';
    query_row __forest_module_schema__.memory_queries%ROWTYPE;
    prepared_row __forest_module_schema__.query_facet_preparations%ROWTYPE;
    check_value jsonb;
    phase_value text;
    facet_total integer := 0;
    ready_total integer := 0;
    unknown_total integer := 0;
    failed_total integer := 0;
    matched_total integer := 0;
    reference_value jsonb := 'null'::jsonb;
BEGIN
    check_value := __forest_module_schema__.memory_read_query_parts(context,
        jsonb_build_object('query_id',requested_query,'after_part_index',-1,'limit',1));
    IF check_value->>'ok' IS DISTINCT FROM 'true' THEN RETURN check_value; END IF;
    SELECT * INTO query_row FROM __forest_module_schema__.memory_queries
    WHERE bot_id=requested_bot AND query_id=requested_query;
    SELECT count(*),count(*) FILTER (WHERE state='READY'),
        count(*) FILTER (WHERE state='UNKNOWN'),
        count(*) FILTER (WHERE state IN ('FAILED','SUPPRESSED'))
    INTO facet_total,ready_total,unknown_total,failed_total
    FROM __forest_module_schema__.embedding_facets
    WHERE bot_id=requested_bot AND query_id=requested_query AND purpose='QUERY';
    SELECT count(*) INTO matched_total FROM __forest_module_schema__.query_match_receipts
    WHERE bot_id=requested_bot AND query_id=requested_query;

    IF query_row.state='FROZEN' THEN
        check_value := __forest_module_schema__.memory_read_retrieval_materialization(context,input);
        IF check_value->>'ok' IS DISTINCT FROM 'true' THEN RETURN check_value; END IF;
        reference_value := check_value#>'{value,evidence_ref}';
        phase_value := CASE WHEN reference_value='null'::jsonb
            THEN 'NEED_MATERIALIZATION' ELSE 'READY' END;
    ELSE
        SELECT * INTO prepared_row FROM __forest_module_schema__.query_facet_preparations
        WHERE bot_id=requested_bot AND query_id=requested_query;
        IF NOT FOUND THEN
            phase_value := 'NEED_PREPARATION';
        ELSIF prepared_row.facet_count<>facet_total THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_QUERY_FACET_MISMATCH',
                    'message','Complete preparation differs from its query facets','status',409));
        ELSIF failed_total>0 THEN
            phase_value := 'FAILED';
        ELSIF unknown_total>0 THEN
            phase_value := 'BLOCKED_UNKNOWN';
        ELSIF ready_total<facet_total THEN
            phase_value := 'WAIT_EMBEDDING';
        ELSIF EXISTS (
            SELECT 1 FROM __forest_module_schema__.query_match_preparations
            WHERE bot_id=requested_bot AND query_id=requested_query AND facet_count=matched_total
        ) THEN
            phase_value := 'NEED_FREEZING';
        ELSE
            phase_value := 'NEED_MATCHING';
        END IF;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
        'protocol','forest.memory-query-progress/1','query_id',requested_query,'phase',phase_value,
        'facet_count',facet_total,'ready_count',ready_total,'unknown_count',unknown_total,
        'failed_count',failed_total,'matched_count',matched_total,'evidence_ref',reference_value));
END;
$$;

CREATE FUNCTION memory_read_recall_work(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    work_row __forest_module_schema__.query_recall_work%ROWTYPE;
    trial_row __forest_module_schema__.memory_trial_inputs%ROWTYPE;
    input_kind_value text;
    check_value jsonb;
    state_value text;
    error_value text;
    reference_value jsonb := 'null'::jsonb;
BEGIN
    SELECT * INTO work_row FROM __forest_module_schema__.query_recall_work
    WHERE bot_id=context->>'bot_id' AND query_id=input->>'query_id';
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_RECALL_NOT_REQUESTED',
                'message','Query has no requested recall work','status',404));
    END IF;
    SELECT input_kind INTO input_kind_value FROM __forest_module_schema__.memory_queries
    WHERE bot_id=work_row.bot_id AND query_id=work_row.query_id;
    IF input_kind_value='ADMIN_TRIAL' THEN
        SELECT * INTO trial_row FROM __forest_module_schema__.memory_trial_inputs
        WHERE bot_id=work_row.bot_id AND query_id=work_row.query_id;
        IF NOT FOUND OR trial_row.generation_id IS DISTINCT FROM context->>'generation_id'
           OR trial_row.expires_at<=clock_timestamp() THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_TRIAL_INPUT_UNAVAILABLE',
                    'message','Trial input has expired or its Generation was replaced','status',410));
        END IF;
    END IF;
    state_value := work_row.state;
    error_value := work_row.last_error_code;
    IF state_value='CLAIMED' AND work_row.cancel_requested THEN
        state_value := 'CANCELLING';
    ELSIF state_value NOT IN ('READY','FAILED','CANCELLED') AND work_row.deadline_at<=clock_timestamp() THEN
        state_value := 'FAILED'; error_value := 'MEMORY_RECALL_DEADLINE';
    END IF;
    IF state_value='READY' THEN
        check_value := __forest_module_schema__.memory_read_query_progress(context,input);
        IF check_value->>'ok' IS DISTINCT FROM 'true' THEN RETURN check_value; END IF;
        IF check_value#>>'{value,phase}' IS DISTINCT FROM 'READY' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_EVIDENCE_UNAVAILABLE',
                    'message','Published recall evidence is unavailable','status',409));
        END IF;
        reference_value := check_value#>'{value,evidence_ref}';
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
        'protocol','forest.memory-recall-work/1','query_id',work_row.query_id,'state',state_value,
        'deadline_at',work_row.deadline_at,'error_code',error_value,'evidence_ref',reference_value));
END;
$$;

CREATE FUNCTION memory_cancel_recall(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    work_row __forest_module_schema__.query_recall_work%ROWTYPE;
    check_value jsonb;
    result_status text;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-recall-cancel/1'
       OR body->>'reason_code' IS DISTINCT FROM 'SUPERSEDED' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_RECALL_CANCEL_INVALID',
                'message','Recall cancellation is invalid','status',422));
    END IF;
    check_value := __forest_module_schema__.memory_assert_public_query(context,body);
    IF check_value->>'ok' IS DISTINCT FROM 'true' THEN RETURN check_value; END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.query_recall_work
    WHERE bot_id=context->>'bot_id' AND query_id=body->>'query_id' FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_RECALL_NOT_REQUESTED',
                'message','Query has no requested recall work','status',404));
    END IF;
    IF work_row.cancel_requested THEN
        IF work_row.cancel_reason IS DISTINCT FROM body->>'reason_code' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_RECALL_CANCEL_CONFLICT',
                    'message','Recall was cancelled for another reason','status',409));
        END IF;
        result_status := CASE WHEN work_row.state='CANCELLED' THEN 'CANCELLED' ELSE 'CANCELLING' END;
    ELSIF work_row.state IN ('READY','FAILED') OR work_row.deadline_at<=clock_timestamp() THEN
        result_status := 'TERMINAL';
    ELSIF work_row.state='CLAIMED' THEN
        UPDATE __forest_module_schema__.query_recall_work
        SET cancel_requested=true,cancel_reason='SUPERSEDED',updated_at=clock_timestamp()
        WHERE bot_id=work_row.bot_id AND query_id=work_row.query_id;
        result_status := 'CANCELLING';
    ELSE
        UPDATE __forest_module_schema__.query_recall_work
        SET state='CANCELLED',cancel_requested=true,cancel_reason='SUPERSEDED',
            updated_at=clock_timestamp()
        WHERE bot_id=work_row.bot_id AND query_id=work_row.query_id;
        result_status := 'CANCELLED';
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
        'protocol','forest.memory-recall-cancelled/1','query_id',work_row.query_id,'status',result_status));
END;
$$;

CREATE FUNCTION memory_request_recall(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    requested_bot text := context->>'bot_id';
    requested_query text := body->>'query_id';
    requested_deadline timestamptz := (body->>'deadline_at')::timestamptz;
    work_row __forest_module_schema__.query_recall_work%ROWTYPE;
    check_value jsonb;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-recall-request/1'
       OR requested_query IS NULL OR requested_deadline IS NULL OR NOT isfinite(requested_deadline) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_RECALL_REQUEST_INVALID',
                'message','Recall request is invalid','status',422));
    END IF;
    check_value := __forest_module_schema__.memory_assert_public_query(context,body);
    IF check_value->>'ok' IS DISTINCT FROM 'true' THEN RETURN check_value; END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.query_recall_work
    WHERE bot_id=requested_bot AND query_id=requested_query;
    IF NOT FOUND THEN
        IF requested_deadline<=clock_timestamp() OR requested_deadline>clock_timestamp()+interval '10 minutes' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_RECALL_DEADLINE_INVALID',
                    'message','New recall deadline must be within ten minutes','status',422));
        END IF;
        check_value := __forest_module_schema__.memory_read_query_parts(context,
            jsonb_build_object('query_id',requested_query,'after_part_index',-1,'limit',1));
        IF check_value->>'ok' IS DISTINCT FROM 'true' THEN RETURN check_value; END IF;
        INSERT INTO __forest_module_schema__.query_recall_work(bot_id,query_id,deadline_at)
        VALUES(requested_bot,requested_query,requested_deadline) ON CONFLICT (bot_id,query_id) DO NOTHING;
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.query_recall_work
    WHERE bot_id=requested_bot AND query_id=requested_query FOR UPDATE;
    IF work_row.deadline_at IS DISTINCT FROM requested_deadline THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_RECALL_REQUEST_CONFLICT',
                'message','Recall deadline is already fixed','status',409));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
        'protocol','forest.memory-recall-requested/1','query_id',work_row.query_id,'deadline_at',work_row.deadline_at));
EXCEPTION WHEN invalid_datetime_format OR datetime_field_overflow THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_RECALL_REQUEST_INVALID',
            'message','Recall deadline is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_claim_recall(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    work_row __forest_module_schema__.query_recall_work%ROWTYPE;
    input_kind_value text;
    task_payload jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_token IS NULL OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_RECALL_CLAIM_INVALID',
                'message','Recall claim is invalid','status',422));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.query_recall_work
    WHERE bot_id=requested_bot AND claim_token=requested_token FOR UPDATE;
    IF FOUND THEN
        SELECT input_kind INTO input_kind_value FROM __forest_module_schema__.memory_queries
        WHERE bot_id=requested_bot AND query_id=work_row.query_id;
        IF input_kind_value='ADMIN_TRIAL' AND NOT EXISTS (
            SELECT 1 FROM __forest_module_schema__.memory_trial_inputs
            WHERE bot_id=requested_bot AND query_id=work_row.query_id
              AND generation_id=context->>'generation_id' AND expires_at>clock_timestamp()
        ) THEN
            UPDATE __forest_module_schema__.query_recall_work
            SET state='FAILED',last_error_code='MEMORY_TRIAL_INPUT_UNAVAILABLE',
                claim_token=NULL,claim_payload=NULL,lease_deadline=NULL,
                updated_at=clock_timestamp()
            WHERE bot_id=requested_bot AND query_id=work_row.query_id;
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',work_row.claim_payload);
    END IF;
    IF EXISTS (
        SELECT 1 FROM __forest_module_schema__.query_recall_settlements
        WHERE bot_id=requested_bot AND claim_token=requested_token
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_RECALL_CLAIM_TOKEN_REUSED',
                'message','Recall claim token has already been settled','status',409));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.query_recall_work
    WHERE bot_id=requested_bot AND state IN ('PENDING','CLAIMED','BLOCKED_UNKNOWN')
      AND (state<>'CLAIMED' OR lease_deadline<=clock_timestamp())
      AND (next_attempt_at<=clock_timestamp() OR deadline_at<=clock_timestamp())
    ORDER BY next_attempt_at,query_id LIMIT 1 FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    IF work_row.cancel_requested THEN
        UPDATE __forest_module_schema__.query_recall_work
        SET state='CANCELLED',claim_token=NULL,claim_payload=NULL,lease_deadline=NULL,
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND query_id=work_row.query_id;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    IF work_row.deadline_at<=clock_timestamp() THEN
        UPDATE __forest_module_schema__.query_recall_work
        SET state='FAILED',last_error_code='MEMORY_RECALL_DEADLINE',claim_token=NULL,
            claim_payload=NULL,lease_deadline=NULL,updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND query_id=work_row.query_id;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    SELECT input_kind INTO input_kind_value FROM __forest_module_schema__.memory_queries
    WHERE bot_id=requested_bot AND query_id=work_row.query_id;
    IF input_kind_value='ADMIN_TRIAL' AND NOT EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_trial_inputs
        WHERE bot_id=requested_bot AND query_id=work_row.query_id
          AND generation_id=context->>'generation_id' AND expires_at>clock_timestamp()
    ) THEN
        UPDATE __forest_module_schema__.query_recall_work
        SET state='FAILED',last_error_code='MEMORY_TRIAL_INPUT_UNAVAILABLE',claim_token=NULL,
            claim_payload=NULL,lease_deadline=NULL,updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND query_id=work_row.query_id;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    task_payload := jsonb_build_object('protocol','forest.owned-work-claim/1',
        'work_id',work_row.query_id,'claim_token',requested_token,'attempt',work_row.attempt_count+1,
        'operation_id','memory-recall:'||encode(sha256(convert_to(requested_bot||'/'||requested_token,'UTF8')),'hex'),
        'causal_root_kind','memory.query','causal_root_id',work_row.query_id,
        'payload',jsonb_build_object('query_id',work_row.query_id,'deadline_at',work_row.deadline_at));
    UPDATE __forest_module_schema__.query_recall_work
    SET state='CLAIMED',claim_token=requested_token,claim_payload=task_payload,
        lease_deadline=clock_timestamp()+requested_lease*interval '1 second',
        attempt_count=attempt_count+1,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND query_id=work_row.query_id;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',task_payload);
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_RECALL_CLAIM_INVALID',
            'message','Recall claim is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_settle_recall(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    work_row __forest_module_schema__.query_recall_work%ROWTYPE;
    prior_row __forest_module_schema__.query_recall_settlements%ROWTYPE;
    progress jsonb;
    phase_value text;
    next_state text := 'PENDING';
    next_error text;
    next_delay integer := 1;
    failures integer := 0;
    result_value jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR requested_token IS NULL OR input->>'attempt' !~ '^[1-9][0-9]*$'
       OR input#>>'{outcome,status}' NOT IN ('SUCCEEDED','FAILED') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_RECALL_SETTLEMENT_INVALID',
                'message','Recall settlement is invalid','status',422));
    END IF;
    SELECT * INTO prior_row FROM __forest_module_schema__.query_recall_settlements
    WHERE bot_id=requested_bot AND claim_token=requested_token;
    IF FOUND THEN
        IF prior_row.input_payload=input THEN RETURN prior_row.result_payload; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_RECALL_SETTLEMENT_CONFLICT',
                'message','Recall claim already has another settlement','status',409));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.query_recall_work
    WHERE bot_id=requested_bot AND claim_token=requested_token FOR UPDATE;
    IF NOT FOUND OR work_row.state<>'CLAIMED' OR work_row.query_id IS DISTINCT FROM input->>'work_id'
       OR work_row.attempt_count IS DISTINCT FROM (input->>'attempt')::integer
       OR work_row.lease_deadline<=clock_timestamp() THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_RECALL_CLAIM_LOST',
                'message','Recall claim is no longer current','status',409));
    END IF;
    IF input#>>'{outcome,status}'='SUCCEEDED'
       AND input#>>'{outcome,result,query_id}' IS DISTINCT FROM work_row.query_id THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_RECALL_SETTLEMENT_INVALID',
                'message','Recall result refers to another query','status',422));
    END IF;
    IF NOT work_row.cancel_requested AND work_row.deadline_at>clock_timestamp() THEN
        progress := __forest_module_schema__.memory_read_query_progress(context,
            jsonb_build_object('query_id',work_row.query_id));
        phase_value := progress#>>'{value,phase}';
    END IF;
    IF work_row.cancel_requested THEN
        next_state := 'CANCELLED';
    ELSIF work_row.deadline_at<=clock_timestamp() THEN
        next_state := 'FAILED'; next_error := 'MEMORY_RECALL_DEADLINE';
    ELSIF progress->>'ok' IS DISTINCT FROM 'true' THEN
        next_state := 'FAILED'; next_error := progress#>>'{error,code}';
    ELSIF phase_value='FAILED' THEN
        next_state := 'FAILED'; next_error := 'MEMORY_RECALL_EMBEDDING_FAILED';
    ELSIF phase_value='BLOCKED_UNKNOWN' THEN
        next_state := 'BLOCKED_UNKNOWN'; next_delay := 10;
    ELSIF phase_value='READY' AND input#>>'{outcome,status}'='SUCCEEDED' THEN
        next_state := 'READY';
    ELSIF input#>>'{outcome,status}'='FAILED' THEN
        failures := LEAST(work_row.failure_count+1,3);
        IF failures>=3 OR (input#>>'{outcome,error,retryable}' IS DISTINCT FROM 'true'
            AND input#>>'{outcome,error,unknown}' IS DISTINCT FROM 'true'
            AND input#>>'{outcome,error,code}' NOT IN ('MEMORY_QUERY_VECTORS_PENDING','MEMORY_QUERY_MATCHES_PENDING')) THEN
            next_state := 'FAILED';
            next_error := COALESCE(input#>>'{outcome,error,code}','MEMORY_RECALL_WORK_FAILED');
        ELSE
            next_delay := power(2,failures)::integer;
        END IF;
    END IF;
    UPDATE __forest_module_schema__.query_recall_work
    SET state=next_state,last_error_code=next_error,failure_count=failures,
        claim_token=NULL,claim_payload=NULL,lease_deadline=NULL,
        next_attempt_at=clock_timestamp()+next_delay*interval '1 second',updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND query_id=work_row.query_id;
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.owned-work-settled/1','work_id',work_row.query_id));
    INSERT INTO __forest_module_schema__.query_recall_settlements(bot_id,claim_token,input_payload,result_payload)
    VALUES(requested_bot,requested_token,input,result_value);
    RETURN result_value;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_RECALL_SETTLEMENT_INVALID',
            'message','Recall settlement is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_publish_retrieval_evidence(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    epoch_value jsonb := envelope->'prior'->(envelope#>>'{input,deletion_epoch_step_id}');
    bundle_row __forest_module_schema__.retrieval_bundles%ROWTYPE;
    check_value jsonb;
    index_value integer;
    selected integer[];
    dependency_units text[];
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR epoch_value->>'protocol' IS DISTINCT FROM 'forest.payload-epoch/1' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EVIDENCE_AUTHORITY_MISSING',
                'message','Evidence publication requires the deletion barrier','status',422));
    END IF;
    SELECT * INTO bundle_row FROM __forest_module_schema__.retrieval_bundles
    WHERE bot_id=context->>'bot_id' AND query_id=body->>'query_id' FOR UPDATE;
    check_value := __forest_module_schema__.memory_read_retrieval_materialization(context,body);
    IF check_value->>'ok' IS DISTINCT FROM 'true' THEN RETURN check_value; END IF;
    IF check_value#>'{value,evidence_ref}'<>'null'::jsonb THEN RETURN check_value; END IF;
    SELECT COALESCE(array_agg(value::integer ORDER BY ordinality),ARRAY[]::integer[]) INTO selected
    FROM jsonb_array_elements_text(body->'item_indexes') WITH ORDINALITY;
    IF selected IS DISTINCT FROM (SELECT COALESCE(array_agg(DISTINCT v ORDER BY v),ARRAY[]::integer[]) FROM unnest(selected) v)
       OR cardinality(selected)+(body->>'missing_count')::integer<>bundle_row.result_count
       OR EXISTS (SELECT 1 FROM unnest(selected) v WHERE v<0 OR v>=bundle_row.result_count)
       OR body#>>'{evidence_ref,owner_component_id}' IS DISTINCT FROM 'memory.graph'
       OR body#>>'{evidence_ref,owner_kind}' IS DISTINCT FROM 'retrieval.evidence'
       OR left(body#>>'{evidence_ref,owner_id}',length(bundle_row.bundle_id)+1)
            IS DISTINCT FROM bundle_row.bundle_id||'/' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EVIDENCE_INVALID',
                'message','Evidence selection or ownership is invalid','status',422));
    END IF;
    -- Hold semantic dependencies until publication commits. Negative writers
    -- use the same order: Payload clock, source, unit, contribution.
    WITH RECURSIVE path AS (
        SELECT i.source_unit_id,i.path_parent_source_unit_id,0 AS depth
        FROM __forest_module_schema__.retrieval_items i
        WHERE i.bot_id=context->>'bot_id' AND i.bundle_id=bundle_row.bundle_id
          AND i.item_index=ANY(selected)
        UNION
        SELECT i.source_unit_id,i.path_parent_source_unit_id,p.depth+1 FROM path p
        JOIN __forest_module_schema__.retrieval_items i
          ON i.bot_id=context->>'bot_id' AND i.bundle_id=bundle_row.bundle_id
         AND i.source_unit_id=p.path_parent_source_unit_id WHERE p.depth<63
    ), dependencies AS (
        SELECT source_unit_id FROM path
        UNION
        SELECT e.source_unit_id FROM path p JOIN __forest_module_schema__.retrieval_relations r
          ON r.bot_id=context->>'bot_id' AND r.bundle_id=bundle_row.bundle_id AND r.target_unit_id=p.source_unit_id
        JOIN __forest_module_schema__.relation_evidence e ON e.bot_id=r.bot_id AND e.contribution_id=r.contribution_id
        UNION
        SELECT source_unit_id FROM __forest_module_schema__.memory_query_parts
        WHERE bot_id=context->>'bot_id' AND query_id=bundle_row.query_id
    ) SELECT COALESCE(array_agg(source_unit_id ORDER BY source_unit_id),ARRAY[]::text[])
    INTO dependency_units FROM dependencies;
    PERFORM 1 FROM __forest_module_schema__.memory_sources s
    WHERE s.bot_id=context->>'bot_id' AND s.source_id IN (
        SELECT source_id FROM __forest_module_schema__.source_units
        WHERE bot_id=s.bot_id AND source_unit_id=ANY(dependency_units))
    ORDER BY s.source_id FOR SHARE;
    PERFORM 1 FROM __forest_module_schema__.source_units u
    WHERE u.bot_id=context->>'bot_id' AND u.source_unit_id=ANY(dependency_units)
    ORDER BY u.source_unit_id FOR SHARE;
    PERFORM 1 FROM __forest_module_schema__.relation_contributions c
    WHERE c.bot_id=context->>'bot_id' AND c.contribution_id IN (
        SELECT contribution_id FROM __forest_module_schema__.retrieval_relations
        WHERE bot_id=c.bot_id AND bundle_id=bundle_row.bundle_id)
    ORDER BY c.contribution_id FOR SHARE;
    FOR index_value IN 0..bundle_row.result_count-1 LOOP
        check_value := __forest_module_schema__.memory_read_retrieval_parts(context,
            jsonb_build_object('query_id',bundle_row.query_id,'item_index',index_value,
                'after_part_index',-1,'limit',1));
        IF check_value->>'ok' IS DISTINCT FROM 'true' THEN RETURN check_value; END IF;
        IF check_value#>'{value,node}'='null'::jsonb AND index_value=ANY(selected) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_EVIDENCE_UNAVAILABLE',
                    'message','Evidence source changed before publication','status',409));
        END IF;
        IF check_value#>'{value,node}'<>'null'::jsonb AND NOT index_value=ANY(selected) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_EVIDENCE_INVALID',
                    'message','Evidence cannot omit an eligible frozen item','status',422));
        END IF;
    END LOOP;
    INSERT INTO __forest_module_schema__.retrieval_materializations(
        bot_id,bundle_id,evidence_ref,item_indexes,accepted_deletion_epoch,content_truncated,missing_count)
    VALUES(context->>'bot_id',bundle_row.bundle_id,body->'evidence_ref',selected,
        (epoch_value->>'deletion_epoch')::bigint,(body->>'content_truncated')::boolean,
        (body->>'missing_count')::integer);
    RETURN __forest_module_schema__.memory_read_retrieval_materialization(context,body);
END;
$$;

CREATE FUNCTION memory_query_occurrence_matches(
    requested_bot text, requested_query text, requested_unit text,
    requested_revision bigint, requested_part text, requested_occurrence text
) RETURNS boolean LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_query_parts p
        JOIN __forest_module_schema__.memory_queries q
          ON q.bot_id=p.bot_id AND q.query_id=p.query_id AND q.state='OPEN'
        JOIN __forest_module_schema__.query_facet_preparations prepared
          ON prepared.bot_id=q.bot_id AND prepared.query_id=q.query_id
        JOIN __forest_module_schema__.source_occurrences o
          ON o.bot_id=p.bot_id AND o.occurrence_id=p.occurrence_id
         AND o.source_unit_id=p.source_unit_id
         AND o.input_batch_ref=q.input_batch_ref AND o.context_revision=q.context_revision
         AND o.eligibility_state IN ('ADMITTED','ELIGIBLE')
        WHERE p.bot_id=requested_bot AND p.query_id=requested_query
          AND p.source_unit_id=requested_unit AND p.source_revision=requested_revision
          AND p.part_id=requested_part AND p.occurrence_id=requested_occurrence
    );
$$;

CREATE FUNCTION memory_claim_vector_publish(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    work_row __forest_module_schema__.vector_publish_batches%ROWTYPE;
    attempt_row __forest_module_schema__.embedding_batch_attempts%ROWTYPE;
    claim_row __forest_module_schema__.embedding_claims%ROWTYPE;
    facets_value jsonb;
    task_payload jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_CLAIM_INVALID',
                'message','Vector publish claim is invalid','status',422));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.vector_publish_batches
    WHERE bot_id=requested_bot AND claim_token=requested_token;
    IF FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',work_row.claim_payload);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_domain_bind_work
        WHERE bot_id=requested_bot AND state='READY'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    INSERT INTO __forest_module_schema__.vector_publish_batches(bot_id,attempt_id)
    SELECT DISTINCT a.bot_id,a.attempt_id
    FROM __forest_module_schema__.embedding_batch_attempts a
    JOIN __forest_module_schema__.embedding_claim_items ci
      ON ci.bot_id=a.bot_id AND ci.claim_id=a.claim_id
    JOIN __forest_module_schema__.embedding_facets f
      ON f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
    WHERE a.bot_id=requested_bot AND a.state='SUCCEEDED'
      AND f.state='EMBEDDED'
    ORDER BY a.bot_id,a.attempt_id LIMIT 32
    ON CONFLICT (bot_id,attempt_id) DO NOTHING;
    SELECT w.* INTO work_row FROM __forest_module_schema__.vector_publish_batches w
    WHERE w.bot_id=requested_bot AND w.next_attempt_at<=clock_timestamp()
      AND (w.state='PENDING'
           OR (w.state='CLAIMED' AND w.lease_deadline<=clock_timestamp()))
    ORDER BY w.next_attempt_at,w.attempt_id LIMIT 1 FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    SELECT * INTO attempt_row FROM __forest_module_schema__.embedding_batch_attempts
    WHERE bot_id=requested_bot AND attempt_id=work_row.attempt_id;
    SELECT * INTO claim_row FROM __forest_module_schema__.embedding_claims
    WHERE bot_id=requested_bot AND claim_id=attempt_row.claim_id;
    IF attempt_row.state<>'SUCCEEDED' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_ATTEMPT_INVALID',
                'message','Vector publish attempt is not settled','status',409));
    END IF;
    SELECT jsonb_agg(jsonb_build_object(
        'facet_id',f.facet_id,'item_index',ci.item_index,
        'modality',f.modality,'state',f.state,'vector_digest',f.vector_digest
    ) ORDER BY ci.item_index) INTO facets_value
    FROM __forest_module_schema__.embedding_claim_items ci
    JOIN __forest_module_schema__.embedding_facets f
      ON f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
    WHERE ci.bot_id=requested_bot AND ci.claim_id=attempt_row.claim_id;
    IF jsonb_array_length(facets_value)<>claim_row.item_count THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_MEMBERS_INVALID',
                'message','Vector publish batch membership changed','status',409));
    END IF;
    task_payload := jsonb_build_object(
        'protocol','forest.owned-work-claim/1',
        'work_id',work_row.attempt_id,'claim_token',requested_token,
        'attempt',work_row.attempt_count+1,
        'operation_id','memory-vector-publish:'||encode(sha256(convert_to(
            requested_bot||'/'||requested_token,'UTF8')),'hex'),
        'causal_root_kind','memory.embedding.batch',
        'causal_root_id',attempt_row.claim_id,
        'payload',jsonb_build_object(
            'attempt_id',work_row.attempt_id,'claim_token',requested_token,
            'space_id',claim_row.space_id,'purpose',claim_row.purpose,
            'result',jsonb_build_object(
                'payload_id',attempt_row.result_payload_id,
                'digest',attempt_row.result_payload_digest,
                'owner_component_id','embedding.multimodal',
                'owner_kind','embedding.result',
                'owner_id',attempt_row.attempt_id),
            'facets',facets_value));
    UPDATE __forest_module_schema__.vector_publish_batches
    SET state='CLAIMED',claim_token=requested_token,claim_payload=task_payload,
        lease_deadline=clock_timestamp()+(requested_lease*interval '1 second'),
        attempt_count=attempt_count+1,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND attempt_id=work_row.attempt_id;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',task_payload);
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_CLAIM_INVALID',
            'message','Vector publish claim is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_publish_vector_batch(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    results_value jsonb := body->'results';
    epoch_value jsonb := envelope#>'{prior,payload_epoch}';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    epoch_number bigint := (epoch_value->>'deletion_epoch')::bigint;
    work_row __forest_module_schema__.vector_publish_batches%ROWTYPE;
    attempt_row __forest_module_schema__.embedding_batch_attempts%ROWTYPE;
    claim_row __forest_module_schema__.embedding_claims%ROWTYPE;
    prior_row __forest_module_schema__.vector_publish_operations%ROWTYPE;
    facet_row record;
    item_result jsonb;
    input_eligible boolean;
    ready_count integer := 0;
    deletion_count integer := 0;
    seen_count integer := 0;
    result_value jsonb;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-vector-publish/1'
       OR epoch_value->>'protocol' IS DISTINCT FROM 'forest.payload-epoch/1'
       OR requested_bot IS NULL OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR epoch_number IS NULL OR epoch_number<0
       OR jsonb_typeof(results_value) IS DISTINCT FROM 'array'
       OR body->>'attempt_id' IS NULL OR body->>'claim_token' IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_INVALID',
                'message','Vector publication evidence is invalid','status',422));
    END IF;
    SELECT * INTO prior_row FROM __forest_module_schema__.vector_publish_operations
    WHERE bot_id=requested_bot AND operation_id=requested_operation;
    IF FOUND THEN
        IF prior_row.input_digest=requested_digest THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
                'value',prior_row.result_payload);
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_CONFLICT',
                'message','Vector publication operation was reused','status',409));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.vector_publish_batches
    WHERE bot_id=requested_bot AND attempt_id=body->>'attempt_id' FOR UPDATE;
    SELECT * INTO attempt_row FROM __forest_module_schema__.embedding_batch_attempts
    WHERE bot_id=requested_bot AND attempt_id=body->>'attempt_id';
    IF work_row IS NULL OR work_row.state<>'CLAIMED'
       OR work_row.claim_token IS DISTINCT FROM body->>'claim_token'
       OR attempt_row IS NULL OR attempt_row.state<>'SUCCEEDED' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_CLAIM_LOST',
                'message','Vector publication claim is no longer current','status',409));
    END IF;
    SELECT * INTO claim_row FROM __forest_module_schema__.embedding_claims
    WHERE bot_id=requested_bot AND claim_id=attempt_row.claim_id;
    IF claim_row.space_id IS DISTINCT FROM
        body->>'space_id' OR jsonb_array_length(results_value)<>claim_row.item_count THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_MEMBERS_INVALID',
                'message','Vector publication membership differs','status',409));
    END IF;
    FOR facet_row IN
        SELECT ci.item_index,f.facet_id,f.state AS facet_state,f.input_kind,
               f.purpose,f.source_revision,f.input_revision,
               f.input_payload_id,f.input_payload_digest,f.space_id,
               f.accepted_deletion_epoch,f.index_ref_id,
               source_data.unit_revision,source_data.unit_state,
               source_data.unit_epoch,source_data.source_state,
               source_data.occurrence_id,
               trial_data.input_revision AS trial_revision,
               trial_data.generation_id AS trial_generation,
               trial_data.expires_at AS trial_expiry,
               trial_data.cancelled_at AS trial_cancelled_at,
               trial_data.query_state,trial_data.query_epoch,
               trial_data.payload_id AS trial_payload_id,
               trial_data.payload_digest AS trial_payload_digest
        FROM __forest_module_schema__.embedding_claim_items ci
        JOIN __forest_module_schema__.embedding_facets f
          ON f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
        LEFT JOIN LATERAL (
            SELECT u.source_revision AS unit_revision,
                   u.eligibility_state AS unit_state,
                   u.accepted_deletion_epoch AS unit_epoch,
                   s.status AS source_state,o.occurrence_id
            FROM __forest_module_schema__.source_units u
            JOIN __forest_module_schema__.memory_sources s
              ON s.bot_id=u.bot_id AND s.source_id=u.source_id
            LEFT JOIN LATERAL (
                SELECT occurrence.occurrence_id
                FROM __forest_module_schema__.source_occurrences occurrence
                WHERE occurrence.bot_id=f.bot_id
                  AND occurrence.source_unit_id=f.source_unit_id
                  AND ((f.purpose='DOCUMENT' AND occurrence.eligibility_state='ELIGIBLE')
                       OR (f.purpose='QUERY' AND
                           __forest_module_schema__.memory_query_occurrence_matches(
                               f.bot_id,f.query_id,f.source_unit_id,f.source_revision,
                               f.part_id,occurrence.occurrence_id)))
                ORDER BY occurrence.occurrence_id LIMIT 1 FOR SHARE
            ) o ON true
            WHERE f.input_kind='SOURCE_UNIT'
              AND u.bot_id=f.bot_id AND u.source_unit_id=f.source_unit_id
            FOR SHARE OF u,s
        ) source_data ON true
        LEFT JOIN LATERAL (
            SELECT t.input_revision,t.generation_id,t.expires_at,t.cancelled_at,
                   q.state AS query_state,
                   q.accepted_deletion_epoch AS query_epoch,
                   p.payload_id,p.payload_digest
            FROM __forest_module_schema__.memory_trial_inputs t
            JOIN __forest_module_schema__.memory_queries q
              ON q.bot_id=t.bot_id AND q.query_id=t.query_id
             AND q.input_kind='ADMIN_TRIAL'
            JOIN __forest_module_schema__.memory_trial_parts p
              ON p.bot_id=t.bot_id AND p.query_id=t.query_id
             AND p.part_id=f.part_id AND p.modality=f.modality
            WHERE f.input_kind='ADMIN_TRIAL'
              AND t.bot_id=f.bot_id AND t.query_id=f.trial_query_id
              AND t.query_id=f.query_id
            FOR SHARE OF t,q,p
        ) trial_data ON true
        WHERE ci.bot_id=requested_bot AND ci.claim_id=attempt_row.claim_id
        ORDER BY ci.item_index FOR UPDATE OF f
    LOOP
        seen_count := seen_count+1;
        item_result := results_value->facet_row.item_index;
        IF item_result->>'facet_id' IS DISTINCT FROM facet_row.facet_id
           OR item_result->>'status' NOT IN ('PUBLISHED','TOMBSTONED','SKIPPED')
           OR facet_row.space_id IS DISTINCT FROM claim_row.space_id
           OR facet_row.purpose IS DISTINCT FROM claim_row.purpose
           OR facet_row.facet_state NOT IN ('EMBEDDED','READY','SUPPRESSED')
           OR facet_row.accepted_deletion_epoch>epoch_number
           OR facet_row.unit_epoch>epoch_number
           OR facet_row.query_epoch>epoch_number THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_MEMBERS_INVALID',
                    'message','Vector publication facet differs','status',409));
        END IF;
        input_eligible := facet_row.facet_state<>'SUPPRESSED' AND (
            (facet_row.input_kind='SOURCE_UNIT'
             AND facet_row.unit_state IN ('ELIGIBLE','REGISTERED')
             AND (facet_row.unit_state='ELIGIBLE' OR facet_row.purpose='QUERY')
             AND facet_row.unit_revision=facet_row.source_revision
             AND facet_row.source_state='ACTIVE'
             AND facet_row.occurrence_id IS NOT NULL)
            OR
            (facet_row.input_kind='ADMIN_TRIAL' AND facet_row.purpose='QUERY'
             AND facet_row.trial_revision=facet_row.input_revision
             AND facet_row.trial_generation=context->>'generation_id'
             AND facet_row.trial_expiry>clock_timestamp()
             AND facet_row.trial_cancelled_at IS NULL
             AND facet_row.query_state='OPEN'
             AND facet_row.trial_payload_id=facet_row.input_payload_id
             AND facet_row.trial_payload_digest=facet_row.input_payload_digest)
        );
        IF input_eligible THEN
            IF item_result->>'status'<>'PUBLISHED' THEN
                RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object('code','MEMORY_VECTOR_REF_NOT_PUBLISHED',
                        'message','Eligible vector ref was not published','status',409));
            END IF;
            IF facet_row.facet_state='EMBEDDED' THEN
                UPDATE __forest_module_schema__.embedding_facets
                SET state='READY',index_ref_id=facet_row.facet_id,
                    published_at=clock_timestamp(),updated_at=clock_timestamp()
                WHERE bot_id=requested_bot AND facet_id=facet_row.facet_id;
            ELSIF facet_row.index_ref_id IS DISTINCT FROM facet_row.facet_id THEN
                RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object('code','MEMORY_VECTOR_REF_CONFLICT',
                        'message','Published vector ref differs','status',409));
            END IF;
            ready_count := ready_count+1;
        ELSE
            INSERT INTO __forest_module_schema__.vector_index_deletions(
                bot_id,index_ref_id,space_id,deletion_epoch)
            VALUES(requested_bot,facet_row.facet_id,facet_row.space_id,epoch_number)
            ON CONFLICT (bot_id,index_ref_id) DO NOTHING;
            IF facet_row.facet_state IN ('EMBEDDED','READY') THEN
                UPDATE __forest_module_schema__.embedding_facets
                SET state='SUPPRESSED',index_ref_id=NULL,vector_digest=NULL,
                    result_payload_id=NULL,result_payload_digest=NULL,
                    published_at=NULL,updated_at=clock_timestamp()
                WHERE bot_id=requested_bot AND facet_id=facet_row.facet_id;
            END IF;
            deletion_count := deletion_count+1;
        END IF;
    END LOOP;
    IF seen_count<>claim_row.item_count THEN
        RAISE EXCEPTION 'Vector publication membership changed under lock'
            USING ERRCODE='55000';
    END IF;
    result_value := jsonb_build_object('protocol','forest.memory-vector-published/1',
        'attempt_id',attempt_row.attempt_id,'ready_count',ready_count,
        'deletion_count',deletion_count);
    INSERT INTO __forest_module_schema__.vector_publish_operations(
        bot_id,operation_id,input_digest,result_payload)
    VALUES(requested_bot,requested_operation,requested_digest,result_value);
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',result_value);
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_INVALID',
            'message','Vector publication evidence is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_settle_vector_publish(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    prior_row __forest_module_schema__.vector_work_settlements%ROWTYPE;
    work_row __forest_module_schema__.vector_publish_batches%ROWTYPE;
    result_value jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR input->>'work_id' IS NULL
       OR input->>'attempt' !~ '^[1-9][0-9]*$'
       OR input#>>'{outcome,status}' NOT IN ('SUCCEEDED','FAILED') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_SETTLEMENT_INVALID',
                'message','Vector work settlement is invalid','status',422));
    END IF;
    SELECT * INTO prior_row FROM __forest_module_schema__.vector_work_settlements
    WHERE bot_id=requested_bot AND claim_token=requested_token;
    IF FOUND THEN
        IF prior_row.input_payload=input THEN RETURN prior_row.result_payload; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_SETTLEMENT_CONFLICT',
                'message','Vector work was settled differently','status',409));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.vector_publish_batches
    WHERE bot_id=requested_bot AND claim_token=requested_token FOR UPDATE;
    IF work_row IS NULL OR work_row.state<>'CLAIMED'
       OR work_row.attempt_id IS DISTINCT FROM input->>'work_id'
       OR work_row.attempt_count IS DISTINCT FROM (input->>'attempt')::integer THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_CLAIM_LOST',
                'message','Vector publish claim is no longer current','status',409));
    END IF;
    IF input#>>'{outcome,status}'='SUCCEEDED' THEN
        IF input#>>'{outcome,result,attempt_id}' IS DISTINCT FROM work_row.attempt_id
           OR EXISTS (
               SELECT 1 FROM __forest_module_schema__.embedding_claim_items ci
               JOIN __forest_module_schema__.embedding_batch_attempts a
                 ON a.bot_id=ci.bot_id AND a.claim_id=ci.claim_id
               JOIN __forest_module_schema__.embedding_facets f
                 ON f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
               WHERE a.bot_id=requested_bot AND a.attempt_id=work_row.attempt_id
                 AND f.state NOT IN ('READY','SUPPRESSED')
           ) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_NOT_COMPLETE',
                    'message','Vector publish result was not committed','status',409));
        END IF;
        UPDATE __forest_module_schema__.vector_publish_batches
        SET state='SETTLED',claim_token=NULL,claim_payload=NULL,lease_deadline=NULL,
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND attempt_id=work_row.attempt_id;
    ELSE
        UPDATE __forest_module_schema__.vector_publish_batches
        SET state='PENDING',claim_token=NULL,claim_payload=NULL,lease_deadline=NULL,
            next_attempt_at=clock_timestamp()+
                (LEAST(power(2,LEAST(attempt_count,8)),300)*interval '1 second'),
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND attempt_id=work_row.attempt_id;
    END IF;
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.owned-work-settled/1',
            'work_id',work_row.attempt_id));
    INSERT INTO __forest_module_schema__.vector_work_settlements(
        bot_id,claim_token,input_payload,result_payload)
    VALUES(requested_bot,requested_token,input,result_value);
    RETURN result_value;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_VECTOR_SETTLEMENT_INVALID',
            'message','Vector work settlement is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_reap_expired_trial(requested_bot text)
RETURNS integer LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    trial_row record;
    gc_cursor __forest_module_schema__.memory_trial_gc_cursors%ROWTYPE;
    claim_ids text[];
    attempt_ids text[];
    prior_gc text;
    considered boolean := false;
BEGIN
    INSERT INTO __forest_module_schema__.memory_trial_gc_cursors(bot_id)
    VALUES(requested_bot) ON CONFLICT (bot_id) DO NOTHING;
    SELECT * INTO gc_cursor FROM __forest_module_schema__.memory_trial_gc_cursors
    WHERE bot_id=requested_bot FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN RETURN 0; END IF;
    -- The durable cursor rotates past unsettled trials; each poll still checks
    -- at most eight rows and deletes at most one. The next empty pass wraps.
    FOR trial_row IN
        SELECT t.query_id,t.expires_at,q.bundle_id
        FROM __forest_module_schema__.memory_trial_inputs t
        JOIN __forest_module_schema__.memory_queries q
          ON q.bot_id=t.bot_id AND q.query_id=t.query_id
         AND q.input_kind='ADMIN_TRIAL'
        WHERE t.bot_id=requested_bot AND t.expires_at<=clock_timestamp()
          AND (gc_cursor.last_expires_at IS NULL OR
               (t.expires_at,t.query_id)>(gc_cursor.last_expires_at,gc_cursor.last_query_id))
        ORDER BY t.expires_at,t.query_id LIMIT 8
        FOR UPDATE OF t SKIP LOCKED
    LOOP
        considered := true;
        UPDATE __forest_module_schema__.memory_trial_gc_cursors
        SET last_expires_at=trial_row.expires_at,last_query_id=trial_row.query_id,
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot;
        UPDATE __forest_module_schema__.query_recall_work w
        SET state='FAILED',last_error_code='MEMORY_TRIAL_INPUT_UNAVAILABLE',
            claim_token=NULL,claim_payload=NULL,lease_deadline=NULL,
            updated_at=clock_timestamp()
        WHERE w.bot_id=requested_bot AND w.query_id=trial_row.query_id
          AND w.state IN ('PENDING','CLAIMED','BLOCKED_UNKNOWN')
          AND (w.state<>'CLAIMED' OR w.lease_deadline<=clock_timestamp());
        IF EXISTS (
            SELECT 1 FROM __forest_module_schema__.embedding_facets f
            LEFT JOIN __forest_module_schema__.vector_index_deletions d
              ON d.bot_id=f.bot_id AND d.index_ref_id=f.facet_id
            WHERE f.bot_id=requested_bot AND f.trial_query_id=trial_row.query_id
              AND (f.state<>'SUPPRESSED'
                   OR d.state IS NOT NULL AND d.state<>'DELETED'
                   OR ((f.attempt_count>0 OR f.current_attempt_id IS NOT NULL)
                       AND d.state IS DISTINCT FROM 'DELETED'))
        ) OR EXISTS (
            SELECT 1 FROM __forest_module_schema__.query_recall_work w
            WHERE w.bot_id=requested_bot AND w.query_id=trial_row.query_id
              AND w.state NOT IN ('READY','FAILED','CANCELLED')
        ) OR EXISTS (
            SELECT 1 FROM __forest_module_schema__.association_journal j
            WHERE j.bot_id=requested_bot AND j.bundle_id=trial_row.bundle_id
        ) THEN CONTINUE; END IF;

        SELECT coalesce(array_agg(DISTINCT ci.claim_id),ARRAY[]::text[])
        INTO claim_ids
        FROM __forest_module_schema__.embedding_claim_items ci
        JOIN __forest_module_schema__.embedding_facets f
          ON f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
        WHERE f.bot_id=requested_bot AND f.trial_query_id=trial_row.query_id;
        SELECT coalesce(array_agg(a.attempt_id),ARRAY[]::text[])
        INTO attempt_ids
        FROM __forest_module_schema__.embedding_batch_attempts a
        WHERE a.bot_id=requested_bot AND a.claim_id=ANY(claim_ids);
        UPDATE __forest_module_schema__.embedding_claims c
        SET state='EXPIRED'
        WHERE c.bot_id=requested_bot AND c.claim_id=ANY(claim_ids)
          AND c.state='CLAIMED' AND c.lease_deadline<=clock_timestamp();
        IF EXISTS (
            SELECT 1 FROM __forest_module_schema__.embedding_claim_items ci
            JOIN __forest_module_schema__.embedding_facets f
              ON f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
            WHERE ci.bot_id=requested_bot AND ci.claim_id=ANY(claim_ids)
              AND (f.input_kind<>'ADMIN_TRIAL' OR f.query_id<>trial_row.query_id)
        ) OR EXISTS (
            SELECT 1 FROM __forest_module_schema__.embedding_claims c
            WHERE c.bot_id=requested_bot AND c.claim_id=ANY(claim_ids)
              AND c.state IN ('CLAIMED','ATTEMPT_BOUND')
        ) OR EXISTS (
            SELECT 1 FROM __forest_module_schema__.embedding_batch_attempts a
            WHERE a.bot_id=requested_bot AND a.attempt_id=ANY(attempt_ids)
              AND a.state IN ('PREPARED','DISPATCHED')
        ) OR EXISTS (
            SELECT 1 FROM __forest_module_schema__.embedding_recovery_work r
            WHERE r.bot_id=requested_bot AND r.claim_id=ANY(claim_ids)
              AND r.state<>'SETTLED'
        ) OR EXISTS (
            SELECT 1 FROM __forest_module_schema__.vector_publish_batches b
            WHERE b.bot_id=requested_bot AND b.attempt_id=ANY(attempt_ids)
              AND b.state<>'SETTLED'
        ) OR EXISTS (
            SELECT 1 FROM __forest_module_schema__.embedding_batch_attempts a
            JOIN __forest_module_schema__.embedding_budget_reservations r
              ON r.bot_id=a.bot_id AND r.reservation_id=a.reservation_id
            WHERE a.bot_id=requested_bot AND a.attempt_id=ANY(attempt_ids)
              AND r.state='RESERVED'
        ) THEN CONTINUE; END IF;

        prior_gc := current_setting('forest.memory_trial_gc',true);
        PERFORM set_config('forest.memory_trial_gc',jsonb_build_object(
            'bot_id',requested_bot,'query_id',trial_row.query_id,
            'bundle_id',trial_row.bundle_id,'claim_ids',to_jsonb(claim_ids),
            'attempt_ids',to_jsonb(attempt_ids))::text,true);
        DELETE FROM __forest_module_schema__.retrieval_relations
        WHERE bot_id=requested_bot AND bundle_id=trial_row.bundle_id;
        DELETE FROM __forest_module_schema__.retrieval_materializations
        WHERE bot_id=requested_bot AND bundle_id=trial_row.bundle_id;
        DELETE FROM __forest_module_schema__.retrieval_items
        WHERE bot_id=requested_bot AND bundle_id=trial_row.bundle_id;
        DELETE FROM __forest_module_schema__.query_seed_matches
        WHERE bot_id=requested_bot AND query_id=trial_row.query_id;
        DELETE FROM __forest_module_schema__.query_match_receipts
        WHERE bot_id=requested_bot AND query_id=trial_row.query_id;
        DELETE FROM __forest_module_schema__.embedding_claim_items
        WHERE bot_id=requested_bot AND claim_id=ANY(claim_ids);
        DELETE FROM __forest_module_schema__.embedding_facets
        WHERE bot_id=requested_bot AND trial_query_id=trial_row.query_id;
        DELETE FROM __forest_module_schema__.vector_publish_batches
        WHERE bot_id=requested_bot AND attempt_id=ANY(attempt_ids);
        DELETE FROM __forest_module_schema__.embedding_recovery_work
        WHERE bot_id=requested_bot AND claim_id=ANY(claim_ids);
        DELETE FROM __forest_module_schema__.embedding_batch_attempts
        WHERE bot_id=requested_bot AND attempt_id=ANY(attempt_ids);
        DELETE FROM __forest_module_schema__.embedding_claims
        WHERE bot_id=requested_bot AND claim_id=ANY(claim_ids);
        DELETE FROM __forest_module_schema__.embedding_work_settlements
        WHERE bot_id=requested_bot AND claim_token=ANY(claim_ids);
        DELETE FROM __forest_module_schema__.query_recall_settlements
        WHERE bot_id=requested_bot
          AND input_payload#>>'{payload,query_id}'=trial_row.query_id;
        DELETE FROM __forest_module_schema__.memory_trial_parts
        WHERE bot_id=requested_bot AND query_id=trial_row.query_id;
        DELETE FROM __forest_module_schema__.memory_trial_inputs
        WHERE bot_id=requested_bot AND query_id=trial_row.query_id;
        DELETE FROM __forest_module_schema__.query_facet_preparations
        WHERE bot_id=requested_bot AND query_id=trial_row.query_id;
        DELETE FROM __forest_module_schema__.query_recall_work
        WHERE bot_id=requested_bot AND query_id=trial_row.query_id;
        DELETE FROM __forest_module_schema__.facet_preparation_operations
        WHERE bot_id=requested_bot AND target_kind='QUERY'
          AND target_id=trial_row.query_id;
        DELETE FROM __forest_module_schema__.query_match_preparations
        WHERE bot_id=requested_bot AND query_id=trial_row.query_id;
        DELETE FROM __forest_module_schema__.memory_queries
        WHERE bot_id=requested_bot AND query_id=trial_row.query_id;
        DELETE FROM __forest_module_schema__.retrieval_bundles
        WHERE bot_id=requested_bot AND bundle_id=trial_row.bundle_id;
        PERFORM set_config('forest.memory_trial_gc',coalesce(prior_gc,''),true);
        RETURN 1;
    END LOOP;
    IF NOT considered THEN
        UPDATE __forest_module_schema__.memory_trial_gc_cursors
        SET last_expires_at=NULL,last_query_id=NULL,updated_at=clock_timestamp()
        WHERE bot_id=requested_bot;
    END IF;
    RETURN 0;
END;
$$;

CREATE FUNCTION memory_claim_vector_deletion(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    work_row __forest_module_schema__.vector_index_deletions%ROWTYPE;
    expired_facet record;
    task_payload jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_DELETE_CLAIM_INVALID',
                'message','Vector deletion claim is invalid','status',422));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.vector_index_deletions
    WHERE bot_id=requested_bot AND claim_token=requested_token;
    IF FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',work_row.claim_payload);
    END IF;
    -- Expiry and cancellation are Memory facts. Retire a bounded set before
    -- selecting deletion work so a silent Bot also removes stale trial vectors.
    FOR expired_facet IN
        SELECT f.facet_id,f.space_id,f.accepted_deletion_epoch,f.state,
               f.attempt_count,f.current_attempt_id
        FROM __forest_module_schema__.memory_trial_inputs t
        JOIN __forest_module_schema__.embedding_facets f
          ON f.bot_id=t.bot_id AND f.trial_query_id=t.query_id
         AND f.input_kind='ADMIN_TRIAL'
        WHERE t.bot_id=requested_bot
          AND (t.expires_at<=clock_timestamp() OR t.cancelled_at IS NOT NULL)
          AND f.state IN ('PENDING','PREPARED','DISPATCHED','EMBEDDED','READY','UNKNOWN','FAILED')
          AND (f.state NOT IN ('DISPATCHED','UNKNOWN') OR NOT EXISTS (
                   SELECT 1 FROM __forest_module_schema__.vector_index_deletions d
                   WHERE d.bot_id=f.bot_id AND d.index_ref_id=f.facet_id
               ))
        ORDER BY t.expires_at,f.facet_id LIMIT 32
        FOR UPDATE OF f SKIP LOCKED
    LOOP
        -- A retried PENDING/PREPARED or FAILED facet may still have a delayed
        -- external write from an earlier attempt. Fence every attempted facet.
        IF expired_facet.attempt_count>0
           OR expired_facet.current_attempt_id IS NOT NULL
           OR expired_facet.state IN ('DISPATCHED','UNKNOWN','EMBEDDED','READY') THEN
            INSERT INTO __forest_module_schema__.vector_index_deletions(
                bot_id,index_ref_id,space_id,deletion_epoch)
            VALUES(requested_bot,expired_facet.facet_id,expired_facet.space_id,
                expired_facet.accepted_deletion_epoch)
            ON CONFLICT (bot_id,index_ref_id) DO NOTHING;
        END IF;
        IF expired_facet.state IN ('PENDING','PREPARED','EMBEDDED','READY','FAILED') THEN
            UPDATE __forest_module_schema__.embedding_facets
            SET state='SUPPRESSED',index_ref_id=NULL,vector_digest=NULL,
                result_payload_id=NULL,result_payload_digest=NULL,
                published_at=NULL,updated_at=clock_timestamp()
            WHERE bot_id=requested_bot AND facet_id=expired_facet.facet_id;
        END IF;
    END LOOP;
    SELECT * INTO work_row FROM __forest_module_schema__.vector_index_deletions
    WHERE bot_id=requested_bot AND state='PENDING'
      AND next_attempt_at<=clock_timestamp()
      AND (claim_token IS NULL OR lease_deadline<=clock_timestamp())
    ORDER BY next_attempt_at,index_ref_id LIMIT 1 FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN
        PERFORM __forest_module_schema__.memory_reap_expired_trial(requested_bot);
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    task_payload := jsonb_build_object(
        'protocol','forest.owned-work-claim/1',
        'work_id',work_row.index_ref_id,'claim_token',requested_token,
        'attempt',work_row.attempt_count+1,
        'operation_id','memory-vector-delete:'||encode(sha256(convert_to(
            requested_bot||'/'||requested_token,'UTF8')),'hex'),
        'causal_root_kind','memory.vector.deletion',
        'causal_root_id',work_row.index_ref_id,
        'payload',jsonb_build_object(
            'index_ref_id',work_row.index_ref_id,'space_id',work_row.space_id));
    UPDATE __forest_module_schema__.vector_index_deletions
    SET claim_token=requested_token,claim_payload=task_payload,
        lease_deadline=clock_timestamp()+(requested_lease*interval '1 second'),
        attempt_count=attempt_count+1
    WHERE bot_id=requested_bot AND index_ref_id=work_row.index_ref_id;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',task_payload);
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_VECTOR_DELETE_CLAIM_INVALID',
            'message','Vector deletion claim is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_settle_vector_deletion(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    prior_row __forest_module_schema__.vector_work_settlements%ROWTYPE;
    work_row __forest_module_schema__.vector_index_deletions%ROWTYPE;
    result_value jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR input->>'work_id' IS NULL
       OR input->>'attempt' !~ '^[1-9][0-9]*$'
       OR input#>>'{outcome,status}' NOT IN ('SUCCEEDED','FAILED') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_SETTLEMENT_INVALID',
                'message','Vector work settlement is invalid','status',422));
    END IF;
    SELECT * INTO prior_row FROM __forest_module_schema__.vector_work_settlements
    WHERE bot_id=requested_bot AND claim_token=requested_token;
    IF FOUND THEN
        IF prior_row.input_payload=input THEN RETURN prior_row.result_payload; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_SETTLEMENT_CONFLICT',
                'message','Vector work was settled differently','status',409));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.vector_index_deletions
    WHERE bot_id=requested_bot AND claim_token=requested_token FOR UPDATE;
    IF work_row IS NULL OR work_row.state<>'PENDING'
       OR work_row.index_ref_id IS DISTINCT FROM input->>'work_id'
       OR work_row.attempt_count IS DISTINCT FROM (input->>'attempt')::integer THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_DELETE_CLAIM_LOST',
                'message','Vector deletion claim is no longer current','status',409));
    END IF;
    IF input#>>'{outcome,status}'='SUCCEEDED' THEN
        IF input#>>'{outcome,result,index_ref_id}' IS DISTINCT FROM work_row.index_ref_id
           OR input#>>'{outcome,result,deleted}' IS DISTINCT FROM 'true' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_VECTOR_DELETE_RESULT_INVALID',
                    'message','Vector deletion result differs','status',409));
        END IF;
        UPDATE __forest_module_schema__.vector_index_deletions
        SET state='DELETED',claim_token=NULL,claim_payload=NULL,lease_deadline=NULL,
            deleted_at=clock_timestamp()
        WHERE bot_id=requested_bot AND index_ref_id=work_row.index_ref_id;
    ELSE
        UPDATE __forest_module_schema__.vector_index_deletions
        SET claim_token=NULL,claim_payload=NULL,lease_deadline=NULL,
            next_attempt_at=clock_timestamp()+
                (LEAST(power(2,LEAST(attempt_count,8)),300)*interval '1 second')
        WHERE bot_id=requested_bot AND index_ref_id=work_row.index_ref_id;
    END IF;
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.owned-work-settled/1',
            'work_id',work_row.index_ref_id));
    INSERT INTO __forest_module_schema__.vector_work_settlements(
        bot_id,claim_token,input_payload,result_payload)
    VALUES(requested_bot,requested_token,input,result_value);
    RETURN result_value;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_VECTOR_SETTLEMENT_INVALID',
            'message','Vector work settlement is invalid','status',422));
END;
$$;

CREATE FUNCTION guard_memory_domain() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Memory embedding space is bound to the Bot lifecycle' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_memory_source() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.source_id,NEW.registration_operation_id,NEW.input_digest,
           NEW.source_kind,NEW.platform,NEW.realm_id,NEW.external_group_id,
           NEW.identity_incarnation,NEW.identity_digest,NEW.trunk_id,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.source_id,OLD.registration_operation_id,OLD.input_digest,
           OLD.source_kind,OLD.platform,OLD.realm_id,OLD.external_group_id,
           OLD.identity_incarnation,OLD.identity_digest,OLD.trunk_id,OLD.created_at) THEN
        RAISE EXCEPTION 'Memory source identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.status='ACTIVE' AND NEW.status='RETIRED' THEN RETURN NEW; END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid Memory source transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION stamp_source_unit_graph_xid() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        NEW.eligible_xid := CASE WHEN NEW.eligibility_state='ELIGIBLE'
            THEN pg_current_xact_id() ELSE NULL END;
    ELSIF OLD.eligibility_state='REGISTERED' AND NEW.eligibility_state='ELIGIBLE' THEN
        NEW.eligible_xid := pg_current_xact_id();
    ELSIF NEW.eligible_xid IS DISTINCT FROM OLD.eligible_xid THEN
        RAISE EXCEPTION 'Memory eligibility transaction is immutable' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION stamp_facet_publication_xid() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state<>'READY' THEN
        NEW.published_xid := NULL;
    ELSIF TG_OP='INSERT' OR OLD.state<>'READY' THEN
        NEW.published_xid := pg_current_xact_id();
    ELSIF NEW.published_xid IS DISTINCT FROM OLD.published_xid THEN
        RAISE EXCEPTION 'Memory publication transaction is immutable' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION guard_source_unit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.source_unit_id,NEW.capture_operation_id,NEW.input_digest,
           NEW.source_id,NEW.trunk_id,NEW.source_order_seq,NEW.source_revision,
           NEW.unit_kind,NEW.modality,NEW.part_count,NEW.content_manifest_digest,
           NEW.author_subject_ref,NEW.author_payload_id,NEW.author_payload_digest,
           NEW.author_owner_component_id,NEW.author_owner_kind,NEW.author_owner_id,
           NEW.accepted_deletion_epoch,NEW.occurred_at,NEW.accepted_at,NEW.accepted_xid)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.source_unit_id,OLD.capture_operation_id,OLD.input_digest,
           OLD.source_id,OLD.trunk_id,OLD.source_order_seq,OLD.source_revision,
           OLD.unit_kind,OLD.modality,OLD.part_count,OLD.content_manifest_digest,
           OLD.author_subject_ref,OLD.author_payload_id,OLD.author_payload_digest,
           OLD.author_owner_component_id,OLD.author_owner_kind,OLD.author_owner_id,
           OLD.accepted_deletion_epoch,OLD.occurred_at,OLD.accepted_at,OLD.accepted_xid) THEN
        RAISE EXCEPTION 'Memory source unit identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.eligibility_state='REGISTERED' AND NEW.eligibility_state IN ('ELIGIBLE','SUPPRESSED')
       AND NEW.eligibility_revision=OLD.eligibility_revision+1 THEN RETURN NEW; END IF;
    IF OLD.eligibility_state='ELIGIBLE' AND NEW.eligibility_state='SUPPRESSED'
       AND NEW.eligibility_revision=OLD.eligibility_revision+1 THEN RETURN NEW; END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid Memory eligibility transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_occurrence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.occurrence_id,NEW.source_unit_id,NEW.binding_ref,
           NEW.conversation_ref,NEW.event_ref,NEW.ingress_seq,NEW.observed_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.occurrence_id,OLD.source_unit_id,OLD.binding_ref,
           OLD.conversation_ref,OLD.event_ref,OLD.ingress_seq,OLD.observed_at) THEN
        RAISE EXCEPTION 'Memory occurrence identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.eligibility_state='REGISTERED' AND NEW.eligibility_state IN ('ADMITTED','ELIGIBLE','SUPPRESSED')
       THEN RETURN NEW; END IF;
    IF OLD.eligibility_state='ADMITTED' AND NEW.eligibility_state IN ('ELIGIBLE','SUPPRESSED')
       THEN RETURN NEW; END IF;
    IF OLD.eligibility_state='ELIGIBLE' AND NEW.eligibility_state='SUPPRESSED'
       THEN RETURN NEW; END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid Memory occurrence transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_relation_contribution() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.contribution_id,NEW.operation_id,NEW.input_digest,
           NEW.relation_id,NEW.relation_kind,NEW.left_kind,NEW.left_id,
           NEW.right_kind,NEW.right_id,NEW.relation_key,NEW.origin_kind,
           NEW.weight_micros,NEW.admitted_deletion_epoch,NEW.created_at,NEW.created_xid)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.contribution_id,OLD.operation_id,OLD.input_digest,
           OLD.relation_id,OLD.relation_kind,OLD.left_kind,OLD.left_id,
           OLD.right_kind,OLD.right_id,OLD.relation_key,OLD.origin_kind,
           OLD.weight_micros,OLD.admitted_deletion_epoch,OLD.created_at,OLD.created_xid) THEN
        RAISE EXCEPTION 'Memory relation contribution identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.active AND NOT NEW.active THEN RETURN NEW; END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid Memory relation transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_memory_query() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE gc jsonb := __forest_module_schema__.memory_trial_gc_context();
BEGIN
    IF TG_OP='DELETE' AND OLD.bot_id=gc->>'bot_id'
       AND OLD.query_id=gc->>'query_id' AND OLD.input_kind='ADMIN_TRIAL' THEN
        RETURN OLD;
    END IF;
    IF ROW(NEW.bot_id,NEW.query_id,NEW.operation_id,NEW.input_digest,
           NEW.input_kind,NEW.current_source_id,NEW.input_batch_ref,NEW.context_revision,
           NEW.query_mode,NEW.fusion_policy_revision,NEW.accepted_deletion_epoch,
           NEW.created_at,NEW.graph_snapshot::text)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.query_id,OLD.operation_id,OLD.input_digest,
           OLD.input_kind,OLD.current_source_id,OLD.input_batch_ref,OLD.context_revision,
           OLD.query_mode,OLD.fusion_policy_revision,OLD.accepted_deletion_epoch,
           OLD.created_at,OLD.graph_snapshot::text) THEN
        RAISE EXCEPTION 'Memory query identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='OPEN' AND NEW.state='FROZEN' THEN RETURN NEW; END IF;
    IF ROW(NEW.state,NEW.bundle_id,NEW.frozen_at)
       IS NOT DISTINCT FROM ROW(OLD.state,OLD.bundle_id,OLD.frozen_at) THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid Memory query transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_trial_input_retention() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expiry timestamptz;
BEGIN
    IF TG_OP='UPDATE' THEN
        IF TG_TABLE_NAME='memory_trial_inputs'
           AND OLD.cancelled_at IS NULL AND NEW.cancelled_at IS NOT NULL
           AND to_jsonb(NEW)-'cancelled_at' = to_jsonb(OLD)-'cancelled_at' THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'Trial input identity is immutable' USING ERRCODE='55000';
    END IF;
    IF TG_TABLE_NAME='memory_trial_inputs' THEN
        expiry := OLD.expires_at;
    ELSE
        SELECT expires_at INTO expiry FROM __forest_module_schema__.memory_trial_inputs
        WHERE bot_id=OLD.bot_id AND query_id=OLD.query_id;
    END IF;
    IF expiry IS NULL OR expiry>clock_timestamp() THEN
        RAISE EXCEPTION 'Active trial input cannot be removed' USING ERRCODE='55000';
    END IF;
    RETURN OLD;
END;
$$;

CREATE FUNCTION guard_embedding_facet() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE gc jsonb := __forest_module_schema__.memory_trial_gc_context();
BEGIN
    IF TG_OP='DELETE' AND OLD.bot_id=gc->>'bot_id'
       AND OLD.query_id=gc->>'query_id' AND OLD.input_kind='ADMIN_TRIAL'
       AND OLD.state='SUPPRESSED' THEN RETURN OLD; END IF;
    IF ROW(NEW.bot_id,NEW.facet_id,NEW.purpose,NEW.query_id,NEW.input_kind,
           NEW.input_ref,NEW.input_revision,NEW.trial_query_id,NEW.source_unit_id,
           NEW.source_revision,NEW.part_id,NEW.modality,NEW.selector_start_char,
           NEW.selector_end_char,NEW.input_payload_id,NEW.input_payload_digest,
           NEW.facet_content_digest,NEW.input_media_type,NEW.input_byte_length,
           NEW.space_id,NEW.accepted_deletion_epoch,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.facet_id,OLD.purpose,OLD.query_id,OLD.input_kind,
           OLD.input_ref,OLD.input_revision,OLD.trial_query_id,OLD.source_unit_id,
           OLD.source_revision,OLD.part_id,OLD.modality,OLD.selector_start_char,
           OLD.selector_end_char,OLD.input_payload_id,OLD.input_payload_digest,
           OLD.facet_content_digest,OLD.input_media_type,OLD.input_byte_length,
           OLD.space_id,OLD.accepted_deletion_epoch,OLD.created_at) THEN
        RAISE EXCEPTION 'Embedding facet identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='PENDING' AND NEW.state IN ('PREPARED','FAILED','SUPPRESSED') THEN RETURN NEW; END IF;
    IF OLD.state='PENDING' AND NEW.state='PENDING'
       AND NEW.next_attempt_at>=OLD.next_attempt_at
       AND ROW(NEW.attempt_count,NEW.current_attempt_id,NEW.index_ref_id,
               NEW.vector_digest,NEW.result_payload_id,NEW.result_payload_digest,
               NEW.published_at)
           IS NOT DISTINCT FROM
           ROW(OLD.attempt_count,OLD.current_attempt_id,OLD.index_ref_id,
               OLD.vector_digest,OLD.result_payload_id,OLD.result_payload_digest,
               OLD.published_at) THEN RETURN NEW; END IF;
    IF OLD.state='PREPARED' AND NEW.state IN ('DISPATCHED','PENDING','SUPPRESSED') THEN RETURN NEW; END IF;
    IF OLD.state='DISPATCHED' AND NEW.state IN (
        'EMBEDDED','UNKNOWN','FAILED','PENDING','SUPPRESSED'
    ) THEN RETURN NEW; END IF;
    IF OLD.state='EMBEDDED' AND NEW.state IN ('READY','SUPPRESSED') THEN RETURN NEW; END IF;
    IF OLD.state='UNKNOWN' AND NEW.state='PENDING'
       AND OLD.current_attempt_id IS NOT NULL AND NEW.current_attempt_id IS NULL
       AND ROW(NEW.attempt_count,NEW.index_ref_id,NEW.vector_digest,
               NEW.result_payload_id,NEW.result_payload_digest,NEW.published_at,
               NEW.failure_code)
           IS NOT DISTINCT FROM
           ROW(OLD.attempt_count,OLD.index_ref_id,OLD.vector_digest,
               OLD.result_payload_id,OLD.result_payload_digest,OLD.published_at,
               OLD.failure_code) THEN RETURN NEW; END IF;
    IF OLD.state IN ('READY','UNKNOWN','FAILED') AND NEW.state='SUPPRESSED' THEN RETURN NEW; END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid embedding facet transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_embedding_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE gc jsonb := __forest_module_schema__.memory_trial_gc_context();
BEGIN
    IF TG_OP='DELETE' AND OLD.bot_id=gc->>'bot_id'
       AND gc->'claim_ids' ? OLD.claim_id
       AND OLD.state IN ('SUCCEEDED','NOT_SENT','UNKNOWN','FAILED') THEN
        RETURN OLD;
    END IF;
    IF ROW(NEW.bot_id,NEW.attempt_id,NEW.claim_id,
           NEW.effect_operation_id,NEW.executor_component_id,
           NEW.executor_generation_id,NEW.fence_graph_id,NEW.fence_graph_revision,
           NEW.accepted_deletion_epoch,NEW.effect_wire_payload_id,
           NEW.effect_wire_payload_digest,NEW.reservation_id,NEW.deadline_at,NEW.prepared_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.attempt_id,OLD.claim_id,
           OLD.effect_operation_id,OLD.executor_component_id,
           OLD.executor_generation_id,OLD.fence_graph_id,OLD.fence_graph_revision,
           OLD.accepted_deletion_epoch,OLD.effect_wire_payload_id,
           OLD.effect_wire_payload_digest,OLD.reservation_id,OLD.deadline_at,OLD.prepared_at) THEN
        RAISE EXCEPTION 'Embedding attempt identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='PREPARED' AND NEW.state IN ('DISPATCHED','NOT_SENT') THEN RETURN NEW; END IF;
    IF OLD.state='DISPATCHED' AND NEW.state IN (
        'SUCCEEDED','NOT_SENT','UNKNOWN','FAILED'
    ) THEN RETURN NEW; END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid embedding attempt transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_vector_index_deletion() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.index_ref_id,NEW.space_id,
           NEW.deletion_epoch,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.index_ref_id,OLD.space_id,
           OLD.deletion_epoch,OLD.created_at) THEN
        RAISE EXCEPTION 'Vector index deletion identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='PENDING' AND NEW.state='PENDING'
       AND NEW.deleted_at IS NULL AND NEW.next_attempt_at>=OLD.next_attempt_at THEN
        IF OLD.claim_token IS NULL AND NEW.claim_token IS NOT NULL
           AND NEW.attempt_count=OLD.attempt_count+1 THEN RETURN NEW; END IF;
        IF OLD.claim_token IS NOT NULL AND NEW.claim_token IS NULL
           AND NEW.attempt_count=OLD.attempt_count THEN RETURN NEW; END IF;
        IF OLD.claim_token IS NOT NULL AND NEW.claim_token IS NOT NULL
           AND OLD.claim_token IS DISTINCT FROM NEW.claim_token
           AND OLD.lease_deadline<=clock_timestamp()
           AND NEW.attempt_count=OLD.attempt_count+1 THEN RETURN NEW; END IF;
    END IF;
    IF OLD.state='PENDING' AND NEW.state='DELETED'
       AND NEW.deleted_at IS NOT NULL AND NEW.claim_token IS NULL
       AND NEW.lease_deadline IS NULL THEN
        RETURN NEW;
    END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid vector index deletion transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_vector_publish_batch() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE gc jsonb := __forest_module_schema__.memory_trial_gc_context();
BEGIN
    IF TG_OP='DELETE' AND OLD.bot_id=gc->>'bot_id'
       AND gc->'attempt_ids' ? OLD.attempt_id AND OLD.state='SETTLED' THEN
        RETURN OLD;
    END IF;
    IF ROW(NEW.bot_id,NEW.attempt_id,NEW.created_at) IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.attempt_id,OLD.created_at) THEN
        RAISE EXCEPTION 'Vector publish work identity is immutable' USING ERRCODE='55000';
    END IF;
    IF NEW.updated_at<OLD.updated_at OR NEW.next_attempt_at<OLD.next_attempt_at THEN
        RAISE EXCEPTION 'Vector publish work moved backward' USING ERRCODE='55000';
    END IF;
    IF OLD.state='PENDING' AND NEW.state='CLAIMED'
       AND NEW.attempt_count=OLD.attempt_count+1 THEN RETURN NEW; END IF;
    IF OLD.state='CLAIMED' AND NEW.state='CLAIMED'
       AND OLD.lease_deadline<=clock_timestamp()
       AND NEW.claim_token IS DISTINCT FROM OLD.claim_token
       AND NEW.attempt_count=OLD.attempt_count+1 THEN RETURN NEW; END IF;
    IF OLD.state='CLAIMED' AND NEW.state IN ('PENDING','SETTLED')
       AND NEW.claim_token IS NULL AND NEW.attempt_count=OLD.attempt_count THEN
        RETURN NEW;
    END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid vector publish work transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_embedding_budget_reservation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.reservation_id,NEW.operation_id,NEW.input_digest,
           NEW.batch_ref,NEW.policy_revision,NEW.budget_day,NEW.request_limit,
           NEW.token_limit,NEW.reserved_tokens,NEW.deadline_at,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.reservation_id,OLD.operation_id,OLD.input_digest,
           OLD.batch_ref,OLD.policy_revision,OLD.budget_day,OLD.request_limit,
           OLD.token_limit,OLD.reserved_tokens,OLD.deadline_at,OLD.created_at) THEN
        RAISE EXCEPTION 'Embedding budget reservation identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='RESERVED' AND NEW.state IN ('RELEASED','CHARGED','UNKNOWN') THEN
        RETURN NEW;
    END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid embedding budget transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_embedding_claim() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE gc jsonb := __forest_module_schema__.memory_trial_gc_context();
BEGIN
    IF TG_OP='DELETE' AND OLD.bot_id=gc->>'bot_id'
       AND gc->'claim_ids' ? OLD.claim_id
       AND OLD.state IN ('EXPIRED','SETTLED') THEN RETURN OLD; END IF;
    IF ROW(NEW.bot_id,NEW.claim_id,NEW.purpose,NEW.space_id,
           NEW.source_epoch_high_watermark,NEW.lease_seconds,NEW.lease_deadline,
           NEW.item_limit,NEW.item_count,NEW.claim_payload,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.claim_id,OLD.purpose,OLD.space_id,
           OLD.source_epoch_high_watermark,OLD.lease_seconds,OLD.lease_deadline,
           OLD.item_limit,OLD.item_count,OLD.claim_payload,OLD.created_at) THEN
        RAISE EXCEPTION 'Embedding claim identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='CLAIMED' AND NEW.state='ATTEMPT_BOUND'
       AND NEW.settled_at IS NULL THEN
        RETURN NEW;
    END IF;
    IF OLD.state IN ('CLAIMED','ATTEMPT_BOUND')
       AND NEW.state='EXPIRED' AND NEW.settled_at IS NULL THEN
        RETURN NEW;
    END IF;
    IF OLD.state='ATTEMPT_BOUND' AND NEW.state='SETTLED'
       AND NEW.settled_at IS NOT NULL THEN
        RETURN NEW;
    END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid embedding claim transition' USING ERRCODE='55000';
END;
$$;

CREATE TRIGGER memory_domain_immutable BEFORE UPDATE OR DELETE ON memory_domains
    FOR EACH ROW EXECUTE FUNCTION guard_memory_domain();
CREATE TRIGGER memory_source_guard BEFORE UPDATE OR DELETE ON memory_sources
    FOR EACH ROW EXECUTE FUNCTION guard_memory_source();
CREATE TRIGGER source_unit_guard BEFORE UPDATE OR DELETE ON source_units
    FOR EACH ROW EXECUTE FUNCTION guard_source_unit();
CREATE TRIGGER source_unit_graph_xid BEFORE INSERT OR UPDATE ON source_units
    FOR EACH ROW EXECUTE FUNCTION stamp_source_unit_graph_xid();
CREATE TRIGGER facet_publication_xid BEFORE INSERT OR UPDATE ON embedding_facets
    FOR EACH ROW EXECUTE FUNCTION stamp_facet_publication_xid();
CREATE TRIGGER unavailable_source_events_immutable BEFORE UPDATE OR DELETE ON unavailable_source_events
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER source_occurrence_guard BEFORE UPDATE OR DELETE ON source_occurrences
    FOR EACH ROW EXECUTE FUNCTION guard_occurrence();
CREATE TRIGGER relation_contribution_guard BEFORE UPDATE OR DELETE ON relation_contributions
    FOR EACH ROW EXECUTE FUNCTION guard_relation_contribution();
CREATE TRIGGER memory_query_guard BEFORE UPDATE OR DELETE ON memory_queries
    FOR EACH ROW EXECUTE FUNCTION guard_memory_query();
CREATE TRIGGER embedding_facet_guard BEFORE UPDATE OR DELETE ON embedding_facets
    FOR EACH ROW EXECUTE FUNCTION guard_embedding_facet();
CREATE TRIGGER embedding_attempt_guard BEFORE UPDATE OR DELETE ON embedding_batch_attempts
    FOR EACH ROW EXECUTE FUNCTION guard_embedding_attempt();
CREATE TRIGGER vector_index_deletion_guard BEFORE UPDATE OR DELETE ON vector_index_deletions
    FOR EACH ROW EXECUTE FUNCTION guard_vector_index_deletion();
CREATE TRIGGER vector_publish_batch_guard BEFORE UPDATE OR DELETE ON vector_publish_batches
    FOR EACH ROW EXECUTE FUNCTION guard_vector_publish_batch();
CREATE TRIGGER embedding_claim_guard BEFORE UPDATE OR DELETE ON embedding_claims
    FOR EACH ROW EXECUTE FUNCTION guard_embedding_claim();
CREATE TRIGGER embedding_claim_items_immutable BEFORE UPDATE OR DELETE ON embedding_claim_items
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER embedding_budget_reservation_guard
    BEFORE UPDATE OR DELETE ON embedding_budget_reservations
    FOR EACH ROW EXECUTE FUNCTION guard_embedding_budget_reservation();
CREATE TRIGGER memory_trunks_immutable BEFORE UPDATE OR DELETE ON memory_trunks
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER source_registration_operations_immutable
    BEFORE UPDATE OR DELETE ON source_registration_operations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER source_position_reservations_immutable
    BEFORE UPDATE OR DELETE ON source_position_reservations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER source_parts_immutable BEFORE UPDATE OR DELETE ON source_parts
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER capture_operations_immutable BEFORE UPDATE OR DELETE ON capture_operations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER eligibility_operations_immutable BEFORE UPDATE OR DELETE ON eligibility_operations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER source_suppressions_immutable BEFORE UPDATE OR DELETE ON source_suppressions
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER source_forget_operations_immutable BEFORE UPDATE OR DELETE ON source_forget_operations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER batch_forget_operations_immutable BEFORE UPDATE OR DELETE ON batch_forget_operations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER forget_selections_immutable BEFORE UPDATE OR DELETE ON forget_selections
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER forget_selection_members_frozen
    BEFORE UPDATE OR DELETE ON forget_selection_members
    FOR EACH ROW EXECUTE FUNCTION guard_forget_selection_member();
CREATE TRIGGER relation_evidence_immutable BEFORE UPDATE OR DELETE ON relation_evidence
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER relation_retractions_immutable BEFORE UPDATE OR DELETE ON relation_retractions
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER memory_query_parts_immutable BEFORE UPDATE OR DELETE ON memory_query_parts
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER memory_trial_inputs_retention BEFORE UPDATE OR DELETE ON memory_trial_inputs
    FOR EACH ROW EXECUTE FUNCTION guard_trial_input_retention();
CREATE TRIGGER memory_trial_parts_retention BEFORE UPDATE OR DELETE ON memory_trial_parts
    FOR EACH ROW EXECUTE FUNCTION guard_trial_input_retention();
CREATE TRIGGER facet_preparation_operations_immutable
    BEFORE UPDATE OR DELETE ON facet_preparation_operations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER query_facet_preparations_immutable
    BEFORE UPDATE OR DELETE ON query_facet_preparations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER retrieval_bundles_immutable BEFORE UPDATE OR DELETE ON retrieval_bundles
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER query_match_preparations_immutable BEFORE UPDATE OR DELETE ON query_match_preparations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER query_match_receipts_immutable BEFORE UPDATE OR DELETE ON query_match_receipts
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER query_seed_matches_immutable BEFORE UPDATE OR DELETE ON query_seed_matches
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER retrieval_items_immutable BEFORE UPDATE OR DELETE ON retrieval_items
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER retrieval_materializations_immutable BEFORE UPDATE OR DELETE ON retrieval_materializations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER retrieval_relations_immutable BEFORE UPDATE OR DELETE ON retrieval_relations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();

CREATE FUNCTION memory_claim_domain_bind(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    work_row __forest_module_schema__.memory_domain_bind_work%ROWTYPE;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_DOMAIN_CLAIM_INVALID',
                'message','Memory domain claim is invalid','status',422));
    END IF;
    INSERT INTO __forest_module_schema__.memory_domain_bind_work(bot_id,state)
    VALUES(requested_bot,CASE WHEN EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_domains WHERE bot_id=requested_bot
    ) THEN 'READY' ELSE 'PENDING' END)
    ON CONFLICT (bot_id) DO NOTHING;
    UPDATE __forest_module_schema__.memory_domain_bind_work
    SET state='PENDING',claim_token=NULL,lease_deadline=NULL,
        next_attempt_at=clock_timestamp(),updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND state='CLAIMED' AND lease_deadline<=clock_timestamp();
    UPDATE __forest_module_schema__.memory_domain_bind_work
    SET state='READY',updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND state='PENDING' AND EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_domains WHERE bot_id=requested_bot
    );
    SELECT * INTO work_row FROM __forest_module_schema__.memory_domain_bind_work
    WHERE bot_id=requested_bot AND state='PENDING'
      AND next_attempt_at<=clock_timestamp() FOR UPDATE SKIP LOCKED;
    IF work_row IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    UPDATE __forest_module_schema__.memory_domain_bind_work
    SET state='CLAIMED',claim_token=requested_token,
        lease_deadline=clock_timestamp()+(requested_lease*interval '1 second'),
        attempt_count=attempt_count+1,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot RETURNING * INTO work_row;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.owned-work-claim/1',
            'work_id','memory.domain',
            'claim_token',work_row.claim_token,
            'attempt',work_row.attempt_count,
            'operation_id','memory-domain-bind:'||work_row.attempt_count,
            'causal_root_kind','memory.domain',
            'causal_root_id',requested_bot,
            'payload',jsonb_build_object(
                'protocol','forest.memory-domain-ensure/1',
                'request_id','domain:'||encode(sha256(convert_to(requested_bot,'UTF8')),'hex')
            )
        ));
END;
$$;

CREATE FUNCTION memory_settle_domain_bind(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    old_input jsonb;
    old_result jsonb;
    result_value jsonb;
    work_row __forest_module_schema__.memory_domain_bind_work%ROWTYPE;
    succeeded boolean := input#>>'{outcome,status}'='SUCCEEDED'
        AND input#>>'{outcome,result,bound}'='true';
BEGIN
    SELECT input_payload,result_payload INTO old_input,old_result
    FROM __forest_module_schema__.memory_domain_bind_settlements
    WHERE claim_token=input->>'claim_token';
    IF FOUND THEN
        IF old_input=input THEN RETURN old_result; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_DOMAIN_SETTLEMENT_CONFLICT',
                'message','Memory domain claim was settled differently','status',409));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.memory_domain_bind_work
    WHERE bot_id=requested_bot FOR UPDATE;
    IF work_row IS NULL OR work_row.state<>'CLAIMED'
       OR input->>'work_id' IS DISTINCT FROM 'memory.domain'
       OR work_row.claim_token IS DISTINCT FROM input->>'claim_token'
       OR work_row.attempt_count IS DISTINCT FROM (input->>'attempt')::integer THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_DOMAIN_CLAIM_LOST',
                'message','Memory domain claim is no longer current','status',409));
    END IF;
    IF succeeded AND EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_domains WHERE bot_id=requested_bot
    ) THEN
        UPDATE __forest_module_schema__.memory_domain_bind_work
        SET state='READY',claim_token=NULL,lease_deadline=NULL,last_error_code=NULL,
            updated_at=clock_timestamp() WHERE bot_id=requested_bot;
    ELSE
        UPDATE __forest_module_schema__.memory_domain_bind_work
        SET state='PENDING',claim_token=NULL,lease_deadline=NULL,
            next_attempt_at=clock_timestamp()+
                (LEAST(power(2,LEAST(attempt_count,8)),300)*interval '1 second'),
            last_error_code=COALESCE(input#>>'{outcome,error,code}',
                'MEMORY_DOMAIN_BIND_RETRY'),updated_at=clock_timestamp()
        WHERE bot_id=requested_bot;
    END IF;
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.owned-work-settled/1',
            'work_id','memory.domain'));
    INSERT INTO __forest_module_schema__.memory_domain_bind_settlements(
        claim_token,input_payload,result_payload)
    VALUES(input->>'claim_token',input,result_value);
    RETURN result_value;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_DOMAIN_SETTLEMENT_INVALID',
            'message','Memory domain settlement is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_claim_facet_preparation(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    work_row __forest_module_schema__.facet_preparation_work%ROWTYPE;
    unit_row __forest_module_schema__.source_units%ROWTYPE;
    domain_row __forest_module_schema__.memory_domains%ROWTYPE;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FACET_CLAIM_INVALID',
                'message','Facet preparation claim is invalid','status',422));
    END IF;
    SELECT * INTO domain_row FROM __forest_module_schema__.memory_domains
    WHERE bot_id=requested_bot;
    IF domain_row IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    WITH expired AS MATERIALIZED (
        SELECT bot_id,source_unit_id
        FROM __forest_module_schema__.facet_preparation_work
        WHERE bot_id=requested_bot AND state='CLAIMED'
          AND lease_deadline<=clock_timestamp()
        ORDER BY lease_deadline,source_unit_id
        FOR UPDATE SKIP LOCKED LIMIT 32
    )
    UPDATE __forest_module_schema__.facet_preparation_work w
    SET state='PENDING',claim_token=NULL,lease_deadline=NULL,
        next_attempt_at=clock_timestamp(),updated_at=clock_timestamp(),
        last_error_code='MEMORY_FACET_LEASE_EXPIRED'
    FROM expired e
    WHERE w.bot_id=e.bot_id AND w.source_unit_id=e.source_unit_id;
    SELECT w.* INTO work_row FROM __forest_module_schema__.facet_preparation_work w
    WHERE w.bot_id=requested_bot AND w.state='PENDING'
      AND w.next_attempt_at<=clock_timestamp()
    ORDER BY w.next_attempt_at,w.source_unit_id
    FOR UPDATE OF w SKIP LOCKED LIMIT 1;
    IF work_row IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    SELECT * INTO unit_row FROM __forest_module_schema__.source_units
    WHERE bot_id=requested_bot AND source_unit_id=work_row.source_unit_id;
    UPDATE __forest_module_schema__.facet_preparation_work
    SET state='CLAIMED',claim_token=requested_token,
        lease_deadline=clock_timestamp()+(requested_lease*interval '1 second'),
        attempt_count=attempt_count+1,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND source_unit_id=work_row.source_unit_id
    RETURNING * INTO work_row;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.owned-work-claim/1',
            'work_id',work_row.source_unit_id,
            'claim_token',work_row.claim_token,
            'attempt',work_row.attempt_count,
            'operation_id','memory-facet-work:'||encode(sha256(convert_to(
                requested_bot||'/'||work_row.source_unit_id||'/'||work_row.attempt_count,
                'UTF8')),'hex'),
            'causal_root_kind','memory.facet',
            'causal_root_id',work_row.source_unit_id,
            'payload',jsonb_build_object(
                'source_unit_id',work_row.source_unit_id,
                'source_revision',unit_row.source_revision,
                'space_id',domain_row.space_id
            )
        ));
END;
$$;

CREATE FUNCTION memory_settle_facet_preparation(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    old_input jsonb;
    old_result jsonb;
    result_value jsonb;
    work_row __forest_module_schema__.facet_preparation_work%ROWTYPE;
    unit_row __forest_module_schema__.source_units%ROWTYPE;
    outcome jsonb := input->'outcome';
    disposition text := input#>>'{outcome,result,disposition}';
    reported_count integer;
    actual_count integer;
BEGIN
    SELECT input_payload,result_payload INTO old_input,old_result
    FROM __forest_module_schema__.facet_preparation_settlements
    WHERE bot_id=requested_bot AND claim_token=input->>'claim_token';
    IF FOUND THEN
        IF old_input=input THEN RETURN old_result; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FACET_SETTLEMENT_CONFLICT',
                'message','Facet claim was settled differently','status',409));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.facet_preparation_work
    WHERE bot_id=requested_bot AND source_unit_id=input->>'work_id' FOR UPDATE;
    IF work_row IS NULL OR work_row.state<>'CLAIMED'
       OR work_row.claim_token IS DISTINCT FROM input->>'claim_token'
       OR work_row.attempt_count IS DISTINCT FROM (input->>'attempt')::integer THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FACET_CLAIM_LOST',
                'message','Facet claim is no longer current','status',409));
    END IF;
    SELECT * INTO unit_row FROM __forest_module_schema__.source_units
    WHERE bot_id=requested_bot AND source_unit_id=work_row.source_unit_id;
    IF unit_row.eligibility_state='SUPPRESSED'
       OR (outcome->>'status'='SUCCEEDED' AND disposition='SUPPRESSED') THEN
        UPDATE __forest_module_schema__.facet_preparation_work
        SET state='SUPPRESSED',claim_token=NULL,lease_deadline=NULL,
            last_error_code=NULL,updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND source_unit_id=work_row.source_unit_id;
    ELSIF outcome->>'status'='SUCCEEDED' AND disposition='FAILED' THEN
        UPDATE __forest_module_schema__.facet_preparation_work
        SET state='FAILED',claim_token=NULL,lease_deadline=NULL,
            last_error_code=COALESCE(outcome#>>'{result,error_code}',
                'MEMORY_FACET_PREPARATION_FAILED'),updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND source_unit_id=work_row.source_unit_id;
    ELSIF outcome->>'status'='SUCCEEDED' AND disposition='READY'
       AND unit_row.eligibility_state='ELIGIBLE' THEN
        reported_count := (outcome#>>'{result,facet_count}')::integer;
        SELECT count(*) INTO actual_count FROM __forest_module_schema__.embedding_facets
        WHERE bot_id=requested_bot AND source_unit_id=work_row.source_unit_id
          AND source_revision=unit_row.source_revision AND purpose='DOCUMENT';
        IF actual_count=reported_count THEN
            UPDATE __forest_module_schema__.facet_preparation_work
            SET state='READY',claim_token=NULL,lease_deadline=NULL,
                prepared_facet_count=reported_count,last_error_code=NULL,
                updated_at=clock_timestamp()
            WHERE bot_id=requested_bot AND source_unit_id=work_row.source_unit_id;
        END IF;
    END IF;
    IF (SELECT state FROM __forest_module_schema__.facet_preparation_work
        WHERE bot_id=requested_bot AND source_unit_id=work_row.source_unit_id)='CLAIMED' THEN
        UPDATE __forest_module_schema__.facet_preparation_work
        SET state='PENDING',claim_token=NULL,lease_deadline=NULL,
            next_attempt_at=clock_timestamp()+
                (LEAST(power(2,LEAST(attempt_count,8)),300)*interval '1 second'),
            last_error_code=COALESCE(outcome#>>'{error,code}',
                'MEMORY_FACET_PREPARATION_RETRY'),updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND source_unit_id=work_row.source_unit_id;
    END IF;
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.owned-work-settled/1',
            'work_id',work_row.source_unit_id));
    INSERT INTO __forest_module_schema__.facet_preparation_settlements(
        bot_id,claim_token,input_payload,result_payload)
    VALUES(requested_bot,input->>'claim_token',input,result_value);
    RETURN result_value;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_FACET_SETTLEMENT_INVALID',
            'message','Facet settlement is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_bind_domain(context jsonb, envelope jsonb)
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
    domain_row __forest_module_schema__.memory_domains%ROWTYPE;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-domain-bind/1'
       OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR body->>'space_id' !~ '^sha256:[a-f0-9]{64}$'
       OR body->>'embedding_model_kind' IS DISTINCT FROM 'TEXT_IMAGE'
       OR body->>'model_revision_digest' !~ '^sha256:[a-f0-9]{64}$'
       OR body->>'dimensions' !~ '^[1-9][0-9]*$'
       OR (body->>'dimensions')::integer NOT BETWEEN 1 AND 4096
       OR body->>'max_batch_items' !~ '^[1-9][0-9]*$'
       OR (body->>'max_batch_items')::integer NOT BETWEEN 1 AND 32
       OR body->>'normalization' IS DISTINCT FROM 'L2'
       OR body->>'fusion_policy_revision' IS DISTINCT FROM 'independent-facets/1' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_DOMAIN_INPUT_INVALID',
                'message','Memory embedding domain input is invalid','status',422));
    END IF;
    INSERT INTO __forest_module_schema__.memory_domains(
        bot_id,initialization_operation_id,input_digest,space_id,embedding_model_kind,
        model_revision_digest,dimensions,max_batch_items,normalization,fusion_policy_revision)
    VALUES(requested_bot,requested_operation,requested_digest,body->>'space_id',
        body->>'embedding_model_kind',body->>'model_revision_digest',
        (body->>'dimensions')::integer,(body->>'max_batch_items')::integer,
        body->>'normalization',
        body->>'fusion_policy_revision')
    ON CONFLICT (bot_id) DO NOTHING;
    SELECT * INTO domain_row FROM __forest_module_schema__.memory_domains
    WHERE bot_id=requested_bot FOR SHARE;
    IF domain_row IS NULL
       OR domain_row.space_id IS DISTINCT FROM body->>'space_id'
       OR domain_row.embedding_model_kind IS DISTINCT FROM body->>'embedding_model_kind'
       OR domain_row.model_revision_digest IS DISTINCT FROM body->>'model_revision_digest'
       OR domain_row.dimensions IS DISTINCT FROM (body->>'dimensions')::integer
       OR domain_row.max_batch_items IS DISTINCT FROM (body->>'max_batch_items')::integer
       OR domain_row.normalization IS DISTINCT FROM body->>'normalization'
       OR domain_row.fusion_policy_revision IS DISTINCT FROM body->>'fusion_policy_revision'
       OR (domain_row.initialization_operation_id=requested_operation
           AND domain_row.input_digest IS DISTINCT FROM requested_digest) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_DOMAIN_IDENTITY_CONFLICT',
                'message','Bot Memory embedding domain is already bound differently','status',409));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-domain/1',
            'space_id',domain_row.space_id,
            'embedding_model_kind',domain_row.embedding_model_kind,
            'model_revision_digest',domain_row.model_revision_digest,
            'dimensions',domain_row.dimensions,
            'normalization',domain_row.normalization,
            'fusion_policy_revision',domain_row.fusion_policy_revision));
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range
    OR not_null_violation OR check_violation THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_DOMAIN_INPUT_INVALID',
            'message','Memory embedding domain input is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_prepare_facets(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    facets jsonb := body->'facets';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    source_id_value text := body->>'source_unit_id';
    input_kind_value text := CASE body->>'protocol'
        WHEN 'forest.memory-document-facets/1' THEN 'SOURCE_UNIT'
        ELSE body#>>'{input_identity,kind}' END;
    input_ref_value text := CASE body->>'protocol'
        WHEN 'forest.memory-document-facets/1' THEN body->>'source_unit_id'
        ELSE body#>>'{input_identity,ref}' END;
    input_revision_value bigint;
    purpose_value text := CASE body->>'protocol'
        WHEN 'forest.memory-document-facets/1' THEN 'DOCUMENT'
        WHEN 'forest.memory-query-part-facets/1' THEN 'QUERY' END;
    target_kind_value text := CASE purpose_value WHEN 'QUERY' THEN 'QUERY' ELSE 'SOURCE_UNIT' END;
    target_id_value text := CASE purpose_value WHEN 'QUERY' THEN body->>'query_id' ELSE source_id_value END;
    batch_index_value integer;
    source_row __forest_module_schema__.source_units%ROWTYPE;
    part_row __forest_module_schema__.source_parts%ROWTYPE;
    trial_row __forest_module_schema__.memory_trial_inputs%ROWTYPE;
    trial_part_row __forest_module_schema__.memory_trial_parts%ROWTYPE;
    query_row __forest_module_schema__.memory_queries%ROWTYPE;
    domain_row __forest_module_schema__.memory_domains%ROWTYPE;
    existing_row __forest_module_schema__.embedding_facets%ROWTYPE;
    operation_row __forest_module_schema__.facet_preparation_operations%ROWTYPE;
    facet jsonb;
    reference jsonb;
    modality_value text;
    start_value integer;
    end_value integer;
    byte_count integer;
    facet_count integer;
    part_modality text;
    part_payload_id text;
    part_payload_digest text;
    part_owner_component text;
    part_owner_kind text;
    part_owner_id text;
    part_media_type text;
    part_byte_length integer;
    accepted_epoch bigint;
    trial_query_id_value text;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR purpose_value IS NULL
       OR requested_operation IS NULL OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR input_kind_value IS NULL OR input_kind_value NOT IN ('SOURCE_UNIT','ADMIN_TRIAL')
       OR input_ref_value IS NULL OR octet_length(input_ref_value) NOT BETWEEN 1 AND 256
       OR (purpose_value='DOCUMENT' AND (
           source_id_value IS NULL OR body->>'source_revision' !~ '^[1-9][0-9]*$'))
       OR (purpose_value='QUERY' AND (
           body#>>'{input_identity,revision}' IS NULL
           OR body#>>'{input_identity,revision}' !~ '^[1-9][0-9]*$'
           OR (input_kind_value='SOURCE_UNIT' AND (
               source_id_value IS DISTINCT FROM input_ref_value
               OR body->>'source_revision' IS DISTINCT FROM
                  body#>>'{input_identity,revision}'))
           OR (input_kind_value='ADMIN_TRIAL' AND (
               body ? 'source_unit_id' OR body ? 'source_revision'
               OR input_ref_value IS DISTINCT FROM body->>'query_id'))))
       OR (purpose_value='DOCUMENT' AND body ? 'input_identity')
       OR body->>'batch_index' IS NULL OR body->>'batch_index' !~ '^[0-9]+$'
       OR jsonb_typeof(facets) IS DISTINCT FROM 'array'
       OR jsonb_array_length(facets) NOT BETWEEN 0 AND 64
       OR (purpose_value='DOCUMENT' AND jsonb_array_length(facets)=0) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FACET_INPUT_INVALID',
                'message','Facet input is invalid','status',422));
    END IF;
    input_revision_value := CASE purpose_value WHEN 'DOCUMENT'
        THEN (body->>'source_revision')::bigint
        ELSE (body#>>'{input_identity,revision}')::bigint END;
    trial_query_id_value := CASE input_kind_value
        WHEN 'ADMIN_TRIAL' THEN body->>'query_id' END;
    batch_index_value := (body->>'batch_index')::integer;
    facet_count := jsonb_array_length(facets);
    IF batch_index_value NOT BETWEEN 0 AND 65535
       OR (SELECT count(DISTINCT item->>'facet_id') FROM jsonb_array_elements(facets) item)
          <> facet_count THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FACET_INPUT_INVALID',
                'message','Document facet batch has duplicate or invalid identities','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/facet-batch/'||requested_bot||'/'||target_kind_value||'/'||target_id_value||'/'||batch_index_value,0));
    SELECT * INTO operation_row FROM __forest_module_schema__.facet_preparation_operations
    WHERE bot_id=requested_bot AND target_kind=target_kind_value
      AND target_id=target_id_value AND batch_index=batch_index_value;
    IF operation_row IS NOT NULL THEN
        IF input_kind_value='ADMIN_TRIAL' AND NOT EXISTS (
            SELECT 1 FROM __forest_module_schema__.memory_trial_inputs t
            WHERE t.bot_id=requested_bot AND t.query_id=body->>'query_id'
              AND t.generation_id=context->>'generation_id'
              AND t.input_revision=input_revision_value
              AND t.expires_at>clock_timestamp()
              AND t.cancelled_at IS NULL
        ) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_TRIAL_INPUT_UNAVAILABLE',
                    'message','Trial input is unavailable','status',410));
        END IF;
        IF operation_row.operation_id IS DISTINCT FROM requested_operation
           OR operation_row.input_digest IS DISTINCT FROM requested_digest
           OR operation_row.facet_count IS DISTINCT FROM facet_count THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_FACET_BATCH_CONFLICT',
                    'message','Facet batch was prepared differently','status',409));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_strip_nulls(jsonb_build_object(
                'protocol','forest.memory-facets-prepared/1',
                'source_unit_id',source_id_value,'query_id',body->>'query_id',
                'input_identity',body->'input_identity',
                'batch_index',batch_index_value,'facet_count',facet_count)));
    END IF;
    SELECT * INTO domain_row FROM __forest_module_schema__.memory_domains
    WHERE bot_id=requested_bot FOR SHARE;
    IF domain_row IS NULL OR domain_row.space_id IS DISTINCT FROM body->>'space_id' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FACET_SOURCE_INELIGIBLE',
                'message','Facet source or embedding space is unavailable','status',409));
    END IF;
    IF input_kind_value='SOURCE_UNIT' THEN
        SELECT u.* INTO source_row FROM __forest_module_schema__.source_units u
        JOIN __forest_module_schema__.memory_sources s
          ON s.bot_id=u.bot_id AND s.source_id=u.source_id AND s.status='ACTIVE'
        WHERE u.bot_id=requested_bot AND u.source_unit_id=source_id_value FOR SHARE OF u;
        IF source_row IS NULL
           OR (purpose_value='DOCUMENT' AND source_row.eligibility_state<>'ELIGIBLE')
           OR (purpose_value='QUERY' AND source_row.eligibility_state NOT IN ('REGISTERED','ELIGIBLE'))
           OR source_row.source_revision IS DISTINCT FROM input_revision_value THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_FACET_SOURCE_INELIGIBLE',
                    'message','Facet source is unavailable','status',409));
        END IF;
        accepted_epoch := source_row.accepted_deletion_epoch;
    ELSE
        SELECT * INTO trial_row FROM __forest_module_schema__.memory_trial_inputs
        WHERE bot_id=requested_bot AND query_id=body->>'query_id' FOR SHARE;
        SELECT * INTO query_row FROM __forest_module_schema__.memory_queries
        WHERE bot_id=requested_bot AND query_id=body->>'query_id' FOR SHARE;
        IF trial_row IS NULL OR query_row IS NULL OR query_row.state<>'OPEN'
           OR query_row.input_kind<>'ADMIN_TRIAL'
           OR trial_row.generation_id IS DISTINCT FROM context->>'generation_id'
           OR trial_row.expires_at<=clock_timestamp()
           OR trial_row.cancelled_at IS NOT NULL
           OR trial_row.input_revision IS DISTINCT FROM input_revision_value THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_TRIAL_INPUT_UNAVAILABLE',
                    'message','Trial input is unavailable','status',410));
        END IF;
        accepted_epoch := query_row.accepted_deletion_epoch;
    END IF;
    IF purpose_value='QUERY' AND input_kind_value='SOURCE_UNIT' AND NOT EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_query_parts qp
        JOIN __forest_module_schema__.memory_queries q USING(bot_id,query_id)
        JOIN __forest_module_schema__.source_occurrences o
          ON o.bot_id=qp.bot_id AND o.occurrence_id=qp.occurrence_id
         AND o.source_unit_id=qp.source_unit_id
        WHERE qp.bot_id=requested_bot AND qp.query_id=target_id_value
          AND qp.query_part_index=batch_index_value AND qp.source_unit_id=source_id_value
          AND qp.source_revision=source_row.source_revision AND qp.part_id=body->>'part_id'
          AND q.state='OPEN' AND o.input_batch_ref=q.input_batch_ref
          AND o.context_revision=q.context_revision AND o.eligibility_state IN ('ADMITTED','ELIGIBLE')
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_PART_MISMATCH',
                'message','Query part differs from its immutable membership','status',409));
    END IF;
    IF purpose_value='QUERY' AND input_kind_value='ADMIN_TRIAL' AND NOT EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_trial_parts p
        WHERE p.bot_id=requested_bot AND p.query_id=body->>'query_id'
          AND p.query_part_index=batch_index_value AND p.part_id=body->>'part_id'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_PART_MISMATCH',
                'message','Trial part differs from immutable membership','status',409));
    END IF;
    FOR facet IN SELECT value FROM jsonb_array_elements(facets) LOOP
        reference := facet->'input_payload';
        modality_value := facet->>'modality';
        byte_count := (facet->>'input_byte_length')::integer;
        start_value := (facet->>'selector_start_char')::integer;
        end_value := (facet->>'selector_end_char')::integer;
        IF input_kind_value='SOURCE_UNIT' THEN
            SELECT * INTO part_row FROM __forest_module_schema__.source_parts
            WHERE bot_id=requested_bot AND source_unit_id=source_id_value
              AND part_id=facet->>'part_id' FOR SHARE;
            part_modality := part_row.part_kind;
            part_payload_id := part_row.payload_id;
            part_payload_digest := part_row.payload_digest;
            part_owner_component := part_row.payload_owner_component_id;
            part_owner_kind := part_row.payload_owner_kind;
            part_owner_id := part_row.payload_owner_id;
            part_media_type := part_row.media_type;
            part_byte_length := part_row.byte_length;
        ELSE
            SELECT * INTO trial_part_row FROM __forest_module_schema__.memory_trial_parts
            WHERE bot_id=requested_bot AND query_id=body->>'query_id'
              AND query_part_index=batch_index_value AND part_id=facet->>'part_id'
            FOR SHARE;
            part_modality := trial_part_row.modality;
            part_payload_id := trial_part_row.payload_id;
            part_payload_digest := trial_part_row.payload_digest;
            part_owner_component := trial_part_row.payload_owner_component_id;
            part_owner_kind := trial_part_row.payload_owner_kind;
            part_owner_id := trial_part_row.payload_owner_id;
            part_media_type := trial_part_row.media_type;
            part_byte_length := trial_part_row.byte_length;
        END IF;
        IF (input_kind_value='SOURCE_UNIT' AND part_row IS NULL)
           OR (input_kind_value='ADMIN_TRIAL' AND trial_part_row IS NULL)
           OR octet_length(facet->>'facet_id') NOT BETWEEN 1 AND 256
           OR (purpose_value='QUERY' AND facet->>'part_id' IS DISTINCT FROM body->>'part_id')
           OR modality_value IS DISTINCT FROM part_modality
           OR reference->>'payload_id' IS DISTINCT FROM part_payload_id
           OR reference->>'digest' IS DISTINCT FROM part_payload_digest
           OR reference->>'owner_component_id' IS DISTINCT FROM part_owner_component
           OR reference->>'owner_kind' IS DISTINCT FROM part_owner_kind
           OR reference->>'owner_id' IS DISTINCT FROM part_owner_id
           OR facet->>'facet_content_digest' !~ '^sha256:[a-f0-9]{64}$'
           OR byte_count NOT BETWEEN 1 AND 33554432
           OR (modality_value='TEXT' AND
               (start_value IS NULL OR end_value IS NULL OR start_value<0 OR end_value<=start_value))
           OR (modality_value='IMAGE' AND
               (start_value IS NOT NULL OR end_value IS NOT NULL
                OR facet->>'facet_content_digest' IS DISTINCT FROM part_payload_digest
                OR byte_count IS DISTINCT FROM part_byte_length)) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_FACET_PART_MISMATCH',
                    'message','Facet does not match the source part','status',409));
        END IF;
        INSERT INTO __forest_module_schema__.embedding_facets(
            bot_id,facet_id,purpose,query_id,input_kind,input_ref,input_revision,
            source_unit_id,source_revision,trial_query_id,part_id,modality,
            selector_start_char,selector_end_char,input_payload_id,input_payload_digest,
            facet_content_digest,input_media_type,input_byte_length,space_id,
            accepted_deletion_epoch)
        VALUES(requested_bot,facet->>'facet_id',purpose_value,body->>'query_id',
            input_kind_value,input_ref_value,input_revision_value,source_id_value,
            source_row.source_revision,trial_query_id_value,
            facet->>'part_id',modality_value,
            start_value,end_value,part_payload_id,part_payload_digest,
            facet->>'facet_content_digest',part_media_type,byte_count,
            domain_row.space_id,accepted_epoch)
        ON CONFLICT (bot_id,facet_id) DO NOTHING;
        SELECT * INTO existing_row FROM __forest_module_schema__.embedding_facets
        WHERE bot_id=requested_bot AND facet_id=facet->>'facet_id';
        IF existing_row.purpose IS DISTINCT FROM purpose_value
           OR existing_row.query_id IS DISTINCT FROM body->>'query_id'
           OR existing_row.input_kind IS DISTINCT FROM input_kind_value
           OR existing_row.input_ref IS DISTINCT FROM input_ref_value
           OR existing_row.input_revision IS DISTINCT FROM input_revision_value
           OR existing_row.source_unit_id IS DISTINCT FROM source_id_value
           OR existing_row.source_revision IS DISTINCT FROM source_row.source_revision
           OR existing_row.trial_query_id IS DISTINCT FROM trial_query_id_value
           OR existing_row.part_id IS DISTINCT FROM facet->>'part_id'
           OR existing_row.modality IS DISTINCT FROM modality_value
           OR existing_row.selector_start_char IS DISTINCT FROM start_value
           OR existing_row.selector_end_char IS DISTINCT FROM end_value
           OR existing_row.input_payload_id IS DISTINCT FROM part_payload_id
           OR existing_row.input_payload_digest IS DISTINCT FROM part_payload_digest
           OR existing_row.input_media_type IS DISTINCT FROM part_media_type
           OR existing_row.facet_content_digest IS DISTINCT FROM facet->>'facet_content_digest'
           OR existing_row.input_byte_length IS DISTINCT FROM byte_count
           OR existing_row.space_id IS DISTINCT FROM domain_row.space_id
           OR existing_row.accepted_deletion_epoch IS DISTINCT FROM accepted_epoch THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_FACET_IDENTITY_CONFLICT',
                    'message','Facet identity names different source content','status',409));
        END IF;
    END LOOP;
    INSERT INTO __forest_module_schema__.facet_preparation_operations(
        bot_id,operation_id,input_digest,target_kind,target_id,batch_index,facet_count)
    VALUES(requested_bot,requested_operation,requested_digest,target_kind_value,
        target_id_value,batch_index_value,facet_count);
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_strip_nulls(jsonb_build_object(
            'protocol','forest.memory-facets-prepared/1',
            'source_unit_id',source_id_value,'query_id',body->>'query_id',
            'input_identity',body->'input_identity',
            'batch_index',batch_index_value,'facet_count',facet_count)));
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range
    OR not_null_violation OR check_violation THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_FACET_INPUT_INVALID',
            'message','Facet input is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_complete_query_facets(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    requested_bot text := context->>'bot_id';
    requested_query text := body->>'query_id';
    epoch_value jsonb := envelope->'prior'->(body->>'deletion_epoch_step_id');
    part_count integer;
    receipt_count integer;
    facet_count_value integer;
    page_check jsonb;
    query_kind text;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-query-complete/1'
       OR epoch_value->>'protocol' IS DISTINCT FROM 'forest.payload-epoch/1'
       OR epoch_value->>'deletion_epoch' IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_INPUT_INVALID',
                'message','Query completion requires a current Payload epoch','status',422));
    END IF;
    SELECT input_kind INTO query_kind FROM __forest_module_schema__.memory_queries
    WHERE bot_id=requested_bot AND query_id=requested_query FOR UPDATE;
    IF query_kind='SOURCE_UNIT' THEN
      PERFORM u.source_unit_id FROM __forest_module_schema__.memory_query_parts qp
    JOIN __forest_module_schema__.source_units u USING(bot_id,source_unit_id)
    JOIN __forest_module_schema__.memory_sources s USING(bot_id,source_id)
    JOIN __forest_module_schema__.source_occurrences o
      ON o.bot_id=qp.bot_id AND o.occurrence_id=qp.occurrence_id
    WHERE qp.bot_id=requested_bot AND qp.query_id=requested_query
    ORDER BY u.source_unit_id,o.occurrence_id FOR SHARE OF u,s,o;
    END IF;
    page_check := __forest_module_schema__.memory_read_query_parts(context,
        jsonb_build_object('query_id',requested_query,'after_part_index',-1,'limit',1));
    IF page_check->>'ok' IS DISTINCT FROM 'true' THEN RETURN page_check; END IF;
    SELECT count(parts.query_part_index),count(op.operation_id),COALESCE(sum(op.facet_count),0)
    INTO part_count,receipt_count,facet_count_value
    FROM (
        SELECT query_part_index FROM __forest_module_schema__.memory_query_parts
        WHERE bot_id=requested_bot AND query_id=requested_query AND query_kind='SOURCE_UNIT'
        UNION ALL
        SELECT query_part_index FROM __forest_module_schema__.memory_trial_parts
        WHERE bot_id=requested_bot AND query_id=requested_query AND query_kind='ADMIN_TRIAL'
    ) parts
    LEFT JOIN __forest_module_schema__.facet_preparation_operations op
      ON op.bot_id=requested_bot AND op.target_kind='QUERY'
     AND op.target_id=requested_query AND op.batch_index=parts.query_part_index;
    IF part_count=0 OR part_count<>receipt_count OR facet_count_value>256
       OR facet_count_value IS DISTINCT FROM (body->>'facet_count')::integer
       OR facet_count_value<>(SELECT count(*) FROM __forest_module_schema__.embedding_facets
            WHERE bot_id=requested_bot AND query_id=requested_query AND purpose='QUERY') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_PREPARATION_INCOMPLETE',
                'message','Query part preparation receipts are incomplete','status',409));
    END IF;
    INSERT INTO __forest_module_schema__.query_facet_preparations(
        bot_id,query_id,facet_count,accepted_deletion_epoch)
    VALUES(requested_bot,requested_query,facet_count_value,(epoch_value->>'deletion_epoch')::bigint)
    ON CONFLICT (bot_id,query_id) DO NOTHING;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-query-facets/1',
            'query_id',requested_query,'facet_count',facet_count_value,'part_count',part_count,
            'status','PREPARED'));
END;
$$;

CREATE FUNCTION memory_count_eligible_embedding_facets(
    requested_bot text, requested_claim text, expected_space text,
    epoch_number bigint, expected_state text, expected_attempt text,
    expected_generation text
) RETURNS integer LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE qualified_count integer;
BEGIN
    WITH locked AS MATERIALIZED (
        SELECT f.facet_id
        FROM __forest_module_schema__.embedding_claim_items ci
        JOIN __forest_module_schema__.embedding_facets f
          ON f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
        LEFT JOIN LATERAL (
            SELECT 1 AS eligible
            FROM __forest_module_schema__.source_units u
            JOIN __forest_module_schema__.memory_sources s
              ON s.bot_id=u.bot_id AND s.source_id=u.source_id AND s.status='ACTIVE'
            JOIN __forest_module_schema__.source_parts p
              ON p.bot_id=f.bot_id AND p.source_unit_id=f.source_unit_id
             AND p.part_id=f.part_id AND p.payload_id=f.input_payload_id
             AND p.payload_digest=f.input_payload_digest
            JOIN LATERAL (
                SELECT o.occurrence_id
                FROM __forest_module_schema__.source_occurrences o
                WHERE o.bot_id=f.bot_id AND o.source_unit_id=f.source_unit_id
                  AND ((f.purpose='DOCUMENT' AND o.eligibility_state='ELIGIBLE')
                       OR (f.purpose='QUERY' AND
                           __forest_module_schema__.memory_query_occurrence_matches(
                               f.bot_id,f.query_id,f.source_unit_id,f.source_revision,
                               f.part_id,o.occurrence_id)))
                ORDER BY o.occurrence_id LIMIT 1 FOR SHARE
            ) occurrence ON true
            WHERE f.input_kind='SOURCE_UNIT'
              AND u.bot_id=f.bot_id AND u.source_unit_id=f.source_unit_id
              AND u.source_revision=f.source_revision
              AND (u.eligibility_state='ELIGIBLE'
                   OR (f.purpose='QUERY' AND u.eligibility_state='REGISTERED'))
            FOR SHARE OF u,s,p
        ) source_member ON true
        LEFT JOIN LATERAL (
            SELECT 1 AS eligible
            FROM __forest_module_schema__.memory_trial_inputs t
            JOIN __forest_module_schema__.memory_queries q
              ON q.bot_id=t.bot_id AND q.query_id=t.query_id
             AND q.input_kind='ADMIN_TRIAL' AND q.state='OPEN'
            JOIN __forest_module_schema__.memory_trial_parts p
              ON p.bot_id=t.bot_id AND p.query_id=t.query_id
             AND p.part_id=f.part_id AND p.modality=f.modality
             AND p.payload_id=f.input_payload_id
             AND p.payload_digest=f.input_payload_digest
            WHERE f.input_kind='ADMIN_TRIAL' AND f.purpose='QUERY'
              AND t.bot_id=f.bot_id AND t.query_id=f.trial_query_id
              AND t.query_id=f.query_id AND t.input_revision=f.input_revision
              AND t.generation_id=expected_generation
              AND t.expires_at>clock_timestamp()
              AND t.cancelled_at IS NULL
            FOR SHARE OF t,q,p
        ) trial_member ON true
        WHERE ci.bot_id=requested_bot AND ci.claim_id=requested_claim
          AND f.state=expected_state AND f.space_id=expected_space
          AND f.current_attempt_id IS NOT DISTINCT FROM expected_attempt
          AND f.accepted_deletion_epoch<=epoch_number
          AND (source_member.eligible IS NOT NULL OR trial_member.eligible IS NOT NULL)
        ORDER BY ci.item_index
        FOR UPDATE OF f
    )
    SELECT count(*) INTO qualified_count FROM locked;
    RETURN qualified_count;
END;
$$;

CREATE FUNCTION memory_prepare_embedding_attempt(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    prior jsonb := envelope->'prior';
    epoch_value jsonb := prior->'payload_epoch';
    budget_value jsonb := prior->'budget';
    effect_value jsonb := prior->'effect';
    permit_value jsonb := effect_value->'permit';
    wire_value jsonb := effect_value->'wire_payload';
    requested_bot text := context->>'bot_id';
    requested_claim text := body->>'claim_id';
    requested_attempt text := body->>'attempt_id';
    requested_effect text := body->>'effect_operation_id';
    epoch_number bigint;
    claim_row __forest_module_schema__.embedding_claims%ROWTYPE;
    existing __forest_module_schema__.embedding_batch_attempts%ROWTYPE;
    domain_space text;
    qualified_count integer;
    updated_count integer;
    output_value jsonb;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-embedding-attempt-prepare/1'
       OR requested_bot IS NULL OR requested_claim IS NULL
       OR requested_attempt IS NULL OR requested_effect IS NULL
       OR epoch_value->>'protocol' IS DISTINCT FROM 'forest.payload-epoch/1'
       OR epoch_value->>'deletion_epoch' !~ '^[0-9]+$'
       OR budget_value->>'protocol' IS DISTINCT FROM 'forest.memory-embedding-budget/1'
       OR budget_value->>'reservation_id' IS DISTINCT FROM body->>'reservation_id'
       OR budget_value->>'state' IS NULL
       OR budget_value->>'state' NOT IN ('RESERVED','CHARGED','UNKNOWN')
       OR effect_value->>'protocol' IS DISTINCT FROM 'forest.effect-command-permit/1'
       OR NULLIF(effect_value->>'authorization_ticket_id','') IS NULL
       OR permit_value->>'protocol' IS DISTINCT FROM 'forest.effect-permit/1'
       OR permit_value->>'effect_operation_id' IS DISTINCT FROM requested_effect
       OR permit_value->>'attempt_id' IS DISTINCT FROM requested_attempt
       OR permit_value->>'attempt_no' IS DISTINCT FROM '1'
       OR permit_value->>'executor_component_id' IS DISTINCT FROM 'embedding.multimodal'
       OR permit_value->>'fence_kind' IS DISTINCT FROM 'runtime.graph'
       OR permit_value->>'wire_payload_id' IS DISTINCT FROM wire_value->>'payload_id'
       OR permit_value->>'wire_payload_digest' IS DISTINCT FROM wire_value->>'digest'
       OR wire_value->>'owner_component_id' IS DISTINCT FROM 'kernel.effect'
       OR wire_value->>'owner_kind' IS DISTINCT FROM 'effect.wire'
       OR wire_value->>'owner_id' IS DISTINCT FROM requested_attempt THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_ATTEMPT_INVALID',
                'message','Embedding attempt preparation is invalid','status',422));
    END IF;
    epoch_number := (epoch_value->>'deletion_epoch')::bigint;
    output_value := jsonb_build_object(
        'protocol','forest.memory-embedding-effect-prepared/1',
        'claim_id',requested_claim,'attempt_id',requested_attempt,
        'effect_operation_id',requested_effect,'state','PREPARED',
        'facet_count',NULL,'authorization_ticket_id',
            effect_value->>'authorization_ticket_id',
        'wire_payload',wire_value,'permit',permit_value);
    SELECT * INTO existing FROM __forest_module_schema__.embedding_batch_attempts
    WHERE bot_id=requested_bot AND
      (attempt_id=requested_attempt OR claim_id=requested_claim
       OR effect_operation_id=requested_effect) FOR UPDATE;
    IF FOUND THEN
        IF existing.attempt_id IS DISTINCT FROM requested_attempt
           OR existing.claim_id IS DISTINCT FROM requested_claim
           OR existing.effect_operation_id IS DISTINCT FROM requested_effect
           OR existing.reservation_id IS DISTINCT FROM body->>'reservation_id'
           OR existing.executor_generation_id IS DISTINCT FROM
              permit_value->>'executor_generation_id'
           OR existing.fence_graph_id IS DISTINCT FROM permit_value->>'fence_id'
           OR existing.fence_graph_revision IS DISTINCT FROM
              (permit_value->>'fence_epoch')::bigint
           OR existing.accepted_deletion_epoch IS DISTINCT FROM epoch_number
           OR existing.effect_wire_payload_id IS DISTINCT FROM wire_value->>'payload_id'
           OR existing.effect_wire_payload_digest IS DISTINCT FROM wire_value->>'digest' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_EMBEDDING_ATTEMPT_CONFLICT',
                    'message','Embedding attempt identity differs','status',409));
        END IF;
        SELECT item_count INTO qualified_count FROM __forest_module_schema__.embedding_claims
        WHERE bot_id=requested_bot AND claim_id=requested_claim;
        output_value := jsonb_set(output_value,'{state}',to_jsonb(existing.state));
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_set(output_value,'{facet_count}',to_jsonb(qualified_count)));
    END IF;
    IF budget_value->>'state' IS DISTINCT FROM 'RESERVED' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_BUDGET_NOT_RESERVED',
                'message','Embedding budget is not reserved','status',409));
    END IF;
    SELECT * INTO claim_row FROM __forest_module_schema__.embedding_claims
    WHERE bot_id=requested_bot AND claim_id=requested_claim FOR UPDATE;
    SELECT space_id INTO domain_space FROM __forest_module_schema__.memory_domains
    WHERE bot_id=requested_bot;
    IF claim_row IS NULL OR claim_row.state<>'CLAIMED'
       OR claim_row.lease_deadline<=clock_timestamp()
       OR claim_row.space_id IS DISTINCT FROM domain_space
       OR claim_row.space_id IS DISTINCT FROM body->>'space_id'
       OR claim_row.source_epoch_high_watermark>epoch_number
       OR claim_row.item_count<1 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_CLAIM_LOST',
                'message','Embedding batch claim is no longer eligible','status',409));
    END IF;
    qualified_count := __forest_module_schema__.memory_count_eligible_embedding_facets(
        requested_bot,requested_claim,claim_row.space_id,epoch_number,
        'PENDING',NULL,context->>'generation_id');
    IF qualified_count<>claim_row.item_count THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_SOURCE_CHANGED',
                'message','Embedding batch source changed before handoff','status',409));
    END IF;
    INSERT INTO __forest_module_schema__.embedding_batch_attempts(
        bot_id,attempt_id,claim_id,effect_operation_id,executor_component_id,
        executor_generation_id,fence_graph_id,fence_graph_revision,
        accepted_deletion_epoch,effect_wire_payload_id,effect_wire_payload_digest,
        reservation_id,deadline_at)
    VALUES(requested_bot,requested_attempt,requested_claim,requested_effect,
        'embedding.multimodal',permit_value->>'executor_generation_id',
        permit_value->>'fence_id',(permit_value->>'fence_epoch')::bigint,
        epoch_number,wire_value->>'payload_id',wire_value->>'digest',
        body->>'reservation_id',(permit_value->>'deadline_at')::timestamptz);
    UPDATE __forest_module_schema__.embedding_claims
    SET state='ATTEMPT_BOUND'
    WHERE bot_id=requested_bot AND claim_id=requested_claim;
    UPDATE __forest_module_schema__.embedding_facets f
    SET state='PREPARED',current_attempt_id=requested_attempt,
        attempt_count=attempt_count+1,updated_at=clock_timestamp()
    FROM __forest_module_schema__.embedding_claim_items ci
    WHERE ci.bot_id=requested_bot AND ci.claim_id=requested_claim
      AND f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id;
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    IF updated_count<>claim_row.item_count THEN
        RAISE EXCEPTION 'Embedding claim membership changed under lock' USING ERRCODE='55000';
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_set(output_value,'{facet_count}',to_jsonb(qualified_count)));
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_EMBEDDING_ATTEMPT_INVALID',
            'message','Embedding attempt preparation is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_mark_embedding_handoff(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    epoch_value jsonb := envelope#>'{prior,payload_epoch}';
    effect_value jsonb := envelope#>'{prior,effect}';
    requested_bot text := context->>'bot_id';
    requested_claim text := body->>'claim_id';
    requested_attempt text := body->>'attempt_id';
    requested_effect text := body->>'effect_operation_id';
    requested_handoff text := body->>'handoff_operation_id';
    epoch_number bigint;
    claim_row __forest_module_schema__.embedding_claims%ROWTYPE;
    attempt_row __forest_module_schema__.embedding_batch_attempts%ROWTYPE;
    domain_space text;
    qualified_count integer;
    updated_count integer;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-embedding-handoff/1'
       OR requested_bot IS NULL OR requested_claim IS NULL
       OR requested_attempt IS NULL OR requested_effect IS NULL
       OR requested_handoff IS NULL
       OR epoch_value->>'protocol' IS DISTINCT FROM 'forest.payload-epoch/1'
       OR epoch_value->>'deletion_epoch' !~ '^[0-9]+$'
       OR effect_value->>'effect_operation_id' IS DISTINCT FROM requested_effect
       OR effect_value->>'attempt_id' IS DISTINCT FROM requested_attempt
       OR effect_value->>'handoff_operation_id' IS DISTINCT FROM requested_handoff
       OR effect_value->>'state' IS DISTINCT FROM 'DISPATCHING' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_HANDOFF_INVALID',
                'message','Embedding handoff envelope is invalid','status',422));
    END IF;
    epoch_number := (epoch_value->>'deletion_epoch')::bigint;
    SELECT * INTO claim_row FROM __forest_module_schema__.embedding_claims
    WHERE bot_id=requested_bot AND claim_id=requested_claim FOR UPDATE;
    SELECT * INTO attempt_row FROM __forest_module_schema__.embedding_batch_attempts
    WHERE bot_id=requested_bot AND attempt_id=requested_attempt FOR UPDATE;
    SELECT space_id INTO domain_space FROM __forest_module_schema__.memory_domains
    WHERE bot_id=requested_bot;
    IF claim_row IS NULL OR attempt_row IS NULL
       OR claim_row.state<>'ATTEMPT_BOUND'
       OR claim_row.lease_deadline<=clock_timestamp()
       OR claim_row.space_id IS DISTINCT FROM domain_space
       OR claim_row.source_epoch_high_watermark>epoch_number
       OR claim_row.item_count<1
       OR attempt_row.claim_id IS DISTINCT FROM requested_claim
       OR attempt_row.effect_operation_id IS DISTINCT FROM requested_effect
       OR attempt_row.state<>'PREPARED'
       OR attempt_row.accepted_deletion_epoch IS DISTINCT FROM epoch_number
       OR attempt_row.deadline_at<=clock_timestamp() THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_HANDOFF_STALE',
                'message','Embedding claim or attempt is no longer eligible','status',409));
    END IF;
    IF effect_value->>'wire_payload_id' IS DISTINCT FROM attempt_row.effect_wire_payload_id
       OR effect_value->>'wire_payload_digest' IS DISTINCT FROM attempt_row.effect_wire_payload_digest
       OR effect_value->>'executor_component_id' IS DISTINCT FROM attempt_row.executor_component_id
       OR effect_value->>'executor_generation_id' IS DISTINCT FROM attempt_row.executor_generation_id
       OR effect_value->>'fence_id' IS DISTINCT FROM attempt_row.fence_graph_id
       OR effect_value->>'fence_epoch' IS DISTINCT FROM attempt_row.fence_graph_revision::text THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_HANDOFF_CONFLICT',
                'message','Effect handoff differs from the frozen embedding attempt','status',409));
    END IF;
    qualified_count := __forest_module_schema__.memory_count_eligible_embedding_facets(
        requested_bot,requested_claim,claim_row.space_id,epoch_number,
        'PREPARED',requested_attempt,context->>'generation_id');
    IF qualified_count<>claim_row.item_count THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_SOURCE_CHANGED',
                'message','Embedding batch source changed before handoff','status',409));
    END IF;
    UPDATE __forest_module_schema__.embedding_batch_attempts
    SET state='DISPATCHED',handoff_operation_id=requested_handoff,
        dispatched_at=clock_timestamp()
    WHERE bot_id=requested_bot AND attempt_id=requested_attempt;
    UPDATE __forest_module_schema__.embedding_facets f
    SET state='DISPATCHED',updated_at=clock_timestamp()
    FROM __forest_module_schema__.embedding_claim_items ci
    WHERE ci.bot_id=requested_bot AND ci.claim_id=requested_claim
      AND f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
      AND f.state='PREPARED' AND f.current_attempt_id=requested_attempt;
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    IF updated_count<>claim_row.item_count THEN
        RAISE EXCEPTION 'Embedding claim membership changed under lock' USING ERRCODE='55000';
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-embedding-handoff/1',
            'claim_id',requested_claim,'attempt_id',requested_attempt,
            'effect_operation_id',requested_effect,'state','DISPATCHED',
            'facet_count',qualified_count));
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_EMBEDDING_HANDOFF_INVALID',
            'message','Embedding handoff envelope is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_abandon_embedding_attempt(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    effect_value jsonb := envelope#>'{prior,effect}';
    budget_value jsonb := envelope#>'{prior,budget}';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    requested_claim text := body->>'claim_id';
    requested_attempt text := body->>'attempt_id';
    requested_effect text := body->>'effect_operation_id';
    claim_row __forest_module_schema__.embedding_claims%ROWTYPE;
    attempt_row __forest_module_schema__.embedding_batch_attempts%ROWTYPE;
    updated_count integer;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-embedding-abandon/1'
       OR requested_bot IS NULL OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR requested_claim IS NULL OR requested_attempt IS NULL
       OR requested_effect IS NULL
       OR effect_value->>'attempt_id' IS DISTINCT FROM requested_attempt
       OR effect_value->>'abandonment_operation_id' IS DISTINCT FROM
          body->>'abandonment_operation_id'
       OR effect_value->>'reason_code' IS DISTINCT FROM body->>'reason_code'
       OR effect_value->>'cancel_effect' IS DISTINCT FROM 'false'
       OR budget_value->>'protocol' IS DISTINCT FROM 'forest.memory-embedding-budget/1'
       OR budget_value->>'reservation_id' IS DISTINCT FROM body->>'reservation_id'
       OR budget_value->>'state' IS DISTINCT FROM 'RELEASED' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_ABANDON_INVALID',
                'message','Embedding abandonment envelope is invalid','status',422));
    END IF;
    SELECT * INTO attempt_row FROM __forest_module_schema__.embedding_batch_attempts
    WHERE bot_id=requested_bot AND attempt_id=requested_attempt FOR UPDATE;
    IF attempt_row IS NULL OR attempt_row.claim_id IS DISTINCT FROM requested_claim
       OR attempt_row.effect_operation_id IS DISTINCT FROM requested_effect
       OR attempt_row.reservation_id IS DISTINCT FROM body->>'reservation_id' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_ATTEMPT_CONFLICT',
                'message','Embedding attempt identity differs','status',409));
    END IF;
    IF attempt_row.state='NOT_SENT'
       AND attempt_row.settlement_operation_id=requested_operation
       AND attempt_row.settlement_input_digest=requested_digest THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('protocol','forest.memory-embedding-abandoned/1',
                'claim_id',requested_claim,'attempt_id',requested_attempt,
                'state','NOT_SENT'));
    END IF;
    SELECT * INTO claim_row FROM __forest_module_schema__.embedding_claims
    WHERE bot_id=requested_bot AND claim_id=requested_claim FOR UPDATE;
    IF attempt_row.state<>'PREPARED' OR claim_row IS NULL
       OR claim_row.state<>'ATTEMPT_BOUND' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_ATTEMPT_NOT_ABANDONABLE',
                'message','Only a prepared embedding attempt can be abandoned','status',409));
    END IF;
    UPDATE __forest_module_schema__.embedding_batch_attempts
    SET state='NOT_SENT',settlement_operation_id=requested_operation,
        settlement_input_digest=requested_digest,
        error_code=body->>'reason_code',settled_at=clock_timestamp()
    WHERE bot_id=requested_bot AND attempt_id=requested_attempt;
    UPDATE __forest_module_schema__.embedding_facets f
    SET state='PENDING',current_attempt_id=NULL,
        next_attempt_at=clock_timestamp()+interval '1 minute',
        updated_at=clock_timestamp()
    FROM __forest_module_schema__.embedding_claim_items ci
    WHERE ci.bot_id=requested_bot AND ci.claim_id=requested_claim
      AND f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
      AND f.state='PREPARED' AND f.current_attempt_id=requested_attempt;
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    IF updated_count<>claim_row.item_count THEN
        RAISE EXCEPTION 'Embedding claim membership changed under lock' USING ERRCODE='55000';
    END IF;
    UPDATE __forest_module_schema__.embedding_claims
    SET state='EXPIRED' WHERE bot_id=requested_bot AND claim_id=requested_claim;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-embedding-abandoned/1',
            'claim_id',requested_claim,'attempt_id',requested_attempt,
            'state','NOT_SENT'));
END;
$$;

CREATE FUNCTION memory_observe_embedding_attempt(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    effect_value jsonb := envelope#>'{prior,effect}';
    effect_observation jsonb := effect_value->'observation';
    effect_evidence jsonb := effect_value->'evidence_payload';
    effect_parents jsonb := effect_value->'evidence_parents';
    budget_value jsonb := envelope#>'{prior,budget}';
    result_value jsonb := body->'result';
    vectors_value jsonb := body->'vectors';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    requested_claim text := body->>'claim_id';
    requested_attempt text := body->>'attempt_id';
    requested_effect text := body->>'effect_operation_id';
    observed_state text := body->>'observed_state';
    budget_state text;
    attempt_state text;
    claim_row __forest_module_schema__.embedding_claims%ROWTYPE;
    attempt_row __forest_module_schema__.embedding_batch_attempts%ROWTYPE;
    facet_row record;
    vector_value jsonb;
    seen_count integer := 0;
BEGIN
    IF jsonb_typeof(effect_parents) IS DISTINCT FROM 'array'
       OR jsonb_typeof(vectors_value) IS DISTINCT FROM 'array' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_OBSERVATION_INVALID',
                'message','Embedding observation evidence is invalid','status',422));
    END IF;
    budget_state := CASE observed_state WHEN 'SUCCEEDED' THEN 'CHARGED'
        WHEN 'NOT_APPLIED' THEN 'RELEASED' WHEN 'UNKNOWN' THEN 'UNKNOWN' END;
    attempt_state := CASE observed_state WHEN 'SUCCEEDED' THEN 'SUCCEEDED'
        WHEN 'NOT_APPLIED' THEN 'NOT_SENT' WHEN 'UNKNOWN' THEN 'UNKNOWN' END;
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-embedding-observe/1'
       OR requested_bot IS NULL OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR requested_claim IS NULL OR requested_attempt IS NULL
       OR requested_effect IS NULL OR budget_state IS NULL
       OR effect_value->>'protocol' IS DISTINCT FROM 'forest.effect-command-observation/1'
       OR effect_observation->>'attempt_id' IS DISTINCT FROM requested_attempt
       OR effect_observation->>'observation_operation_id' IS DISTINCT FROM
          body->>'observation_operation_id'
       OR effect_observation->>'observed_status' IS DISTINCT FROM observed_state
       OR effect_observation->>'resulting_state' IS DISTINCT FROM observed_state
       OR effect_observation->>'evidence_kind' IS DISTINCT FROM 'executor'
       OR effect_observation->>'accepted' IS DISTINCT FROM 'true'
       OR effect_evidence->>'owner_component_id' IS DISTINCT FROM 'kernel.effect'
       OR effect_evidence->>'owner_kind' IS DISTINCT FROM 'effect.evidence'
       OR effect_evidence->>'owner_id' IS DISTINCT FROM body->>'observation_operation_id'
       OR effect_evidence->>'digest' IS DISTINCT FROM body->>'evidence_digest'
       OR budget_value->>'protocol' IS DISTINCT FROM 'forest.memory-embedding-budget/1'
       OR budget_value->>'reservation_id' IS DISTINCT FROM body->>'reservation_id'
       OR budget_value->>'state' IS DISTINCT FROM budget_state
       OR budget_value->>'charged_tokens' IS DISTINCT FROM body->>'charged_tokens'
       OR (observed_state='SUCCEEDED' AND (
           result_value IS NULL OR result_value='null'::jsonb
           OR result_value->>'owner_component_id' IS DISTINCT FROM 'embedding.multimodal'
           OR result_value->>'owner_kind' IS DISTINCT FROM 'embedding.result'
           OR result_value->>'owner_id' IS DISTINCT FROM requested_attempt
           OR jsonb_array_length(effect_parents)<>1
           OR effect_parents->0 IS DISTINCT FROM result_value
       ))
       OR (observed_state<>'SUCCEEDED' AND (
           result_value IS DISTINCT FROM 'null'::jsonb
           OR jsonb_array_length(effect_parents)<>0
           OR jsonb_array_length(vectors_value)<>0
           OR body->>'charged_tokens' IS NOT NULL
       )) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_OBSERVATION_INVALID',
                'message','Embedding observation evidence is invalid','status',422));
    END IF;
    SELECT * INTO attempt_row FROM __forest_module_schema__.embedding_batch_attempts
    WHERE bot_id=requested_bot AND attempt_id=requested_attempt FOR UPDATE;
    IF attempt_row IS NULL OR attempt_row.claim_id IS DISTINCT FROM requested_claim
       OR attempt_row.effect_operation_id IS DISTINCT FROM requested_effect
       OR attempt_row.reservation_id IS DISTINCT FROM body->>'reservation_id' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_ATTEMPT_CONFLICT',
                'message','Embedding attempt identity differs','status',409));
    END IF;
    IF attempt_row.state=attempt_state
       AND attempt_row.settlement_operation_id=requested_operation
       AND attempt_row.settlement_input_digest=requested_digest THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('protocol','forest.memory-embedding-observed/1',
                'claim_id',requested_claim,'attempt_id',requested_attempt,
                'state',attempt_state));
    END IF;
    SELECT * INTO claim_row FROM __forest_module_schema__.embedding_claims
    WHERE bot_id=requested_bot AND claim_id=requested_claim FOR UPDATE;
    IF attempt_row.state<>'DISPATCHED' OR claim_row IS NULL
       OR claim_row.state<>'ATTEMPT_BOUND' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_OBSERVATION_STALE',
                'message','Embedding observation boundary is closed','status',409));
    END IF;
    IF observed_state='SUCCEEDED' AND
       (jsonb_array_length(vectors_value)<>claim_row.item_count
        OR body->>'charged_tokens' !~ '^[0-9]+$') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_VECTOR_SET_INVALID',
                'message','Embedding vector set differs from the claim','status',422));
    END IF;
    FOR facet_row IN
        SELECT ci.item_index,ci.facet_id,f.state,f.current_attempt_id
        FROM __forest_module_schema__.embedding_claim_items ci
        JOIN __forest_module_schema__.embedding_facets f
          ON f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
        WHERE ci.bot_id=requested_bot AND ci.claim_id=requested_claim
        ORDER BY ci.item_index FOR UPDATE OF f
    LOOP
        seen_count := seen_count+1;
        IF facet_row.state NOT IN ('DISPATCHED','SUPPRESSED')
           OR (facet_row.state='DISPATCHED' AND
               facet_row.current_attempt_id IS DISTINCT FROM requested_attempt) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_EMBEDDING_FACET_CHANGED',
                    'message','Embedding facet changed before observation','status',409));
        END IF;
        IF observed_state='SUCCEEDED' THEN
            vector_value := vectors_value->facet_row.item_index;
            IF vector_value->>'facet_id' IS DISTINCT FROM facet_row.facet_id
               OR vector_value->>'index' IS DISTINCT FROM facet_row.item_index::text
               OR vector_value->>'vector_digest' !~ '^sha256:[a-f0-9]{64}$' THEN
                RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object('code','MEMORY_EMBEDDING_VECTOR_SET_INVALID',
                        'message','Embedding vector set differs from the claim','status',422));
            END IF;
            IF facet_row.state='DISPATCHED' THEN
                UPDATE __forest_module_schema__.embedding_facets
                SET state='EMBEDDED',vector_digest=vector_value->>'vector_digest',
                    result_payload_id=result_value->>'payload_id',
                    result_payload_digest=result_value->>'digest',
                    updated_at=clock_timestamp()
                WHERE bot_id=requested_bot AND facet_id=facet_row.facet_id;
            END IF;
        ELSIF facet_row.state='DISPATCHED' THEN
            UPDATE __forest_module_schema__.embedding_facets
            SET state=CASE observed_state WHEN 'NOT_APPLIED' THEN 'PENDING'
                    ELSE 'UNKNOWN' END,
                current_attempt_id=CASE observed_state WHEN 'NOT_APPLIED' THEN NULL
                    ELSE current_attempt_id END,
                next_attempt_at=CASE observed_state WHEN 'NOT_APPLIED' THEN
                    clock_timestamp()+interval '1 minute' ELSE next_attempt_at END,
                updated_at=clock_timestamp()
            WHERE bot_id=requested_bot AND facet_id=facet_row.facet_id;
        END IF;
    END LOOP;
    IF seen_count<>claim_row.item_count THEN
        RAISE EXCEPTION 'Embedding claim membership changed under lock' USING ERRCODE='55000';
    END IF;
    UPDATE __forest_module_schema__.embedding_batch_attempts
    SET state=attempt_state,settlement_operation_id=requested_operation,
        settlement_input_digest=requested_digest,
        result_payload_id=CASE WHEN observed_state='SUCCEEDED' THEN
            result_value->>'payload_id' ELSE NULL END,
        result_payload_digest=CASE WHEN observed_state='SUCCEEDED' THEN
            result_value->>'digest' ELSE NULL END,
        error_code=body->>'error_code',settled_at=clock_timestamp()
    WHERE bot_id=requested_bot AND attempt_id=requested_attempt;
    UPDATE __forest_module_schema__.embedding_claims
    SET state=CASE observed_state WHEN 'NOT_APPLIED' THEN 'EXPIRED' ELSE 'SETTLED' END,
        settled_at=CASE observed_state WHEN 'NOT_APPLIED' THEN NULL
            ELSE clock_timestamp() END
    WHERE bot_id=requested_bot AND claim_id=requested_claim;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-embedding-observed/1',
            'claim_id',requested_claim,'attempt_id',requested_attempt,
            'state',attempt_state));
END;
$$;

CREATE FUNCTION memory_reserve_embedding_budget(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    reservation_value text := body->>'reservation_id';
    request_limit_value integer;
    token_limit_value bigint;
    reserved_tokens_value bigint;
    budget_day_value date;
    existing __forest_module_schema__.embedding_budget_reservations%ROWTYPE;
    day_row __forest_module_schema__.embedding_budget_days%ROWTYPE;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-embedding-budget-reserve/1'
       OR requested_bot IS NULL OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR reservation_value IS NULL OR octet_length(reservation_value) NOT BETWEEN 1 AND 256
       OR octet_length(body->>'batch_ref') NOT BETWEEN 1 AND 256
       OR body->>'policy_revision' IS DISTINCT FROM 'daily-cost-cap/1'
       OR body->>'request_limit' !~ '^[1-9][0-9]*$'
       OR body->>'token_limit' !~ '^[1-9][0-9]*$'
       OR body->>'reserved_tokens' !~ '^[1-9][0-9]*$' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_BUDGET_INPUT_INVALID',
                'message','Embedding budget reservation is invalid','status',422));
    END IF;
    request_limit_value := (body->>'request_limit')::integer;
    token_limit_value := (body->>'token_limit')::bigint;
    reserved_tokens_value := (body->>'reserved_tokens')::bigint;
    IF request_limit_value NOT BETWEEN 1 AND 1000000
       OR token_limit_value NOT BETWEEN 1 AND 1000000000000
       OR reserved_tokens_value NOT BETWEEN 1 AND LEAST(token_limit_value,1000000000) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_BUDGET_INPUT_INVALID',
                'message','Embedding budget limits are invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/budget-operation/'||requested_bot||'/'||requested_operation,0));
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/budget-batch/'||requested_bot||'/'||(body->>'batch_ref'),0));
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/budget-reservation/'||requested_bot||'/'||reservation_value,0));
    SELECT * INTO existing FROM __forest_module_schema__.embedding_budget_reservations
    WHERE bot_id=requested_bot AND
      (reservation_id=reservation_value OR operation_id=requested_operation
       OR batch_ref=(body->>'batch_ref')) FOR UPDATE;
    IF FOUND THEN
        IF existing.reservation_id IS DISTINCT FROM reservation_value
           OR existing.operation_id IS DISTINCT FROM requested_operation
           OR existing.batch_ref IS DISTINCT FROM body->>'batch_ref'
           OR existing.input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_EMBEDDING_BUDGET_CONFLICT',
                    'message','Embedding budget reservation identity differs','status',409));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('protocol','forest.memory-embedding-budget/1',
                'reservation_id',existing.reservation_id,'state',existing.state,
                'budget_day',existing.budget_day,'reserved_tokens',existing.reserved_tokens,
                'charged_tokens',existing.charged_tokens));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_domains WHERE bot_id=requested_bot
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_DOMAIN_UNBOUND',
                'message','Embedding domain is not bound','status',409));
    END IF;
    budget_day_value := (clock_timestamp() AT TIME ZONE 'UTC')::date;
    INSERT INTO __forest_module_schema__.embedding_budget_days(bot_id,budget_day)
    VALUES(requested_bot,budget_day_value) ON CONFLICT DO NOTHING;
    SELECT * INTO day_row FROM __forest_module_schema__.embedding_budget_days
    WHERE bot_id=requested_bot AND budget_day=budget_day_value FOR UPDATE;
    IF day_row.reserved_requests::bigint+day_row.settled_requests+
       day_row.unknown_requests+1 > request_limit_value
       OR day_row.reserved_tokens+day_row.charged_tokens+
          day_row.unknown_tokens+reserved_tokens_value > token_limit_value THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_BUDGET_EXHAUSTED',
                'message','Daily embedding budget is exhausted','status',429));
    END IF;
    INSERT INTO __forest_module_schema__.embedding_budget_reservations(
        bot_id,reservation_id,operation_id,input_digest,batch_ref,policy_revision,
        budget_day,request_limit,token_limit,reserved_tokens,deadline_at)
    VALUES(requested_bot,reservation_value,requested_operation,requested_digest,
        body->>'batch_ref',body->>'policy_revision',budget_day_value,
        request_limit_value,token_limit_value,reserved_tokens_value,
        clock_timestamp()+interval '10 minutes');
    UPDATE __forest_module_schema__.embedding_budget_days
    SET reserved_requests=reserved_requests+1,
        reserved_tokens=reserved_tokens+reserved_tokens_value,
        revision=revision+1,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND budget_day=budget_day_value;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-embedding-budget/1',
            'reservation_id',reservation_value,'state','RESERVED',
            'budget_day',budget_day_value,'reserved_tokens',reserved_tokens_value,
            'charged_tokens',NULL));
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range
    OR not_null_violation OR check_violation THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_EMBEDDING_BUDGET_INPUT_INVALID',
            'message','Embedding budget reservation is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_settle_embedding_budget(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    state_value text := body->>'state';
    charged_value bigint;
    reservation_row __forest_module_schema__.embedding_budget_reservations%ROWTYPE;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-embedding-budget-settle/1'
       OR requested_bot IS NULL OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR octet_length(body->>'reservation_id') NOT BETWEEN 1 AND 256
       OR state_value NOT IN ('RELEASED','CHARGED','UNKNOWN')
       OR (state_value='CHARGED' AND body->>'charged_tokens' !~ '^[0-9]+$')
       OR (state_value<>'CHARGED' AND body->'charged_tokens'<>'null'::jsonb)
       OR (body->>'error_code' IS NOT NULL AND
           octet_length(body->>'error_code') NOT BETWEEN 1 AND 128) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_BUDGET_SETTLEMENT_INVALID',
                'message','Embedding budget settlement is invalid','status',422));
    END IF;
    charged_value := (body->>'charged_tokens')::bigint;
    SELECT * INTO reservation_row FROM __forest_module_schema__.embedding_budget_reservations
    WHERE bot_id=requested_bot AND reservation_id=body->>'reservation_id' FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_BUDGET_MISSING',
                'message','Embedding budget reservation is missing','status',404));
    END IF;
    IF reservation_row.state<>'RESERVED' THEN
        IF reservation_row.settlement_operation_id IS DISTINCT FROM requested_operation
           OR reservation_row.settlement_input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_EMBEDDING_BUDGET_CONFLICT',
                    'message','Embedding budget was settled differently','status',409));
        END IF;
    ELSE
        PERFORM 1 FROM __forest_module_schema__.embedding_budget_days
        WHERE bot_id=requested_bot AND budget_day=reservation_row.budget_day FOR UPDATE;
        UPDATE __forest_module_schema__.embedding_budget_reservations
        SET state=state_value,settlement_operation_id=requested_operation,
            settlement_input_digest=requested_digest,charged_tokens=charged_value,
            error_code=body->>'error_code',settled_at=clock_timestamp()
        WHERE bot_id=requested_bot AND reservation_id=reservation_row.reservation_id
        RETURNING * INTO reservation_row;
        UPDATE __forest_module_schema__.embedding_budget_days
        SET reserved_requests=reserved_requests-1,
            reserved_tokens=reserved_tokens-reservation_row.reserved_tokens,
            settled_requests=settled_requests+CASE WHEN state_value='CHARGED' THEN 1 ELSE 0 END,
            charged_tokens=charged_tokens+CASE WHEN state_value='CHARGED' THEN charged_value ELSE 0 END,
            unknown_requests=unknown_requests+CASE WHEN state_value='UNKNOWN' THEN 1 ELSE 0 END,
            unknown_tokens=unknown_tokens+CASE WHEN state_value='UNKNOWN'
                THEN reservation_row.reserved_tokens ELSE 0 END,
            revision=revision+1,updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND budget_day=reservation_row.budget_day;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-embedding-budget/1',
            'reservation_id',reservation_row.reservation_id,'state',reservation_row.state,
            'budget_day',reservation_row.budget_day,
            'reserved_tokens',reservation_row.reserved_tokens,
            'charged_tokens',reservation_row.charged_tokens));
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range
    OR not_null_violation OR check_violation THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_EMBEDDING_BUDGET_SETTLEMENT_INVALID',
            'message','Embedding budget settlement is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_promote_association(
    requested_bot text, requested_journal text,
    requested_token text DEFAULT NULL, requested_attempt integer DEFAULT NULL
)
RETURNS boolean LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    journal_row __forest_module_schema__.association_journal%ROWTYPE;
    trigger_row record;
    seed_row record;
    left_ref text;
    right_ref text;
    pair_digest text;
    contribution_ref text;
    new_contribution integer;
BEGIN
    -- Forget locks source units before it suppresses pending journals. Acquire
    -- the same source locks first, otherwise promotion can hold the journal
    -- while its evidence FK waits for a source held by forget (a deadlock).
    SELECT * INTO journal_row FROM __forest_module_schema__.association_journal
    WHERE bot_id=requested_bot AND journal_id=requested_journal;
    IF journal_row IS NULL OR journal_row.state<>'PENDING_ARCHIVE' THEN RETURN false; END IF;
    PERFORM 1 FROM __forest_module_schema__.source_units
    WHERE bot_id=requested_bot AND source_unit_id IN (
        journal_row.trigger_source_unit_id,journal_row.seed_source_unit_id)
    ORDER BY source_unit_id FOR SHARE;
    SELECT * INTO journal_row FROM __forest_module_schema__.association_journal
    WHERE bot_id=requested_bot AND journal_id=requested_journal FOR UPDATE;
    IF journal_row IS NULL OR journal_row.state<>'PENDING_ARCHIVE' THEN RETURN false; END IF;
    IF requested_token IS NOT NULL AND (
        journal_row.claim_token IS DISTINCT FROM requested_token
        OR journal_row.attempt_count IS DISTINCT FROM requested_attempt
        OR journal_row.lease_deadline<=clock_timestamp()
    ) THEN RETURN false; END IF;
    IF requested_token IS NULL AND journal_row.claim_token IS NOT NULL THEN RETURN false; END IF;
    SELECT u.eligibility_state,s.status INTO trigger_row
    FROM __forest_module_schema__.source_units u
    JOIN __forest_module_schema__.memory_sources s USING(bot_id,source_id)
    WHERE u.bot_id=requested_bot AND u.source_unit_id=journal_row.trigger_source_unit_id;
    SELECT u.eligibility_state,s.status INTO seed_row
    FROM __forest_module_schema__.source_units u
    JOIN __forest_module_schema__.memory_sources s USING(bot_id,source_id)
    WHERE u.bot_id=requested_bot AND u.source_unit_id=journal_row.seed_source_unit_id;
    IF trigger_row IS NULL OR seed_row IS NULL
       OR trigger_row.eligibility_state='SUPPRESSED' OR seed_row.eligibility_state='SUPPRESSED'
       OR trigger_row.status<>'ACTIVE' OR seed_row.status<>'ACTIVE' THEN
        UPDATE __forest_module_schema__.association_journal SET state='SUPPRESSED'
        WHERE bot_id=requested_bot AND journal_id=requested_journal;
        RETURN false;
    END IF;
    IF trigger_row.eligibility_state<>'ELIGIBLE'
       OR seed_row.eligibility_state<>'ELIGIBLE' THEN RETURN false; END IF;
    left_ref := LEAST(journal_row.trigger_source_unit_id,journal_row.seed_source_unit_id);
    right_ref := GREATEST(journal_row.trigger_source_unit_id,journal_row.seed_source_unit_id);
    pair_digest := encode(sha256(convert_to(
        jsonb_build_array(requested_bot,left_ref,right_ref)::text,'UTF8')),'hex');
    contribution_ref := 'association:' || pair_digest;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/association/' || requested_bot || '/' || pair_digest,0));
    INSERT INTO __forest_module_schema__.relation_contributions(
        bot_id,contribution_id,operation_id,input_digest,relation_id,relation_kind,
        left_kind,left_id,right_kind,right_id,origin_kind,weight_micros,
        admitted_deletion_epoch,cross_trunk
    )
    SELECT requested_bot,contribution_ref,contribution_ref,
        'sha256:' || pair_digest,'association:' || pair_digest,'ASSOCIATES_WITH',
        'SOURCE_UNIT',left_ref,'SOURCE_UNIT',right_ref,'MODEL',900000,
        GREATEST(trigger_unit.accepted_deletion_epoch,seed_unit.accepted_deletion_epoch),
        trigger_unit.trunk_id<>seed_unit.trunk_id
    FROM __forest_module_schema__.source_units trigger_unit
    CROSS JOIN __forest_module_schema__.source_units seed_unit
    WHERE trigger_unit.bot_id=requested_bot
      AND trigger_unit.source_unit_id=journal_row.trigger_source_unit_id
      AND seed_unit.bot_id=requested_bot
      AND seed_unit.source_unit_id=journal_row.seed_source_unit_id
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS new_contribution = ROW_COUNT;
    IF new_contribution=1 THEN
        INSERT INTO __forest_module_schema__.relation_evidence(
            bot_id,contribution_id,evidence_index,source_unit_id,source_revision,part_id
        ) VALUES
            (requested_bot,contribution_ref,0,journal_row.trigger_source_unit_id,
             journal_row.trigger_source_revision,journal_row.trigger_part_id),
            (requested_bot,contribution_ref,1,journal_row.seed_source_unit_id,
             journal_row.seed_source_revision,journal_row.seed_part_id);
    ELSIF NOT EXISTS (
        SELECT 1 FROM __forest_module_schema__.relation_contributions
        WHERE bot_id=requested_bot AND contribution_id=contribution_ref AND active
    ) OR (
        SELECT count(*) FROM __forest_module_schema__.relation_evidence
        WHERE bot_id=requested_bot AND contribution_id=contribution_ref
    )<>2 THEN
        UPDATE __forest_module_schema__.association_journal SET state='SUPPRESSED'
        WHERE bot_id=requested_bot AND journal_id=requested_journal;
        RETURN false;
    END IF;
    UPDATE __forest_module_schema__.association_journal
    SET state='PROMOTED',contribution_id=contribution_ref,promoted_at=clock_timestamp()
    WHERE bot_id=requested_bot AND journal_id=requested_journal;
    RETURN true;
END;
$$;

CREATE FUNCTION memory_claim_association_promotion(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    work_row __forest_module_schema__.association_journal%ROWTYPE;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_ASSOCIATION_CLAIM_INVALID',
                'message','Association claim is invalid','status',422));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.association_journal j
    WHERE j.bot_id=requested_bot AND j.state='PENDING_ARCHIVE'
      AND j.promotion_ready AND j.next_attempt_at<=clock_timestamp()
      AND (j.claim_token IS NULL OR j.lease_deadline<=clock_timestamp())
    ORDER BY j.next_attempt_at,j.journal_id
    FOR UPDATE OF j SKIP LOCKED LIMIT 1;
    IF work_row IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    UPDATE __forest_module_schema__.association_journal
    SET claim_token=requested_token,
        lease_deadline=clock_timestamp()+(requested_lease*interval '1 second'),
        attempt_count=attempt_count+1
    WHERE bot_id=requested_bot AND journal_id=work_row.journal_id
    RETURNING * INTO work_row;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.owned-work-claim/1',
            'work_id',work_row.journal_id,
            'claim_token',work_row.claim_token,
            'attempt',work_row.attempt_count,
            'operation_id','memory-association-work:'||encode(sha256(convert_to(
                jsonb_build_array(requested_bot,work_row.journal_id,
                    work_row.attempt_count)::text,'UTF8')),'hex'),
            'causal_root_kind','memory.association',
            'causal_root_id',work_row.journal_id,
            'payload',jsonb_build_object('journal_id',work_row.journal_id,
                'claim_token',work_row.claim_token,'attempt',work_row.attempt_count)
        ));
END;
$$;

CREATE FUNCTION memory_promote_claimed_association(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    work_row __forest_module_schema__.association_journal%ROWTYPE;
BEGIN
    IF input->>'journal_id' IS NULL OR input->>'claim_token' IS NULL
       OR input->>'attempt' IS NULL
       OR input->>'attempt' !~ '^[1-9][0-9]*$' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_ASSOCIATION_CLAIM_INVALID',
                'message','Association promotion claim is invalid','status',422));
    END IF;
    PERFORM __forest_module_schema__.memory_promote_association(
        requested_bot,input->>'journal_id',input->>'claim_token',
        (input->>'attempt')::integer);
    SELECT * INTO work_row FROM __forest_module_schema__.association_journal
    WHERE bot_id=requested_bot AND journal_id=input->>'journal_id';
    IF work_row IS NULL OR work_row.claim_token IS DISTINCT FROM input->>'claim_token'
       OR work_row.attempt_count IS DISTINCT FROM (input->>'attempt')::integer
       OR work_row.lease_deadline<=clock_timestamp() THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_ASSOCIATION_CLAIM_LOST',
                'message','Association claim is no longer current','status',409));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('journal_id',work_row.journal_id,
            'state',work_row.state,'promoted',work_row.state='PROMOTED'));
END;
$$;

CREATE FUNCTION memory_settle_association_promotion(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    old_input jsonb;
    old_result jsonb;
    work_row __forest_module_schema__.association_journal%ROWTYPE;
    result_value jsonb;
BEGIN
    SELECT input_payload,result_payload INTO old_input,old_result
    FROM __forest_module_schema__.association_promotion_settlements
    WHERE bot_id=requested_bot AND claim_token=input->>'claim_token';
    IF FOUND THEN
        IF old_input=input THEN RETURN old_result; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_ASSOCIATION_SETTLEMENT_CONFLICT',
                'message','Association claim was settled differently','status',409));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.association_journal
    WHERE bot_id=requested_bot AND journal_id=input->>'work_id' FOR UPDATE;
    IF work_row IS NULL OR work_row.claim_token IS DISTINCT FROM input->>'claim_token'
       OR work_row.attempt_count IS DISTINCT FROM (input->>'attempt')::integer THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_ASSOCIATION_CLAIM_LOST',
                'message','Association claim is no longer current','status',409));
    END IF;
    UPDATE __forest_module_schema__.association_journal
    SET claim_token=NULL,lease_deadline=NULL,
        next_attempt_at=CASE WHEN state='PENDING_ARCHIVE' THEN
            clock_timestamp()+
                (LEAST(power(2,LEAST(attempt_count,8)),300)*interval '1 second')
            ELSE next_attempt_at END
    WHERE bot_id=requested_bot AND journal_id=work_row.journal_id;
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.owned-work-settled/1',
            'work_id',work_row.journal_id));
    INSERT INTO __forest_module_schema__.association_promotion_settlements(
        bot_id,claim_token,input_payload,result_payload)
    VALUES(requested_bot,input->>'claim_token',input,result_value);
    RETURN result_value;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_ASSOCIATION_SETTLEMENT_INVALID',
            'message','Association settlement is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_associate_final_reply(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    agent_value jsonb := envelope#>'{prior,agent}';
    conversation_value jsonb := envelope#>'{prior,conversation}';
    proposals jsonb := agent_value#>'{learning,proposals}';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    proposal jsonb;
    seed jsonb;
    event_ref_value text;
    trigger_unit text;
    trigger_revision bigint;
    trigger_part text;
    seed_unit text;
    seed_revision bigint;
    journal_ref text;
    old_journal record;
    journaled_count integer := 0;
    trigger_state text;
    skipped_count integer := 0;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'conversation.scheduler'
       OR requested_operation IS NULL OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR agent_value->>'state' IS DISTINCT FROM 'COMPLETED'
       OR agent_value->>'conversation_ref' IS DISTINCT FROM body->>'conversation_ref'
       OR agent_value#>>'{learning,protocol}' IS DISTINCT FROM 'forest.agent-learning/1'
       OR jsonb_typeof(proposals) IS DISTINCT FROM 'array'
       OR jsonb_array_length(proposals)>128
       OR conversation_value->>'reply_ref' IS DISTINCT FROM body->>'reply_ref'
       OR jsonb_typeof(conversation_value->'answered_events') IS DISTINCT FROM 'array' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_ASSOCIATION_INPUT_INVALID',
                'message','Final reply learning authority is incomplete','status',422));
    END IF;
    FOR proposal IN SELECT value FROM jsonb_array_elements(proposals) LOOP
        seed := proposal->'seed';
        SELECT answered->>'event_ref' INTO event_ref_value
        FROM jsonb_array_elements(conversation_value->'answered_events') answered
        WHERE answered->>'trigger_ref'=proposal->>'trigger_ref' LIMIT 1;
        IF event_ref_value IS NULL OR jsonb_typeof(seed->'part_ids')<>'array'
           OR jsonb_array_length(seed->'part_ids')=0 THEN
            skipped_count := skipped_count+1;
            CONTINUE;
        END IF;
        SELECT o.source_unit_id,qp.source_revision,qp.part_id
        INTO trigger_unit,trigger_revision,trigger_part
        FROM __forest_module_schema__.source_occurrences o
        JOIN __forest_module_schema__.memory_query_parts qp
          ON qp.bot_id=o.bot_id AND qp.occurrence_id=o.occurrence_id
        JOIN __forest_module_schema__.memory_queries q
          ON q.bot_id=qp.bot_id AND q.query_id=qp.query_id
        WHERE o.bot_id=requested_bot
          AND o.conversation_ref=body->>'conversation_ref'
          AND o.event_ref=event_ref_value
          AND q.query_id=seed->>'query_id'
          AND qp.source_unit_id=o.source_unit_id
        ORDER BY qp.query_part_index LIMIT 1;
        SELECT ri.source_unit_id,ri.source_revision INTO seed_unit,seed_revision
        FROM __forest_module_schema__.retrieval_items ri
        JOIN __forest_module_schema__.retrieval_bundles b
          ON b.bot_id=ri.bot_id AND b.bundle_id=ri.bundle_id
        JOIN __forest_module_schema__.source_parts p
          ON p.bot_id=ri.bot_id AND p.source_unit_id=ri.source_unit_id
         AND p.part_id=seed#>>'{part_ids,0}'
        WHERE ri.bot_id=requested_bot AND ri.bundle_id=seed->>'bundle_id'
          AND b.query_id=seed->>'query_id'
          AND ri.source_unit_id=seed->>'source_unit_id'
          AND ri.source_revision=(seed->>'source_revision')::bigint
          AND ri.rank_score=(seed->>'rank_score')::double precision
          AND ri.is_seed;
        IF trigger_unit IS NULL OR seed_unit IS NULL OR trigger_unit=seed_unit THEN
            skipped_count := skipped_count+1;
            CONTINUE;
        END IF;
        PERFORM 1 FROM __forest_module_schema__.source_units
        WHERE bot_id=requested_bot AND source_unit_id IN (trigger_unit,seed_unit)
        ORDER BY source_unit_id FOR SHARE;
        IF NOT EXISTS (
            SELECT 1 FROM __forest_module_schema__.source_units u
            JOIN __forest_module_schema__.memory_sources s USING(bot_id,source_id)
            WHERE u.bot_id=requested_bot AND u.source_unit_id=trigger_unit
              AND u.source_revision=trigger_revision
              AND u.eligibility_state<>'SUPPRESSED' AND s.status='ACTIVE'
        ) OR NOT EXISTS (
            SELECT 1 FROM __forest_module_schema__.source_units u
            JOIN __forest_module_schema__.memory_sources s USING(bot_id,source_id)
            WHERE u.bot_id=requested_bot AND u.source_unit_id=seed_unit
              AND u.source_revision=seed_revision
              AND u.eligibility_state='ELIGIBLE' AND s.status='ACTIVE'
        ) THEN
            skipped_count := skipped_count+1;
            CONTINUE;
        END IF;
        SELECT eligibility_state INTO trigger_state
        FROM __forest_module_schema__.source_units
        WHERE bot_id=requested_bot AND source_unit_id=trigger_unit;
        journal_ref := 'association-journal:' || encode(sha256(convert_to(
            jsonb_build_array(requested_bot,requested_operation,
                seed->>'bundle_id',trigger_unit,seed_unit)::text,'UTF8')),'hex');
        INSERT INTO __forest_module_schema__.association_journal(
            bot_id,journal_id,operation_id,input_digest,reply_ref,bundle_id,
            query_id,trigger_ref,trigger_source_unit_id,trigger_source_revision,
            trigger_part_id,seed_source_unit_id,seed_source_revision,seed_part_id,
            seed_rank_score,promotion_ready
        ) VALUES (
            requested_bot,journal_ref,requested_operation,requested_digest,
            body->>'reply_ref',seed->>'bundle_id',seed->>'query_id',
            proposal->>'trigger_ref',trigger_unit,trigger_revision,trigger_part,
            seed_unit,seed_revision,seed#>>'{part_ids,0}',
            (seed->>'rank_score')::double precision,
            trigger_state='ELIGIBLE'
        ) ON CONFLICT DO NOTHING;
        SELECT * INTO old_journal FROM __forest_module_schema__.association_journal
        WHERE bot_id=requested_bot AND journal_id=journal_ref;
        IF old_journal IS NULL OR old_journal.input_digest IS DISTINCT FROM requested_digest
           OR old_journal.reply_ref IS DISTINCT FROM body->>'reply_ref'
           OR old_journal.trigger_ref IS DISTINCT FROM proposal->>'trigger_ref'
           OR old_journal.trigger_part_id IS DISTINCT FROM trigger_part
           OR old_journal.seed_part_id IS DISTINCT FROM seed#>>'{part_ids,0}' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_ASSOCIATION_REPLAY_CONFLICT',
                    'message','Association journal identity differs','status',409));
        END IF;
        journaled_count := journaled_count+1;
    END LOOP;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.association-journal/2',
            'journaled',journaled_count,'skipped',skipped_count));
END;
$$;

CREATE FUNCTION memory_ensure_source(requested_bot text, source_value jsonb)
RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    source_id_value text := source_value->>'source_id';
    identity_digest_value text := 'sha256:'||encode(sha256(convert_to(source_value::text,'UTF8')),'hex');
    source_row record; identity_row record;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/source/'||requested_bot||'/'||source_id_value,0));
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/source-identity/'||requested_bot||'/'||identity_digest_value,0));
    SELECT * INTO source_row FROM __forest_module_schema__.memory_sources
    WHERE bot_id=requested_bot AND source_id=source_id_value FOR UPDATE;
    SELECT * INTO identity_row FROM __forest_module_schema__.memory_sources
    WHERE bot_id=requested_bot AND identity_digest=identity_digest_value FOR UPDATE;
    IF source_row.source_id IS NOT NULL AND identity_row.source_id IS NOT NULL
       AND source_row.source_id IS DISTINCT FROM identity_row.source_id THEN
        RETURN jsonb_build_object('ok',false,'error','MEMORY_SOURCE_IDENTITY_CONFLICT');
    END IF;
    IF source_row.source_id IS NULL THEN
        source_row := identity_row;
    END IF;
    IF source_row.source_id IS NOT NULL THEN
        IF source_row.source_id IS DISTINCT FROM source_id_value
           OR source_row.source_kind IS DISTINCT FROM source_value->>'source_kind'
           OR source_row.platform IS DISTINCT FROM source_value->>'platform'
           OR source_row.realm_id IS DISTINCT FROM source_value->>'realm_id'
           OR source_row.external_group_id IS DISTINCT FROM source_value->>'external_group_id'
           OR source_row.identity_incarnation IS DISTINCT FROM (source_value->>'identity_incarnation')::bigint
           OR source_row.trunk_id IS DISTINCT FROM source_value->>'trunk_id' THEN
            RETURN jsonb_build_object('ok',false,'error','MEMORY_SOURCE_IDENTITY_CONFLICT');
        END IF;
        RETURN jsonb_build_object('ok',true,'duplicate',true,'identity_digest',identity_digest_value);
    END IF;
    INSERT INTO __forest_module_schema__.memory_sources(
        bot_id,source_id,registration_operation_id,input_digest,source_kind,platform,realm_id,
        external_group_id,identity_incarnation,identity_digest,trunk_id)
    VALUES(requested_bot,source_id_value,
        'register:'||encode(sha256(convert_to(source_id_value,'UTF8')),'hex'),identity_digest_value,
        source_value->>'source_kind',source_value->>'platform',source_value->>'realm_id',
        source_value->>'external_group_id',(source_value->>'identity_incarnation')::bigint,
        identity_digest_value,source_value->>'trunk_id');
    IF source_value->>'trunk_id' IS NOT NULL THEN
        INSERT INTO __forest_module_schema__.memory_trunks(bot_id,trunk_id,source_id)
        VALUES(requested_bot,source_value->>'trunk_id',source_id_value);
    END IF;
    INSERT INTO __forest_module_schema__.source_clocks(bot_id,source_id)
    VALUES(requested_bot,source_id_value);
    RETURN jsonb_build_object('ok',true,'duplicate',false,'identity_digest',identity_digest_value);
END;
$$;

CREATE FUNCTION memory_reserve_source_position(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input'; source_value jsonb := body->'identity';
    prior_value jsonb := envelope->'prior'->(body->>'conversation_step_id');
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    source_id_value text := source_value->>'source_id';
    unit_id_value text := body->>'source_unit_id';
    ingress_seq_value bigint; source_order_value bigint;
    ensured jsonb; existing record;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'adapter.onebot'
       OR requested_operation IS NULL OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR jsonb_typeof(source_value) IS DISTINCT FROM 'object'
       OR source_value->>'source_kind' NOT IN ('GROUP','DIRECT')
       OR source_value->>'platform' IS DISTINCT FROM 'onebot-v11'
       OR unit_id_value IS NULL OR body->>'conversation_step_id' IS DISTINCT FROM 'conversation'
       OR prior_value->>'conversation_id' IS DISTINCT FROM body->>'conversation_ref'
       OR prior_value->>'event_id' IS DISTINCT FROM body->>'event_ref'
       OR prior_value->>'ingress_seq' !~ '^[0-9]+$' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','MEMORY_POSITION_INPUT_INVALID','message','Source position admission is invalid','status',422));
    END IF;
    ingress_seq_value := (prior_value->>'ingress_seq')::bigint;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/eligibility/'||requested_bot||'/'||
        (body->>'conversation_ref')||'/'||(body->>'event_ref'),0));
    ensured := __forest_module_schema__.memory_ensure_source(requested_bot,source_value);
    IF ensured->>'ok' IS DISTINCT FROM 'true' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code',ensured->>'error','message','Memory source identity differs','status',409));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/unit/'||requested_bot||'/'||unit_id_value,0));
    SELECT * INTO existing FROM __forest_module_schema__.source_position_reservations
    WHERE bot_id=requested_bot AND source_unit_id=unit_id_value;
    IF FOUND THEN
        IF existing.operation_id IS DISTINCT FROM requested_operation
           OR existing.input_digest IS DISTINCT FROM requested_digest
           OR existing.source_id IS DISTINCT FROM source_id_value
           OR existing.identity_digest IS DISTINCT FROM ensured->>'identity_digest'
           OR existing.conversation_ref IS DISTINCT FROM body->>'conversation_ref'
           OR existing.event_ref IS DISTINCT FROM body->>'event_ref'
           OR existing.ingress_seq IS DISTINCT FROM ingress_seq_value THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
                'code','MEMORY_POSITION_CONFLICT','message','Source position reservation differs','status',409));
        END IF;
        source_order_value := existing.source_order_seq;
    ELSE
        IF EXISTS (SELECT 1 FROM __forest_module_schema__.source_units
                   WHERE bot_id=requested_bot AND source_unit_id=unit_id_value) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
                'code','MEMORY_POSITION_CONFLICT','message','Source unit predates position reservation','status',409));
        END IF;
        UPDATE __forest_module_schema__.source_clocks
        SET next_source_order_seq=next_source_order_seq+1,updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND source_id=source_id_value
        RETURNING next_source_order_seq-1 INTO source_order_value;
        INSERT INTO __forest_module_schema__.source_position_reservations(
            bot_id,source_unit_id,operation_id,input_digest,source_id,identity_digest,
            source_was_existing,source_order_seq,conversation_ref,event_ref,ingress_seq)
        VALUES(requested_bot,unit_id_value,requested_operation,requested_digest,
            source_id_value,ensured->>'identity_digest',(ensured->>'duplicate')::boolean,source_order_value,
            body->>'conversation_ref',body->>'event_ref',ingress_seq_value);
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
        'protocol','forest.memory-source-position/1','source_id',source_id_value,
        'source_unit_id',unit_id_value,'source_order_seq',source_order_value));
EXCEPTION WHEN invalid_text_representation OR check_violation OR not_null_violation THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
        'code','MEMORY_POSITION_INPUT_INVALID','message','Source position input is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_capture_source(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input'; capture jsonb := body->'capture'; prior jsonb := envelope->'prior';
    requested_bot text := context->>'bot_id'; requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest'; caller_component text := envelope#>>'{caller,component_id}';
    source_value jsonb := capture->'identity'; occurrence_value jsonb := capture->'occurrence';
    author_value jsonb := capture->'author'; parts_value jsonb := capture->'parts';
    source_id_value text := source_value->>'source_id'; unit_id_value text := capture->>'source_unit_id';
    occurrence_id_value text := occurrence_value->>'occurrence_id'; conversation_step text := body->>'conversation_step_id';
    epoch_step text := body->>'deletion_epoch_step_id'; conversation_value jsonb; epoch_value jsonb;
    ingress_seq_value bigint; deletion_epoch_value bigint; identity_digest_value text;
    manifest_digest_value text; modality_value text; source_order_value bigint; ensured jsonb;
    reservation_row record; unit_row record; occurrence_row record; operation_row record; pending_row record;
    archive_pending_row record;
    part jsonb; duplicate_value boolean := false; eligibility_value text := 'REGISTERED';
    eligibility_operation_id text; eligibility_input_digest text;
    current_eligibility_revision bigint;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR caller_component NOT IN ('adapter.onebot','memory.graph')
       OR requested_operation IS NULL OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR jsonb_typeof(source_value) IS DISTINCT FROM 'object'
       OR jsonb_typeof(occurrence_value) IS DISTINCT FROM 'object'
       OR jsonb_typeof(author_value) IS DISTINCT FROM 'object'
       OR jsonb_typeof(parts_value) IS DISTINCT FROM 'array'
       OR jsonb_array_length(parts_value) NOT BETWEEN 1 AND 4096 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','MEMORY_CAPTURE_INPUT_INVALID','message','Memory capture envelope is invalid','status',422));
    END IF;
    epoch_value := prior->epoch_step;
    IF epoch_step IS NULL OR epoch_value->>'protocol' IS DISTINCT FROM 'forest.payload-epoch/1'
       OR epoch_value->>'deletion_epoch' !~ '^[0-9]+$' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','MEMORY_CAPTURE_AUTHORITY_MISSING','message','Payload deletion epoch is unavailable','status',503));
    END IF;
    deletion_epoch_value := (epoch_value->>'deletion_epoch')::bigint;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/eligibility/' || requested_bot || '/' ||
        (occurrence_value->>'conversation_ref') || '/' || (occurrence_value->>'event_ref'),0
    ));
    IF EXISTS (
        SELECT 1 FROM __forest_module_schema__.unavailable_source_events
        WHERE bot_id=requested_bot AND conversation_ref=occurrence_value->>'conversation_ref'
          AND event_ref=occurrence_value->>'event_ref'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_SOURCE_UNAVAILABLE',
                'message','Unavailable source cannot later be captured','status',409));
    END IF;
    IF conversation_step IS NOT NULL THEN
        conversation_value := prior->conversation_step;
        IF conversation_value->>'conversation_id' IS DISTINCT FROM occurrence_value->>'conversation_ref'
           OR conversation_value->>'event_id' IS DISTINCT FROM occurrence_value->>'event_ref'
           OR conversation_value->>'ingress_seq' !~ '^[0-9]+$' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
                'code','MEMORY_CAPTURE_CONVERSATION_CONFLICT','message','Conversation admission differs from Memory occurrence','status',409));
        END IF;
        ingress_seq_value := (conversation_value->>'ingress_seq')::bigint;
    ELSE
        IF caller_component<>'memory.graph' OR occurrence_value->>'ingress_seq' !~ '^[0-9]+$' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
                'code','MEMORY_CAPTURE_CONVERSATION_CONFLICT','message','Memory capture has no admitted ingress sequence','status',409));
        END IF;
        ingress_seq_value := (occurrence_value->>'ingress_seq')::bigint;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/capture/'||requested_bot||'/'||requested_operation,0));
    SELECT * INTO operation_row FROM __forest_module_schema__.capture_operations
    WHERE bot_id=requested_bot AND operation_id=requested_operation;
    IF FOUND THEN
        IF operation_row.input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
                'code','MEMORY_CAPTURE_CONFLICT','message','Memory capture operation input differs','status',409));
        END IF;
        SELECT eligibility_state INTO eligibility_value FROM __forest_module_schema__.source_units
        WHERE bot_id=requested_bot AND source_unit_id=operation_row.source_unit_id;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
            'protocol','forest.memory-source-captured/1','source_id',source_id_value,
            'source_unit_id',operation_row.source_unit_id,'occurrence_id',operation_row.occurrence_id,
            'duplicate_source',operation_row.duplicate_source,'eligibility_state',eligibility_value));
    END IF;
    identity_digest_value := 'sha256:'||encode(sha256(convert_to(source_value::text,'UTF8')),'hex');
    SELECT * INTO reservation_row FROM __forest_module_schema__.source_position_reservations
    WHERE bot_id=requested_bot AND source_unit_id=unit_id_value;
    IF FOUND THEN
        IF reservation_row.source_id IS DISTINCT FROM source_id_value
           OR reservation_row.identity_digest IS DISTINCT FROM identity_digest_value
           OR reservation_row.conversation_ref IS DISTINCT FROM occurrence_value->>'conversation_ref'
           OR reservation_row.event_ref IS DISTINCT FROM occurrence_value->>'event_ref'
           OR reservation_row.ingress_seq IS DISTINCT FROM ingress_seq_value THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
                'code','MEMORY_POSITION_CONFLICT','message','Captured source differs from admitted position','status',409));
        END IF;
        source_order_value := reservation_row.source_order_seq;
    ELSIF source_value->>'source_kind' IN ('GROUP','DIRECT')
          AND source_value->>'platform'='onebot-v11' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','MEMORY_POSITION_MISSING','message','OneBot message has no admitted source position','status',409));
    END IF;
    ensured := __forest_module_schema__.memory_ensure_source(requested_bot,source_value);
    IF ensured->>'ok' IS DISTINCT FROM 'true' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code',ensured->>'error','message','Memory source identity differs','status',409));
    END IF;
    duplicate_value := COALESCE(reservation_row.source_was_existing,(ensured->>'duplicate')::boolean);
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/unit/'||requested_bot||'/'||unit_id_value,0));
    SELECT * INTO unit_row FROM __forest_module_schema__.source_units
    WHERE bot_id=requested_bot AND source_unit_id=unit_id_value FOR UPDATE;
    IF FOUND THEN
        IF unit_row.input_digest IS DISTINCT FROM requested_digest
           OR unit_row.source_id IS DISTINCT FROM source_id_value
           OR (source_order_value IS NOT NULL AND unit_row.source_order_seq IS DISTINCT FROM source_order_value)
           OR unit_row.source_revision IS DISTINCT FROM (capture->>'source_revision')::bigint THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
                'code','MEMORY_SOURCE_UNIT_CONFLICT','message','Memory source unit identity differs','status',409));
        END IF;
    ELSE
        IF source_order_value IS NULL THEN
            UPDATE __forest_module_schema__.source_clocks SET next_source_order_seq=next_source_order_seq+1,
                updated_at=clock_timestamp() WHERE bot_id=requested_bot AND source_id=source_id_value
            RETURNING next_source_order_seq-1 INTO source_order_value;
        END IF;
        manifest_digest_value := 'sha256:'||encode(sha256(convert_to(parts_value::text,'UTF8')),'hex');
        SELECT CASE WHEN bool_and(value->>'part_kind'='TEXT') THEN 'TEXT'
                    WHEN bool_and(value->>'part_kind'='IMAGE') THEN 'IMAGE' ELSE 'MIXED' END
        INTO modality_value FROM jsonb_array_elements(parts_value);
        INSERT INTO __forest_module_schema__.source_units(
            bot_id,source_unit_id,capture_operation_id,input_digest,source_id,trunk_id,
            source_order_seq,source_revision,unit_kind,modality,part_count,content_manifest_digest,
            author_subject_ref,author_payload_id,author_payload_digest,author_owner_component_id,
            author_owner_kind,author_owner_id,accepted_deletion_epoch,occurred_at)
        VALUES(requested_bot,unit_id_value,requested_operation,requested_digest,source_id_value,
            source_value->>'trunk_id',source_order_value,(capture->>'source_revision')::bigint,
            capture->>'unit_kind',modality_value,jsonb_array_length(parts_value),manifest_digest_value,
            capture->>'author_subject_ref',author_value->>'payload_id',author_value->>'digest',
            author_value->>'owner_component_id',author_value->>'owner_kind',author_value->>'owner_id',
            deletion_epoch_value,(capture->>'occurred_at')::timestamptz);
        FOR part IN SELECT value FROM jsonb_array_elements(parts_value) LOOP
            INSERT INTO __forest_module_schema__.source_parts(
                bot_id,source_unit_id,part_id,part_index,part_kind,media_type,payload_id,payload_digest,
                payload_owner_component_id,payload_owner_kind,payload_owner_id,width,height,byte_length)
            VALUES(requested_bot,unit_id_value,part->>'part_id',(part->>'part_index')::integer,
                part->>'part_kind',part->>'media_type',part#>>'{payload,payload_id}',part#>>'{payload,digest}',
                part#>>'{payload,owner_component_id}',part#>>'{payload,owner_kind}',part#>>'{payload,owner_id}',
                (part->>'width')::integer,(part->>'height')::integer,(part->>'byte_length')::bigint);
        END LOOP;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/occurrence/'||requested_bot||'/'||occurrence_id_value,0));
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/event/'||requested_bot||'/'||(occurrence_value->>'binding_ref')||'/'||
        (occurrence_value->>'event_ref'),0));
    SELECT * INTO occurrence_row FROM __forest_module_schema__.source_occurrences
    WHERE bot_id=requested_bot AND (occurrence_id=occurrence_id_value OR
        (binding_ref=occurrence_value->>'binding_ref' AND event_ref=occurrence_value->>'event_ref'))
    FOR UPDATE;
    IF FOUND THEN
        IF occurrence_row.occurrence_id IS DISTINCT FROM occurrence_id_value
           OR occurrence_row.source_unit_id IS DISTINCT FROM unit_id_value
           OR occurrence_row.conversation_ref IS DISTINCT FROM occurrence_value->>'conversation_ref'
           OR occurrence_row.ingress_seq IS DISTINCT FROM ingress_seq_value THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
                'code','MEMORY_OCCURRENCE_CONFLICT','message','Memory occurrence identity differs','status',409));
        END IF;
    ELSE
        INSERT INTO __forest_module_schema__.source_occurrences(
            bot_id,occurrence_id,source_unit_id,binding_ref,conversation_ref,event_ref,ingress_seq,observed_at)
        VALUES(requested_bot,occurrence_id_value,unit_id_value,occurrence_value->>'binding_ref',
            occurrence_value->>'conversation_ref',occurrence_value->>'event_ref',ingress_seq_value,
            (occurrence_value->>'observed_at')::timestamptz);
    END IF;
    SELECT * INTO pending_row FROM __forest_module_schema__.pending_input_events
    WHERE bot_id=requested_bot AND conversation_ref=occurrence_value->>'conversation_ref'
      AND event_ref=occurrence_value->>'event_ref' FOR UPDATE;
    IF FOUND AND pending_row.state='PENDING_CAPTURE' THEN
        UPDATE __forest_module_schema__.source_occurrences SET eligibility_state='ADMITTED',
            input_batch_ref=pending_row.input_batch_ref,context_revision=pending_row.context_revision,
            qualified_at=clock_timestamp() WHERE bot_id=requested_bot AND occurrence_id=occurrence_id_value;
        eligibility_operation_id := 'eligibility:' || encode(sha256(convert_to(jsonb_build_array(
            requested_bot,pending_row.operation_id,occurrence_id_value
        )::text,'UTF8')),'hex');
        eligibility_input_digest := 'sha256:' || encode(sha256(convert_to(jsonb_build_array(
            unit_id_value,occurrence_id_value,pending_row.input_batch_ref,
            pending_row.context_revision,pending_row.accepted_deletion_epoch
        )::text,'UTF8')),'hex');
        SELECT eligibility_revision INTO current_eligibility_revision
        FROM __forest_module_schema__.source_units
        WHERE bot_id=requested_bot AND source_unit_id=unit_id_value;
        INSERT INTO __forest_module_schema__.eligibility_operations(
            bot_id,operation_id,input_digest,source_unit_id,occurrence_id,
            input_batch_ref,context_revision,resulting_eligibility_revision
        ) VALUES (
            requested_bot,eligibility_operation_id,eligibility_input_digest,
            unit_id_value,occurrence_id_value,pending_row.input_batch_ref,
            pending_row.context_revision,current_eligibility_revision
        );
        UPDATE __forest_module_schema__.pending_input_events SET state='RESOLVED',
            source_unit_id=unit_id_value,occurrence_id=occurrence_id_value,resolved_at=clock_timestamp()
        WHERE bot_id=requested_bot AND conversation_ref=occurrence_value->>'conversation_ref'
          AND event_ref=occurrence_value->>'event_ref';
    END IF;
    SELECT * INTO archive_pending_row FROM __forest_module_schema__.pending_archive_events
    WHERE bot_id=requested_bot AND conversation_ref=occurrence_value->>'conversation_ref'
      AND event_ref=occurrence_value->>'event_ref' FOR UPDATE;
    IF archive_pending_row.event_ref IS NOT NULL AND archive_pending_row.state='PENDING_CAPTURE' THEN
        SELECT * INTO occurrence_row FROM __forest_module_schema__.source_occurrences
        WHERE bot_id=requested_bot AND occurrence_id=occurrence_id_value FOR UPDATE;
        IF occurrence_row.eligibility_state='SUPPRESSED' OR
           (occurrence_row.input_batch_ref IS NOT NULL AND
            occurrence_row.input_batch_ref IS DISTINCT FROM archive_pending_row.input_batch_ref) OR
           (occurrence_row.context_revision IS NOT NULL AND
            occurrence_row.context_revision IS DISTINCT FROM archive_pending_row.context_revision) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_ARCHIVE_CONFLICT',
                    'message','Archived occurrence differs from captured source','status',409));
        END IF;
        UPDATE __forest_module_schema__.source_occurrences
        SET eligibility_state='ELIGIBLE',input_batch_ref=archive_pending_row.input_batch_ref,
            context_revision=archive_pending_row.context_revision,qualified_at=clock_timestamp()
        WHERE bot_id=requested_bot AND occurrence_id=occurrence_id_value
          AND eligibility_state IN ('REGISTERED','ADMITTED');
        UPDATE __forest_module_schema__.source_units
        SET eligibility_state='ELIGIBLE',eligibility_revision=eligibility_revision+1,
            eligible_at=clock_timestamp(),updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND source_unit_id=unit_id_value
          AND eligibility_state='REGISTERED';
        SELECT * INTO unit_row FROM __forest_module_schema__.source_units
        WHERE bot_id=requested_bot AND source_unit_id=unit_id_value;
        INSERT INTO __forest_module_schema__.archive_qualification_operations(
            bot_id,operation_id,input_digest,compaction_ref,conversation_ref,event_ref,
            source_unit_id,occurrence_id,resulting_eligibility_revision)
        VALUES(requested_bot,archive_pending_row.operation_id,
            archive_pending_row.input_digest,archive_pending_row.compaction_ref,
            archive_pending_row.conversation_ref,archive_pending_row.event_ref,
            unit_id_value,occurrence_id_value,unit_row.eligibility_revision);
        IF unit_row.eligibility_state='ELIGIBLE' THEN
            INSERT INTO __forest_module_schema__.facet_preparation_work(
                bot_id,source_unit_id)
            VALUES(requested_bot,unit_id_value)
            ON CONFLICT (bot_id,source_unit_id) DO NOTHING;
        END IF;
        UPDATE __forest_module_schema__.pending_archive_events
        SET state='RESOLVED',source_unit_id=unit_id_value,
            occurrence_id=occurrence_id_value,resolved_at=clock_timestamp()
        WHERE bot_id=requested_bot AND conversation_ref=occurrence_value->>'conversation_ref'
          AND event_ref=occurrence_value->>'event_ref';

        UPDATE __forest_module_schema__.association_journal
        SET promotion_ready=true
        WHERE bot_id=requested_bot AND trigger_source_unit_id=unit_id_value
          AND state='PENDING_ARCHIVE' AND NOT promotion_ready;
    END IF;
    SELECT eligibility_state INTO eligibility_value FROM __forest_module_schema__.source_units
    WHERE bot_id=requested_bot AND source_unit_id=unit_id_value;
    INSERT INTO __forest_module_schema__.capture_operations(
        bot_id,operation_id,input_digest,source_unit_id,occurrence_id,duplicate_source)
    VALUES(requested_bot,requested_operation,requested_digest,unit_id_value,occurrence_id_value,duplicate_value);
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
        'protocol','forest.memory-source-captured/1','source_id',source_id_value,
        'source_unit_id',unit_id_value,'occurrence_id',occurrence_id_value,
        'duplicate_source',duplicate_value,'eligibility_state',eligibility_value));
EXCEPTION WHEN invalid_text_representation OR datetime_field_overflow OR not_null_violation OR check_violation THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
        'code','MEMORY_CAPTURE_INPUT_INVALID','message','Memory capture payload is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_forget_work_projection(requested_bot text, requested_id text)
RETURNS jsonb LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT jsonb_build_object(
        'protocol',CASE WHEN w.work_kind='BATCH'
            THEN 'forest.memory-forget-batch-work/1'
            ELSE 'forest.memory-forget-work/1' END,
        'request_id',w.request_id,
        'state',w.state,
        'attempt_count',w.attempt_count,
        'result',w.result_payload,
        'error_code',w.error_code,
        'created_at',w.created_at,
        'updated_at',w.updated_at) || CASE WHEN w.work_kind='BATCH' THEN
            jsonb_build_object('selection_id',w.selection_id,
                'after_ordinal',w.after_ordinal,'batch_size',w.batch_size)
            ELSE jsonb_build_object('source_unit_id',w.source_unit_id) END
    FROM __forest_module_schema__.management_forget_work w
    WHERE w.bot_id=requested_bot AND w.request_id=requested_id
$$;

CREATE FUNCTION memory_committed_forget_result(
    requested_bot text, requested_kind text, requested_id text
) RETURNS jsonb LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
    SELECT CASE WHEN requested_kind='BATCH' THEN (
        SELECT o.result_payload FROM __forest_module_schema__.batch_forget_operations o
        WHERE o.bot_id=requested_bot AND o.operation_id=requested_id
    ) ELSE (
        SELECT o.result_payload FROM __forest_module_schema__.source_forget_operations o
        WHERE o.bot_id=requested_bot AND o.operation_id=requested_id
    ) END
$$;

CREATE FUNCTION memory_enqueue_management_forget(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_id text := context->>'operation_id';
    requested_digest text := 'sha256:' ||
        encode(sha256(convert_to(input::text,'UTF8')),'hex');
    plan_value jsonb := input->'plan';
    existing record;
BEGIN
    IF requested_id IS NULL OR octet_length(requested_id) NOT BETWEEN 1 AND 256
       OR jsonb_typeof(plan_value) IS DISTINCT FROM 'object'
       OR plan_value->>'protocol' IS DISTINCT FROM 'forest.memory-forget-impact-plan/1'
       OR plan_value->>'source_unit_id' IS DISTINCT FROM input->>'source_unit_id'
       OR plan_value->>'plan_digest' IS DISTINCT FROM ('sha256:' ||
           encode(sha256(convert_to((plan_value-'plan_digest')::text,'UTF8')),'hex'))
       OR input->>'requested_by' IS NULL
       OR octet_length(input->>'requested_by') NOT BETWEEN 1 AND 256 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_MANAGEMENT_INPUT_INVALID',
                'message','Management forget request is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/management-forget/' || requested_bot || '/' || requested_id,0));
    SELECT * INTO existing FROM __forest_module_schema__.management_forget_work
    WHERE bot_id=requested_bot AND request_id=requested_id;
    IF FOUND THEN
        IF existing.input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_FORGET_OPERATION_CONFLICT',
                    'message','Management forget request differs','status',409));
        END IF;
    ELSE
        IF NOT EXISTS (
            SELECT 1 FROM __forest_module_schema__.source_units
            WHERE bot_id=requested_bot AND source_unit_id=input->>'source_unit_id') THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_SOURCE_NOT_FOUND',
                    'message','Memory source unit does not exist','status',404));
        END IF;
        INSERT INTO __forest_module_schema__.management_forget_work(
            bot_id,request_id,input_digest,source_unit_id,requested_by,plan)
        VALUES(requested_bot,requested_id,requested_digest,
            input->>'source_unit_id',input->>'requested_by',plan_value);
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.memory-forget-enqueued/1','request_id',requested_id));
END;
$$;

CREATE FUNCTION memory_query_management_forget(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    result_value jsonb;
BEGIN
    result_value := __forest_module_schema__.memory_forget_work_projection(
        context->>'bot_id',input->>'request_id');
    IF result_value IS NULL OR result_value->>'protocol'<>'forest.memory-forget-work/1' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_REQUEST_NOT_FOUND',
                'message','Memory forget request does not exist','status',404));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',result_value);
END;
$$;

CREATE FUNCTION memory_enqueue_management_forget_batch(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_id text := context->>'operation_id';
    requested_digest text := 'sha256:' ||
        encode(sha256(convert_to(input::text,'UTF8')),'hex');
    requested_selection text := input->>'selection_id';
    requested_actor text := input->>'requested_by';
    after_value integer := (input->>'after_ordinal')::integer;
    batch_size_value integer := (input->>'batch_size')::integer;
    plan_value jsonb := input->'plan';
    selection_row __forest_module_schema__.forget_selections%ROWTYPE;
    existing __forest_module_schema__.management_forget_work%ROWTYPE;
BEGIN
    IF requested_id IS NULL OR octet_length(requested_id) NOT BETWEEN 1 AND 256
       OR requested_selection IS NULL OR octet_length(requested_selection) NOT BETWEEN 1 AND 256
       OR requested_actor IS NULL OR octet_length(requested_actor) NOT BETWEEN 1 AND 256
       OR after_value IS NULL OR after_value<0
       OR batch_size_value IS NULL OR batch_size_value NOT BETWEEN 1 AND 64
       OR jsonb_typeof(plan_value) IS DISTINCT FROM 'object'
       OR plan_value->>'protocol' IS DISTINCT FROM 'forest.memory-forget-batch-impact-plan/1'
       OR plan_value->>'selection_id' IS DISTINCT FROM requested_selection
       OR (plan_value->>'first_ordinal')::integer IS DISTINCT FROM after_value+1
       OR plan_value->>'plan_digest' IS DISTINCT FROM ('sha256:' ||
           encode(sha256(convert_to((plan_value-'plan_digest')::text,'UTF8')),'hex')) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_BATCH_MANAGEMENT_INPUT_INVALID',
                'message','Management batch forget request is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/management-forget/' || requested_bot || '/' || requested_id,0));
    SELECT * INTO existing FROM __forest_module_schema__.management_forget_work
    WHERE bot_id=requested_bot AND request_id=requested_id;
    IF FOUND THEN
        IF existing.work_kind IS DISTINCT FROM 'BATCH'
           OR existing.input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_FORGET_OPERATION_CONFLICT',
                    'message','Management forget request differs','status',409));
        END IF;
    ELSE
        SELECT * INTO selection_row FROM __forest_module_schema__.forget_selections
        WHERE bot_id=requested_bot AND selection_id=requested_selection
          AND requested_by=requested_actor;
        IF selection_row IS NULL THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_FORGET_SELECTION_NOT_FOUND',
                    'message','Forget selection does not exist','status',404));
        END IF;
        IF after_value>=selection_row.member_count
           OR (plan_value->>'last_ordinal')::integer IS DISTINCT FROM
              LEAST(after_value+batch_size_value,selection_row.member_count)
           OR plan_value->>'member_digest' IS DISTINCT FROM selection_row.member_digest
           OR (plan_value->>'batch_member_count')::integer IS DISTINCT FROM
              LEAST(batch_size_value,selection_row.member_count-after_value) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_FORGET_BATCH_SELECTION_CONFLICT',
                    'message','Batch plan differs from the frozen selection','status',409));
        END IF;
        INSERT INTO __forest_module_schema__.management_forget_work(
            bot_id,request_id,input_digest,work_kind,selection_id,after_ordinal,
            batch_size,requested_by,plan)
        VALUES(requested_bot,requested_id,requested_digest,'BATCH',requested_selection,
               after_value,batch_size_value,requested_actor,plan_value);
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.memory-forget-batch-enqueued/1','request_id',requested_id));
END;
$$;

CREATE FUNCTION memory_query_management_forget_batch(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE
    result_value jsonb;
BEGIN
    SELECT __forest_module_schema__.memory_forget_work_projection(w.bot_id,w.request_id)
    INTO result_value FROM __forest_module_schema__.management_forget_work w
    WHERE w.bot_id=context->>'bot_id' AND w.request_id=input->>'request_id'
      AND w.work_kind='BATCH' AND w.requested_by=input->>'requested_by';
    IF result_value IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_BATCH_REQUEST_NOT_FOUND',
                'message','Management batch forget request does not exist','status',404));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
                              'value',result_value);
END;
$$;

CREATE FUNCTION memory_claim_management_forget(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    work_row __forest_module_schema__.management_forget_work%ROWTYPE;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_token IS NULL OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_CLAIM_INVALID',
                'message','Management forget claim is invalid','status',422));
    END IF;
    WITH expired AS MATERIALIZED (
        SELECT bot_id,request_id,work_kind,attempt_count
        FROM __forest_module_schema__.management_forget_work
        WHERE bot_id=requested_bot AND state='RUNNING'
          AND lease_deadline<=clock_timestamp()
        ORDER BY lease_deadline,request_id
        FOR UPDATE SKIP LOCKED LIMIT 32
    ), resolved AS MATERIALIZED (
        SELECT e.bot_id,e.request_id,e.attempt_count,
               __forest_module_schema__.memory_committed_forget_result(
                   e.bot_id,e.work_kind,e.request_id) AS committed_result
        FROM expired e
    )
    UPDATE __forest_module_schema__.management_forget_work w
    SET state=CASE WHEN r.committed_result IS NOT NULL
            THEN 'SUCCEEDED' WHEN r.attempt_count<3 THEN 'QUEUED' ELSE 'UNKNOWN' END,
        claim_token=NULL,lease_deadline=NULL,
        result_payload=r.committed_result,
        error_code=CASE WHEN r.committed_result IS NOT NULL
            OR r.attempt_count<3 THEN NULL ELSE 'MEMORY_FORGET_OUTCOME_UNKNOWN' END,
        reconcile_checked_at=CASE WHEN r.committed_result IS NULL
            AND r.attempt_count>=3 THEN clock_timestamp()
            ELSE w.reconcile_checked_at END,
        updated_at=clock_timestamp()
    FROM resolved r
    WHERE w.bot_id=r.bot_id AND w.request_id=r.request_id;
    WITH pending AS MATERIALIZED (
        SELECT bot_id,request_id,work_kind
        FROM __forest_module_schema__.management_forget_work
        WHERE bot_id=requested_bot AND state='UNKNOWN'
          AND (reconcile_checked_at IS NULL OR
               reconcile_checked_at<=clock_timestamp()-interval '60 seconds')
        ORDER BY reconcile_checked_at NULLS FIRST,request_id
        FOR UPDATE SKIP LOCKED LIMIT 16
    ), resolved AS MATERIALIZED (
        SELECT p.bot_id,p.request_id,
               __forest_module_schema__.memory_committed_forget_result(
                   p.bot_id,p.work_kind,p.request_id) AS committed_result
        FROM pending p
    )
    UPDATE __forest_module_schema__.management_forget_work w
    SET state=CASE WHEN r.committed_result IS NOT NULL THEN 'SUCCEEDED'
                   ELSE 'UNKNOWN' END,
        result_payload=r.committed_result,
        error_code=CASE WHEN r.committed_result IS NOT NULL THEN NULL
                        ELSE w.error_code END,
        reconcile_checked_at=clock_timestamp(),
        updated_at=CASE WHEN r.committed_result IS NOT NULL
                        THEN clock_timestamp() ELSE w.updated_at END
    FROM resolved r
    WHERE w.bot_id=r.bot_id AND w.request_id=r.request_id;
    SELECT * INTO work_row FROM __forest_module_schema__.management_forget_work
    WHERE bot_id=requested_bot AND state='QUEUED'
    ORDER BY created_at,request_id FOR UPDATE SKIP LOCKED LIMIT 1;
    IF work_row IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    UPDATE __forest_module_schema__.management_forget_work
    SET state='RUNNING',claim_token=requested_token,
        lease_deadline=clock_timestamp()+(requested_lease*interval '1 second'),
        attempt_count=attempt_count+1,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND request_id=work_row.request_id RETURNING * INTO work_row;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.owned-work-claim/1',
            'work_id',work_row.request_id,
            'claim_token',requested_token,
            'attempt',work_row.attempt_count,
            'operation_id','memory-forget-work:' || encode(sha256(convert_to(
                work_row.request_id || '/' || work_row.attempt_count,'UTF8')),'hex'),
            'causal_root_kind',CASE WHEN work_row.work_kind='BATCH'
                THEN 'memory.forget.batch' ELSE 'memory.forget' END,
            'causal_root_id',work_row.request_id,
            'payload',jsonb_build_object(
                'request_id',work_row.request_id,
                'work_kind',work_row.work_kind,
                'plan',work_row.plan) || CASE WHEN work_row.work_kind='BATCH' THEN
                    jsonb_build_object('selection_id',work_row.selection_id,
                        'requested_by',work_row.requested_by,
                        'after_ordinal',work_row.after_ordinal,
                        'batch_size',work_row.batch_size)
                    ELSE jsonb_build_object('source_unit_id',work_row.source_unit_id) END));
END;
$$;

CREATE FUNCTION memory_settle_management_forget(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    old_row record;
    work_row __forest_module_schema__.management_forget_work%ROWTYPE;
    result_value jsonb;
    next_state text;
BEGIN
    SELECT * INTO old_row FROM __forest_module_schema__.management_forget_settlements
    WHERE bot_id=requested_bot AND claim_token=input->>'claim_token';
    IF old_row IS NOT NULL THEN
        IF old_row.input_payload=input THEN RETURN old_row.result_payload; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_SETTLEMENT_CONFLICT',
                'message','Management forget claim was settled differently','status',409));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.management_forget_work
    WHERE bot_id=requested_bot AND request_id=input->>'work_id' FOR UPDATE;
    IF work_row IS NULL OR work_row.state<>'RUNNING'
       OR work_row.claim_token IS DISTINCT FROM input->>'claim_token'
       OR work_row.attempt_count IS DISTINCT FROM (input->>'attempt')::integer THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_CLAIM_LOST',
                'message','Management forget claim is no longer current','status',409));
    END IF;
    result_value := __forest_module_schema__.memory_committed_forget_result(
        requested_bot,work_row.work_kind,work_row.request_id);
    IF result_value IS NOT NULL THEN
        next_state := 'SUCCEEDED';
    ELSIF input#>>'{outcome,status}'='SUCCEEDED' THEN
        next_state := 'UNKNOWN';
    ELSIF input#>>'{outcome,error,retryable}'='true' AND work_row.attempt_count<3 THEN
        next_state := 'QUEUED';
    ELSIF input#>>'{outcome,error,unknown}'='true' THEN
        next_state := 'UNKNOWN';
    ELSE
        next_state := 'FAILED';
    END IF;
    UPDATE __forest_module_schema__.management_forget_work
    SET state=next_state,claim_token=NULL,lease_deadline=NULL,
        result_payload=CASE WHEN next_state='SUCCEEDED' THEN result_value ELSE NULL END,
        error_code=CASE WHEN next_state='SUCCEEDED' THEN NULL
                        WHEN next_state='UNKNOWN' THEN 'MEMORY_FORGET_OUTCOME_UNKNOWN'
                        ELSE input#>>'{outcome,error,code}' END,
        reconcile_checked_at=CASE WHEN next_state='UNKNOWN'
            THEN clock_timestamp() ELSE reconcile_checked_at END,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND request_id=work_row.request_id;
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.owned-work-settled/1',
            'work_id',work_row.request_id));
    INSERT INTO __forest_module_schema__.management_forget_settlements(
        bot_id,claim_token,input_payload,result_payload)
    VALUES(requested_bot,input->>'claim_token',input,result_value);
    RETURN result_value;
END;
$$;

CREATE FUNCTION memory_source_forget_impact(requested_bot text, selected_ids text[])
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    affected_ids text[] := ARRAY[]::text[];
    vector_ids text[] := ARRAY[]::text[];
    relation_ids text[] := ARRAY[]::text[];
    journal_ids text[] := ARRAY[]::text[];
    scope_value jsonb;
BEGIN
    SELECT COALESCE(array_agg(u.source_unit_id ORDER BY u.source_unit_id),ARRAY[]::text[])
    INTO affected_ids
    FROM __forest_module_schema__.source_units u
    WHERE u.bot_id=requested_bot AND u.eligibility_state<>'SUPPRESSED' AND (
        u.author_payload_id=ANY(selected_ids) OR EXISTS (
            SELECT 1 FROM __forest_module_schema__.source_parts p
            WHERE p.bot_id=u.bot_id AND p.source_unit_id=u.source_unit_id
              AND p.payload_id=ANY(selected_ids)));
    SELECT COALESCE(array_agg(f.facet_id ORDER BY f.facet_id),ARRAY[]::text[])
    INTO vector_ids
    FROM __forest_module_schema__.embedding_facets f
    WHERE f.bot_id=requested_bot AND f.source_unit_id=ANY(affected_ids)
      AND NOT EXISTS (
          SELECT 1 FROM __forest_module_schema__.vector_index_deletions d
          WHERE d.bot_id=f.bot_id AND d.index_ref_id=f.facet_id);
    SELECT COALESCE(array_agg(c.contribution_id ORDER BY c.contribution_id),ARRAY[]::text[])
    INTO relation_ids
    FROM __forest_module_schema__.relation_contributions c
    WHERE c.bot_id=requested_bot AND c.active AND (
        (c.left_kind='SOURCE_UNIT' AND c.left_id=ANY(affected_ids)) OR
        (c.right_kind='SOURCE_UNIT' AND c.right_id=ANY(affected_ids)) OR EXISTS (
            SELECT 1 FROM __forest_module_schema__.relation_evidence e
            WHERE e.bot_id=c.bot_id AND e.contribution_id=c.contribution_id
              AND e.source_unit_id=ANY(affected_ids)));
    SELECT COALESCE(array_agg(j.journal_id ORDER BY j.journal_id),ARRAY[]::text[])
    INTO journal_ids
    FROM __forest_module_schema__.association_journal j
    WHERE j.bot_id=requested_bot AND j.state='PENDING_ARCHIVE'
      AND (j.trigger_source_unit_id=ANY(affected_ids)
           OR j.seed_source_unit_id=ANY(affected_ids));
    scope_value := jsonb_build_object(
        'source_unit_ids',to_jsonb(affected_ids),
        'vector_index_ref_ids',to_jsonb(vector_ids),
        'relation_contribution_ids',to_jsonb(relation_ids),
        'association_journal_ids',to_jsonb(journal_ids));
    RETURN jsonb_build_object(
        'memory_impact_digest','sha256:' ||
            encode(sha256(convert_to(scope_value::text,'UTF8')),'hex'),
        'source_unit_count',cardinality(affected_ids),
        'vector_deletion_count',cardinality(vector_ids),
        'relation_retraction_count',cardinality(relation_ids),
        'association_journal_count',cardinality(journal_ids),
        'source_unit_sample',to_jsonb(affected_ids[1:20]));
END;
$$;

CREATE FUNCTION memory_preview_source_forget(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    prior jsonb := envelope->'prior';
    prepared jsonb := prior->(body->>'prepare_step_id');
    preview jsonb := prior->(body->>'preview_step_id');
    requested_bot text := context->>'bot_id';
    selected_ids text[];
    impact jsonb;
    plan_value jsonb;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'source_unit_id' IS NULL
       OR octet_length(body->>'source_unit_id') NOT BETWEEN 1 AND 256
       OR prepared->>'protocol' IS DISTINCT FROM 'forest.payload-forget-plan/1'
       OR preview->>'protocol' IS DISTINCT FROM 'forest.payload-forget-preview/1'
       OR preview->>'selection_digest' !~ '^sha256:[a-f0-9]{64}$'
       OR preview->>'deletion_epoch' !~ '^[0-9]+$'
       OR preview->>'selected_count' !~ '^[0-9]+$'
       OR jsonb_typeof(preview->'selected_payload_ids') IS DISTINCT FROM 'array'
       OR jsonb_array_length(preview->'selected_payload_ids') > 100000 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_PREVIEW_INVALID',
                'message','Memory forget preview is invalid','status',422));
    END IF;
    SELECT array_agg(value ORDER BY value) INTO selected_ids
    FROM jsonb_array_elements_text(preview->'selected_payload_ids');
    IF selected_ids IS NULL OR cardinality(selected_ids)<>(preview->>'selected_count')::integer
       OR prepared->>'anchor_payload_id' <> ALL(selected_ids)
       OR NOT EXISTS (
           SELECT 1 FROM __forest_module_schema__.source_units u
           WHERE u.bot_id=requested_bot AND u.source_unit_id=body->>'source_unit_id'
             AND u.author_payload_id=prepared->>'anchor_payload_id'
             AND u.eligibility_state<>'SUPPRESSED') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_PREVIEW_CHANGED',
                'message','Memory forget preview target changed','status',409));
    END IF;
    impact := __forest_module_schema__.memory_source_forget_impact(requested_bot,selected_ids);
    plan_value := jsonb_build_object(
        'protocol','forest.memory-forget-impact-plan/1',
        'source_unit_id',body->>'source_unit_id',
        'anchor_payload_id',prepared->>'anchor_payload_id',
        'deletion_epoch',(preview->>'deletion_epoch')::bigint,
        'selection_digest',preview->>'selection_digest',
        'selected_payload_count',(preview->>'selected_count')::integer)
        || impact;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',plan_value || jsonb_build_object('plan_digest','sha256:' ||
            encode(sha256(convert_to(plan_value::text,'UTF8')),'hex')));
END;
$$;

CREATE FUNCTION memory_prepare_source_forget(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    requested_bot text := context->>'bot_id';
    unit_row record;
    suppression_row record;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'operation_id' IS NULL
       OR octet_length(body->>'operation_id') NOT BETWEEN 1 AND 256
       OR body->>'source_unit_id' IS NULL
       OR octet_length(body->>'source_unit_id') NOT BETWEEN 1 AND 256 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_INPUT_INVALID',
                'message','Memory forget request is invalid','status',422));
    END IF;
    SELECT * INTO unit_row FROM __forest_module_schema__.source_units
    WHERE bot_id=requested_bot AND source_unit_id=body->>'source_unit_id';
    IF unit_row IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_SOURCE_NOT_FOUND',
                'message','Memory source unit does not exist','status',404));
    END IF;
    SELECT * INTO suppression_row FROM __forest_module_schema__.source_suppressions
    WHERE bot_id=requested_bot AND source_unit_id=unit_row.source_unit_id;
    IF unit_row.eligibility_state='SUPPRESSED' AND
       (suppression_row IS NULL OR suppression_row.operation_id IS DISTINCT FROM
        body->>'operation_id') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_SOURCE_SUPPRESSED',
                'message','Memory source unit was already forgotten','status',409));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.payload-forget-plan/1',
            'anchor_payload_id',unit_row.author_payload_id));
END;
$$;

CREATE FUNCTION memory_apply_forget_effects(
    requested_bot text, requested_operation text, requested_digest text,
    selected_ids text[], deletion_epoch_value bigint, required_source_ids text[])
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    affected_ids text[] := ARRAY[]::text[];
    deleted_refs integer := 0;
    retracted_count integer := 0;
BEGIN
    SELECT COALESCE(array_agg(locked.source_unit_id ORDER BY locked.source_unit_id),
                    ARRAY[]::text[])
    INTO affected_ids
    FROM (
        SELECT u.source_unit_id FROM __forest_module_schema__.source_units u
        WHERE u.bot_id=requested_bot AND u.eligibility_state<>'SUPPRESSED' AND (
            u.author_payload_id=ANY(selected_ids) OR EXISTS (
                SELECT 1 FROM __forest_module_schema__.source_parts p
                WHERE p.bot_id=u.bot_id AND p.source_unit_id=u.source_unit_id
                  AND p.payload_id=ANY(selected_ids)))
        ORDER BY u.source_unit_id FOR UPDATE
    ) locked;
    IF required_source_ids IS NULL OR cardinality(required_source_ids)=0 OR
       NOT required_source_ids <@ affected_ids THEN
        RETURN NULL;
    END IF;
    INSERT INTO __forest_module_schema__.source_suppressions(
        bot_id,operation_id,input_digest,source_unit_id,deletion_epoch,reason_code)
    SELECT requested_bot,requested_operation,requested_digest,source_unit_id,
           deletion_epoch_value,'PUBLIC_FORGET'
    FROM unnest(affected_ids) AS selected(source_unit_id);
    UPDATE __forest_module_schema__.source_units
    SET eligibility_state='SUPPRESSED',eligibility_revision=eligibility_revision+1,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND source_unit_id=ANY(affected_ids);
    UPDATE __forest_module_schema__.source_occurrences
    SET eligibility_state='SUPPRESSED',input_batch_ref=NULL,context_revision=NULL
    WHERE bot_id=requested_bot AND source_unit_id=ANY(affected_ids)
      AND eligibility_state<>'SUPPRESSED';
    INSERT INTO __forest_module_schema__.vector_index_deletions(
        bot_id,index_ref_id,space_id,deletion_epoch)
    SELECT bot_id,facet_id,space_id,deletion_epoch_value
    FROM __forest_module_schema__.embedding_facets
    WHERE bot_id=requested_bot AND source_unit_id=ANY(affected_ids)
    ON CONFLICT (bot_id,index_ref_id) DO NOTHING;
    GET DIAGNOSTICS deleted_refs = ROW_COUNT;
    UPDATE __forest_module_schema__.embedding_facets
    SET state='SUPPRESSED',index_ref_id=NULL,vector_digest=NULL,
        result_payload_id=NULL,result_payload_digest=NULL,published_at=NULL,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND source_unit_id=ANY(affected_ids)
      AND state<>'SUPPRESSED';
    UPDATE __forest_module_schema__.facet_preparation_work
    SET state='SUPPRESSED',claim_token=NULL,lease_deadline=NULL,
        prepared_facet_count=NULL,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND source_unit_id=ANY(affected_ids)
      AND state NOT IN ('SUPPRESSED','CLAIMED');
    UPDATE __forest_module_schema__.association_journal
    SET state='SUPPRESSED'
    WHERE bot_id=requested_bot AND state='PENDING_ARCHIVE'
      AND (trigger_source_unit_id=ANY(affected_ids)
           OR seed_source_unit_id=ANY(affected_ids));
    UPDATE __forest_module_schema__.relation_contributions c SET active=false
    WHERE c.bot_id=requested_bot AND c.active AND (
        (c.left_kind='SOURCE_UNIT' AND c.left_id=ANY(affected_ids)) OR
        (c.right_kind='SOURCE_UNIT' AND c.right_id=ANY(affected_ids)) OR EXISTS (
            SELECT 1 FROM __forest_module_schema__.relation_evidence e
            WHERE e.bot_id=c.bot_id AND e.contribution_id=c.contribution_id
              AND e.source_unit_id=ANY(affected_ids)));
    GET DIAGNOSTICS retracted_count = ROW_COUNT;
    RETURN jsonb_build_object(
        'suppressed_source_units',cardinality(affected_ids),
        'vector_deletions',deleted_refs,'retracted_relations',retracted_count);
END;
$$;

CREATE FUNCTION memory_apply_source_forget(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    prior jsonb := envelope->'prior';
    prepared jsonb := prior->(body->>'prepare_step_id');
    forgotten jsonb := prior->(body->>'forget_step_id');
    requested_bot text := context->>'bot_id';
    requested_digest text := context->>'participant_input_digest';
    operation_row record;
    selected_ids text[];
    deletion_epoch_value bigint;
    effects jsonb;
    impact_value jsonb;
    current_plan jsonb;
    result_value jsonb;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'operation_id' IS NULL
       OR octet_length(body->>'operation_id') NOT BETWEEN 1 AND 256
       OR body->>'source_unit_id' IS NULL
       OR octet_length(body->>'source_unit_id') NOT BETWEEN 1 AND 256
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR prepared->>'protocol' IS DISTINCT FROM 'forest.payload-forget-plan/1'
       OR forgotten->>'protocol' IS DISTINCT FROM 'forest.payload-forgotten/1'
       OR forgotten->>'operation_id' IS DISTINCT FROM body->>'operation_id'
       OR forgotten->>'selection_digest' !~ '^sha256:[a-f0-9]{64}$'
       OR forgotten->>'deletion_epoch' !~ '^[1-9][0-9]*$'
       OR jsonb_typeof(forgotten->'selected_payload_ids') IS DISTINCT FROM 'array'
       OR jsonb_array_length(forgotten->'selected_payload_ids') > 100000 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_AUTHORITY_INVALID',
                'message','Payload forget receipt is invalid','status',422));
    END IF;
    SELECT array_agg(value ORDER BY value) INTO selected_ids
    FROM jsonb_array_elements_text(forgotten->'selected_payload_ids');
    IF selected_ids IS NULL OR prepared->>'anchor_payload_id' <> ALL(selected_ids) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_ANCHOR_MISSING',
                'message','Payload forget selection omitted the Memory anchor','status',409));
    END IF;
    deletion_epoch_value := (forgotten->>'deletion_epoch')::bigint;
    IF NOT EXISTS (
        SELECT 1 FROM __forest_module_schema__.source_units
        WHERE bot_id=requested_bot AND source_unit_id=body->>'source_unit_id'
          AND author_payload_id=prepared->>'anchor_payload_id'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_ANCHOR_MISSING',
                'message','Payload forget anchor differs from the Memory source','status',409));
    END IF;
    SELECT * INTO operation_row FROM __forest_module_schema__.source_forget_operations
    WHERE bot_id=requested_bot AND operation_id=body->>'operation_id';
    IF operation_row IS NOT NULL THEN
        IF operation_row.input_digest IS DISTINCT FROM requested_digest
           OR operation_row.source_unit_id IS DISTINCT FROM body->>'source_unit_id'
           OR operation_row.selection_digest IS DISTINCT FROM forgotten->>'selection_digest'
           OR operation_row.deletion_epoch IS DISTINCT FROM deletion_epoch_value THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_FORGET_OPERATION_CONFLICT',
                    'message','Memory forget operation differs','status',409));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',operation_row.result_payload);
    END IF;
    IF body ? 'plan' THEN
        impact_value := __forest_module_schema__.memory_source_forget_impact(
            requested_bot,selected_ids);
        current_plan := jsonb_build_object(
            'protocol','forest.memory-forget-impact-plan/1',
            'source_unit_id',body->>'source_unit_id',
            'anchor_payload_id',prepared->>'anchor_payload_id',
            'deletion_epoch',deletion_epoch_value-1,
            'selection_digest',forgotten->>'selection_digest',
            'selected_payload_count',cardinality(selected_ids)) || impact_value;
        current_plan := current_plan || jsonb_build_object(
            'plan_digest','sha256:' ||
                encode(sha256(convert_to(current_plan::text,'UTF8')),'hex'));
        IF body->'plan' IS DISTINCT FROM current_plan THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_FORGET_PLAN_CHANGED',
                    'message','Memory forget impact differs from the approved plan',
                    'status',409));
        END IF;
    END IF;
    effects := __forest_module_schema__.memory_apply_forget_effects(
        requested_bot,body->>'operation_id',requested_digest,selected_ids,
        deletion_epoch_value,ARRAY[body->>'source_unit_id']);
    IF effects IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_ANCHOR_MISSING',
                'message','Memory source unit was not selected by Payload','status',409));
    END IF;
    result_value := jsonb_build_object('protocol','forest.memory-source-forgotten/1',
        'source_unit_id',body->>'source_unit_id','deletion_epoch',deletion_epoch_value)
        || effects;
    INSERT INTO __forest_module_schema__.source_forget_operations(
        bot_id,operation_id,input_digest,source_unit_id,selection_digest,
        deletion_epoch,result_payload)
    VALUES(requested_bot,body->>'operation_id',requested_digest,
        body->>'source_unit_id',forgotten->>'selection_digest',
        deletion_epoch_value,result_value);
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',result_value);
END;
$$;

CREATE FUNCTION memory_mark_source_unavailable(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input'; requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id'; requested_digest text := context->>'participant_input_digest';
    existing record; pending_row record;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR requested_operation IS NULL OR requested_digest !~ '^sha256:[a-f0-9]{64}$' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','MEMORY_UNAVAILABLE_INPUT_INVALID','message','Memory unavailable envelope is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/eligibility/' || requested_bot || '/' ||
        (body->>'conversation_ref') || '/' || (body->>'event_ref'),0
    ));
    IF EXISTS (
        SELECT 1 FROM __forest_module_schema__.source_occurrences
        WHERE bot_id=requested_bot AND conversation_ref=body->>'conversation_ref'
          AND event_ref=body->>'event_ref'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_SOURCE_ALREADY_CAPTURED',
                'message','Captured source cannot become unavailable','status',409));
    END IF;
    INSERT INTO __forest_module_schema__.unavailable_source_events(
        bot_id,conversation_ref,event_ref,operation_id,input_digest,error_code)
    VALUES(requested_bot,body->>'conversation_ref',body->>'event_ref',requested_operation,
        requested_digest,body->>'error_code') ON CONFLICT DO NOTHING;
    SELECT * INTO existing FROM __forest_module_schema__.unavailable_source_events
    WHERE bot_id=requested_bot AND conversation_ref=body->>'conversation_ref' AND event_ref=body->>'event_ref';
    IF existing.operation_id IS DISTINCT FROM requested_operation
       OR existing.input_digest IS DISTINCT FROM requested_digest THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','MEMORY_UNAVAILABLE_CONFLICT','message','Memory unavailable identity differs','status',409));
    END IF;
    SELECT * INTO pending_row FROM __forest_module_schema__.pending_input_events
    WHERE bot_id=requested_bot AND conversation_ref=body->>'conversation_ref' AND event_ref=body->>'event_ref' FOR UPDATE;
    IF FOUND AND pending_row.state='PENDING_CAPTURE' THEN
        UPDATE __forest_module_schema__.pending_input_events SET state='UNAVAILABLE',
            error_code=body->>'error_code',resolved_at=clock_timestamp()
        WHERE bot_id=requested_bot AND conversation_ref=body->>'conversation_ref' AND event_ref=body->>'event_ref';
    END IF;
    UPDATE __forest_module_schema__.pending_archive_events
    SET state='UNAVAILABLE',error_code=body->>'error_code',resolved_at=clock_timestamp()
    WHERE bot_id=requested_bot AND conversation_ref=body->>'conversation_ref'
      AND event_ref=body->>'event_ref' AND state='PENDING_CAPTURE';
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
        'protocol','forest.memory-source-unavailable/1','conversation_ref',body->>'conversation_ref',
        'event_ref',body->>'event_ref','error_code',body->>'error_code'));
END;
$$;

CREATE FUNCTION memory_qualify_input_batch(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    prior jsonb := envelope->'prior';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    selection_step text := body->>'selection_step_id';
    epoch_step text := body->>'deletion_epoch_step_id';
    selection jsonb;
    epoch_value jsonb;
    requested_batch text;
    requested_conversation text;
    requested_context_revision bigint;
    requested_deletion_epoch bigint;
    event_refs jsonb;
    event_ref_value text;
    occurrence_row record;
    unit_row record;
    pending_row record;
    unavailable_row record;
    replay_row record;
    item_operation_id text;
    item_digest text;
    qualified_count integer := 0;
    pending_count integer := 0;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' NOT IN ('agent.runtime','conversation.scheduler')
       OR body IS NULL OR prior IS NULL
       OR selection_step IS NULL OR epoch_step IS NULL
       OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MEMORY_INPUT_BATCH_INVALID',
                'message','Memory InputBatch envelope is invalid','status',422
            )
        );
    END IF;
    selection := prior->selection_step;
    epoch_value := prior->epoch_step;
    IF selection IS NULL OR epoch_value IS NULL
       OR epoch_value->>'protocol' IS DISTINCT FROM 'forest.payload-epoch/1'
       OR epoch_value->>'deletion_epoch' IS NULL
       OR epoch_value->>'deletion_epoch' !~ '^[0-9]+$' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MEMORY_INPUT_AUTHORITY_MISSING',
                'message','Conversation selection or Payload epoch is unavailable','status',503
            )
        );
    END IF;
    requested_deletion_epoch := (epoch_value->>'deletion_epoch')::bigint;
    IF selection->>'status' IS DISTINCT FROM 'BATCH' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'protocol','forest.memory-input-qualified/1',
                'input_batch_ref',NULL,
                'qualified_count',0,
                'pending_count',0,
                'deletion_epoch',requested_deletion_epoch
            )
        );
    END IF;
    requested_batch := selection->>'input_batch_id';
    requested_conversation := selection->>'conversation_id';
    event_refs := selection->'event_ids';
    IF requested_batch IS NULL OR requested_conversation IS NULL
       OR selection->>'context_revision' IS NULL
       OR selection->>'context_revision' !~ '^[0-9]+$'
       OR jsonb_typeof(event_refs) IS DISTINCT FROM 'array'
       OR jsonb_array_length(event_refs) > 100000 THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MEMORY_INPUT_BATCH_INVALID',
                'message','Conversation selection is not one bounded InputBatch','status',422
            )
        );
    END IF;
    requested_context_revision := (selection->>'context_revision')::bigint;
    IF requested_context_revision < 1 THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MEMORY_INPUT_BATCH_INVALID',
                'message','Conversation selection revision is invalid','status',422
            )
        );
    END IF;
    IF (SELECT count(*) FROM jsonb_array_elements_text(event_refs))
       <> (SELECT count(DISTINCT value) FROM jsonb_array_elements_text(event_refs)) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MEMORY_INPUT_BATCH_INVALID',
                'message','InputBatch event identities repeat','status',422
            )
        );
    END IF;
    -- Reserve the whole InputBatch in a stable order before touching rows.
    FOR event_ref_value IN
        SELECT value FROM jsonb_array_elements_text(event_refs) ORDER BY value
    LOOP
        PERFORM pg_advisory_xact_lock(hashtextextended(
            'forest.memory.graph/eligibility/' || requested_bot || '/' ||
            requested_conversation || '/' || event_ref_value,0
        ));
    END LOOP;
    FOR event_ref_value IN
        SELECT value FROM jsonb_array_elements_text(event_refs) ORDER BY value
    LOOP
        IF octet_length(event_ref_value) NOT BETWEEN 1 AND 256 THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','MEMORY_INPUT_BATCH_INVALID',
                    'message','InputBatch event identity is invalid','status',422
                )
            );
        END IF;
        SELECT * INTO occurrence_row
        FROM __forest_module_schema__.source_occurrences
        WHERE bot_id=requested_bot AND conversation_ref=requested_conversation
          AND event_ref=event_ref_value
        FOR UPDATE;
        IF occurrence_row IS NULL THEN
            item_digest := 'sha256:' || encode(sha256(convert_to(jsonb_build_array(
                requested_digest,requested_batch,requested_conversation,event_ref_value,
                requested_context_revision,requested_deletion_epoch
            )::text,'UTF8')),'hex');
            SELECT * INTO unavailable_row
            FROM __forest_module_schema__.unavailable_source_events
            WHERE bot_id=requested_bot AND conversation_ref=requested_conversation
              AND event_ref=event_ref_value;
            INSERT INTO __forest_module_schema__.pending_input_events(
                bot_id,conversation_ref,event_ref,operation_id,input_digest,
                input_batch_ref,context_revision,accepted_deletion_epoch,state,error_code,resolved_at
            ) VALUES (
                requested_bot,requested_conversation,event_ref_value,requested_operation,
                item_digest,requested_batch,requested_context_revision,
                requested_deletion_epoch,
                CASE WHEN unavailable_row IS NULL THEN 'PENDING_CAPTURE' ELSE 'UNAVAILABLE' END,
                unavailable_row.error_code,
                CASE WHEN unavailable_row IS NULL THEN NULL ELSE clock_timestamp() END
            ) ON CONFLICT (bot_id,conversation_ref,event_ref) DO NOTHING;
            SELECT * INTO pending_row
            FROM __forest_module_schema__.pending_input_events
            WHERE bot_id=requested_bot AND conversation_ref=requested_conversation
              AND event_ref=event_ref_value;
            IF pending_row.input_digest IS DISTINCT FROM item_digest THEN
                RETURN jsonb_build_object(
                    'protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object(
                        'code','MEMORY_ELIGIBILITY_CONFLICT',
                        'message','Pending Memory eligibility differs','status',409
                    )
                );
            END IF;
            IF pending_row.state='PENDING_CAPTURE' THEN
                pending_count := pending_count + 1;
            ELSE
                qualified_count := qualified_count + 1;
            END IF;
            CONTINUE;
        END IF;
        SELECT * INTO unit_row
        FROM __forest_module_schema__.source_units
        WHERE bot_id=requested_bot AND source_unit_id=occurrence_row.source_unit_id
        FOR UPDATE;
        IF unit_row IS NULL THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','MEMORY_SOURCE_NOT_FOUND',
                    'message','Memory source unit is unavailable','status',404
                )
            );
        END IF;
        IF unit_row.eligibility_state='SUPPRESSED'
           OR occurrence_row.eligibility_state='SUPPRESSED' THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','MEMORY_SOURCE_SUPPRESSED',
                    'message','Memory source unit was removed','status',409
                )
            );
        END IF;
        IF occurrence_row.eligibility_state IN ('ADMITTED','ELIGIBLE') AND (
            occurrence_row.input_batch_ref IS DISTINCT FROM requested_batch
            OR occurrence_row.context_revision IS DISTINCT FROM requested_context_revision
        ) THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','MEMORY_ELIGIBILITY_CONFLICT',
                    'message','Memory occurrence qualification differs','status',409
                )
            );
        END IF;
        item_operation_id := 'eligibility:' || encode(sha256(convert_to(jsonb_build_array(
            requested_bot,requested_operation,occurrence_row.occurrence_id
        )::text,'UTF8')),'hex');
        item_digest := 'sha256:' || encode(sha256(convert_to(jsonb_build_array(
            occurrence_row.source_unit_id,occurrence_row.occurrence_id,requested_batch,
            requested_context_revision,requested_deletion_epoch
        )::text,'UTF8')),'hex');
        SELECT * INTO replay_row
        FROM __forest_module_schema__.eligibility_operations
        WHERE bot_id=requested_bot AND operation_id=item_operation_id;
        IF replay_row IS NOT NULL AND replay_row.input_digest IS DISTINCT FROM item_digest THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','MEMORY_ELIGIBILITY_CONFLICT',
                    'message','Memory eligibility replay differs','status',409
                )
            );
        END IF;
        IF occurrence_row.eligibility_state='REGISTERED' THEN
            UPDATE __forest_module_schema__.source_occurrences
            SET eligibility_state='ADMITTED',input_batch_ref=requested_batch,
                context_revision=requested_context_revision,qualified_at=clock_timestamp()
            WHERE bot_id=requested_bot AND occurrence_id=occurrence_row.occurrence_id;
        END IF;
        IF replay_row IS NULL THEN
            INSERT INTO __forest_module_schema__.eligibility_operations(
                bot_id,operation_id,input_digest,source_unit_id,occurrence_id,
                input_batch_ref,context_revision,resulting_eligibility_revision
            ) VALUES (
                requested_bot,item_operation_id,item_digest,occurrence_row.source_unit_id,
                occurrence_row.occurrence_id,requested_batch,requested_context_revision,
                unit_row.eligibility_revision
            );
        END IF;
        qualified_count := qualified_count + 1;
    END LOOP;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.memory-input-qualified/1',
            'input_batch_ref',requested_batch,
            'qualified_count',qualified_count,
            'pending_count',pending_count,
            'deletion_epoch',requested_deletion_epoch
        )
    );
END;
$$;

CREATE FUNCTION memory_qualify_archive(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    prior jsonb := envelope->'prior';
    prepared jsonb := prior->'prepare';
    committed jsonb := prior->'commit';
    epoch_value jsonb := prior->'payload_epoch';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    archive_value jsonb;
    units jsonb;
    unit_value jsonb;
    expected_events jsonb;
    event_ref_value text;
    conversation_ref_value text;
    compaction_ref_value text;
    batch_ref_value text;
    context_revision_value bigint;
    deletion_epoch_value bigint;
    item_operation text;
    item_digest text;
    occurrence_row record;
    unit_row record;
    pending_row record;
    unavailable_row record;
    replay_row record;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'conversation.scheduler'
       OR requested_operation IS NULL OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR prepared IS NULL OR committed IS NULL
       OR epoch_value->>'protocol' IS DISTINCT FROM 'forest.payload-epoch/1'
       OR epoch_value->>'deletion_epoch' !~ '^[0-9]+$' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_ARCHIVE_INPUT_INVALID',
                'message','Memory archive authority is incomplete','status',422));
    END IF;
    IF prepared->>'status'='SKIP' THEN
        IF committed ? 'memory_archive' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_ARCHIVE_CONFLICT',
                    'message','Skipped compaction claimed Memory archive','status',409));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',committed);
    END IF;
    archive_value := committed->'memory_archive';
    units := prepared#>'{commit,units}';
    conversation_ref_value := prepared#>>'{commit,conversation_id}';
    compaction_ref_value := prepared#>>'{commit,compaction_id}';
    deletion_epoch_value := (epoch_value->>'deletion_epoch')::bigint;
    IF prepared->>'status' IS DISTINCT FROM 'WRITE'
       OR jsonb_typeof(units) IS DISTINCT FROM 'array'
       OR jsonb_typeof(archive_value) IS DISTINCT FROM 'object'
       OR archive_value->>'compaction_ref' IS DISTINCT FROM compaction_ref_value
       OR committed->>'conversation_id' IS DISTINCT FROM conversation_ref_value
       OR jsonb_array_length(units) IS DISTINCT FROM
          (prepared#>>'{commit,archived_unit_count}')::bigint THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_ARCHIVE_CONFLICT',
                'message','Conversation archive membership differs','status',409));
    END IF;
    SELECT COALESCE(jsonb_agg(item->>'event_id' ORDER BY ordinality)
        FILTER (WHERE item->>'event_id' IS NOT NULL),'[]'::jsonb)
    INTO expected_events
    FROM jsonb_array_elements(units) WITH ORDINALITY AS u(item,ordinality);
    IF archive_value->'event_refs' IS DISTINCT FROM expected_events THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_ARCHIVE_CONFLICT',
            'message','Conversation archive event set differs','status',409));
    END IF;
    -- Capture, InputBatch selection and archive may overlap. Acquire every
    -- event fence in the same order before locking any occurrence or unit row.
    FOR event_ref_value IN
        SELECT value FROM jsonb_array_elements_text(expected_events) ORDER BY value
    LOOP
        PERFORM pg_advisory_xact_lock(hashtextextended(
            'forest.memory.graph/eligibility/' || requested_bot || '/' ||
            conversation_ref_value || '/' || event_ref_value,0
        ));
    END LOOP;
    FOR unit_value IN SELECT value FROM jsonb_array_elements(units) LOOP
        event_ref_value := unit_value->>'event_id';
        IF event_ref_value IS NULL THEN CONTINUE; END IF;
        batch_ref_value := unit_value->>'input_batch_id';
        IF batch_ref_value IS NULL OR unit_value->>'context_revision' !~ '^[1-9][0-9]*$'
           OR octet_length(event_ref_value) NOT BETWEEN 1 AND 256 THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_ARCHIVE_INPUT_INVALID',
                    'message','Archived event has no stable InputBatch','status',422));
        END IF;
        context_revision_value := (unit_value->>'context_revision')::bigint;
        item_operation := 'archive:' || encode(sha256(convert_to(jsonb_build_array(
            requested_bot,compaction_ref_value,event_ref_value
        )::text,'UTF8')),'hex');
        item_digest := 'sha256:' || encode(sha256(convert_to(jsonb_build_array(
            requested_digest,compaction_ref_value,conversation_ref_value,event_ref_value,
            batch_ref_value,context_revision_value,deletion_epoch_value
        )::text,'UTF8')),'hex');
        SELECT * INTO replay_row FROM __forest_module_schema__.archive_qualification_operations
        WHERE bot_id=requested_bot AND compaction_ref=compaction_ref_value
          AND event_ref=event_ref_value;
        IF replay_row IS NOT NULL THEN
            IF replay_row.operation_id IS DISTINCT FROM item_operation
               OR replay_row.input_digest IS DISTINCT FROM item_digest THEN
                RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object('code','MEMORY_ARCHIVE_CONFLICT',
                        'message','Archive replay differs','status',409));
            END IF;
            CONTINUE;
        END IF;
        SELECT * INTO occurrence_row FROM __forest_module_schema__.source_occurrences
        WHERE bot_id=requested_bot AND conversation_ref=conversation_ref_value
          AND event_ref=event_ref_value FOR UPDATE;
        IF occurrence_row IS NULL THEN
            SELECT * INTO pending_row FROM __forest_module_schema__.pending_input_events
            WHERE bot_id=requested_bot AND conversation_ref=conversation_ref_value
              AND event_ref=event_ref_value FOR UPDATE;
            IF pending_row.event_ref IS NOT NULL AND (
                pending_row.input_batch_ref IS DISTINCT FROM batch_ref_value OR
                pending_row.context_revision IS DISTINCT FROM context_revision_value
            ) THEN
                RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object('code','MEMORY_ARCHIVE_CONFLICT',
                        'message','Pending input and archive disagree','status',409));
            END IF;
            SELECT * INTO unavailable_row FROM __forest_module_schema__.unavailable_source_events
            WHERE bot_id=requested_bot AND conversation_ref=conversation_ref_value
              AND event_ref=event_ref_value;
            INSERT INTO __forest_module_schema__.pending_archive_events(
                bot_id,conversation_ref,event_ref,operation_id,input_digest,
                compaction_ref,input_batch_ref,context_revision,accepted_deletion_epoch,
                state,error_code,resolved_at)
            VALUES(requested_bot,conversation_ref_value,event_ref_value,
                item_operation,item_digest,compaction_ref_value,batch_ref_value,
                context_revision_value,deletion_epoch_value,
                CASE WHEN unavailable_row IS NULL THEN 'PENDING_CAPTURE' ELSE 'UNAVAILABLE' END,
                unavailable_row.error_code,
                CASE WHEN unavailable_row IS NULL THEN NULL ELSE clock_timestamp() END)
            ON CONFLICT (bot_id,conversation_ref,event_ref) DO NOTHING;
            SELECT * INTO pending_row FROM __forest_module_schema__.pending_archive_events
            WHERE bot_id=requested_bot AND conversation_ref=conversation_ref_value
              AND event_ref=event_ref_value;
            IF pending_row.input_digest IS DISTINCT FROM item_digest
               OR pending_row.compaction_ref IS DISTINCT FROM compaction_ref_value THEN
                RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object('code','MEMORY_ARCHIVE_CONFLICT',
                        'message','Pending archive identity differs','status',409));
            END IF;
            CONTINUE;
        END IF;
        IF occurrence_row.eligibility_state='SUPPRESSED'
           OR (occurrence_row.input_batch_ref IS NOT NULL AND
               occurrence_row.input_batch_ref IS DISTINCT FROM batch_ref_value)
           OR (occurrence_row.context_revision IS NOT NULL AND
               occurrence_row.context_revision IS DISTINCT FROM context_revision_value) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_ARCHIVE_CONFLICT',
                    'message','Archived occurrence differs from Memory capture','status',409));
        END IF;
        UPDATE __forest_module_schema__.source_occurrences
        SET eligibility_state='ELIGIBLE',input_batch_ref=batch_ref_value,
            context_revision=context_revision_value,qualified_at=clock_timestamp()
        WHERE bot_id=requested_bot AND occurrence_id=occurrence_row.occurrence_id
          AND eligibility_state IN ('REGISTERED','ADMITTED');
        UPDATE __forest_module_schema__.source_units
        SET eligibility_state='ELIGIBLE',eligibility_revision=eligibility_revision+1,
            eligible_at=clock_timestamp(),updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND source_unit_id=occurrence_row.source_unit_id
          AND eligibility_state='REGISTERED';
        SELECT * INTO unit_row FROM __forest_module_schema__.source_units
        WHERE bot_id=requested_bot AND source_unit_id=occurrence_row.source_unit_id;
        IF unit_row IS NULL OR unit_row.eligibility_state<>'ELIGIBLE' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_ARCHIVE_CONFLICT',
                    'message','Archived source unit is unavailable','status',409));
        END IF;
        INSERT INTO __forest_module_schema__.archive_qualification_operations(
            bot_id,operation_id,input_digest,compaction_ref,conversation_ref,event_ref,
            source_unit_id,occurrence_id,resulting_eligibility_revision)
        VALUES(requested_bot,item_operation,item_digest,compaction_ref_value,
            conversation_ref_value,event_ref_value,occurrence_row.source_unit_id,
            occurrence_row.occurrence_id,unit_row.eligibility_revision);
        INSERT INTO __forest_module_schema__.facet_preparation_work(bot_id,source_unit_id)
        VALUES(requested_bot,occurrence_row.source_unit_id)
        ON CONFLICT (bot_id,source_unit_id) DO NOTHING;

        UPDATE __forest_module_schema__.association_journal
        SET promotion_ready=true
        WHERE bot_id=requested_bot AND trigger_source_unit_id=occurrence_row.source_unit_id
          AND state='PENDING_ARCHIVE' AND NOT promotion_ready;
    END LOOP;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',committed);
END;
$$;

CREATE FUNCTION memory_read_facet_source(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_source text := input->>'source_unit_id';
    after_index integer := (input->>'after_part_index')::integer;
    requested_limit integer := (input->>'limit')::integer;
    unit_row __forest_module_schema__.source_units%ROWTYPE;
    parts_value jsonb;
    last_index integer;
    has_more_value boolean := false;
BEGIN
    IF requested_source IS NULL OR octet_length(requested_source) NOT BETWEEN 1 AND 256
       OR after_index IS NULL OR after_index NOT BETWEEN -1 AND 4095
       OR requested_limit IS NULL OR requested_limit NOT BETWEEN 1 AND 64 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FACET_SOURCE_INPUT_INVALID',
                'message','Facet source page request is invalid','status',422));
    END IF;
    SELECT u.* INTO unit_row FROM __forest_module_schema__.source_units u
    JOIN __forest_module_schema__.memory_sources s
      ON s.bot_id=u.bot_id AND s.source_id=u.source_id AND s.status='ACTIVE'
    WHERE u.bot_id=requested_bot AND u.source_unit_id=requested_source
      AND u.eligibility_state='ELIGIBLE';
    IF unit_row IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('status','UNAVAILABLE','source_revision',NULL,
                'parts','[]'::jsonb,'next_part_index',NULL,'has_more',false));
    END IF;
    SELECT COALESCE(jsonb_agg(page.part ORDER BY page.part_index),'[]'::jsonb),
           max(page.part_index)
    INTO parts_value,last_index
    FROM (
        SELECT p.part_index,jsonb_build_object(
            'part_id',p.part_id,'part_index',p.part_index,'part_kind',p.part_kind,
            'media_type',p.media_type,'byte_length',p.byte_length,
            'payload',jsonb_build_object(
                'payload_id',p.payload_id,'digest',p.payload_digest,
                'owner_component_id',p.payload_owner_component_id,
                'owner_kind',p.payload_owner_kind,'owner_id',p.payload_owner_id
            )
        ) AS part
        FROM __forest_module_schema__.source_parts p
        WHERE p.bot_id=requested_bot AND p.source_unit_id=requested_source
          AND p.part_index>after_index
        ORDER BY p.part_index LIMIT requested_limit
    ) page;
    IF last_index IS NOT NULL THEN
        SELECT EXISTS(
            SELECT 1 FROM __forest_module_schema__.source_parts p
            WHERE p.bot_id=requested_bot AND p.source_unit_id=requested_source
              AND p.part_index>last_index
        ) INTO has_more_value;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('status','READY',
            'source_revision',unit_row.source_revision,
            'parts',parts_value,'next_part_index',last_index,
            'has_more',has_more_value));
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_FACET_SOURCE_INPUT_INVALID',
            'message','Facet source page request is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_claim_embedding_batch(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    prior_claim __forest_module_schema__.embedding_claims%ROWTYPE;
    domain_space text;
    domain_batch_limit integer;
    deletion_epoch_value bigint;
    selected_facets jsonb;
    selected_purpose text;
    claim_value jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_CLAIM_INVALID',
                'message','Embedding batch claim is invalid','status',422));
    END IF;
    -- The exclusion join cannot see a concurrent transaction's new claim.
    -- Serialize batch selection for this Bot before reading candidates.
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
        requested_bot||'/memory.embedding.claim',0));
    SELECT * INTO prior_claim FROM __forest_module_schema__.embedding_claims
    WHERE bot_id=requested_bot AND claim_id=requested_token;
    IF FOUND THEN
        IF prior_claim.lease_seconds IS DISTINCT FROM requested_lease THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_EMBEDDING_CLAIM_CONFLICT',
                    'message','Embedding claim token was reused differently','status',409));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',prior_claim.claim_payload);
    END IF;
    SELECT space_id,max_batch_items INTO domain_space,domain_batch_limit
    FROM __forest_module_schema__.memory_domains
    WHERE bot_id=requested_bot;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    WITH expired AS MATERIALIZED (
        SELECT c.bot_id,c.claim_id
        FROM __forest_module_schema__.embedding_claims c
        WHERE c.bot_id=requested_bot AND c.state='CLAIMED'
          AND c.lease_deadline<=clock_timestamp()
        ORDER BY c.lease_deadline,c.claim_id
        FOR UPDATE OF c SKIP LOCKED LIMIT 32
    )
    UPDATE __forest_module_schema__.embedding_claims c
    SET state='EXPIRED'
    FROM expired e
    WHERE c.bot_id=e.bot_id AND c.claim_id=e.claim_id;
    WITH candidates AS MATERIALIZED (
        SELECT f.*,source_data.trunk_id,source_data.author_subject_ref,
               source_data.conversation_ref,
               coalesce(source_data.width,trial_data.width) AS width,
               coalesce(source_data.height,trial_data.height) AS height,
               coalesce(source_data.payload_owner_component_id,
                        trial_data.payload_owner_component_id) AS payload_owner_component_id,
               coalesce(source_data.payload_owner_kind,
                        trial_data.payload_owner_kind) AS payload_owner_kind,
               coalesce(source_data.payload_owner_id,
                        trial_data.payload_owner_id) AS payload_owner_id
        FROM __forest_module_schema__.embedding_facets f
        LEFT JOIN LATERAL (
            SELECT u.trunk_id,u.author_subject_ref,p.width,p.height,
                   p.payload_owner_component_id,p.payload_owner_kind,p.payload_owner_id,
                   o.conversation_ref
            FROM __forest_module_schema__.source_units u
            JOIN __forest_module_schema__.memory_sources s
              ON s.bot_id=u.bot_id AND s.source_id=u.source_id AND s.status='ACTIVE'
            JOIN __forest_module_schema__.source_parts p
              ON p.bot_id=u.bot_id AND p.source_unit_id=u.source_unit_id
             AND p.part_id=f.part_id AND p.payload_id=f.input_payload_id
             AND p.payload_digest=f.input_payload_digest
            JOIN LATERAL (
                SELECT occurrence.conversation_ref
                FROM __forest_module_schema__.source_occurrences occurrence
                WHERE occurrence.bot_id=f.bot_id
                  AND occurrence.source_unit_id=f.source_unit_id
                  AND ((f.purpose='DOCUMENT' AND occurrence.eligibility_state='ELIGIBLE')
                       OR (f.purpose='QUERY' AND
                           __forest_module_schema__.memory_query_occurrence_matches(
                               f.bot_id,f.query_id,f.source_unit_id,f.source_revision,
                               f.part_id,occurrence.occurrence_id)))
                ORDER BY occurrence.observed_at,occurrence.occurrence_id LIMIT 1
            ) o ON true
            WHERE f.input_kind='SOURCE_UNIT'
              AND u.bot_id=f.bot_id AND u.source_unit_id=f.source_unit_id
              AND u.source_revision=f.source_revision
              AND (u.eligibility_state='ELIGIBLE'
                   OR (f.purpose='QUERY' AND u.eligibility_state='REGISTERED'))
        ) source_data ON true
        LEFT JOIN LATERAL (
            SELECT p.width,p.height,p.payload_owner_component_id,
                   p.payload_owner_kind,p.payload_owner_id
            FROM __forest_module_schema__.memory_trial_inputs t
            JOIN __forest_module_schema__.memory_queries q
              ON q.bot_id=t.bot_id AND q.query_id=t.query_id
             AND q.input_kind='ADMIN_TRIAL' AND q.state='OPEN'
            JOIN __forest_module_schema__.memory_trial_parts p
              ON p.bot_id=t.bot_id AND p.query_id=t.query_id
             AND p.part_id=f.part_id AND p.modality=f.modality
             AND p.payload_id=f.input_payload_id
             AND p.payload_digest=f.input_payload_digest
            WHERE f.input_kind='ADMIN_TRIAL' AND f.purpose='QUERY'
              AND t.bot_id=f.bot_id AND t.query_id=f.trial_query_id
              AND t.query_id=f.query_id AND t.input_revision=f.input_revision
              AND t.generation_id=context->>'generation_id'
              AND t.expires_at>clock_timestamp()
              AND t.cancelled_at IS NULL
        ) trial_data ON true
        WHERE f.bot_id=requested_bot
          AND f.state='PENDING' AND f.space_id=domain_space
          AND f.next_attempt_at<=clock_timestamp()
          AND (source_data.conversation_ref IS NOT NULL
               OR trial_data.payload_owner_id IS NOT NULL)
          AND NOT EXISTS (
              SELECT 1 FROM __forest_module_schema__.embedding_claim_items ci
              JOIN __forest_module_schema__.embedding_claims c
                ON c.bot_id=ci.bot_id AND c.claim_id=ci.claim_id
              WHERE ci.bot_id=f.bot_id AND ci.facet_id=f.facet_id
                AND c.state IN ('CLAIMED','ATTEMPT_BOUND')
                AND c.lease_deadline>clock_timestamp()
          )
        ORDER BY CASE f.purpose WHEN 'QUERY' THEN 0 ELSE 1 END,
                 f.created_at,f.facet_id LIMIT domain_batch_limit
        FOR UPDATE OF f SKIP LOCKED
    ), focus AS (
        SELECT purpose,query_id FROM candidates
        ORDER BY CASE purpose WHEN 'QUERY' THEN 0 ELSE 1 END,created_at,facet_id LIMIT 1
    ), ranked AS (
        SELECT candidates.*,
               -- Materialization bounds every image rendition to 4 MiB before
               -- building the wire request. Budget the bounded wire input,
               -- not the potentially 32 MiB archived original.
               sum(CASE WHEN modality='IMAGE'
                        THEN least(input_byte_length,4194304) ELSE 0 END)
                   OVER (ORDER BY candidates.created_at,candidates.facet_id) AS image_bytes
        FROM candidates JOIN focus ON candidates.purpose=focus.purpose
          AND candidates.query_id IS NOT DISTINCT FROM focus.query_id
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'facet_id',facet_id,'part_id',part_id,'modality',modality,
        'selector_start_char',selector_start_char,
        'selector_end_char',selector_end_char,
        'input_payload',jsonb_build_object(
            'payload_id',input_payload_id,'digest',input_payload_digest,
            'owner_component_id',payload_owner_component_id,
            'owner_kind',payload_owner_kind,'owner_id',payload_owner_id),
        'facet_content_digest',facet_content_digest,
        'input_byte_length',input_byte_length,'media_type',input_media_type,
        'width',width,'height',height
    ) || CASE input_kind WHEN 'ADMIN_TRIAL' THEN jsonb_build_object(
        'input_kind','ADMIN_TRIAL','input_ref',input_ref,
        'input_revision',input_revision,'query_id',query_id)
      ELSE jsonb_build_object(
        'source_unit_id',source_unit_id,'source_revision',source_revision,
        'trunk_id',trunk_id,'conversation_ref',conversation_ref,
        'author_ref',author_subject_ref) END
      ORDER BY created_at,facet_id),'[]'::jsonb),
        max(accepted_deletion_epoch),min(purpose)
    INTO selected_facets,deletion_epoch_value,selected_purpose
    FROM ranked WHERE image_bytes<=8388608;
    IF jsonb_array_length(selected_facets)=0 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    claim_value := jsonb_build_object(
        'protocol','forest.owned-work-claim/1',
        'work_id',requested_token,'claim_token',requested_token,'attempt',1,
        'operation_id','memory-embedding-work:'||encode(sha256(convert_to(
            requested_bot||'/'||requested_token,'UTF8')),'hex'),
        'causal_root_kind','memory.embedding.batch','causal_root_id',requested_token,
        'payload',jsonb_build_object(
            'claim_id',requested_token,'space_id',domain_space,'purpose',selected_purpose,
            'source_epoch_high_watermark',deletion_epoch_value,
            'facets',selected_facets));
    INSERT INTO __forest_module_schema__.embedding_claims(
        bot_id,claim_id,purpose,space_id,source_epoch_high_watermark,
        lease_seconds,lease_deadline,item_limit,item_count,claim_payload)
    VALUES(requested_bot,requested_token,selected_purpose,domain_space,deletion_epoch_value,
        requested_lease,clock_timestamp()+(requested_lease*interval '1 second'),
        domain_batch_limit,jsonb_array_length(selected_facets),claim_value);
    INSERT INTO __forest_module_schema__.embedding_claim_items(
        bot_id,claim_id,item_index,facet_id)
    SELECT requested_bot,requested_token,ordinality-1,facet->>'facet_id'
    FROM jsonb_array_elements(selected_facets) WITH ORDINALITY AS item(facet,ordinality);
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',claim_value);
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_EMBEDDING_CLAIM_INVALID',
            'message','Embedding batch claim is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_claim_active_embedding_batch(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
BEGIN
    -- A stored domain alone is insufficient: only a successful provider-backed
    -- domain.bind owned task activates autonomous embedding dispatch.
    IF NOT EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_domain_bind_work
        WHERE bot_id=context->>'bot_id' AND state='READY'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',NULL);
    END IF;
    RETURN __forest_module_schema__.memory_claim_embedding_batch(context,input);
END;
$$;

CREATE FUNCTION memory_claim_embedding_recovery(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    work_row __forest_module_schema__.embedding_recovery_work%ROWTYPE;
    attempt_row __forest_module_schema__.embedding_batch_attempts%ROWTYPE;
    task_payload jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_RECOVERY_CLAIM_INVALID',
                'message','Embedding recovery claim is invalid','status',422));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.embedding_recovery_work
    WHERE bot_id=requested_bot AND claim_token=requested_token;
    IF FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',work_row.claim_payload);
    END IF;
    INSERT INTO __forest_module_schema__.embedding_recovery_work(bot_id,claim_id)
    SELECT a.bot_id,a.claim_id
    FROM __forest_module_schema__.embedding_batch_attempts a
    JOIN __forest_module_schema__.embedding_claims c
      ON c.bot_id=a.bot_id AND c.claim_id=a.claim_id
    WHERE a.bot_id=requested_bot AND a.state IN ('PREPARED','DISPATCHED')
      AND c.state='ATTEMPT_BOUND' AND c.lease_deadline<=clock_timestamp()
    ORDER BY c.lease_deadline,a.attempt_id LIMIT 32
    ON CONFLICT (bot_id,claim_id) DO NOTHING;
    SELECT w.* INTO work_row
    FROM __forest_module_schema__.embedding_recovery_work w
    JOIN __forest_module_schema__.embedding_batch_attempts a
      ON a.bot_id=w.bot_id AND a.claim_id=w.claim_id
    WHERE w.bot_id=requested_bot AND a.state IN ('PREPARED','DISPATCHED')
      AND w.next_attempt_at<=clock_timestamp()
      AND (w.state='PENDING'
           OR (w.state='CLAIMED' AND w.lease_deadline<=clock_timestamp()))
    ORDER BY w.next_attempt_at,w.claim_id
    LIMIT 1 FOR UPDATE OF w SKIP LOCKED;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',NULL);
    END IF;
    SELECT * INTO attempt_row FROM __forest_module_schema__.embedding_batch_attempts
    WHERE bot_id=requested_bot AND claim_id=work_row.claim_id FOR UPDATE;
    IF attempt_row.state NOT IN ('PREPARED','DISPATCHED') THEN
        UPDATE __forest_module_schema__.embedding_recovery_work
        SET state='SETTLED',claim_token=NULL,claim_payload=NULL,lease_deadline=NULL,
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND claim_id=work_row.claim_id;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',NULL);
    END IF;
    task_payload := jsonb_build_object(
        'protocol','forest.owned-work-claim/1',
        'work_id',work_row.claim_id,'claim_token',requested_token,
        'attempt',work_row.attempt_count+1,
        'operation_id','memory-embedding-recovery:'||encode(sha256(convert_to(
            requested_bot||'/'||requested_token,'UTF8')),'hex'),
        'causal_root_kind','memory.embedding.batch',
        'causal_root_id',work_row.claim_id,
        'payload',jsonb_build_object(
            'mode',CASE attempt_row.state WHEN 'PREPARED' THEN 'ABANDON'
                ELSE 'OBSERVE_UNKNOWN' END,
            'claim_id',work_row.claim_id,
            'attempt_id',attempt_row.attempt_id,
            'effect_operation_id',attempt_row.effect_operation_id,
            'reservation_id',attempt_row.reservation_id));
    UPDATE __forest_module_schema__.embedding_recovery_work
    SET state='CLAIMED',claim_token=requested_token,claim_payload=task_payload,
        lease_deadline=clock_timestamp()+(requested_lease*interval '1 second'),
        attempt_count=attempt_count+1,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND claim_id=work_row.claim_id;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',task_payload);
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_EMBEDDING_RECOVERY_CLAIM_INVALID',
            'message','Embedding recovery claim is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_reject_embedding_facet(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    requested_bot text := context->>'bot_id';
    requested_claim text := body->>'claim_id';
    requested_facet text := body->>'facet_id';
    claim_row __forest_module_schema__.embedding_claims%ROWTYPE;
    facet_row __forest_module_schema__.embedding_facets%ROWTYPE;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR envelope#>>'{caller,generation_id}' IS DISTINCT FROM context->>'generation_id'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-embedding-facet-reject/1'
       OR body->>'reason_code' IS DISTINCT FROM 'EMBEDDING_FACET_INVALID'
       OR requested_bot IS NULL OR requested_claim IS NULL OR requested_facet IS NULL
       OR body->>'facet_content_digest' !~ '^sha256:[a-f0-9]{64}$'
       OR body->>'space_id' !~ '^sha256:[a-f0-9]{64}$' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_FACET_REJECTION_INVALID',
                'message','Embedding facet rejection is invalid','status',422));
    END IF;
    SELECT * INTO claim_row FROM __forest_module_schema__.embedding_claims
    WHERE bot_id=requested_bot AND claim_id=requested_claim FOR UPDATE;
    IF claim_row IS NULL OR claim_row.state<>'CLAIMED'
       OR claim_row.lease_deadline<=clock_timestamp()
       OR claim_row.space_id IS DISTINCT FROM body->>'space_id'
       OR EXISTS (SELECT 1 FROM __forest_module_schema__.embedding_batch_attempts
                  WHERE bot_id=requested_bot AND claim_id=requested_claim) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_CLAIM_LOST',
                'message','Embedding claim is no longer rejectable','status',409));
    END IF;
    SELECT f.* INTO facet_row FROM __forest_module_schema__.embedding_facets f
    JOIN __forest_module_schema__.embedding_claim_items ci
      ON ci.bot_id=f.bot_id AND ci.facet_id=f.facet_id
    WHERE ci.bot_id=requested_bot AND ci.claim_id=requested_claim
      AND ci.facet_id=requested_facet FOR UPDATE OF f;
    IF facet_row IS NULL OR facet_row.state<>'PENDING'
       OR facet_row.space_id IS DISTINCT FROM claim_row.space_id
       OR facet_row.facet_content_digest IS DISTINCT FROM body->>'facet_content_digest' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_FACET_CHANGED',
                'message','Embedding facet changed before rejection','status',409));
    END IF;
    UPDATE __forest_module_schema__.embedding_facets
    SET state='FAILED',failure_code=body->>'reason_code',updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND facet_id=requested_facet;
    UPDATE __forest_module_schema__.embedding_claims
    SET state='EXPIRED' WHERE bot_id=requested_bot AND claim_id=requested_claim;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-embedding-facet-rejected/1',
            'claim_id',requested_claim,'facet_id',requested_facet,'state','FAILED'));
END;
$$;

CREATE FUNCTION memory_settle_embedding_work(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    prior_input jsonb;
    prior_result jsonb;
    result_value jsonb;
    recovery_row __forest_module_schema__.embedding_recovery_work%ROWTYPE;
    claim_row __forest_module_schema__.embedding_claims%ROWTYPE;
    attempt_row __forest_module_schema__.embedding_batch_attempts%ROWTYPE;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR input->>'work_id' IS NULL
       OR input->>'attempt' !~ '^[1-9][0-9]*$'
       OR input#>>'{outcome,status}' IS NULL
       OR input#>>'{outcome,status}' NOT IN ('SUCCEEDED','FAILED') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_SETTLEMENT_INVALID',
                'message','Embedding work settlement is invalid','status',422));
    END IF;
    SELECT input_payload,result_payload INTO prior_input,prior_result
    FROM __forest_module_schema__.embedding_work_settlements
    WHERE bot_id=requested_bot AND claim_token=requested_token;
    IF FOUND THEN
        IF prior_input=input THEN RETURN prior_result; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_SETTLEMENT_CONFLICT',
                'message','Embedding work was settled differently','status',409));
    END IF;
    SELECT * INTO recovery_row FROM __forest_module_schema__.embedding_recovery_work
    WHERE bot_id=requested_bot AND claim_token=requested_token FOR UPDATE;
    IF FOUND THEN
        IF recovery_row.state<>'CLAIMED'
           OR input->>'work_id' IS DISTINCT FROM recovery_row.claim_id
           OR (input->>'attempt')::integer IS DISTINCT FROM recovery_row.attempt_count THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_EMBEDDING_RECOVERY_CLAIM_LOST',
                    'message','Embedding recovery claim is no longer current','status',409));
        END IF;
        SELECT * INTO attempt_row FROM __forest_module_schema__.embedding_batch_attempts
        WHERE bot_id=requested_bot AND claim_id=recovery_row.claim_id FOR UPDATE;
        IF attempt_row.state IN ('PREPARED','DISPATCHED') THEN
            UPDATE __forest_module_schema__.embedding_recovery_work
            SET state='PENDING',claim_token=NULL,claim_payload=NULL,lease_deadline=NULL,
                next_attempt_at=clock_timestamp()+
                    (LEAST(power(2,LEAST(attempt_count,8)),300)*interval '1 second'),
                updated_at=clock_timestamp()
            WHERE bot_id=requested_bot AND claim_id=recovery_row.claim_id;
        ELSE
            UPDATE __forest_module_schema__.embedding_recovery_work
            SET state='SETTLED',claim_token=NULL,claim_payload=NULL,lease_deadline=NULL,
                updated_at=clock_timestamp()
            WHERE bot_id=requested_bot AND claim_id=recovery_row.claim_id;
        END IF;
    ELSE
        SELECT * INTO claim_row FROM __forest_module_schema__.embedding_claims
        WHERE bot_id=requested_bot AND claim_id=requested_token FOR UPDATE;
        IF claim_row IS NULL OR input->>'work_id' IS DISTINCT FROM requested_token
           OR input->>'attempt' IS DISTINCT FROM '1' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_EMBEDDING_CLAIM_LOST',
                    'message','Embedding batch claim is no longer current','status',409));
        END IF;
        SELECT * INTO attempt_row FROM __forest_module_schema__.embedding_batch_attempts
        WHERE bot_id=requested_bot AND claim_id=requested_token FOR UPDATE;
        IF attempt_row IS NULL AND claim_row.state='CLAIMED' THEN
            UPDATE __forest_module_schema__.embedding_claims
            SET state='EXPIRED' WHERE bot_id=requested_bot AND claim_id=requested_token;
            UPDATE __forest_module_schema__.embedding_facets f
            SET next_attempt_at=CASE
                    WHEN input#>>'{outcome,error,code}'='MEMORY_EMBEDDING_BUDGET_EXHAUSTED'
                    THEN (date_trunc('day',clock_timestamp() AT TIME ZONE 'UTC')
                          +interval '1 day') AT TIME ZONE 'UTC'
                    ELSE clock_timestamp()+interval '1 minute' END,
                updated_at=clock_timestamp()
            FROM __forest_module_schema__.embedding_claim_items ci
            WHERE ci.bot_id=requested_bot AND ci.claim_id=requested_token
              AND f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
              AND f.state='PENDING';
        END IF;
    END IF;
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.owned-work-settled/1',
            'work_id',input->>'work_id'));
    INSERT INTO __forest_module_schema__.embedding_work_settlements(
        bot_id,claim_token,input_payload,result_payload)
    VALUES(requested_bot,requested_token,input,result_value);
    RETURN result_value;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_EMBEDDING_SETTLEMENT_INVALID',
            'message','Embedding work settlement is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_settle_embedding_batch(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE sql VOLATILE SET search_path = pg_catalog AS $$
    SELECT __forest_module_schema__.memory_settle_embedding_work(context,input)
$$;

CREATE FUNCTION memory_settle_embedding_recovery(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE sql VOLATILE SET search_path = pg_catalog AS $$
    SELECT __forest_module_schema__.memory_settle_embedding_work(context,input)
$$;

CREATE FUNCTION memory_read_embedding_candidates(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_limit integer := (input->>'limit')::integer;
    domain_space text;
    candidates jsonb;
    candidate_count integer;
BEGIN
    IF requested_bot IS NULL OR requested_limit IS NULL
       OR requested_limit NOT BETWEEN 1 AND 32 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_CANDIDATES_INVALID',
                'message','Embedding candidate limit is invalid','status',422));
    END IF;
    SELECT space_id INTO domain_space FROM __forest_module_schema__.memory_domains
    WHERE bot_id=requested_bot;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('status','UNBOUND','space_id',NULL,
                'facets','[]'::jsonb,'has_more',false));
    END IF;
    WITH selected AS (
        SELECT f.*,u.trunk_id,u.author_subject_ref,p.width,p.height,
               p.payload_owner_component_id,p.payload_owner_kind,p.payload_owner_id,
               o.conversation_ref
        FROM __forest_module_schema__.embedding_facets f
        JOIN __forest_module_schema__.source_units u
          ON u.bot_id=f.bot_id AND u.source_unit_id=f.source_unit_id
         AND u.source_revision=f.source_revision AND u.eligibility_state='ELIGIBLE'
        JOIN __forest_module_schema__.memory_sources s
          ON s.bot_id=u.bot_id AND s.source_id=u.source_id AND s.status='ACTIVE'
        JOIN __forest_module_schema__.source_parts p
          ON p.bot_id=f.bot_id AND p.source_unit_id=f.source_unit_id
         AND p.part_id=f.part_id AND p.payload_id=f.input_payload_id
         AND p.payload_digest=f.input_payload_digest
        JOIN LATERAL (
            SELECT occurrence.conversation_ref
            FROM __forest_module_schema__.source_occurrences occurrence
            WHERE occurrence.bot_id=f.bot_id
              AND occurrence.source_unit_id=f.source_unit_id
              AND occurrence.eligibility_state='ELIGIBLE'
            ORDER BY occurrence.observed_at,occurrence.occurrence_id LIMIT 1
        ) o ON true
        WHERE f.bot_id=requested_bot AND f.purpose='DOCUMENT'
          AND f.state='PENDING' AND f.space_id=domain_space
        ORDER BY f.created_at,f.facet_id LIMIT requested_limit+1
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'facet_id',facet_id,'source_unit_id',source_unit_id,
        'source_revision',source_revision,'part_id',part_id,
        'trunk_id',trunk_id,'conversation_ref',conversation_ref,
        'author_ref',author_subject_ref,'modality',modality,
        'selector_start_char',selector_start_char,
        'selector_end_char',selector_end_char,
        'input_payload',jsonb_build_object(
            'payload_id',input_payload_id,'digest',input_payload_digest,
            'owner_component_id',payload_owner_component_id,
            'owner_kind',payload_owner_kind,'owner_id',payload_owner_id),
        'facet_content_digest',facet_content_digest,
        'input_byte_length',input_byte_length,'media_type',input_media_type,
        'width',width,'height',height
    ) ORDER BY created_at,facet_id)
        FILTER (WHERE ordinal <= requested_limit),'[]'::jsonb),count(*)
    INTO candidates,candidate_count
    FROM (
        SELECT selected.*,row_number() OVER (ORDER BY created_at,facet_id) AS ordinal
        FROM selected
    ) page;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('status','READY','space_id',domain_space,
            'facets',candidates,'has_more',candidate_count>requested_limit));
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_EMBEDDING_CANDIDATES_INVALID',
            'message','Embedding candidate limit is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_read_exact_sources(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog
AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_ids jsonb := input->'source_unit_ids';
    evidence_value jsonb;
    missing_value jsonb;
BEGIN
    IF jsonb_typeof(requested_ids) IS DISTINCT FROM 'array'
       OR jsonb_array_length(requested_ids) NOT BETWEEN 1 AND 8
       OR (SELECT count(*)<>count(DISTINCT value)
           FROM jsonb_array_elements_text(requested_ids)) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MEMORY_EXACT_RECALL_INPUT_INVALID',
                'message','Exact recall requires one to eight unique source unit IDs',
                'status',422
            )
        );
    END IF;
    WITH requested AS (
        SELECT value AS source_unit_id,ordinality
        FROM jsonb_array_elements_text(requested_ids) WITH ORDINALITY
    ), found AS (
        SELECT r.ordinality,jsonb_build_object(
            'source_unit_id',u.source_unit_id,
            'source_revision',u.source_revision,
            'trunk_id',u.trunk_id,
            'source_id',u.source_id,
            'source_kind',s.source_kind,
            'platform',s.platform,
            'realm_id',s.realm_id,
            'external_group_id',s.external_group_id,
            'author_subject_ref',u.author_subject_ref,
            'occurred_at',u.occurred_at,
            'parts',COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                    'part_id',p.part_id,
                    'part_index',p.part_index,
                    'part_kind',p.part_kind,
                    'media_type',p.media_type,
                    'payload',jsonb_build_object(
                        'payload_id',p.payload_id,
                        'digest',p.payload_digest,
                        'owner_component_id',p.payload_owner_component_id,
                        'owner_kind',p.payload_owner_kind,
                        'owner_id',p.payload_owner_id
                    ),
                    'width',p.width,
                    'height',p.height
                ) ORDER BY p.part_index,p.part_id)
                FROM __forest_module_schema__.source_parts p
                WHERE p.bot_id=requested_bot AND p.source_unit_id=u.source_unit_id
            ),'[]'::jsonb)
        ) AS document
        FROM requested r
        JOIN __forest_module_schema__.source_units u
          ON u.bot_id=requested_bot AND u.source_unit_id=r.source_unit_id
             AND u.eligibility_state='ELIGIBLE'
        JOIN __forest_module_schema__.memory_sources s
          ON s.bot_id=u.bot_id AND s.source_id=u.source_id AND s.status='ACTIVE'
        WHERE (SELECT count(*) FROM __forest_module_schema__.source_parts p
               WHERE p.bot_id=requested_bot AND p.source_unit_id=u.source_unit_id)
              = u.part_count
    )
    SELECT COALESCE(jsonb_agg(document ORDER BY ordinality),'[]'::jsonb)
    INTO evidence_value FROM found;
    WITH requested AS (
        SELECT value AS source_unit_id,ordinality
        FROM jsonb_array_elements_text(requested_ids) WITH ORDINALITY
    )
    SELECT COALESCE(jsonb_agg(r.source_unit_id ORDER BY r.ordinality)
        FILTER (WHERE u.source_unit_id IS NULL),'[]'::jsonb)
    INTO missing_value
    FROM requested r
    LEFT JOIN (
        SELECT u.source_unit_id
        FROM __forest_module_schema__.source_units u
        JOIN __forest_module_schema__.memory_sources s
          ON s.bot_id=u.bot_id AND s.source_id=u.source_id AND s.status='ACTIVE'
        WHERE u.bot_id=requested_bot AND u.eligibility_state='ELIGIBLE'
          AND (SELECT count(*) FROM __forest_module_schema__.source_parts p
               WHERE p.bot_id=requested_bot AND p.source_unit_id=u.source_unit_id)
              = u.part_count
    ) u ON u.source_unit_id=r.source_unit_id;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'evidence',evidence_value,
            'missing_source_unit_ids',missing_value
        )
    );
END;
$$;

CREATE FUNCTION memory_list_trunks(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_limit integer := COALESCE((input->>'limit')::integer,50);
    result jsonb;
    more boolean;
    next_after text;
BEGIN
    -- Live keyset browsing: trunk identities are immutable. A fresh first page
    -- observes new trunks; an existing cursor never duplicates a prior row.
    WITH page AS MATERIALIZED (
        SELECT t.trunk_id,t.source_id,s.platform,s.realm_id,s.external_group_id,
            s.status
        FROM __forest_module_schema__.memory_trunks t
        JOIN __forest_module_schema__.memory_sources s
            ON s.bot_id=t.bot_id AND s.source_id=t.source_id
        WHERE t.bot_id=context->>'bot_id'
            AND (input->>'after_trunk_id' IS NULL
                OR t.trunk_id > input->>'after_trunk_id')
            AND (input->>'external_group_id' IS NULL
                OR s.external_group_id=input->>'external_group_id')
        ORDER BY t.trunk_id
        LIMIT requested_limit+1
    ), selected AS (
        SELECT * FROM page ORDER BY trunk_id LIMIT requested_limit
    ), counted AS (
        SELECT p.trunk_id,p.source_id,p.platform,p.realm_id,p.external_group_id,
            counts.registered_sources,
            CASE WHEN p.status='ACTIVE' THEN counts.eligible_sources ELSE 0 END
                AS eligible_sources
        FROM selected p
        CROSS JOIN LATERAL (
            SELECT count(*) AS registered_sources,
                count(*) FILTER (WHERE u.eligibility_state='ELIGIBLE')
                    AS eligible_sources
            FROM __forest_module_schema__.source_units u
            WHERE u.bot_id=context->>'bot_id' AND u.trunk_id=p.trunk_id
        ) counts
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
            'trunk_id',q.trunk_id,'source_id',q.source_id,
            'platform',q.platform,'realm_id',q.realm_id,
            'external_group_id',q.external_group_id,
            'registered_sources',q.registered_sources,
            'eligible_sources',q.eligible_sources
        ) ORDER BY q.trunk_id),'[]'::jsonb),
        (SELECT count(*) > requested_limit FROM page),max(q.trunk_id)
    INTO result,more,next_after
    FROM counted q;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('items',result,'has_more',more,
            'next_after',CASE WHEN more THEN next_after ELSE NULL END));
END;
$$;

CREATE FUNCTION memory_filtered_source_units(
    requested_bot text, filters jsonb, snapshot_time timestamptz,
    snapshot_value pg_snapshot)
RETURNS TABLE (
    source_unit_id text, source_id text, trunk_id text, source_kind text,
    platform text, realm_id text, external_group_id text,
    author_subject_ref text, unit_kind text, modality text,
    occurred_at timestamptz, source_order_seq bigint, part_count integer,
    eligibility_state text, source_status text, source_revision bigint,
    author_payload_id text, document_facet_count integer,
    ready_document_facet_count integer, failed_document_facet_count integer,
    unknown_document_facet_count integer,
    index_coverage text)
-- Keep this function inlineable: source pagination must push ORDER BY/LIMIT
-- into the indexed source scan before calculating facet coverage.
-- All relation names are schema-qualified; builtins below use pg_catalog.
LANGUAGE sql STABLE AS $$
    SELECT u.source_unit_id,u.source_id,u.trunk_id,s.source_kind,
        s.platform,s.realm_id,s.external_group_id,u.author_subject_ref,
        u.unit_kind,u.modality,u.occurred_at,u.source_order_seq,u.part_count,
        u.eligibility_state,s.status,u.source_revision,u.author_payload_id,
        coverage.facet_count,coverage.ready_count,coverage.failed_count,
        coverage.unknown_count,
        coverage.index_coverage
    FROM __forest_module_schema__.source_units u
    JOIN __forest_module_schema__.memory_sources s
      ON s.bot_id=u.bot_id AND s.source_id=u.source_id
    CROSS JOIN LATERAL (
        SELECT pg_catalog.count(*)::integer AS facet_count,
               pg_catalog.count(*) FILTER (WHERE f.state='READY')::integer AS ready_count,
               pg_catalog.count(*) FILTER (WHERE f.state='FAILED')::integer AS failed_count,
               pg_catalog.count(*) FILTER (WHERE f.state='UNKNOWN')::integer AS unknown_count,
               CASE WHEN pg_catalog.count(*)=0 THEN 'NO_FACETS'
                    WHEN pg_catalog.count(*) FILTER (WHERE f.state='READY')=pg_catalog.count(*)
                        THEN 'COMPLETE'
                    WHEN pg_catalog.count(*) FILTER (WHERE f.state='READY')=0
                        THEN 'NOT_READY'
                    ELSE 'PARTIAL' END AS index_coverage
        FROM __forest_module_schema__.embedding_facets f
        WHERE f.bot_id=u.bot_id AND f.source_unit_id=u.source_unit_id
          AND f.source_revision=u.source_revision AND f.purpose='DOCUMENT'
    ) coverage
    WHERE u.bot_id=requested_bot
      AND u.eligibility_state=COALESCE(filters->>'eligibility_state','ELIGIBLE')
      AND s.status=COALESCE(filters->>'source_status','ACTIVE')
      AND u.accepted_at<=snapshot_time
      AND pg_catalog.pg_visible_in_snapshot(u.accepted_xid,snapshot_value)
      AND (u.eligible_at IS NULL OR u.eligible_at<=snapshot_time)
      AND (COALESCE(filters->>'eligibility_state','ELIGIBLE')<>'ELIGIBLE' OR
           (u.eligible_xid IS NOT NULL AND
            pg_catalog.pg_visible_in_snapshot(u.eligible_xid,snapshot_value)))
      AND (filters->>'trunk_id' IS NULL OR u.trunk_id=filters->>'trunk_id')
      AND (filters->>'source_unit_id' IS NULL OR
           u.source_unit_id=filters->>'source_unit_id')
      AND (filters->>'source_id' IS NULL OR u.source_id=filters->>'source_id')
      AND (NOT filters ? 'group_source_ids' OR
           (s.source_kind='GROUP' AND u.source_id IN (
              SELECT value FROM pg_catalog.jsonb_array_elements_text(filters->'group_source_ids'))))
      AND (filters->>'source_kind' IS NULL OR s.source_kind=filters->>'source_kind')
      AND (filters->>'platform' IS NULL OR s.platform=filters->>'platform')
      AND (filters->>'realm_id' IS NULL OR s.realm_id=filters->>'realm_id')
      AND (filters->>'external_group_id' IS NULL OR
           s.external_group_id=filters->>'external_group_id')
      AND (filters->>'binding_ref' IS NULL OR EXISTS (
          SELECT 1 FROM __forest_module_schema__.source_occurrences o
          WHERE o.bot_id=u.bot_id AND o.source_unit_id=u.source_unit_id
            AND o.binding_ref=filters->>'binding_ref'))
      AND (filters->>'author_subject_ref' IS NULL OR
           u.author_subject_ref=filters->>'author_subject_ref')
      AND (filters->>'modality' IS NULL OR u.modality=filters->>'modality')
      AND (filters->>'unit_kind' IS NULL OR u.unit_kind=filters->>'unit_kind')
      AND (filters->>'index_coverage' IS NULL OR
           coverage.index_coverage=filters->>'index_coverage')
      AND (filters->>'only_unknown_facets' IS DISTINCT FROM 'true' OR
           coverage.unknown_count>0)
      AND (filters->>'source_order_from' IS NULL OR
           u.source_order_seq>=(filters->>'source_order_from')::bigint)
      AND (filters->>'source_order_to' IS NULL OR
           u.source_order_seq<(filters->>'source_order_to')::bigint)
      AND (filters->>'occurred_from' IS NULL OR
           u.occurred_at>=(filters->>'occurred_from')::timestamptz)
      AND (filters->>'occurred_to' IS NULL OR
           u.occurred_at<(filters->>'occurred_to')::timestamptz);
$$;

CREATE FUNCTION memory_query_sources(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    page_limit integer;
    snapshot_time timestamptz;
    snapshot_value pg_snapshot;
    filter_digest text;
    from_time timestamptz;
    to_time timestamptz;
    after_time timestamptz;
    selected_sources text[];
    selected_state text := COALESCE(input->>'eligibility_state','ELIGIBLE');
    selected_source_status text := COALESCE(input->>'source_status','ACTIVE');
    selected_index_coverage text := input->>'index_coverage';
    selected_unknown boolean := input->>'only_unknown_facets'='true';
    candidate_source_ids text[];
    items jsonb;
    more boolean;
    cursor_value jsonb;
BEGIN
    page_limit := COALESCE((input->>'limit')::integer,50);
    snapshot_time := COALESCE((input->>'snapshot_accepted_at')::timestamptz,clock_timestamp());
    snapshot_value := COALESCE((input->>'snapshot_xid')::pg_snapshot,pg_current_snapshot());
    filter_digest := 'sha256:' || encode(sha256(convert_to((
        jsonb_build_array(input - 'limit' - 'snapshot_accepted_at' - 'snapshot_xid'
            - 'after_occurred_at' - 'after_source_unit_id' - 'cursor_filter_digest',
            snapshot_value::text,snapshot_time::text)
    )::text,'UTF8')),'hex');
    from_time := (input->>'occurred_from')::timestamptz;
    to_time := (input->>'occurred_to')::timestamptz;
    after_time := (input->>'after_occurred_at')::timestamptz;
    IF input ? 'group_source_ids' THEN
        SELECT COALESCE(array_agg(value ORDER BY value),ARRAY[]::text[])
        INTO selected_sources FROM jsonb_array_elements_text(input->'group_source_ids');
    END IF;
    IF page_limit NOT BETWEEN 1 AND 100 OR
       selected_state NOT IN ('REGISTERED','ELIGIBLE','SUPPRESSED') OR
       selected_source_status NOT IN ('ACTIVE','RETIRED') OR
       (selected_index_coverage IS NOT NULL AND selected_index_coverage NOT IN
           ('NO_FACETS','NOT_READY','PARTIAL','COMPLETE')) OR
       (input ? 'only_unknown_facets' AND
           jsonb_typeof(input->'only_unknown_facets')<>'boolean') OR
       (input ? 'group_source_ids' AND (cardinality(selected_sources)=0 OR
          cardinality(selected_sources)>256)) OR
       (input ? 'source_order_from' AND NOT (input ? 'source_id')) OR
       (input ? 'source_order_to' AND NOT (input ? 'source_id')) OR
       ((input->>'source_order_from')::bigint > (input->>'source_order_to')::bigint) OR
       (from_time IS NOT NULL AND to_time IS NOT NULL AND from_time>=to_time) OR
       ((after_time IS NOT NULL) <> (input ? 'after_source_unit_id')) OR
       (after_time IS NOT NULL AND (NOT input ? 'snapshot_accepted_at'
          OR NOT input ? 'snapshot_xid' OR
          input->>'cursor_filter_digest' IS DISTINCT FROM filter_digest)) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_INVALID',
                'message','Source page limit or cursor is invalid','status',422));
    END IF;
    -- Rare facet filters start from a bounded partial index. If the candidate
    -- set is large, use the ordered source scan rather than materializing it.
    IF input ? 'source_unit_id' THEN
        candidate_source_ids := ARRAY[input->>'source_unit_id'];
    ELSIF selected_unknown THEN
        SELECT array_agg(candidate.source_unit_id)
        INTO candidate_source_ids
        FROM (
            SELECT DISTINCT f.source_unit_id
            FROM __forest_module_schema__.embedding_facets f
            WHERE f.bot_id=context->>'bot_id'
              AND f.purpose='DOCUMENT' AND f.state='UNKNOWN'
            ORDER BY f.source_unit_id LIMIT 8193
        ) candidate;
    ELSIF selected_index_coverage IN ('COMPLETE','PARTIAL') THEN
        SELECT array_agg(candidate.source_unit_id)
        INTO candidate_source_ids
        FROM (
            SELECT DISTINCT f.source_unit_id
            FROM __forest_module_schema__.embedding_facets f
            WHERE f.bot_id=context->>'bot_id'
              AND f.purpose='DOCUMENT' AND f.state='READY'
            ORDER BY f.source_unit_id LIMIT 8193
        ) candidate;
    END IF;
    IF input ? 'source_unit_id' OR
       ((selected_unknown OR selected_index_coverage IN ('COMPLETE','PARTIAL')) AND
        COALESCE(cardinality(candidate_source_ids),0)<=8192) THEN
        WITH page AS (
            SELECT u.source_unit_id,u.source_id,u.trunk_id,u.source_kind,
                u.platform,u.realm_id,u.external_group_id,u.author_subject_ref,
                u.unit_kind,u.modality,u.occurred_at,u.source_order_seq,u.part_count,
                u.eligibility_state,u.source_status,u.document_facet_count,
                u.ready_document_facet_count,u.failed_document_facet_count,
                u.unknown_document_facet_count,u.index_coverage
            FROM unnest(COALESCE(candidate_source_ids,ARRAY[]::text[])) candidate(source_unit_id)
            JOIN __forest_module_schema__.memory_filtered_source_units(
                context->>'bot_id',input,snapshot_time,snapshot_value) u
              ON u.source_unit_id=candidate.source_unit_id
            WHERE after_time IS NULL OR
                (u.occurred_at,u.source_unit_id)<
                (after_time,input->>'after_source_unit_id')
            ORDER BY u.occurred_at DESC,u.source_unit_id DESC LIMIT page_limit+1
        ), visible AS (
            SELECT * FROM page ORDER BY occurred_at DESC,source_unit_id DESC LIMIT page_limit
        )
        SELECT COALESCE((SELECT jsonb_agg(to_jsonb(v) ORDER BY v.occurred_at DESC,v.source_unit_id DESC)
                         FROM visible v),'[]'::jsonb),
               (SELECT count(*)>page_limit FROM page),
               (SELECT jsonb_build_object('occurred_at',v.occurred_at,
                           'source_unit_id',v.source_unit_id)
                FROM visible v ORDER BY v.occurred_at,v.source_unit_id LIMIT 1)
        INTO items,more,cursor_value;
    ELSE
        WITH page AS (
            SELECT u.source_unit_id,u.source_id,u.trunk_id,u.source_kind,
                u.platform,u.realm_id,u.external_group_id,u.author_subject_ref,
                u.unit_kind,u.modality,u.occurred_at,u.source_order_seq,u.part_count,
                u.eligibility_state,u.source_status,u.document_facet_count,
                u.ready_document_facet_count,u.failed_document_facet_count,
                u.unknown_document_facet_count,u.index_coverage
            FROM __forest_module_schema__.memory_filtered_source_units(
                context->>'bot_id',input,snapshot_time,snapshot_value) u
            WHERE after_time IS NULL OR
                (u.occurred_at,u.source_unit_id)<(after_time,input->>'after_source_unit_id')
            ORDER BY u.occurred_at DESC,u.source_unit_id DESC LIMIT page_limit+1
        ), visible AS (
            SELECT * FROM page ORDER BY occurred_at DESC,source_unit_id DESC LIMIT page_limit
        )
        SELECT COALESCE((SELECT jsonb_agg(to_jsonb(v) ORDER BY v.occurred_at DESC,v.source_unit_id DESC)
                         FROM visible v),'[]'::jsonb),
               (SELECT count(*)>page_limit FROM page),
               (SELECT jsonb_build_object('occurred_at',v.occurred_at,
                           'source_unit_id',v.source_unit_id)
                FROM visible v ORDER BY v.occurred_at,v.source_unit_id LIMIT 1)
        INTO items,more,cursor_value;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('snapshot_accepted_at',snapshot_time,
            'snapshot_xid',snapshot_value::text,
            'filter_digest',filter_digest,
            'items',items,'has_more',more,
            'next_cursor',CASE WHEN more THEN cursor_value ELSE NULL END));
EXCEPTION WHEN invalid_text_representation OR invalid_datetime_format
    OR datetime_field_overflow OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_QUERY_INVALID',
            'message','Source query input is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_query_source_embedding_facets(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_unit text := input->>'source_unit_id';
    requested_state text := input->>'state';
    after_facet text := input->>'after_facet_id';
    page_limit integer := COALESCE((input->>'limit')::integer,50);
    source_row record;
    items jsonb;
    more boolean;
    next_after text;
BEGIN
    IF jsonb_typeof(input) IS DISTINCT FROM 'object'
       OR input - ARRAY['source_unit_id','state','after_facet_id','limit']::text[]
           <> '{}'::jsonb
       OR requested_unit IS NULL OR octet_length(requested_unit) NOT BETWEEN 1 AND 256
       OR (requested_state IS NOT NULL AND requested_state NOT IN
           ('PENDING','PREPARED','DISPATCHED','EMBEDDED','READY','UNKNOWN',
            'FAILED','SUPPRESSED'))
       OR (after_facet IS NOT NULL AND octet_length(after_facet) NOT BETWEEN 1 AND 256)
       OR page_limit NOT BETWEEN 1 AND 100 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_INVALID',
                'message','Source facet query is invalid','status',422));
    END IF;
    SELECT u.source_id,u.source_revision,u.trunk_id,s.external_group_id,
        u.author_subject_ref INTO source_row
    FROM __forest_module_schema__.source_units u
    JOIN __forest_module_schema__.memory_sources s
      ON s.bot_id=u.bot_id AND s.source_id=u.source_id
    WHERE u.bot_id=requested_bot AND u.source_unit_id=requested_unit;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_SOURCE_NOT_FOUND',
                'message','Source unit does not exist','status',404));
    END IF;
    WITH page AS (
        SELECT f.facet_id,f.part_id,f.modality,f.state,f.failure_code,
            f.created_at,f.current_attempt_id AS attempt_id,
            a.claim_id,a.state AS attempt_state,a.effect_operation_id,
            a.executor_generation_id,a.dispatched_at,a.settled_at,
            r.budget_day::text AS budget_day,
            r.state AS reservation_state,r.reserved_tokens,r.charged_tokens,
            r.error_code AS reservation_error_code
        FROM __forest_module_schema__.embedding_facets f
        LEFT JOIN __forest_module_schema__.embedding_batch_attempts a
          ON a.bot_id=f.bot_id AND a.attempt_id=f.current_attempt_id
        LEFT JOIN __forest_module_schema__.embedding_budget_reservations r
          ON r.bot_id=a.bot_id AND r.reservation_id=a.reservation_id
        WHERE f.bot_id=requested_bot AND f.source_unit_id=requested_unit
          AND f.source_revision=source_row.source_revision AND f.purpose='DOCUMENT'
          AND (requested_state IS NULL OR f.state=requested_state)
          AND (after_facet IS NULL OR f.facet_id>after_facet)
        ORDER BY f.facet_id LIMIT page_limit+1
    ), visible AS (
        SELECT * FROM page ORDER BY facet_id LIMIT page_limit
    ), annotated AS (
        SELECT v.*,history.attempt_id AS last_unknown_attempt_id
        FROM visible v
        LEFT JOIN LATERAL (
            SELECT a.attempt_id
            FROM __forest_module_schema__.embedding_claim_items ci
            JOIN __forest_module_schema__.embedding_batch_attempts a
              ON a.bot_id=ci.bot_id AND a.claim_id=ci.claim_id
            WHERE ci.bot_id=requested_bot AND ci.facet_id=v.facet_id
              AND a.state='UNKNOWN'
            ORDER BY a.settled_at DESC,a.attempt_id DESC LIMIT 1
        ) history ON true
    )
    SELECT COALESCE((SELECT jsonb_agg(to_jsonb(v) ORDER BY v.facet_id)
                     FROM annotated v),'[]'::jsonb),
           (SELECT count(*)>page_limit FROM page),
           (SELECT v.facet_id FROM visible v ORDER BY v.facet_id DESC LIMIT 1)
    INTO items,more,next_after;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'source_unit_id',requested_unit,'source_id',source_row.source_id,
            'source_revision',source_row.source_revision,
            'trunk_id',source_row.trunk_id,
            'external_group_id',source_row.external_group_id,
            'author_subject_ref',source_row.author_subject_ref,
            'items',items,'has_more',more,
            'next_after',CASE WHEN more THEN next_after ELSE NULL END));
END;
$$;

CREATE FUNCTION memory_inspect_embedding_attempt(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_attempt text := input->>'attempt_id';
    attempt_row record;
    members jsonb;
BEGIN
    IF jsonb_typeof(input) IS DISTINCT FROM 'object'
       OR input - ARRAY['attempt_id']::text[] <> '{}'::jsonb
       OR requested_attempt IS NULL
       OR octet_length(requested_attempt) NOT BETWEEN 1 AND 256 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_ATTEMPT_INPUT_INVALID',
                'message','Embedding attempt inspection input is invalid','status',422));
    END IF;
    SELECT a.attempt_id,a.claim_id,c.purpose,c.space_id,c.item_count,
           a.state AS attempt_state,a.effect_operation_id,
           a.executor_generation_id,a.dispatched_at,a.settled_at,
           r.budget_day::text AS budget_day,r.state AS reservation_state,
           r.reserved_tokens,r.charged_tokens,
           retry.operation_id AS retry_operation_id,
           retry.requested_by AS retry_requested_by,
           retry.created_at AS retry_created_at
    INTO attempt_row
    FROM __forest_module_schema__.embedding_batch_attempts a
    JOIN __forest_module_schema__.embedding_claims c
      ON c.bot_id=a.bot_id AND c.claim_id=a.claim_id
    LEFT JOIN __forest_module_schema__.embedding_budget_reservations r
      ON r.bot_id=a.bot_id AND r.reservation_id=a.reservation_id
    LEFT JOIN __forest_module_schema__.embedding_unknown_retry_operations retry
      ON retry.bot_id=a.bot_id AND retry.attempt_id=a.attempt_id
    WHERE a.bot_id=requested_bot AND a.attempt_id=requested_attempt;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_ATTEMPT_NOT_FOUND',
                'message','Embedding attempt does not exist','status',404));
    END IF;
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'item_index',ci.item_index,'facet_id',f.facet_id,
        'modality',f.modality,'state',f.state,
        'current_attempt_id',f.current_attempt_id,
        'retry_eligible',(
            attempt_row.attempt_state='UNKNOWN'
            AND attempt_row.reservation_state='UNKNOWN'
            AND attempt_row.purpose='DOCUMENT'
            AND f.state='UNKNOWN' AND f.current_attempt_id=requested_attempt
            AND f.space_id=attempt_row.space_id
            AND d.space_id=attempt_row.space_id
            AND u.eligibility_state='ELIGIBLE' AND s.status='ACTIVE'
        ),
        'source_unit_id',f.source_unit_id,'source_revision',f.source_revision,
        'trunk_id',u.trunk_id,'external_group_id',s.external_group_id,
        'author_subject_ref',u.author_subject_ref
    ) ORDER BY ci.item_index),'[]'::jsonb)
    INTO members
    FROM __forest_module_schema__.embedding_claim_items ci
    JOIN __forest_module_schema__.embedding_facets f
      ON f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
    LEFT JOIN __forest_module_schema__.source_units u
      ON u.bot_id=f.bot_id AND u.source_unit_id=f.source_unit_id
     AND u.source_revision=f.source_revision
    LEFT JOIN __forest_module_schema__.memory_sources s
      ON s.bot_id=u.bot_id AND s.source_id=u.source_id
    LEFT JOIN __forest_module_schema__.memory_domains d
      ON d.bot_id=f.bot_id
    WHERE ci.bot_id=requested_bot AND ci.claim_id=attempt_row.claim_id;
    IF jsonb_array_length(members) IS DISTINCT FROM attempt_row.item_count THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_ATTEMPT_MEMBERSHIP_INVALID',
                'message','Embedding attempt membership is incomplete','status',409));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.memory-embedding-attempt/1',
            'attempt_id',attempt_row.attempt_id,'claim_id',attempt_row.claim_id,
            'purpose',attempt_row.purpose,'attempt_state',attempt_row.attempt_state,
            'effect_operation_id',attempt_row.effect_operation_id,
            'executor_generation_id',attempt_row.executor_generation_id,
            'dispatched_at',attempt_row.dispatched_at,'settled_at',attempt_row.settled_at,
            'budget_day',attempt_row.budget_day,
            'reservation_state',attempt_row.reservation_state,
            'reserved_tokens',attempt_row.reserved_tokens,
            'charged_tokens',attempt_row.charged_tokens,
            'retry_operation_id',attempt_row.retry_operation_id,
            'retry_requested_by',attempt_row.retry_requested_by,
            'retry_created_at',attempt_row.retry_created_at,
            'item_count',attempt_row.item_count,'items',members));
END;
$$;

CREATE FUNCTION memory_retry_unknown_embedding(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_attempt text := input->>'attempt_id';
    requested_actor text := input->>'requested_by';
    requested_digest text := 'sha256:' ||
        encode(sha256(convert_to(input::text,'UTF8')),'hex');
    existing __forest_module_schema__.embedding_unknown_retry_operations%ROWTYPE;
    attempt_row record;
    selected_ids jsonb;
    selected_count integer;
    updated_count integer;
    result_value jsonb;
BEGIN
    IF requested_bot IS NULL OR requested_operation IS NULL
       OR octet_length(requested_operation) NOT BETWEEN 1 AND 256
       OR requested_attempt IS NULL OR octet_length(requested_attempt) NOT BETWEEN 1 AND 256
       OR requested_actor IS NULL OR octet_length(requested_actor) NOT BETWEEN 1 AND 256
       OR jsonb_typeof(input) IS DISTINCT FROM 'object'
       OR input - ARRAY['attempt_id','expected_facet_ids',
                        'acknowledge_possible_duplicate_charge','requested_by']::text[]
          <> '{}'::jsonb
       OR input->>'acknowledge_possible_duplicate_charge' IS DISTINCT FROM 'true'
       OR jsonb_typeof(input->'expected_facet_ids') IS DISTINCT FROM 'array'
       OR jsonb_array_length(input->'expected_facet_ids') NOT BETWEEN 1 AND 32 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_RETRY_INPUT_INVALID',
                'message','Unknown embedding retry input is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/embedding-unknown-retry/' || requested_bot || '/' ||
        requested_operation,0));
    SELECT * INTO existing
    FROM __forest_module_schema__.embedding_unknown_retry_operations
    WHERE bot_id=requested_bot AND operation_id=requested_operation FOR UPDATE;
    IF FOUND THEN
        IF existing.input_digest=requested_digest THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
                'value',existing.result_payload);
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_RETRY_OPERATION_CONFLICT',
                'message','Retry operation was reused with different input','status',409));
    END IF;
    SELECT a.attempt_id,a.claim_id,a.state AS attempt_state,c.state AS claim_state,
           c.purpose,c.space_id,r.state AS reservation_state,r.reserved_tokens
    INTO attempt_row
    FROM __forest_module_schema__.embedding_batch_attempts a
    JOIN __forest_module_schema__.embedding_claims c
      ON c.bot_id=a.bot_id AND c.claim_id=a.claim_id
    JOIN __forest_module_schema__.embedding_budget_reservations r
      ON r.bot_id=a.bot_id AND r.reservation_id=a.reservation_id
    WHERE a.bot_id=requested_bot AND a.attempt_id=requested_attempt
    FOR UPDATE OF a,c,r;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_ATTEMPT_NOT_FOUND',
                'message','Embedding attempt does not exist','status',404));
    END IF;
    SELECT * INTO existing
    FROM __forest_module_schema__.embedding_unknown_retry_operations
    WHERE bot_id=requested_bot AND attempt_id=requested_attempt;
    IF FOUND OR attempt_row.attempt_state<>'UNKNOWN'
       OR attempt_row.claim_state<>'SETTLED'
       OR attempt_row.purpose<>'DOCUMENT'
       OR attempt_row.reservation_state<>'UNKNOWN' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_RETRY_BOUNDARY_CLOSED',
                'message','Unknown embedding attempt cannot be requeued','status',409));
    END IF;
    SELECT COALESCE(jsonb_agg(facet_id ORDER BY item_index),'[]'::jsonb),count(*)
    INTO selected_ids,selected_count
    FROM (
        SELECT ci.item_index,f.facet_id
        FROM __forest_module_schema__.embedding_claim_items ci
        JOIN __forest_module_schema__.embedding_facets f
          ON f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
        JOIN __forest_module_schema__.source_units u
          ON u.bot_id=f.bot_id AND u.source_unit_id=f.source_unit_id
         AND u.source_revision=f.source_revision AND u.eligibility_state='ELIGIBLE'
        JOIN __forest_module_schema__.memory_sources s
          ON s.bot_id=u.bot_id AND s.source_id=u.source_id AND s.status='ACTIVE'
        JOIN __forest_module_schema__.memory_domains d
          ON d.bot_id=f.bot_id AND d.space_id=f.space_id
        WHERE ci.bot_id=requested_bot AND ci.claim_id=attempt_row.claim_id
          AND f.state='UNKNOWN' AND f.current_attempt_id=requested_attempt
          AND f.purpose='DOCUMENT' AND f.space_id=attempt_row.space_id
        ORDER BY ci.item_index FOR UPDATE OF f,u,s
    ) eligible;
    IF selected_count=0 OR selected_ids IS DISTINCT FROM input->'expected_facet_ids' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_RETRY_MEMBERS_CHANGED',
                'message','Unknown embedding members changed','status',409));
    END IF;
    -- The original attempt and UNKNOWN budget stay terminal. The ordinary
    -- claim path will create a new attempt and reserve new daily budget.
    UPDATE __forest_module_schema__.embedding_facets
    SET state='PENDING',current_attempt_id=NULL,
        next_attempt_at=clock_timestamp(),updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND facet_id IN (
        SELECT jsonb_array_elements_text(selected_ids)
    ) AND state='UNKNOWN' AND current_attempt_id=requested_attempt;
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    IF updated_count<>selected_count THEN
        RAISE EXCEPTION 'Unknown embedding retry lost locked members' USING ERRCODE='55000';
    END IF;
    result_value := jsonb_build_object(
        'protocol','forest.memory-embedding-unknown-retry/1',
        'operation_id',requested_operation,'attempt_id',requested_attempt,
        'state','REQUEUED','requeued_count',selected_count,
        'previous_budget_state','UNKNOWN',
        'previous_reserved_tokens',attempt_row.reserved_tokens);
    INSERT INTO __forest_module_schema__.embedding_unknown_retry_operations(
        bot_id,operation_id,input_digest,attempt_id,requested_by,result_payload)
    VALUES(requested_bot,requested_operation,requested_digest,requested_attempt,
        requested_actor,result_value);
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',result_value);
END;
$$;

CREATE FUNCTION memory_freeze_forget_selection(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_selection text := input->>'selection_id';
    requested_digest text := 'sha256:' ||
        encode(sha256(convert_to(input::text,'UTF8')),'hex');
    requested_filters jsonb := input->'filters';
    snapshot_value pg_snapshot := pg_current_snapshot();
    snapshot_time timestamptz := clock_timestamp();
    validation jsonb;
    existing __forest_module_schema__.forget_selections%ROWTYPE;
    selected_count integer;
    selected_digest text;
BEGIN
    IF requested_operation IS NULL OR
       octet_length(requested_operation) NOT BETWEEN 1 AND 256 OR
       requested_selection IS NULL OR
       octet_length(requested_selection) NOT BETWEEN 1 AND 256 OR
       input->>'requested_by' IS NULL OR
       octet_length(input->>'requested_by') NOT BETWEEN 1 AND 256 OR
       jsonb_typeof(requested_filters) IS DISTINCT FROM 'object' OR
       requested_filters->>'eligibility_state' = 'SUPPRESSED' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_SELECTION_INVALID',
                'message','Forget selection input is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/forget-selection-operation/' || requested_bot ||
        '/' || requested_operation,0));
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/forget-selection-id/' || requested_bot ||
        '/' || requested_selection,0));
    SELECT * INTO existing FROM __forest_module_schema__.forget_selections
    WHERE bot_id=requested_bot AND
          (selection_id=requested_selection OR operation_id=requested_operation);
    IF existing IS NOT NULL THEN
        IF existing.operation_id IS DISTINCT FROM requested_operation OR
           existing.selection_id IS DISTINCT FROM requested_selection OR
           existing.input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_FORGET_SELECTION_CONFLICT',
                    'message','Forget selection identity differs','status',409));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('protocol','forest.memory-forget-selection/1',
                'selection_id',existing.selection_id,
                'member_count',existing.member_count,
                'member_digest',existing.member_digest,
                'snapshot_xid',existing.snapshot_xid::text,
                'snapshot_accepted_at',existing.snapshot_accepted_at));
    END IF;
    validation := __forest_module_schema__.memory_query_sources(
        context,requested_filters || jsonb_build_object('limit',1,
            'snapshot_xid',snapshot_value::text,
            'snapshot_accepted_at',snapshot_time));
    IF validation->>'ok' IS DISTINCT FROM 'true' THEN
        RETURN validation;
    END IF;
    INSERT INTO __forest_module_schema__.forget_selection_members(
        bot_id,selection_id,ordinal,source_unit_id,source_revision,
        anchor_payload_id)
    SELECT requested_bot,requested_selection,
           row_number() OVER (ORDER BY chosen.occurred_at DESC,
               chosen.source_unit_id DESC),
           chosen.source_unit_id,chosen.source_revision,chosen.author_payload_id
    FROM (
        SELECT source_unit_id,source_revision,author_payload_id,occurred_at
        FROM __forest_module_schema__.memory_filtered_source_units(
            requested_bot,requested_filters,snapshot_time,snapshot_value)
        ORDER BY occurred_at DESC,source_unit_id DESC LIMIT 1000001
    ) chosen;
    GET DIAGNOSTICS selected_count = ROW_COUNT;
    IF selected_count=0 OR selected_count>1000000 THEN
        DELETE FROM __forest_module_schema__.forget_selection_members
        WHERE bot_id=requested_bot AND selection_id=requested_selection;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_SELECTION_SIZE_INVALID',
                'message','Forget selection must contain 1..1000000 units',
                'status',422));
    END IF;
    SELECT 'sha256:' || encode(sha256(convert_to(string_agg(
        encode(sha256(convert_to(jsonb_build_array(source_unit_id,
            source_revision,anchor_payload_id)::text,'UTF8')),'hex'),''
            ORDER BY ordinal),'UTF8')),'hex')
    INTO selected_digest
    FROM __forest_module_schema__.forget_selection_members
    WHERE bot_id=requested_bot AND selection_id=requested_selection;
    INSERT INTO __forest_module_schema__.forget_selections(
        bot_id,selection_id,operation_id,input_digest,requested_by,filters,
        snapshot_xid,snapshot_accepted_at,member_count,member_digest)
    VALUES(requested_bot,requested_selection,requested_operation,requested_digest,
        input->>'requested_by',requested_filters,snapshot_value,snapshot_time,
        selected_count,selected_digest);
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-forget-selection/1',
            'selection_id',requested_selection,'member_count',selected_count,
            'member_digest',selected_digest,'snapshot_xid',snapshot_value::text,
            'snapshot_accepted_at',snapshot_time));
END;
$$;

CREATE FUNCTION memory_query_forget_selection(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    selection_row __forest_module_schema__.forget_selections%ROWTYPE;
    after_ordinal integer := COALESCE((input->>'after_ordinal')::integer,0);
    page_limit integer := COALESCE((input->>'limit')::integer,50);
    items jsonb;
    applied_batch_count integer;
    applied_member_count integer;
    first_gap_ordinal integer;
    last_applied_ordinal integer;
    next_unapplied_ordinal integer;
    progress_state text;
BEGIN
    IF after_ordinal<0 OR page_limit NOT BETWEEN 1 AND 100 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_SELECTION_QUERY_INVALID',
                'message','Forget selection page is invalid','status',422));
    END IF;
    SELECT * INTO selection_row FROM __forest_module_schema__.forget_selections
    WHERE bot_id=context->>'bot_id' AND selection_id=input->>'selection_id'
      AND requested_by=input->>'requested_by';
    IF selection_row IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_SELECTION_NOT_FOUND',
                'message','Forget selection does not exist','status',404));
    END IF;
    WITH ordered AS (
        SELECT first_ordinal,last_ordinal,
               COALESCE(max(last_ordinal) OVER (
                   ORDER BY first_ordinal,last_ordinal
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
               ),0) AS previous_end
        FROM __forest_module_schema__.batch_forget_operations
        WHERE bot_id=selection_row.bot_id
          AND selection_id=selection_row.selection_id
    )
    SELECT count(*)::integer,
           COALESCE(sum(last_ordinal-first_ordinal+1),0)::integer,
           min(previous_end+1) FILTER (WHERE first_ordinal>previous_end+1),
           COALESCE(max(last_ordinal),0)
    INTO applied_batch_count,applied_member_count,first_gap_ordinal,
         last_applied_ordinal
    FROM ordered;
    next_unapplied_ordinal := COALESCE(first_gap_ordinal,
                                      last_applied_ordinal+1);
    progress_state := CASE WHEN applied_batch_count=0 THEN 'FROZEN'
        WHEN next_unapplied_ordinal>selection_row.member_count THEN 'COMPLETE'
        ELSE 'PARTIAL' END;
    SELECT COALESCE(jsonb_agg(to_jsonb(page) ORDER BY page.ordinal),'[]'::jsonb)
    INTO items FROM (
        SELECT m.ordinal,m.source_unit_id,m.source_revision,m.anchor_payload_id,
               CASE WHEN u.eligibility_state<>'SUPPRESSED' THEN 'ACTIVE'
                    WHEN b.operation_id IS NOT NULL THEN 'FORGOTTEN_BY_SELECTION'
                    ELSE 'SUPPRESSED_EXTERNALLY' END AS forget_state,
               CASE WHEN u.eligibility_state='SUPPRESSED'
                    THEN s.operation_id ELSE NULL END AS forget_operation_id
        FROM __forest_module_schema__.forget_selection_members m
        JOIN __forest_module_schema__.source_units u
          ON u.bot_id=m.bot_id AND u.source_unit_id=m.source_unit_id
        LEFT JOIN __forest_module_schema__.source_suppressions s
          ON s.bot_id=m.bot_id AND s.source_unit_id=m.source_unit_id
        LEFT JOIN __forest_module_schema__.batch_forget_operations b
          ON b.bot_id=s.bot_id AND b.operation_id=s.operation_id
         AND b.selection_id=m.selection_id
        WHERE m.bot_id=selection_row.bot_id AND
              m.selection_id=selection_row.selection_id AND m.ordinal>after_ordinal
        ORDER BY m.ordinal LIMIT page_limit
    ) page;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-forget-selection-page/1',
            'selection_id',selection_row.selection_id,
            'member_count',selection_row.member_count,
            'member_digest',selection_row.member_digest,
            'progress_state',progress_state,
            'applied_batch_count',applied_batch_count,
            'applied_member_count',applied_member_count,
            'next_unapplied_ordinal',CASE
                WHEN progress_state='COMPLETE' THEN NULL
                ELSE next_unapplied_ordinal END,
            'snapshot_xid',selection_row.snapshot_xid::text,
            'snapshot_accepted_at',selection_row.snapshot_accepted_at,
            'filters',selection_row.filters,'items',items,
            'has_more',after_ordinal+jsonb_array_length(items)<selection_row.member_count));
END;
$$;

CREATE FUNCTION memory_prepare_forget_batch(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    requested_bot text := context->>'bot_id';
    selection_row __forest_module_schema__.forget_selections%ROWTYPE;
    after_value integer := (body->>'after_ordinal')::integer;
    batch_size integer := (body->>'batch_size')::integer;
    last_value integer;
    expected_count integer;
    live_count integer;
    anchor_ids text[];
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1' OR
       envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph' OR
       body->>'selection_id' IS NULL OR
       octet_length(body->>'selection_id') NOT BETWEEN 1 AND 256 OR
       body->>'requested_by' IS NULL OR
       octet_length(body->>'requested_by') NOT BETWEEN 1 AND 256 OR
       (body ? 'operation_id' AND
        octet_length(body->>'operation_id') NOT BETWEEN 1 AND 256) OR
       after_value IS NULL OR after_value<0 OR
       batch_size IS NULL OR batch_size NOT BETWEEN 1 AND 64 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_BATCH_INPUT_INVALID',
                'message','Forget batch request is invalid','status',422));
    END IF;
    SELECT * INTO selection_row FROM __forest_module_schema__.forget_selections
    WHERE bot_id=requested_bot AND selection_id=body->>'selection_id'
      AND requested_by=body->>'requested_by';
    IF selection_row IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_SELECTION_NOT_FOUND',
                'message','Forget selection does not exist','status',404));
    END IF;
    IF after_value>=selection_row.member_count THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_BATCH_EXHAUSTED',
                'message','Forget selection has no remaining batch','status',409));
    END IF;
    last_value := LEAST(after_value+batch_size,selection_row.member_count);
    expected_count := last_value-after_value;
    SELECT count(*),array_agg(DISTINCT m.anchor_payload_id ORDER BY m.anchor_payload_id)
    INTO live_count,anchor_ids
    FROM __forest_module_schema__.forget_selection_members m
    JOIN __forest_module_schema__.source_units u
      ON u.bot_id=m.bot_id AND u.source_unit_id=m.source_unit_id
     AND u.source_revision=m.source_revision
     AND u.author_payload_id=m.anchor_payload_id
    LEFT JOIN __forest_module_schema__.source_suppressions s
      ON s.bot_id=u.bot_id AND s.source_unit_id=u.source_unit_id
    WHERE m.bot_id=requested_bot AND m.selection_id=selection_row.selection_id
      AND m.ordinal>after_value AND m.ordinal<=last_value
      AND (u.eligibility_state<>'SUPPRESSED' OR
           (body ? 'operation_id' AND
            s.operation_id=body->>'operation_id'));
    IF live_count<>expected_count OR anchor_ids IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_BATCH_CHANGED',
                'message','A frozen forget member is no longer eligible','status',409));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.payload-forget-batch-plan/1',
            'anchor_payload_ids',to_jsonb(anchor_ids)));
END;
$$;

CREATE FUNCTION memory_forget_batch_impact_plan(
    requested_bot text, selection_id_value text, member_digest_value text,
    first_ordinal_value integer, last_ordinal_value integer,
    root_member_digest_value text, root_sample_value jsonb,
    deletion_epoch_value bigint, selection_digest_value text,
    selected_ids text[])
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    plan_value jsonb;
BEGIN
    plan_value := jsonb_build_object(
        'protocol','forest.memory-forget-batch-impact-plan/1',
        'selection_id',selection_id_value,'member_digest',member_digest_value,
        'first_ordinal',first_ordinal_value,'last_ordinal',last_ordinal_value,
        'batch_member_count',last_ordinal_value-first_ordinal_value+1,
        'root_member_digest',root_member_digest_value,
        'batch_source_unit_sample',root_sample_value,
        'deletion_epoch',deletion_epoch_value,
        'selection_digest',selection_digest_value,
        'selected_payload_count',cardinality(selected_ids)) ||
        __forest_module_schema__.memory_source_forget_impact(requested_bot,selected_ids);
    RETURN plan_value || jsonb_build_object('plan_digest','sha256:' ||
        encode(sha256(convert_to(plan_value::text,'UTF8')),'hex'));
END;
$$;

CREATE FUNCTION memory_preview_forget_batch(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    prior jsonb := envelope->'prior';
    prepared jsonb := prior->(body->>'prepare_step_id');
    preview jsonb := prior->(body->>'preview_step_id');
    requested_bot text := context->>'bot_id';
    selection_row __forest_module_schema__.forget_selections%ROWTYPE;
    after_value integer := (body->>'after_ordinal')::integer;
    batch_size integer := (body->>'batch_size')::integer;
    last_value integer;
    member_count integer;
    live_count integer;
    anchor_ids text[];
    selected_ids text[];
    root_digest text;
    root_sample jsonb;
    plan_value jsonb;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1' OR
       envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph' OR
       body->>'selection_id' IS NULL OR
       octet_length(body->>'selection_id') NOT BETWEEN 1 AND 256 OR
       body->>'requested_by' IS NULL OR
       octet_length(body->>'requested_by') NOT BETWEEN 1 AND 256 OR
       after_value IS NULL OR after_value<0 OR
       batch_size IS NULL OR batch_size NOT BETWEEN 1 AND 64 OR
       prepared->>'protocol' IS DISTINCT FROM 'forest.payload-forget-batch-plan/1' OR
       jsonb_typeof(prepared->'anchor_payload_ids') IS DISTINCT FROM 'array' OR
       preview->>'protocol' IS DISTINCT FROM 'forest.payload-forget-preview/1' OR
       preview->>'selection_digest' !~ '^sha256:[a-f0-9]{64}$' OR
       preview->>'deletion_epoch' !~ '^[0-9]+$' OR
       preview->>'selected_count' !~ '^[0-9]+$' OR
       jsonb_typeof(preview->'selected_payload_ids') IS DISTINCT FROM 'array' OR
       jsonb_array_length(preview->'selected_payload_ids') > 100000 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_BATCH_PREVIEW_INVALID',
                'message','Forget batch preview is invalid','status',422));
    END IF;
    SELECT * INTO selection_row FROM __forest_module_schema__.forget_selections
    WHERE bot_id=requested_bot AND selection_id=body->>'selection_id'
      AND requested_by=body->>'requested_by';
    IF selection_row IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_SELECTION_NOT_FOUND',
                'message','Forget selection does not exist','status',404));
    END IF;
    IF after_value>=selection_row.member_count THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_BATCH_EXHAUSTED',
                'message','Forget selection has no remaining batch','status',409));
    END IF;
    last_value := LEAST(after_value+batch_size,selection_row.member_count);
    member_count := last_value-after_value;
    SELECT count(*),array_agg(DISTINCT m.anchor_payload_id ORDER BY m.anchor_payload_id),
           'sha256:' || encode(sha256(convert_to(jsonb_agg(
               jsonb_build_array(m.source_unit_id,m.source_revision,
                   m.anchor_payload_id) ORDER BY m.ordinal)::text,'UTF8')),'hex'),
           to_jsonb((array_agg(m.source_unit_id ORDER BY m.ordinal))[1:20])
    INTO live_count,anchor_ids,root_digest,root_sample
    FROM __forest_module_schema__.forget_selection_members m
    JOIN __forest_module_schema__.source_units u
      ON u.bot_id=m.bot_id AND u.source_unit_id=m.source_unit_id
     AND u.source_revision=m.source_revision
     AND u.author_payload_id=m.anchor_payload_id
     AND u.eligibility_state<>'SUPPRESSED'
    WHERE m.bot_id=requested_bot AND m.selection_id=selection_row.selection_id
      AND m.ordinal>after_value AND m.ordinal<=last_value;
    SELECT array_agg(value ORDER BY value) INTO selected_ids
    FROM jsonb_array_elements_text(preview->'selected_payload_ids');
    IF live_count<>member_count OR anchor_ids IS NULL OR
       prepared->'anchor_payload_ids' IS DISTINCT FROM to_jsonb(anchor_ids) OR
       selected_ids IS NULL OR
       cardinality(selected_ids)<>(preview->>'selected_count')::integer OR
       NOT (anchor_ids <@ selected_ids) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_BATCH_CHANGED',
                'message','Forget batch members or Payload closure changed',
                'status',409));
    END IF;
    plan_value := __forest_module_schema__.memory_forget_batch_impact_plan(
        requested_bot,selection_row.selection_id,selection_row.member_digest,
        after_value+1,last_value,root_digest,root_sample,
        (preview->>'deletion_epoch')::bigint,preview->>'selection_digest',
        selected_ids);
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',plan_value);
END;
$$;

CREATE FUNCTION memory_apply_forget_batch(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    prior jsonb := envelope->'prior';
    prepared jsonb := prior->(body->>'prepare_step_id');
    forgotten jsonb := prior->(body->>'forget_step_id');
    requested_bot text := context->>'bot_id';
    requested_digest text := context->>'participant_input_digest';
    selection_row __forest_module_schema__.forget_selections%ROWTYPE;
    operation_row __forest_module_schema__.batch_forget_operations%ROWTYPE;
    after_value integer := (body->>'after_ordinal')::integer;
    batch_size integer := (body->>'batch_size')::integer;
    last_value integer;
    live_count integer;
    root_ids text[];
    anchor_ids text[];
    selected_ids text[];
    root_digest text;
    root_sample jsonb;
    current_plan jsonb;
    effects jsonb;
    result_value jsonb;
    deletion_epoch_value bigint;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1' OR
       envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph' OR
       requested_digest !~ '^sha256:[a-f0-9]{64}$' OR
       body->>'operation_id' IS NULL OR
       octet_length(body->>'operation_id') NOT BETWEEN 1 AND 256 OR
       body->>'selection_id' IS NULL OR
       octet_length(body->>'selection_id') NOT BETWEEN 1 AND 256 OR
       body->>'requested_by' IS NULL OR
       octet_length(body->>'requested_by') NOT BETWEEN 1 AND 256 OR
       after_value IS NULL OR after_value<0 OR
       batch_size IS NULL OR batch_size NOT BETWEEN 1 AND 64 OR
       jsonb_typeof(body->'plan') IS DISTINCT FROM 'object' OR
       prepared->>'protocol' IS DISTINCT FROM 'forest.payload-forget-batch-plan/1' OR
       jsonb_typeof(prepared->'anchor_payload_ids') IS DISTINCT FROM 'array' OR
       forgotten->>'protocol' IS DISTINCT FROM 'forest.payload-forgotten/1' OR
       forgotten->>'operation_id' IS DISTINCT FROM body->>'operation_id' OR
       forgotten->>'selection_digest' !~ '^sha256:[a-f0-9]{64}$' OR
       forgotten->>'deletion_epoch' !~ '^[1-9][0-9]*$' OR
       jsonb_typeof(forgotten->'selected_payload_ids') IS DISTINCT FROM 'array' OR
       jsonb_array_length(forgotten->'selected_payload_ids') > 100000 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_BATCH_AUTHORITY_INVALID',
                'message','Batch forget receipt is invalid','status',422));
    END IF;
    SELECT * INTO selection_row FROM __forest_module_schema__.forget_selections
    WHERE bot_id=requested_bot AND selection_id=body->>'selection_id'
      AND requested_by=body->>'requested_by';
    IF selection_row IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_SELECTION_NOT_FOUND',
                'message','Forget selection does not exist','status',404));
    END IF;
    IF after_value>=selection_row.member_count THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_BATCH_EXHAUSTED',
                'message','Forget selection has no remaining batch','status',409));
    END IF;
    last_value := LEAST(after_value+batch_size,selection_row.member_count);
    SELECT count(*),array_agg(m.source_unit_id ORDER BY m.ordinal),
           array_agg(DISTINCT m.anchor_payload_id ORDER BY m.anchor_payload_id),
           'sha256:' || encode(sha256(convert_to(jsonb_agg(
               jsonb_build_array(m.source_unit_id,m.source_revision,
                   m.anchor_payload_id) ORDER BY m.ordinal)::text,'UTF8')),'hex'),
           to_jsonb((array_agg(m.source_unit_id ORDER BY m.ordinal))[1:20])
    INTO live_count,root_ids,anchor_ids,root_digest,root_sample
    FROM __forest_module_schema__.forget_selection_members m
    JOIN __forest_module_schema__.source_units u
      ON u.bot_id=m.bot_id AND u.source_unit_id=m.source_unit_id
     AND u.source_revision=m.source_revision
     AND u.author_payload_id=m.anchor_payload_id
    LEFT JOIN __forest_module_schema__.source_suppressions s
      ON s.bot_id=u.bot_id AND s.source_unit_id=u.source_unit_id
    WHERE m.bot_id=requested_bot AND m.selection_id=selection_row.selection_id
      AND m.ordinal>after_value AND m.ordinal<=last_value
      AND (u.eligibility_state<>'SUPPRESSED' OR
           s.operation_id=body->>'operation_id');
    SELECT array_agg(value ORDER BY value) INTO selected_ids
    FROM jsonb_array_elements_text(forgotten->'selected_payload_ids');
    IF live_count<>last_value-after_value OR anchor_ids IS NULL OR
       prepared->'anchor_payload_ids' IS DISTINCT FROM to_jsonb(anchor_ids) OR
       selected_ids IS NULL OR NOT (anchor_ids <@ selected_ids) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_BATCH_CHANGED',
                'message','Frozen batch or Payload closure changed','status',409));
    END IF;
    deletion_epoch_value := (forgotten->>'deletion_epoch')::bigint;
    SELECT * INTO operation_row FROM __forest_module_schema__.batch_forget_operations
    WHERE bot_id=requested_bot AND operation_id=body->>'operation_id';
    IF operation_row IS NOT NULL THEN
        IF operation_row.input_digest IS DISTINCT FROM requested_digest OR
           operation_row.selection_id IS DISTINCT FROM selection_row.selection_id OR
           operation_row.requested_by IS DISTINCT FROM body->>'requested_by' OR
           operation_row.first_ordinal IS DISTINCT FROM after_value+1 OR
           operation_row.last_ordinal IS DISTINCT FROM last_value OR
           operation_row.selection_digest IS DISTINCT FROM forgotten->>'selection_digest' OR
           operation_row.deletion_epoch IS DISTINCT FROM deletion_epoch_value THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_FORGET_BATCH_OPERATION_CONFLICT',
                    'message','Batch forget operation differs','status',409));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',operation_row.result_payload);
    END IF;
    current_plan := __forest_module_schema__.memory_forget_batch_impact_plan(
        requested_bot,selection_row.selection_id,selection_row.member_digest,
        after_value+1,last_value,root_digest,root_sample,
        deletion_epoch_value-1,forgotten->>'selection_digest',selected_ids);
    IF body->'plan' IS DISTINCT FROM current_plan THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_BATCH_PLAN_CHANGED',
                'message','Batch impact differs from the approved plan','status',409));
    END IF;
    effects := __forest_module_schema__.memory_apply_forget_effects(
        requested_bot,body->>'operation_id',requested_digest,selected_ids,
        deletion_epoch_value,root_ids);
    IF effects IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_BATCH_CHANGED',
                'message','Frozen batch roots were not selected','status',409));
    END IF;
    result_value := jsonb_build_object('protocol','forest.memory-forget-batch-applied/1',
        'selection_id',selection_row.selection_id,'first_ordinal',after_value+1,
        'last_ordinal',last_value,'deletion_epoch',deletion_epoch_value,
        'plan_digest',current_plan->>'plan_digest') || effects;
    INSERT INTO __forest_module_schema__.batch_forget_operations(
        bot_id,operation_id,input_digest,selection_id,requested_by,
        first_ordinal,last_ordinal,member_digest,root_member_digest,
        selection_digest,deletion_epoch,result_payload)
    VALUES(requested_bot,body->>'operation_id',requested_digest,
        selection_row.selection_id,body->>'requested_by',after_value+1,last_value,
        selection_row.member_digest,root_digest,forgotten->>'selection_digest',
        deletion_epoch_value,result_value);
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',result_value);
END;
$$;

CREATE FUNCTION memory_query_relations(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    endpoint_kind text := input->>'endpoint_kind';
    endpoint_id text := input->>'endpoint_id';
    has_endpoint boolean := input ? 'endpoint_kind' AND input ? 'endpoint_id';
    page_limit integer := COALESCE((input->>'limit')::integer,10);
    snapshot_value pg_snapshot;
    filter_digest text;
    items jsonb;
    more boolean;
    cursor_value jsonb;
BEGIN
    snapshot_value := COALESCE((input->>'snapshot_xid')::pg_snapshot,pg_current_snapshot());
    filter_digest := 'sha256:' || encode(sha256(convert_to(
        jsonb_build_array(input - 'limit' - 'snapshot_xid' - 'after_relation_id'
            - 'after_contribution_id' - 'cursor_filter_digest',
            snapshot_value::text)::text,'UTF8')),'hex');
    IF page_limit NOT BETWEEN 1 AND 20 OR
       ((input ? 'endpoint_kind') <> (input ? 'endpoint_id')) OR
       (NOT has_endpoint AND input ? 'direction') OR
       ((input ? 'after_relation_id') <> (input ? 'after_contribution_id')) OR
       (input ? 'after_relation_id' AND (NOT (input ? 'snapshot_xid') OR
          input->>'cursor_filter_digest' IS DISTINCT FROM filter_digest)) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_INVALID',
                'message','Relation page limit or cursor is invalid','status',422));
    END IF;
    WITH page AS (
        SELECT c.contribution_id,c.relation_id,c.relation_kind,c.relation_key,
            c.left_kind,c.left_id,c.right_kind,c.right_id,c.weight_micros,
            c.origin_kind,c.created_at,c.cross_trunk,
            CASE WHEN c.relation_kind='ASSOCIATES_WITH' THEN 'UNDIRECTED'
                 WHEN NOT has_endpoint THEN 'CANONICAL'
                 WHEN c.left_kind=endpoint_kind AND c.left_id=endpoint_id
                    THEN 'OUTBOUND' ELSE 'INBOUND' END AS direction
        FROM __forest_module_schema__.relation_contributions c
        WHERE c.bot_id=context->>'bot_id' AND c.active
          AND pg_visible_in_snapshot(c.created_xid,snapshot_value)
          AND (NOT has_endpoint OR
              (c.left_kind=endpoint_kind AND c.left_id=endpoint_id) OR
              (c.right_kind=endpoint_kind AND c.right_id=endpoint_id))
          AND (NOT (input ? 'relation_kinds') OR c.relation_kind IN (
              SELECT value FROM jsonb_array_elements_text(input->'relation_kinds')))
          AND (NOT (input ? 'origin_kinds') OR c.origin_kind IN (
              SELECT value FROM jsonb_array_elements_text(input->'origin_kinds')))
          AND (NOT input ? 'cross_trunk' OR
               c.cross_trunk=(input->>'cross_trunk')::boolean)
          AND (NOT (input ? 'evidence_trunk_id') AND
               NOT (input ? 'evidence_external_group_id') AND
               NOT (input ? 'evidence_author_subject_ref') OR
               c.contribution_id IN (
            SELECT ef.contribution_id FROM __forest_module_schema__.source_units uf
            JOIN __forest_module_schema__.relation_evidence ef
              ON ef.bot_id=uf.bot_id AND ef.source_unit_id=uf.source_unit_id
            JOIN __forest_module_schema__.memory_sources sf
              ON sf.bot_id=uf.bot_id AND sf.source_id=uf.source_id
            WHERE uf.bot_id=context->>'bot_id'
              AND (input->>'evidence_trunk_id' IS NULL OR
                   uf.trunk_id=input->>'evidence_trunk_id')
              AND (input->>'evidence_external_group_id' IS NULL OR
                   sf.external_group_id=input->>'evidence_external_group_id')
              AND (input->>'evidence_author_subject_ref' IS NULL OR
                   uf.author_subject_ref=input->>'evidence_author_subject_ref')
          ))
          AND (input->>'direction' IS NULL OR
              (CASE WHEN c.relation_kind='ASSOCIATES_WITH' THEN 'UNDIRECTED'
                    WHEN c.left_kind=endpoint_kind AND c.left_id=endpoint_id
                         THEN 'OUTBOUND' ELSE 'INBOUND' END)=input->>'direction')
          AND (NOT has_endpoint OR endpoint_kind<>'SOURCE_UNIT' OR EXISTS (
            SELECT 1 FROM __forest_module_schema__.source_units ue
            JOIN __forest_module_schema__.memory_sources se
              ON se.bot_id=ue.bot_id AND se.source_id=ue.source_id
            WHERE ue.bot_id=c.bot_id AND ue.source_unit_id=endpoint_id
              AND ue.eligibility_state='ELIGIBLE' AND se.status='ACTIVE'
              AND pg_visible_in_snapshot(ue.eligible_xid,snapshot_value)
          ))
          AND (c.left_kind<>'SOURCE_UNIT' OR EXISTS (
            SELECT 1 FROM __forest_module_schema__.source_units ul
            JOIN __forest_module_schema__.memory_sources sl
              ON sl.bot_id=ul.bot_id AND sl.source_id=ul.source_id
            WHERE ul.bot_id=c.bot_id AND ul.source_unit_id=c.left_id
              AND ul.eligibility_state='ELIGIBLE' AND sl.status='ACTIVE'
              AND pg_visible_in_snapshot(ul.eligible_xid,snapshot_value)
          ))
          AND (c.right_kind<>'SOURCE_UNIT' OR EXISTS (
            SELECT 1 FROM __forest_module_schema__.source_units ur
            JOIN __forest_module_schema__.memory_sources sr
              ON sr.bot_id=ur.bot_id AND sr.source_id=ur.source_id
            WHERE ur.bot_id=c.bot_id AND ur.source_unit_id=c.right_id
              AND ur.eligibility_state='ELIGIBLE' AND sr.status='ACTIVE'
              AND pg_visible_in_snapshot(ur.eligible_xid,snapshot_value)
          ))
          AND (NOT input ? 'after_relation_id' OR
            (c.relation_id,c.contribution_id)>(input->>'after_relation_id',input->>'after_contribution_id'))
          AND NOT EXISTS (
            SELECT 1 FROM __forest_module_schema__.relation_evidence e0
            JOIN __forest_module_schema__.source_units u0
              ON u0.bot_id=e0.bot_id AND u0.source_unit_id=e0.source_unit_id
            JOIN __forest_module_schema__.memory_sources s0
              ON s0.bot_id=u0.bot_id AND s0.source_id=u0.source_id
            WHERE e0.bot_id=c.bot_id AND e0.contribution_id=c.contribution_id
              AND (u0.eligibility_state<>'ELIGIBLE' OR s0.status<>'ACTIVE'
                   OR NOT pg_visible_in_snapshot(u0.eligible_xid,snapshot_value))
          )
        ORDER BY c.relation_id,c.contribution_id LIMIT page_limit+1
    ), visible AS (
        SELECT p.* FROM (SELECT * FROM page ORDER BY relation_id,contribution_id
              LIMIT page_limit) p
    )
    SELECT COALESCE((SELECT jsonb_agg(to_jsonb(v)||jsonb_build_object(
        'evidence_count',(
            SELECT count(*) FROM __forest_module_schema__.relation_evidence e
            WHERE e.bot_id=context->>'bot_id' AND e.contribution_id=v.contribution_id
        ),
        'evidence',COALESCE((
            SELECT jsonb_agg(to_jsonb(evidence)-'evidence_index' ORDER BY evidence.evidence_index)
            FROM (
                SELECT e.evidence_index,e.source_unit_id,e.source_revision,e.part_id,
                    u.trunk_id,s.source_id,s.source_kind,s.platform,s.realm_id,
                    s.external_group_id,u.author_subject_ref
                FROM __forest_module_schema__.relation_evidence e
                JOIN __forest_module_schema__.source_units u
                  ON u.bot_id=e.bot_id AND u.source_unit_id=e.source_unit_id
                JOIN __forest_module_schema__.memory_sources s
                  ON s.bot_id=u.bot_id AND s.source_id=u.source_id
                WHERE e.bot_id=context->>'bot_id' AND e.contribution_id=v.contribution_id
                ORDER BY e.evidence_index LIMIT 16
            ) evidence
        ),'[]'::jsonb)) ORDER BY v.relation_id,v.contribution_id)
        FROM visible v),'[]'::jsonb),
        (SELECT count(*)>page_limit FROM page),
        (SELECT jsonb_build_object('relation_id',v.relation_id,
            'contribution_id',v.contribution_id)
         FROM visible v ORDER BY v.relation_id DESC,v.contribution_id DESC LIMIT 1)
    INTO items,more,cursor_value;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('snapshot_xid',snapshot_value::text,
            'filter_digest',filter_digest,
            'items',items,'has_more',more,
            'next_cursor',CASE WHEN more THEN cursor_value ELSE NULL END));
EXCEPTION WHEN invalid_text_representation THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_QUERY_INVALID',
            'message','Relation snapshot is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_search_relations(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT __forest_module_schema__.memory_query_relations(context,input)
$$;

CREATE FUNCTION memory_query_relation_evidence(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_relation text := input->>'relation_id';
    requested_contribution text := input->>'contribution_id';
    requested_after integer := COALESCE((input->>'after_evidence_index')::integer,-1);
    page_limit integer := COALESCE((input->>'limit')::integer,16);
    snapshot_value pg_snapshot;
    total_count integer;
    items jsonb;
    last_index integer;
    more boolean;
BEGIN
    snapshot_value := (input->>'snapshot_xid')::pg_snapshot;
    IF requested_bot IS NULL OR requested_relation IS NULL OR
       requested_contribution IS NULL OR snapshot_value IS NULL OR
       requested_after NOT BETWEEN -1 AND 63 OR page_limit NOT BETWEEN 1 AND 16 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_INVALID',
                'message','Relation evidence request is invalid','status',422));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM __forest_module_schema__.relation_contributions c
        WHERE c.bot_id=requested_bot AND c.relation_id=requested_relation
          AND c.contribution_id=requested_contribution AND c.active
          AND pg_visible_in_snapshot(c.created_xid,snapshot_value)
          AND (c.left_kind<>'SOURCE_UNIT' OR EXISTS (
            SELECT 1 FROM __forest_module_schema__.source_units u
            JOIN __forest_module_schema__.memory_sources s
              ON s.bot_id=u.bot_id AND s.source_id=u.source_id
            WHERE u.bot_id=c.bot_id AND u.source_unit_id=c.left_id
              AND u.eligibility_state='ELIGIBLE' AND s.status='ACTIVE'
              AND pg_visible_in_snapshot(u.eligible_xid,snapshot_value)
          ))
          AND (c.right_kind<>'SOURCE_UNIT' OR EXISTS (
            SELECT 1 FROM __forest_module_schema__.source_units u
            JOIN __forest_module_schema__.memory_sources s
              ON s.bot_id=u.bot_id AND s.source_id=u.source_id
            WHERE u.bot_id=c.bot_id AND u.source_unit_id=c.right_id
              AND u.eligibility_state='ELIGIBLE' AND s.status='ACTIVE'
              AND pg_visible_in_snapshot(u.eligible_xid,snapshot_value)
          ))
          AND NOT EXISTS (
            SELECT 1 FROM __forest_module_schema__.relation_evidence e
            JOIN __forest_module_schema__.source_units u
              ON u.bot_id=e.bot_id AND u.source_unit_id=e.source_unit_id
            JOIN __forest_module_schema__.memory_sources s
              ON s.bot_id=u.bot_id AND s.source_id=u.source_id
            WHERE e.bot_id=c.bot_id AND e.contribution_id=c.contribution_id
              AND (u.eligibility_state<>'ELIGIBLE' OR s.status<>'ACTIVE'
                   OR NOT pg_visible_in_snapshot(u.eligible_xid,snapshot_value))
          )
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_RELATION_NOT_VISIBLE',
                'message','Relation is not visible at the requested snapshot','status',404));
    END IF;
    SELECT count(*) INTO total_count
    FROM __forest_module_schema__.relation_evidence e
    WHERE e.bot_id=requested_bot AND e.contribution_id=requested_contribution;
    SELECT COALESCE(jsonb_agg(to_jsonb(page) ORDER BY page.evidence_index),'[]'::jsonb),
           max(page.evidence_index)
    INTO items,last_index
    FROM (
        SELECT e.evidence_index,e.source_unit_id,e.source_revision,e.part_id,
            u.trunk_id,s.source_id,s.source_kind,s.platform,s.realm_id,
            s.external_group_id,u.author_subject_ref
        FROM __forest_module_schema__.relation_evidence e
        JOIN __forest_module_schema__.source_units u
          ON u.bot_id=e.bot_id AND u.source_unit_id=e.source_unit_id
        JOIN __forest_module_schema__.memory_sources s
          ON s.bot_id=u.bot_id AND s.source_id=u.source_id
        WHERE e.bot_id=requested_bot AND e.contribution_id=requested_contribution
          AND e.evidence_index>requested_after
        ORDER BY e.evidence_index LIMIT page_limit
    ) page;
    SELECT EXISTS (
        SELECT 1 FROM __forest_module_schema__.relation_evidence e
        WHERE e.bot_id=requested_bot AND e.contribution_id=requested_contribution
          AND e.evidence_index>last_index
    ) INTO more;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('relation_id',requested_relation,
            'contribution_id',requested_contribution,'snapshot_xid',snapshot_value::text,
            'total_count',total_count,'items',items,'has_more',more,
            'next_after',CASE WHEN more THEN last_index ELSE NULL END));
EXCEPTION WHEN invalid_text_representation THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_QUERY_INVALID',
            'message','Relation snapshot is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_query_neighbourhood(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    max_hops integer := COALESCE((input->>'max_hops')::integer,2);
    max_nodes integer := COALESCE((input->>'max_nodes')::integer,16);
    max_edges integer := COALESCE((input->>'max_edges')::integer,16);
    snapshot_value pg_snapshot := pg_current_snapshot();
    request_base jsonb := input - 'endpoint_kind' - 'endpoint_id'
        - 'max_hops' - 'max_nodes' - 'max_edges';
    queue jsonb := jsonb_build_array(jsonb_build_object(
        'kind',input->>'endpoint_kind','id',input->>'endpoint_id','depth',0));
    nodes jsonb := queue;
    edges jsonb := '[]'::jsonb;
    seen_nodes text[] := ARRAY[jsonb_build_array(
        input->>'endpoint_kind',input->>'endpoint_id')::text];
    seen_edges text[] := ARRAY[]::text[];
    head integer := 0;
    pages_read integer := 0;
    current_node jsonb;
    current_kind text;
    current_id text;
    current_depth integer;
    next_kind text;
    next_id text;
    next_key text;
    request_value jsonb;
    page_result jsonb;
    page_value jsonb;
    edge_value jsonb;
    edge_id text;
    cursor_value jsonb;
    reason text;
    deadline_at timestamptz := clock_timestamp() + interval '8 seconds';
BEGIN
    IF max_hops NOT BETWEEN 1 AND 3 OR max_nodes NOT BETWEEN 1 AND 32
       OR max_edges NOT BETWEEN 1 AND 32
       OR input->>'endpoint_kind' NOT IN ('SOURCE_UNIT','ASSERTION','ENTITY','ARTIFACT')
       OR input->>'endpoint_id' IS NULL
       OR octet_length(input->>'endpoint_id') NOT BETWEEN 1 AND 256 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_INVALID',
                'message','Graph traversal request is invalid','status',422));
    END IF;
    <<walk>>
    WHILE head<jsonb_array_length(queue) LOOP
        current_node := queue->head;
        head := head+1;
        current_depth := (current_node->>'depth')::integer;
        IF current_depth>=max_hops THEN CONTINUE; END IF;
        current_kind := current_node->>'kind';
        current_id := current_node->>'id';
        cursor_value := NULL;
        LOOP
            IF pages_read>=64 THEN reason := 'PAGE_LIMIT'; EXIT walk; END IF;
            IF clock_timestamp()>=deadline_at THEN reason := 'DEADLINE'; EXIT walk; END IF;
            request_value := request_base || jsonb_build_object(
                'endpoint_kind',current_kind,'endpoint_id',current_id,
                'snapshot_xid',snapshot_value::text,'limit',20);
            IF cursor_value IS NOT NULL THEN
                request_value := request_value || jsonb_build_object(
                    'after_relation_id',cursor_value->>'relation_id',
                    'after_contribution_id',cursor_value->>'contribution_id',
                    'cursor_filter_digest',page_value->>'filter_digest');
            END IF;
            page_result := __forest_module_schema__.memory_query_relations(context,request_value);
            IF page_result->>'ok' IS DISTINCT FROM 'true' THEN RETURN page_result; END IF;
            page_value := page_result->'value';
            pages_read := pages_read+1;
            FOR edge_value IN SELECT value FROM jsonb_array_elements(page_value->'items') LOOP
                edge_id := edge_value->>'contribution_id';
                IF edge_id=ANY(seen_edges) THEN CONTINUE; END IF;
                IF jsonb_array_length(edges)>=max_edges THEN
                    reason := 'EDGE_LIMIT'; EXIT walk;
                END IF;
                IF edge_value->>'left_kind'=current_kind
                   AND edge_value->>'left_id'=current_id THEN
                    next_kind := edge_value->>'right_kind';
                    next_id := edge_value->>'right_id';
                ELSE
                    next_kind := edge_value->>'left_kind';
                    next_id := edge_value->>'left_id';
                END IF;
                next_key := jsonb_build_array(next_kind,next_id)::text;
                IF NOT next_key=ANY(seen_nodes) AND jsonb_array_length(nodes)>=max_nodes THEN
                    reason := 'NODE_LIMIT'; EXIT walk;
                END IF;
                seen_edges := array_append(seen_edges,edge_id);
                edges := edges || jsonb_build_array(edge_value || jsonb_build_object(
                    'traversal_depth',current_depth+1,'from_kind',current_kind,
                    'from_id',current_id,'evidence',(
                        SELECT COALESCE(jsonb_agg(e.value ORDER BY e.ordinality),'[]'::jsonb)
                        FROM jsonb_array_elements(edge_value->'evidence') WITH ORDINALITY e(value,ordinality)
                        WHERE e.ordinality<=4)));
                IF NOT next_key=ANY(seen_nodes) THEN
                    seen_nodes := array_append(seen_nodes,next_key);
                    current_node := jsonb_build_object(
                        'kind',next_kind,'id',next_id,'depth',current_depth+1);
                    nodes := nodes || jsonb_build_array(current_node);
                    queue := queue || jsonb_build_array(current_node);
                END IF;
            END LOOP;
            EXIT WHEN page_value->>'has_more' IS DISTINCT FROM 'true';
            cursor_value := page_value->'next_cursor';
        END LOOP;
    END LOOP;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'snapshot_xid',snapshot_value::text,'nodes',nodes,'edges',edges,
            'pages_read',pages_read,'truncated',reason IS NOT NULL,
            'truncation_reason',reason));
END;
$$;
