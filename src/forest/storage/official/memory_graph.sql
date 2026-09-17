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
    normalization text NOT NULL CHECK (normalization IN ('L2','NONE')),
    fusion_policy_revision text NOT NULL
        CHECK (octet_length(fusion_policy_revision) BETWEEN 1 AND 256),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (bot_id,initialization_operation_id),
    UNIQUE (bot_id,space_id)
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
    occurred_at timestamptz NOT NULL,
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,source_unit_id),
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
        eligibility_state IN ('REGISTERED','ELIGIBLE','SUPPRESSED')
    ),
    observed_at timestamptz NOT NULL,
    qualified_at timestamptz,
    PRIMARY KEY (bot_id,occurrence_id),
    UNIQUE (bot_id,binding_ref,event_ref),
    UNIQUE (bot_id,conversation_ref,event_ref),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    CHECK ((input_batch_ref IS NULL) = (context_revision IS NULL)),
    CHECK ((eligibility_state='ELIGIBLE') = (input_batch_ref IS NOT NULL))
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
    resulting_eligibility_revision bigint NOT NULL CHECK (resulting_eligibility_revision > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
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
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,contribution_id),
    UNIQUE (bot_id,operation_id),
    UNIQUE (bot_id,relation_id,contribution_id),
    CHECK (ROW(left_kind,left_id) IS DISTINCT FROM ROW(right_kind,right_id))
);

CREATE TABLE relation_evidence (
    bot_id text NOT NULL,
    contribution_id text NOT NULL,
    evidence_index integer NOT NULL CHECK (evidence_index >= 0),
    source_unit_id text NOT NULL,
    source_revision bigint NOT NULL CHECK (source_revision > 0),
    part_id text NOT NULL CHECK (octet_length(part_id) BETWEEN 1 AND 256),
    PRIMARY KEY (bot_id,contribution_id,evidence_index),
    UNIQUE (bot_id,contribution_id,source_unit_id,source_revision,part_id),
    FOREIGN KEY (bot_id,contribution_id)
        REFERENCES relation_contributions(bot_id,contribution_id),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id)
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

CREATE INDEX sources_external_group_idx ON memory_sources(
    bot_id,platform,realm_id,external_group_id,identity_incarnation
);
CREATE UNIQUE INDEX sources_group_identity_uniq ON memory_sources(
    bot_id,platform,realm_id,external_group_id,identity_incarnation
) WHERE source_kind='GROUP';
CREATE INDEX source_units_browse_idx ON source_units(
    bot_id,eligibility_state,occurred_at DESC,source_unit_id DESC
);
CREATE INDEX source_units_trunk_idx ON source_units(
    bot_id,trunk_id,source_order_seq,source_unit_id
);
CREATE INDEX source_units_author_idx ON source_units(
    bot_id,author_subject_ref,occurred_at DESC,source_unit_id DESC
);
CREATE INDEX source_units_modality_idx ON source_units(
    bot_id,modality,occurred_at DESC,source_unit_id DESC
);
CREATE INDEX relation_left_idx ON relation_contributions(
    bot_id,left_kind,left_id,active,relation_id
);
CREATE INDEX relation_right_idx ON relation_contributions(
    bot_id,right_kind,right_id,active,relation_id
);

CREATE FUNCTION reject_immutable_row() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'memory.graph immutable fact cannot change' USING ERRCODE='55000';
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

CREATE FUNCTION guard_source_unit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.source_unit_id,NEW.capture_operation_id,NEW.input_digest,
           NEW.source_id,NEW.trunk_id,NEW.source_order_seq,NEW.source_revision,
           NEW.unit_kind,NEW.modality,NEW.part_count,NEW.content_manifest_digest,
           NEW.author_subject_ref,NEW.author_payload_id,NEW.author_payload_digest,
           NEW.author_owner_component_id,NEW.author_owner_kind,NEW.author_owner_id,
           NEW.accepted_deletion_epoch,NEW.occurred_at,NEW.accepted_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.source_unit_id,OLD.capture_operation_id,OLD.input_digest,
           OLD.source_id,OLD.trunk_id,OLD.source_order_seq,OLD.source_revision,
           OLD.unit_kind,OLD.modality,OLD.part_count,OLD.content_manifest_digest,
           OLD.author_subject_ref,OLD.author_payload_id,OLD.author_payload_digest,
           OLD.author_owner_component_id,OLD.author_owner_kind,OLD.author_owner_id,
           OLD.accepted_deletion_epoch,OLD.occurred_at,OLD.accepted_at) THEN
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
    IF OLD.eligibility_state='REGISTERED' AND NEW.eligibility_state IN ('ELIGIBLE','SUPPRESSED')
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
           NEW.weight_micros,NEW.admitted_deletion_epoch,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.contribution_id,OLD.operation_id,OLD.input_digest,
           OLD.relation_id,OLD.relation_kind,OLD.left_kind,OLD.left_id,
           OLD.right_kind,OLD.right_id,OLD.relation_key,OLD.origin_kind,
           OLD.weight_micros,OLD.admitted_deletion_epoch,OLD.created_at) THEN
        RAISE EXCEPTION 'Memory relation contribution identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.active AND NOT NEW.active THEN RETURN NEW; END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid Memory relation transition' USING ERRCODE='55000';
END;
$$;

CREATE TRIGGER memory_domain_immutable BEFORE UPDATE OR DELETE ON memory_domains
    FOR EACH ROW EXECUTE FUNCTION guard_memory_domain();
CREATE TRIGGER memory_source_guard BEFORE UPDATE OR DELETE ON memory_sources
    FOR EACH ROW EXECUTE FUNCTION guard_memory_source();
CREATE TRIGGER source_unit_guard BEFORE UPDATE OR DELETE ON source_units
    FOR EACH ROW EXECUTE FUNCTION guard_source_unit();
CREATE TRIGGER source_occurrence_guard BEFORE UPDATE OR DELETE ON source_occurrences
    FOR EACH ROW EXECUTE FUNCTION guard_occurrence();
CREATE TRIGGER relation_contribution_guard BEFORE UPDATE OR DELETE ON relation_contributions
    FOR EACH ROW EXECUTE FUNCTION guard_relation_contribution();
CREATE TRIGGER memory_trunks_immutable BEFORE UPDATE OR DELETE ON memory_trunks
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER source_registration_operations_immutable
    BEFORE UPDATE OR DELETE ON source_registration_operations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER source_parts_immutable BEFORE UPDATE OR DELETE ON source_parts
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER capture_operations_immutable BEFORE UPDATE OR DELETE ON capture_operations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER eligibility_operations_immutable BEFORE UPDATE OR DELETE ON eligibility_operations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER source_suppressions_immutable BEFORE UPDATE OR DELETE ON source_suppressions
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER relation_evidence_immutable BEFORE UPDATE OR DELETE ON relation_evidence
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER relation_retractions_immutable BEFORE UPDATE OR DELETE ON relation_retractions
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
