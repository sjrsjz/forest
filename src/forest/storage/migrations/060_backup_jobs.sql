-- Durable encrypted backup jobs. Archive keys are encrypted by the deployment
-- vault; recovery-passphrase wrapping lives only in the non-secret archive header.
CREATE TABLE backup_jobs (
    backup_id text PRIMARY KEY,
    subject_id text NOT NULL REFERENCES web_accounts(id),
    idempotency_key text NOT NULL,
    input_digest text NOT NULL,
    include_packages boolean NOT NULL,
    state text NOT NULL DEFAULT 'QUEUED'
        CHECK (state IN ('QUEUED','CAPTURING','SEALING','READY','INCONSISTENT','FAILED','EXPIRED')),
    archive_key_ciphertext text NOT NULL,
    archive_header jsonb NOT NULL,
    owner_id text,
    owner_epoch bigint NOT NULL DEFAULT 0 CHECK (owner_epoch >= 0),
    lease_expires_at timestamptz,
    database_total integer NOT NULL DEFAULT 0 CHECK (database_total >= 0),
    database_completed integer NOT NULL DEFAULT 0 CHECK (
        database_completed >= 0 AND database_completed <= database_total
    ),
    roster_start_digest text,
    roster_end_digest text,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(subject_id,idempotency_key),
    CHECK ((state IN ('CAPTURING','SEALING')) = (owner_id IS NOT NULL)),
    CHECK ((owner_id IS NULL) = (lease_expires_at IS NULL)),
    CHECK (state <> 'FAILED' OR error_code IS NOT NULL)
);
CREATE INDEX backup_jobs_resume_idx ON backup_jobs(state,lease_expires_at,created_at);
CREATE UNIQUE INDEX backup_jobs_single_capture ON backup_jobs ((true))
    WHERE state IN ('CAPTURING','SEALING');

CREATE TABLE backup_database_cuts (
    backup_id text NOT NULL REFERENCES backup_jobs(backup_id),
    database_scope text NOT NULL CHECK (database_scope IN ('deployment','bot')),
    bot_id text NOT NULL DEFAULT '',
    identity_digest text NOT NULL,
    migration_digest text NOT NULL,
    transaction_snapshot text NOT NULL,
    wal_lsn text NOT NULL,
    deletion_epoch bigint,
    member_count integer NOT NULL CHECK (member_count >= 0),
    byte_size bigint NOT NULL CHECK (byte_size >= 0),
    content_digest text NOT NULL,
    captured_at timestamptz NOT NULL,
    PRIMARY KEY(backup_id,database_scope,bot_id),
    CHECK ((database_scope='deployment')=(bot_id = '')),
    CHECK ((database_scope='bot')=(deletion_epoch IS NOT NULL))
);
CREATE TRIGGER backup_database_cuts_immutable BEFORE UPDATE OR DELETE ON backup_database_cuts
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();

CREATE TABLE backup_artifacts (
    backup_id text PRIMARY KEY REFERENCES backup_jobs(backup_id),
    relative_path text NOT NULL UNIQUE,
    ciphertext_digest text NOT NULL,
    byte_size bigint NOT NULL CHECK (byte_size > 0),
    manifest_digest text NOT NULL,
    ready_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    deleted_at timestamptz,
    CHECK (expires_at > ready_at)
);

CREATE TABLE backup_events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    backup_id text NOT NULL REFERENCES backup_jobs(backup_id),
    owner_epoch bigint NOT NULL,
    step text NOT NULL,
    details jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER backup_events_immutable BEFORE UPDATE OR DELETE ON backup_events
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();

CREATE FUNCTION forest_backup_job_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.backup_id,NEW.subject_id,NEW.idempotency_key,NEW.input_digest,
           NEW.include_packages,NEW.archive_key_ciphertext,NEW.archive_header,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.backup_id,OLD.subject_id,OLD.idempotency_key,OLD.input_digest,
           OLD.include_packages,OLD.archive_key_ciphertext,OLD.archive_header,OLD.created_at) THEN
        RAISE EXCEPTION 'Backup job identity is immutable' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER backup_job_identity BEFORE UPDATE OR DELETE ON backup_jobs
    FOR EACH ROW EXECUTE FUNCTION forest_backup_job_identity();

CREATE FUNCTION forest_backup_job_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' OR OLD.state IN ('FAILED','EXPIRED')
       OR OLD.state IN ('READY','INCONSISTENT') AND NEW.state<>'EXPIRED' THEN
        RAISE EXCEPTION 'Terminal backup job is immutable' USING ERRCODE='55000';
    END IF;
    IF NOT (
        OLD.state='QUEUED' AND NEW.state IN ('CAPTURING','FAILED')
        OR OLD.state='CAPTURING' AND NEW.state IN ('CAPTURING','SEALING','FAILED')
        OR OLD.state='SEALING' AND NEW.state IN ('CAPTURING','READY','INCONSISTENT','FAILED')
        OR OLD.state IN ('READY','INCONSISTENT') AND NEW.state='EXPIRED'
    ) THEN
        RAISE EXCEPTION 'Invalid backup job state transition' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER backup_job_transition BEFORE UPDATE OR DELETE ON backup_jobs
    FOR EACH ROW EXECUTE FUNCTION forest_backup_job_transition();

CREATE FUNCTION forest_backup_artifact_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE'
       OR ROW(NEW.backup_id,NEW.relative_path,NEW.ciphertext_digest,NEW.byte_size,
              NEW.manifest_digest,NEW.ready_at,NEW.expires_at)
          IS DISTINCT FROM
          ROW(OLD.backup_id,OLD.relative_path,OLD.ciphertext_digest,OLD.byte_size,
              OLD.manifest_digest,OLD.ready_at,OLD.expires_at)
       OR OLD.deleted_at IS NOT NULL AND NEW.deleted_at IS DISTINCT FROM OLD.deleted_at THEN
        RAISE EXCEPTION 'Backup artifact identity is immutable' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER backup_artifact_guard BEFORE UPDATE OR DELETE ON backup_artifacts
    FOR EACH ROW EXECUTE FUNCTION forest_backup_artifact_guard();
