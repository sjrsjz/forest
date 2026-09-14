-- Durable verification and restore into isolated, unregistered databases.
CREATE TABLE restore_stage_jobs (
    restore_id text PRIMARY KEY,
    backup_id text NOT NULL REFERENCES backup_artifacts(backup_id),
    subject_id text NOT NULL REFERENCES web_accounts(id),
    idempotency_key text NOT NULL,
    input_digest text NOT NULL,
    artifact_digest text NOT NULL,
    archive_key_ciphertext text NOT NULL,
    state text NOT NULL DEFAULT 'QUEUED' CHECK (
        state IN ('QUEUED','VERIFYING','RESTORING','VALIDATING','STAGED','FAILED','EXPIRED')
    ),
    owner_id text,
    owner_epoch bigint NOT NULL DEFAULT 0 CHECK(owner_epoch>=0),
    lease_expires_at timestamptz,
    database_total integer NOT NULL CHECK(database_total BETWEEN 1 AND 1024),
    database_completed integer NOT NULL DEFAULT 0 CHECK(
        database_completed>=0 AND database_completed<=database_total
    ),
    manifest_digest text,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(subject_id,idempotency_key),
    CHECK ((state IN ('VERIFYING','RESTORING','VALIDATING'))=(owner_id IS NOT NULL)),
    CHECK ((owner_id IS NULL)=(lease_expires_at IS NULL)),
    CHECK (state<>'FAILED' OR error_code IS NOT NULL)
);
CREATE UNIQUE INDEX restore_stage_single_active ON restore_stage_jobs ((true))
    WHERE state IN ('VERIFYING','RESTORING','VALIDATING');
CREATE INDEX restore_stage_resume ON restore_stage_jobs(state,lease_expires_at,created_at);

CREATE TABLE restore_stage_targets (
    restore_id text NOT NULL REFERENCES restore_stage_jobs(restore_id),
    ordinal integer NOT NULL CHECK(ordinal BETWEEN 0 AND 1023),
    database_scope text NOT NULL CHECK(database_scope IN ('deployment','bot')),
    bot_id text NOT NULL DEFAULT '',
    database_name text NOT NULL UNIQUE,
    content_digest text NOT NULL,
    migration_digest text NOT NULL,
    deletion_epoch bigint,
    state text NOT NULL DEFAULT 'PLANNED' CHECK(
        state IN ('PLANNED','CREATING','CREATED','VALIDATED','STAGED','FAILED')
    ),
    restored_rows bigint CHECK(restored_rows IS NULL OR restored_rows>=0),
    quarantine_digest text,
    error_code text,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(restore_id,ordinal),
    UNIQUE(restore_id,database_scope,bot_id),
    CHECK ((database_scope='deployment')=(bot_id='')),
    CHECK ((database_scope='bot')=(deletion_epoch IS NOT NULL))
);

CREATE TABLE restore_stage_events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    restore_id text NOT NULL REFERENCES restore_stage_jobs(restore_id),
    owner_epoch bigint NOT NULL,
    step text NOT NULL,
    details jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER restore_stage_events_immutable BEFORE UPDATE OR DELETE ON restore_stage_events
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();

CREATE FUNCTION forest_restore_job_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE'
       OR ROW(NEW.restore_id,NEW.backup_id,NEW.subject_id,NEW.idempotency_key,
              NEW.input_digest,NEW.artifact_digest,NEW.archive_key_ciphertext,
              NEW.database_total,NEW.created_at)
          IS DISTINCT FROM
          ROW(OLD.restore_id,OLD.backup_id,OLD.subject_id,OLD.idempotency_key,
              OLD.input_digest,OLD.artifact_digest,OLD.archive_key_ciphertext,
              OLD.database_total,OLD.created_at) THEN
        RAISE EXCEPTION 'Restore job identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state IN ('STAGED','FAILED','EXPIRED') THEN
        RAISE EXCEPTION 'Terminal restore job is immutable' USING ERRCODE='55000';
    END IF;
    IF NOT (
        OLD.state='QUEUED' AND NEW.state IN ('VERIFYING','FAILED')
        OR OLD.state='VERIFYING' AND NEW.state IN ('VERIFYING','RESTORING','FAILED')
        OR OLD.state='RESTORING' AND NEW.state IN ('VERIFYING','RESTORING','VALIDATING','FAILED')
        OR OLD.state='VALIDATING' AND NEW.state IN ('VERIFYING','VALIDATING','STAGED','FAILED')
    ) THEN
        RAISE EXCEPTION 'Invalid restore job state transition' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER restore_stage_job_guard BEFORE UPDATE OR DELETE ON restore_stage_jobs
    FOR EACH ROW EXECUTE FUNCTION forest_restore_job_identity();

CREATE FUNCTION forest_restore_target_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE'
       OR ROW(NEW.restore_id,NEW.ordinal,NEW.database_scope,NEW.bot_id,
              NEW.database_name,NEW.content_digest,NEW.migration_digest,NEW.deletion_epoch)
          IS DISTINCT FROM
          ROW(OLD.restore_id,OLD.ordinal,OLD.database_scope,OLD.bot_id,
              OLD.database_name,OLD.content_digest,OLD.migration_digest,OLD.deletion_epoch)
       OR OLD.state IN ('STAGED','FAILED') THEN
        RAISE EXCEPTION 'Restore target identity or terminal state is immutable' USING ERRCODE='55000';
    END IF;
    IF NOT (
        OLD.state='PLANNED' AND NEW.state IN ('CREATING','FAILED')
        OR OLD.state='CREATING' AND NEW.state IN ('CREATING','CREATED','FAILED')
        OR OLD.state='CREATED' AND NEW.state IN ('CREATED','VALIDATED','FAILED')
        OR OLD.state='VALIDATED' AND NEW.state IN ('CREATED','STAGED','FAILED')
    ) THEN
        RAISE EXCEPTION 'Invalid restore target state transition' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER restore_stage_target_guard BEFORE UPDATE OR DELETE ON restore_stage_targets
    FOR EACH ROW EXECUTE FUNCTION forest_restore_target_guard();
