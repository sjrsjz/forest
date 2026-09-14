-- Resumable, bounded admission of an encrypted Forest backup downloaded from
-- another installation. Plaintext and passphrases never enter this ledger.
CREATE TABLE backup_import_jobs (
    import_id text PRIMARY KEY,
    subject_id text NOT NULL REFERENCES web_accounts(id),
    idempotency_key text NOT NULL,
    input_digest text NOT NULL,
    expected_size bigint NOT NULL CHECK(expected_size BETWEEN 1 AND 68719493184),
    state text NOT NULL DEFAULT 'RESERVED' CHECK(state IN (
        'RESERVED','UPLOADING','UPLOADED','QUEUED','IMPORTING','IMPORTED','FAILED','EXPIRED'
    )),
    upload_path text,
    ciphertext_digest text,
    byte_size bigint CHECK(byte_size IS NULL OR byte_size=expected_size),
    passphrase_digest text,
    archive_key_ciphertext text,
    backup_id text,
    manifest_digest text,
    database_total integer CHECK(database_total IS NULL OR database_total BETWEEN 1 AND 1024),
    owner_id text,
    owner_epoch bigint NOT NULL DEFAULT 0 CHECK(owner_epoch>=0),
    lease_expires_at timestamptz,
    error_code text,
    expires_at timestamptz NOT NULL DEFAULT clock_timestamp()+interval '24 hours',
    imported_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(subject_id,idempotency_key),
    UNIQUE(upload_path),
    CHECK((state IN ('UPLOADING','IMPORTING'))=(owner_id IS NOT NULL)),
    CHECK((owner_id IS NULL)=(lease_expires_at IS NULL)),
    CHECK(state NOT IN ('UPLOADED','QUEUED','IMPORTING','IMPORTED') OR (
        upload_path IS NOT NULL AND ciphertext_digest IS NOT NULL AND byte_size IS NOT NULL
    )),
    CHECK(state NOT IN ('QUEUED','IMPORTING','IMPORTED') OR (
        passphrase_digest IS NOT NULL AND archive_key_ciphertext IS NOT NULL
        AND backup_id IS NOT NULL
    )),
    CHECK((state='IMPORTED')=(imported_at IS NOT NULL)),
    CHECK((state='FAILED')=(error_code IS NOT NULL)),
    CHECK(expires_at>created_at)
);
CREATE INDEX backup_import_resume
    ON backup_import_jobs(state,lease_expires_at,created_at);

CREATE TABLE backup_import_events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    import_id text NOT NULL REFERENCES backup_import_jobs(import_id),
    owner_epoch bigint NOT NULL,
    step text NOT NULL,
    details jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER backup_import_events_immutable
    BEFORE UPDATE OR DELETE ON backup_import_events
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();

CREATE TABLE backup_import_cleanup_receipts (
    cleanup_id text PRIMARY KEY,
    import_id text NOT NULL UNIQUE REFERENCES backup_import_jobs(import_id),
    upload_path text NOT NULL,
    ciphertext_digest text NOT NULL,
    details jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER backup_import_cleanup_receipts_immutable
    BEFORE UPDATE OR DELETE ON backup_import_cleanup_receipts
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();

CREATE FUNCTION forest_backup_import_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE'
       OR ROW(NEW.import_id,NEW.subject_id,NEW.idempotency_key,NEW.input_digest,
              NEW.expected_size,NEW.expires_at,NEW.created_at)
          IS DISTINCT FROM
          ROW(OLD.import_id,OLD.subject_id,OLD.idempotency_key,OLD.input_digest,
              OLD.expected_size,OLD.expires_at,OLD.created_at)
       OR OLD.state IN ('IMPORTED','FAILED','EXPIRED')
       OR NEW.owner_epoch<OLD.owner_epoch THEN
        RAISE EXCEPTION 'Backup import identity or terminal state is immutable'
            USING ERRCODE='55000';
    END IF;
    IF OLD.state IN ('UPLOADED','QUEUED','IMPORTING')
       AND ROW(NEW.upload_path,NEW.ciphertext_digest,NEW.byte_size)
           IS DISTINCT FROM ROW(OLD.upload_path,OLD.ciphertext_digest,OLD.byte_size) THEN
        RAISE EXCEPTION 'Uploaded backup import identity is immutable'
            USING ERRCODE='55000';
    END IF;
    IF OLD.state IN ('QUEUED','IMPORTING')
       AND ROW(NEW.passphrase_digest,NEW.archive_key_ciphertext,NEW.backup_id)
           IS DISTINCT FROM ROW(OLD.passphrase_digest,OLD.archive_key_ciphertext,OLD.backup_id) THEN
        RAISE EXCEPTION 'Finalized backup import identity is immutable'
            USING ERRCODE='55000';
    END IF;
    IF NOT (
        OLD.state='RESERVED' AND NEW.state IN ('UPLOADING','EXPIRED')
        OR OLD.state='UPLOADING' AND NEW.state IN ('UPLOADING','RESERVED','UPLOADED')
        OR OLD.state='UPLOADED' AND NEW.state IN ('QUEUED','EXPIRED')
        OR OLD.state='QUEUED' AND NEW.state IN ('IMPORTING','FAILED')
        OR OLD.state='IMPORTING' AND NEW.state IN ('IMPORTING','IMPORTED','FAILED')
    ) THEN
        RAISE EXCEPTION 'Invalid backup import transition' USING ERRCODE='55000';
    END IF;
    IF NEW.owner_epoch=OLD.owner_epoch+1 THEN
        IF NEW.state NOT IN ('UPLOADING','IMPORTING') THEN
            RAISE EXCEPTION 'Backup import claim epoch is invalid' USING ERRCODE='55000';
        END IF;
    ELSIF NEW.owner_epoch<>OLD.owner_epoch THEN
        RAISE EXCEPTION 'Backup import claim epoch is invalid' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER backup_import_guard
    BEFORE UPDATE OR DELETE ON backup_import_jobs
    FOR EACH ROW EXECUTE FUNCTION forest_backup_import_guard();
