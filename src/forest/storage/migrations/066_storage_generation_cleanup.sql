-- Superseded managed Bot databases use the existing durable cleanup worker.
-- The immutable generation remains as directory history; a separate retirement
-- fact is the logical tombstone that authorizes physical database deletion.
ALTER TABLE storage_cleanup_plans
    DROP CONSTRAINT storage_cleanup_plans_resource_kind_check;
ALTER TABLE storage_cleanup_plans
    ADD CHECK(resource_kind IN (
        'BACKUP_ARTIFACT','RESTORE_STAGE','BOT_STORAGE_GENERATION'
    ));

ALTER TABLE storage_cleanup_jobs
    DROP CONSTRAINT storage_cleanup_jobs_resource_kind_check;
ALTER TABLE storage_cleanup_jobs
    ADD CHECK(resource_kind IN (
        'BACKUP_ARTIFACT','RESTORE_STAGE','BOT_STORAGE_GENERATION'
    ));

CREATE TABLE bot_storage_generation_retirements (
    bot_id text NOT NULL,
    storage_generation bigint NOT NULL CHECK(storage_generation>0),
    cleanup_id text NOT NULL UNIQUE REFERENCES storage_cleanup_jobs(cleanup_id),
    retired_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,storage_generation),
    FOREIGN KEY(bot_id,storage_generation)
        REFERENCES bot_storage_generations(bot_id,storage_generation)
);

CREATE FUNCTION forest_storage_generation_retirement_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'Storage generation retirement is immutable'
            USING ERRCODE='55000';
    END IF;
    IF NOT EXISTS(
        SELECT 1 FROM storage_cleanup_jobs c
        WHERE c.cleanup_id=NEW.cleanup_id
          AND c.resource_kind='BOT_STORAGE_GENERATION'
          AND c.resource_id=NEW.bot_id||':'||NEW.storage_generation::text
    ) THEN
        RAISE EXCEPTION 'Storage generation retirement requires its cleanup job'
            USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER bot_storage_generation_retirement_guard
    BEFORE INSERT OR UPDATE OR DELETE ON bot_storage_generation_retirements
    FOR EACH ROW EXECUTE FUNCTION forest_storage_generation_retirement_guard();

CREATE INDEX bot_storage_generation_retirements_cleanup
    ON bot_storage_generation_retirements(cleanup_id);
