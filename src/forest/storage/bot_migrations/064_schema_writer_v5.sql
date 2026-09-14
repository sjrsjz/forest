-- Shell gate v7 introduces durable Context commands. A generation-4 Host
-- cannot safely claim those requests, so the dispatcher installs this gate in
-- the same migration transaction before publishing the new command storage.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.bot_schema_writer_gate
        WHERE singleton AND minimum_generation >= 5
    ) THEN
        RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
    END IF;
END;
$$;
