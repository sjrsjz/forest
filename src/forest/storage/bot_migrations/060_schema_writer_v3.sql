-- The dispatcher raises the writer gate in the same transaction before any
-- Job body is moved. A pre-060 process can therefore never publish raw JSONB
-- into the post-cutover schema after migration commits.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.bot_schema_writer_gate
        WHERE singleton AND minimum_generation >= 3
    ) THEN
        RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
    END IF;
END;
$$;
