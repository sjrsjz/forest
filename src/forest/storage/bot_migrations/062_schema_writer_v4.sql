-- The dispatcher raises the writer gate before a Shell JSONB column becomes
-- payload-only. A generation-3 process cannot publish a half-migrated request.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.bot_schema_writer_gate
        WHERE singleton AND minimum_generation >= 4
    ) THEN
        RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
    END IF;
END;
$$;
