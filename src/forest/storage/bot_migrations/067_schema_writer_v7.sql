-- Shell gate v9 adds fixed Core rescue spellings and P0 status admission. A
-- generation-6 Host must not claim requests whose durable priority it cannot
-- interpret or execute under the new grammar.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.bot_schema_writer_gate
        WHERE singleton AND minimum_generation >= 7
    ) THEN
        RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
    END IF;
END;
$$;
