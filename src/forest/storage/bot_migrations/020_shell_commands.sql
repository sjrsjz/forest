ALTER TABLE public.bot_shell_requests
    ADD COLUMN arguments jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN priority smallint NOT NULL DEFAULT 1 CHECK (priority BETWEEN 0 AND 2);

CREATE FUNCTION public.forest_shell_arguments_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.arguments,NEW.priority) IS DISTINCT FROM ROW(OLD.arguments,OLD.priority) THEN
        RAISE EXCEPTION 'Shell arguments and priority are immutable' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER bot_shell_arguments_immutable BEFORE UPDATE ON public.bot_shell_requests
    FOR EACH ROW EXECUTE FUNCTION public.forest_shell_arguments_immutable();
