ALTER TABLE public.bot_calls ADD COLUMN agent_tool_id text;
ALTER TABLE public.bot_calls ADD CONSTRAINT bot_call_agent_tool_fk
    FOREIGN KEY (bot_id,agent_tool_id) REFERENCES public.bot_agent_tools(bot_id,operation_id);
ALTER TABLE public.bot_calls ADD CONSTRAINT bot_call_agent_lane
    CHECK (agent_tool_id IS NULL OR admission_lane='DATA');
CREATE FUNCTION public.forest_agent_call_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.agent_tool_id IS DISTINCT FROM OLD.agent_tool_id THEN
        RAISE EXCEPTION 'Forest Agent call authority is immutable' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER bot_agent_call_identity BEFORE UPDATE OF agent_tool_id ON public.bot_calls
    FOR EACH ROW EXECUTE FUNCTION public.forest_agent_call_identity();
