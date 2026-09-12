ALTER TABLE public.bot_message_inbox ADD COLUMN origin text NOT NULL DEFAULT 'platform_input'
    CHECK (origin IN ('platform_input','chat_output','control_output'));
ALTER TABLE public.bot_context_units
    DROP CONSTRAINT bot_context_units_kind_check,
    ADD CONSTRAINT bot_context_units_kind_check CHECK (kind IN ('INPUT','REPLY','CONTROL')),
    ADD COLUMN control_command_id text,
    ADD COLUMN control_parts jsonb,
    ADD UNIQUE (bot_id,control_command_id);
CREATE TABLE public.bot_message_echo_aliases (
    bot_id text NOT NULL,
    event_id text NOT NULL,
    intent_id text NOT NULL,
    attempt_id text NOT NULL,
    platform_message_id text NOT NULL,
    origin text NOT NULL CHECK (origin IN ('chat_output','control_output')),
    reply_id text,
    command_id text,
    detected_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    applied_view_revision bigint,
    PRIMARY KEY (bot_id,event_id),
    FOREIGN KEY (bot_id,event_id) REFERENCES public.bot_message_inbox(bot_id,event_id),
    FOREIGN KEY (bot_id,intent_id) REFERENCES public.bot_send_intents(bot_id,intent_id),
    FOREIGN KEY (bot_id,attempt_id) REFERENCES public.bot_send_attempts(bot_id,attempt_id),
    CHECK ((origin='chat_output' AND reply_id IS NOT NULL AND command_id IS NULL)
        OR (origin='control_output' AND command_id IS NOT NULL AND reply_id IS NULL))
);
CREATE INDEX bot_unresolved_self_echoes ON public.bot_message_inbox(bot_id,platform_message_id)
    WHERE author->>'is_self'='true' AND origin='platform_input';
CREATE FUNCTION public.forest_echo_alias_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.event_id,NEW.intent_id,NEW.attempt_id,NEW.platform_message_id,
           NEW.origin,NEW.reply_id,NEW.command_id)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.event_id,OLD.intent_id,OLD.attempt_id,OLD.platform_message_id,
           OLD.origin,OLD.reply_id,OLD.command_id) THEN
        RAISE EXCEPTION 'Confirmed echo identity is immutable' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER bot_echo_alias_identity BEFORE UPDATE ON public.bot_message_echo_aliases
    FOR EACH ROW EXECUTE FUNCTION public.forest_echo_alias_immutable();
