ALTER TABLE public.bot_conversations
    ADD COLUMN actor_binding_epoch bigint,
    ADD COLUMN actor_policy_revision bigint,
    ADD COLUMN actor_maintenance_epoch bigint,
    ADD COLUMN actor_storage_generation bigint;

CREATE TABLE public.bot_shell_requests (
    bot_id text NOT NULL,
    command_id text NOT NULL,
    event_id text NOT NULL,
    conversation_id text NOT NULL,
    binding_id text NOT NULL,
    command_name text NOT NULL,
    raw_digest text NOT NULL,
    gate_version text NOT NULL DEFAULT 'shell-gate-v1',
    status text NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','CLAIMED','COMPLETED')),
    claim_epoch bigint NOT NULL DEFAULT 0,
    owner_id text,
    host_incarnation text,
    binding_epoch bigint,
    policy_revision bigint,
    expires_at timestamptz,
    operation_id text,
    result_digest text,
    outcome jsonb,
    delivery jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (bot_id,command_id),
    UNIQUE (bot_id,event_id),
    FOREIGN KEY (bot_id,event_id) REFERENCES public.bot_message_inbox(bot_id,event_id),
    FOREIGN KEY (bot_id,conversation_id) REFERENCES public.bot_conversations(bot_id,conversation_id),
    FOREIGN KEY (bot_id,binding_id) REFERENCES public.bot_bindings(bot_id,binding_id)
);
CREATE INDEX bot_shell_pending ON public.bot_shell_requests(bot_id,status,created_at);
CREATE FUNCTION public.forest_shell_source_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.command_id,NEW.event_id,NEW.conversation_id,NEW.binding_id,
           NEW.command_name,NEW.raw_digest,NEW.gate_version)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.command_id,OLD.event_id,OLD.conversation_id,OLD.binding_id,
           OLD.command_name,OLD.raw_digest,OLD.gate_version) THEN
        RAISE EXCEPTION 'Shell source and route are immutable' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER bot_shell_source_immutable BEFORE UPDATE ON public.bot_shell_requests
    FOR EACH ROW EXECUTE FUNCTION public.forest_shell_source_immutable();
