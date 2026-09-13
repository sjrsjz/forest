-- This SQL and its versioned Python data handler form one atomic migration.
-- The handler installs references and clears old bodies before restoring guards.
DROP TRIGGER bot_context_unit_identity ON public.bot_context_units;
DROP TRIGGER bot_conversation_replies_immutable ON public.bot_conversation_replies;
ALTER TABLE public.bot_context_units ADD COLUMN payload_id text;
ALTER TABLE public.bot_conversation_replies ADD COLUMN payload_id text;
ALTER TABLE public.bot_conversation_replies ALTER COLUMN parts DROP NOT NULL;
ALTER TABLE public.bot_context_units ADD FOREIGN KEY(bot_id,payload_id)
    REFERENCES public.bot_payload_headers(bot_id,payload_id);
ALTER TABLE public.bot_conversation_replies ADD FOREIGN KEY(bot_id,payload_id)
    REFERENCES public.bot_payload_headers(bot_id,payload_id);
