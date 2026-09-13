-- Prepare schema; the hashed input_payloads_v1 Python handler imports all
-- legacy content and installs final constraints in this same transaction.
ALTER TABLE public.bot_message_inbox ADD COLUMN payload_id text,
 ADD FOREIGN KEY(bot_id,payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ALTER COLUMN segments DROP NOT NULL;
ALTER TABLE public.bot_inbound_media_refs ADD COLUMN source_payload_id text,
 ADD FOREIGN KEY(bot_id,source_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ALTER COLUMN source DROP NOT NULL;
ALTER TABLE public.bot_media_captures ADD COLUMN payload_id text, ADD COLUMN manifest_payload_id text,
 ADD FOREIGN KEY(bot_id,payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ADD FOREIGN KEY(bot_id,manifest_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ALTER COLUMN content DROP NOT NULL, ALTER COLUMN source_manifest DROP NOT NULL;
DROP TRIGGER bot_message_source_immutable ON public.bot_message_inbox;
DROP TRIGGER bot_media_source_immutable ON public.bot_inbound_media_refs;
DROP TRIGGER bot_media_captures_immutable ON public.bot_media_captures;
