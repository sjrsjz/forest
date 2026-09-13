-- Binding names and settings are content; connection ownership stays metadata.
DROP TRIGGER bot_binding_plans_immutable ON public.bot_binding_plans;
DROP TRIGGER bot_binding_revisions_immutable ON public.bot_binding_revisions;
ALTER TABLE public.bot_bindings ALTER COLUMN display_name DROP NOT NULL,
 ALTER COLUMN config DROP NOT NULL, ALTER COLUMN config DROP DEFAULT,
 ADD COLUMN initial_payload_id text, ADD COLUMN initial_payload_digest text,
 ADD COLUMN config_payload_id text, ADD COLUMN config_payload_digest text,
 ADD FOREIGN KEY(bot_id,initial_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ADD FOREIGN KEY(bot_id,config_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
ALTER TABLE public.bot_binding_plans ALTER COLUMN values DROP NOT NULL,
 ADD COLUMN values_payload_id text, ADD COLUMN values_payload_digest text,
 ADD COLUMN settings_enabled boolean,
 ADD FOREIGN KEY(bot_id,values_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
ALTER TABLE public.bot_binding_revisions ALTER COLUMN values DROP NOT NULL,
 ADD COLUMN values_payload_id text, ADD COLUMN values_payload_digest text,
 ADD COLUMN settings_enabled boolean,
 ADD FOREIGN KEY(bot_id,values_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
