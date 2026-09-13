-- 045's frozen data handler installs complete references before final guards.
DROP TRIGGER bot_agent_config_plans_immutable ON public.bot_agent_config_plans;
DROP TRIGGER bot_agent_config_revisions_immutable ON public.bot_agent_config_revisions;
DROP TRIGGER bot_agent_run_identity ON public.bot_agent_runs;
ALTER TABLE public.bot_agent_config_plans ALTER COLUMN values DROP NOT NULL,
 ADD COLUMN values_payload_id text,
 ADD COLUMN values_payload_digest text,
 ADD COLUMN values_metadata jsonb,
 ADD FOREIGN KEY(bot_id,values_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
ALTER TABLE public.bot_agent_config_revisions ALTER COLUMN values DROP NOT NULL,
 ADD COLUMN values_payload_id text,
 ADD COLUMN values_payload_digest text,
 ADD COLUMN values_metadata jsonb,
 ADD FOREIGN KEY(bot_id,values_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
ALTER TABLE public.bot_agent_runs ALTER COLUMN spec DROP NOT NULL,
 ADD COLUMN spec_payload_id text,
 ADD COLUMN spec_payload_digest text,
 ADD COLUMN spec_metadata jsonb,
 ADD COLUMN spec_imported_legacy boolean NOT NULL DEFAULT false,
 ADD FOREIGN KEY(bot_id,spec_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
