-- Prepare removable Kernel/state content; kernel_payloads_v1 imports and seals
-- these columns in the same transaction. No running process reads old bodies.
DROP TRIGGER bot_state_steps_immutable ON public.bot_state_steps;
DROP TRIGGER bot_state_revisions_immutable ON public.bot_state_revisions;
DROP TRIGGER bot_state_migration_sources_immutable ON public.bot_state_migration_sources;
DROP TRIGGER bot_state_migration_results_immutable ON public.bot_state_migration_results;

ALTER TABLE public.bot_calls ADD COLUMN input_payload_id text,
 ADD COLUMN input_legacy_unknown boolean NOT NULL DEFAULT true,
 ADD COLUMN result_payload_id text, ADD COLUMN result_digest text,
 ADD FOREIGN KEY(bot_id,input_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ADD FOREIGN KEY(bot_id,result_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
ALTER TABLE public.bot_component_state ADD COLUMN value_payload_id text, ADD COLUMN value_digest text,
 ADD FOREIGN KEY(bot_id,value_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
ALTER TABLE public.bot_state_revisions ALTER COLUMN value DROP NOT NULL,
 ADD COLUMN value_payload_id text, ADD COLUMN value_digest text,
 ADD FOREIGN KEY(bot_id,value_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
ALTER TABLE public.bot_state_steps ALTER COLUMN receipt DROP NOT NULL,
 ADD COLUMN receipt_metadata jsonb, ADD COLUMN value_payload_id text, ADD COLUMN value_digest text,
 ADD FOREIGN KEY(bot_id,value_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
ALTER TABLE public.bot_state_migration_sources ALTER COLUMN source_rows DROP NOT NULL,
 ADD COLUMN source_payload_id text,
 ADD FOREIGN KEY(bot_id,source_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
ALTER TABLE public.bot_state_migration_results ALTER COLUMN result_values DROP NOT NULL,
 ADD COLUMN result_payload_id text,
 ADD FOREIGN KEY(bot_id,result_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
