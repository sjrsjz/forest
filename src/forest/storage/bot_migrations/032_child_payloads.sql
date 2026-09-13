-- Prepare immutable Child identities for the same-transaction content handler.
DROP TRIGGER bot_child_identity ON public.bot_agent_children;
DROP TRIGGER bot_child_attempt_identity ON public.bot_child_model_attempts;
DROP TRIGGER bot_child_result_once ON public.bot_agent_children;
DROP TRIGGER bot_child_attempt_result_once ON public.bot_child_model_attempts;
ALTER TABLE public.bot_agent_children
 ALTER COLUMN snapshot DROP NOT NULL, ALTER COLUMN spec DROP NOT NULL,
 ADD COLUMN snapshot_payload_id text, ADD COLUMN spec_payload_id text,
 ADD COLUMN result_payload_id text, ADD COLUMN spec_metadata jsonb NOT NULL DEFAULT '{}';
ALTER TABLE public.bot_child_model_attempts
 ALTER COLUMN request_body DROP NOT NULL, ALTER COLUMN manifest DROP NOT NULL,
 ADD COLUMN request_payload_id text, ADD COLUMN manifest_payload_id text,
 ADD COLUMN result_payload_id text, ADD COLUMN result_metadata jsonb;
