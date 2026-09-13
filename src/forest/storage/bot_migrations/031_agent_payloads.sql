-- Preparation for the transaction-local agent_payloads_v1 data migration.
-- The handler imports exact legacy bodies and installs permanent constraints.
DROP TRIGGER bot_agent_step_identity ON public.bot_agent_steps;
DROP TRIGGER bot_agent_step_output_once ON public.bot_agent_steps;
DROP TRIGGER bot_model_manifests_immutable ON public.bot_model_request_manifests;
DROP TRIGGER bot_model_attempt_identity ON public.bot_model_transport_attempts;
DROP TRIGGER bot_model_observation_once ON public.bot_model_transport_attempts;
DROP TRIGGER bot_agent_tool_identity ON public.bot_agent_tools;
DROP TRIGGER bot_agent_tool_observe ON public.bot_agent_tools;
DROP TRIGGER bot_agent_tool_observations_immutable ON public.bot_agent_tool_observations;

ALTER TABLE public.bot_agent_steps ALTER COLUMN snapshot DROP NOT NULL,
 ADD COLUMN snapshot_payload_id text,
 ADD COLUMN output_payload_id text,
 ADD COLUMN closed_parts_payload_id text;
ALTER TABLE public.bot_model_request_manifests ALTER COLUMN manifest DROP NOT NULL,
 ADD COLUMN manifest_payload_id text;
ALTER TABLE public.bot_model_transport_attempts ALTER COLUMN request_body DROP NOT NULL,
 ADD COLUMN request_payload_id text,
 ADD COLUMN result_payload_id text,
 ADD COLUMN result_metadata jsonb NOT NULL DEFAULT '{}';
ALTER TABLE public.bot_agent_tools ALTER COLUMN arguments DROP NOT NULL,
 ADD COLUMN arguments_payload_id text,
 ADD COLUMN result_payload_id text,
 ADD COLUMN result_outcome text,
 ADD COLUMN result_metadata jsonb NOT NULL DEFAULT '{}';
ALTER TABLE public.bot_agent_tool_observations ALTER COLUMN observation DROP NOT NULL,
 ADD COLUMN observation_payload_id text,
 ADD COLUMN observation_refs jsonb NOT NULL DEFAULT '{}';
