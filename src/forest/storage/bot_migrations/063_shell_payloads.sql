-- Shell routing and priority stay permanent; typed arguments, formatted
-- outcomes, and delivery receipts move behind source-bound payload identities.
DROP TRIGGER bot_shell_source_immutable ON public.bot_shell_requests;
DROP TRIGGER bot_shell_arguments_immutable ON public.bot_shell_requests;

ALTER TABLE public.bot_shell_requests
 DROP CONSTRAINT bot_shell_requests_status_check,
 ALTER COLUMN arguments DROP NOT NULL,
 ADD COLUMN arguments_payload_id text,
 ADD COLUMN arguments_payload_digest text,
 ADD COLUMN arguments_metadata jsonb,
 ADD COLUMN outcome_payload_id text,
 ADD COLUMN outcome_payload_digest text,
 ADD COLUMN outcome_metadata jsonb,
 ADD COLUMN delivery_payload_id text,
 ADD COLUMN delivery_payload_digest text,
 ADD COLUMN delivery_metadata jsonb,
 ADD COLUMN legacy_result_untracked boolean NOT NULL DEFAULT false,
 ADD COLUMN terminal_reason text,
 ADD CHECK(status IN ('PENDING','CLAIMED','COMPLETED','CANCELLED')),
 ADD FOREIGN KEY(bot_id,arguments_payload_id)
  REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ADD FOREIGN KEY(bot_id,outcome_payload_id)
  REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ADD FOREIGN KEY(bot_id,delivery_payload_id)
  REFERENCES public.bot_payload_headers(bot_id,payload_id);
