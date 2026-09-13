-- Only exactly associated control receipts enter this family. Configuration
-- bodies and unrelated audit/outbox events remain explicit later-family work.
DROP TRIGGER bot_audit_immutable ON public.bot_audit_records;
ALTER TABLE public.bot_audit_records ALTER COLUMN details DROP NOT NULL,
 ADD COLUMN control_result_payload_id text,
 ADD COLUMN control_result_payload_digest text,
 ADD COLUMN control_subject_id text,
 ADD FOREIGN KEY(bot_id,control_result_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
ALTER TABLE public.bot_outbox_events ALTER COLUMN payload DROP NOT NULL,
 ADD COLUMN control_result_payload_id text,
 ADD COLUMN control_result_payload_digest text,
 ADD COLUMN control_subject_id text,
 ADD FOREIGN KEY(bot_id,control_result_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
CREATE TABLE public.bot_control_value_sources (
 bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
 family text NOT NULL CHECK(family IN ('config','agent.config','memory.config','binding','job')),
 source_kind text NOT NULL CHECK(source_kind IN ('PLAN','REVISION')),
 source_id text NOT NULL,
 operation_id text,
 payload_id text NOT NULL,
 payload_digest text NOT NULL CHECK(payload_digest ~ '^sha256:[a-f0-9]{64}$'),
 legacy_untracked boolean NOT NULL DEFAULT false,
 PRIMARY KEY(bot_id,family,source_kind,source_id),
 FOREIGN KEY(bot_id,operation_id) REFERENCES public.bot_web_operations(bot_id,operation_id),
 FOREIGN KEY(bot_id,payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 CHECK(legacy_untracked OR operation_id IS NOT NULL)
);
