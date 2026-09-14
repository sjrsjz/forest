-- New Shell admissions bind the requested service class to the verified
-- principal and exact effective Binding configuration used for P0 admission.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.bot_schema_writer_gate
        WHERE singleton AND minimum_generation >= 8
    ) THEN
        RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
    END IF;
END;
$$;

ALTER TABLE public.bot_shell_requests
    ADD COLUMN requested_priority smallint,
    ADD COLUMN priority_eligible boolean NOT NULL DEFAULT false,
    ADD COLUMN priority_policy text NOT NULL DEFAULT 'legacy-v1',
    ADD COLUMN priority_basis_binding_epoch bigint,
    ADD COLUMN priority_basis_config_revision bigint;

-- The existing terminal guard rejects every column change after completion.
-- Migration owns the database after installing writer generation 8, and only
-- copies the already immutable legacy priority into its new audit column.
ALTER TABLE public.bot_shell_requests DISABLE TRIGGER bot_shell_result_guard;
UPDATE public.bot_shell_requests SET requested_priority=priority;
ALTER TABLE public.bot_shell_requests ENABLE TRIGGER bot_shell_result_guard;

ALTER TABLE public.bot_shell_requests
    ALTER COLUMN requested_priority SET NOT NULL,
    ALTER COLUMN requested_priority SET DEFAULT 1,
    ADD CHECK(requested_priority BETWEEN 0 AND 2),
    ADD CHECK(NOT priority_eligible OR requested_priority=0),
    ADD CHECK(
        gate_version<>'shell-gate-v10' OR (
            priority_policy='current-binding-admin-v1'
            AND priority_basis_binding_epoch IS NOT NULL
            AND priority_basis_binding_epoch>=1
            AND priority_basis_config_revision IS NOT NULL
            AND priority_basis_config_revision>=1
            AND priority_eligible=(requested_priority=0 AND priority=0)
            AND priority=(CASE
                WHEN requested_priority=0 AND priority_eligible THEN 0
                WHEN requested_priority=0 THEN 1
                ELSE requested_priority
            END)
        )
    );

CREATE OR REPLACE FUNCTION public.forest_shell_arguments_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF ROW(NEW.arguments,NEW.priority,NEW.arguments_payload_id,NEW.arguments_payload_digest,
     NEW.arguments_metadata,NEW.requested_priority,NEW.priority_eligible,NEW.priority_policy,
     NEW.priority_basis_binding_epoch,NEW.priority_basis_config_revision)
  IS DISTINCT FROM ROW(OLD.arguments,OLD.priority,OLD.arguments_payload_id,OLD.arguments_payload_digest,
     OLD.arguments_metadata,OLD.requested_priority,OLD.priority_eligible,OLD.priority_policy,
     OLD.priority_basis_binding_epoch,OLD.priority_basis_config_revision) THEN
  RAISE EXCEPTION 'Shell arguments and priority basis are immutable' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;
