-- A reset has different retention semantics from compaction. Generation-10
-- writers only know COMPACT and would otherwise execute RESET as a compaction.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.bot_schema_writer_gate
        WHERE singleton AND minimum_generation >= 11
    ) THEN
        RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
    END IF;
END;
$$;

ALTER TABLE public.bot_context_management_requests
    DROP CONSTRAINT bot_context_management_requests_kind_check,
    ADD COLUMN mode text NOT NULL DEFAULT 'SAFE_POINT',
    ADD CONSTRAINT bot_context_management_requests_kind_check
        CHECK(kind IN ('COMPACT','RESET')),
    ADD CONSTRAINT bot_context_management_requests_mode_check
        CHECK(mode='SAFE_POINT');

ALTER TABLE public.bot_context_management_requests
    ALTER COLUMN mode DROP DEFAULT;

CREATE OR REPLACE FUNCTION public.forest_context_management_request_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'CONTEXT_MANAGEMENT_IMMUTABLE' USING ERRCODE='55000';
    END IF;
    IF ROW(NEW.bot_id,NEW.request_id,NEW.operation_id,NEW.conversation_id,
           NEW.kind,NEW.mode,NEW.expected_view_revision,NEW.requested_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.request_id,OLD.operation_id,OLD.conversation_id,
           OLD.kind,OLD.mode,OLD.expected_view_revision,OLD.requested_at) THEN
        RAISE EXCEPTION 'CONTEXT_MANAGEMENT_IDENTITY_IMMUTABLE' USING ERRCODE='55000';
    END IF;
    IF OLD.status <> 'PENDING'
       AND ROW(NEW.status,NEW.applied_view_revision,NEW.terminal_reason,NEW.completed_at)
           IS DISTINCT FROM
           ROW(OLD.status,OLD.applied_view_revision,OLD.terminal_reason,OLD.completed_at) THEN
        RAISE EXCEPTION 'CONTEXT_MANAGEMENT_TERMINAL' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;

ALTER TABLE public.bot_shell_requests
    ADD CONSTRAINT bot_shell_requests_v12_priority_check CHECK (
        gate_version<>'shell-gate-v12' OR (
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
