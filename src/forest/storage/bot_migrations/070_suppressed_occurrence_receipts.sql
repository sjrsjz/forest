-- A source that is already forgotten can still receive an authenticated,
-- replay-stable metadata ACK without recreating any message or media body.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.bot_schema_writer_gate
        WHERE singleton AND minimum_generation >= 10
    ) THEN
        RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
    END IF;
END;
$$;

CREATE TABLE public.bot_suppressed_input_occurrences (
    bot_id text NOT NULL,
    receipt_id text NOT NULL CHECK(length(receipt_id) BETWEEN 1 AND 256),
    event_id text NOT NULL CHECK(length(event_id) BETWEEN 1 AND 256),
    occurrence_id text NOT NULL CHECK(length(occurrence_id) BETWEEN 1 AND 256),
    source_event_id text NOT NULL CHECK(length(source_event_id) BETWEEN 1 AND 256),
    source_unit_id text NOT NULL CHECK(length(source_unit_id) BETWEEN 1 AND 256),
    conversation_id text NOT NULL CHECK(length(conversation_id) BETWEEN 1 AND 256),
    binding_id text NOT NULL,
    first_binding_epoch bigint NOT NULL CHECK(first_binding_epoch>0),
    platform text NOT NULL CHECK(length(platform) BETWEEN 1 AND 64),
    channel_kind text NOT NULL CHECK(channel_kind IN ('group','private')),
    channel_id text NOT NULL CHECK(length(channel_id) BETWEEN 1 AND 256),
    thread_id text NOT NULL CHECK(length(thread_id)<=256),
    platform_message_id text NOT NULL CHECK(length(platform_message_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK(input_digest ~ '^sha256:[0-9a-f]{64}$'),
    observed_at timestamptz NOT NULL,
    suppression_source_unit_id text NOT NULL CHECK(
        length(suppression_source_unit_id) BETWEEN 1 AND 256
    ),
    suppression_part_id text NOT NULL CHECK(length(suppression_part_id) BETWEEN 1 AND 256),
    suppression_operation_id text NOT NULL,
    suppression_deletion_epoch bigint NOT NULL CHECK(suppression_deletion_epoch>0),
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,receipt_id),
    UNIQUE(bot_id,event_id),
    UNIQUE(bot_id,occurrence_id),
    FOREIGN KEY(bot_id,binding_id)
        REFERENCES public.bot_bindings(bot_id,binding_id),
    FOREIGN KEY(bot_id,suppression_operation_id)
        REFERENCES public.bot_payload_forget_requests(bot_id,operation_id)
);

CREATE FUNCTION public.forest_suppressed_occurrence_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.bot_payload_suppression x
        WHERE x.bot_id=NEW.bot_id
          AND x.source_unit_id=NEW.suppression_source_unit_id
          AND x.part_id=NEW.suppression_part_id
          AND x.operation_id=NEW.suppression_operation_id
          AND x.deletion_epoch=NEW.suppression_deletion_epoch
    ) OR NOT EXISTS (
        SELECT 1
        FROM public.forest_source_dependency_closure(
            NEW.bot_id,
            jsonb_build_array(jsonb_build_object(
                'source_unit_id',NEW.source_unit_id,
                'source_revision',1,
                'part_id','*'
            )),
            4096
        ) d
        WHERE d.source_unit_id=NEW.suppression_source_unit_id
          AND (d.part_id='*' OR NEW.suppression_part_id='*'
               OR d.part_id=NEW.suppression_part_id)
    ) THEN
        RAISE EXCEPTION 'SUPPRESSED_OCCURRENCE_PROOF_INVALID' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER bot_suppressed_occurrence_proof
    BEFORE INSERT ON public.bot_suppressed_input_occurrences
    FOR EACH ROW EXECUTE FUNCTION public.forest_suppressed_occurrence_guard();
CREATE TRIGGER bot_suppressed_input_occurrences_immutable
    BEFORE UPDATE OR DELETE ON public.bot_suppressed_input_occurrences
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
