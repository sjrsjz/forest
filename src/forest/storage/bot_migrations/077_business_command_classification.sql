-- A temporary registry/policy read failure remains pending until a fixed deadline.
DO $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.bot_schema_writer_gate
               WHERE singleton AND minimum_generation>=16) THEN
  RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
 END IF;
END;
$$;

ALTER TABLE public.bot_message_inbox
 ADD COLUMN business_classification_pending boolean NOT NULL DEFAULT false,
 ADD COLUMN business_fallback_decision text
  CHECK(business_fallback_decision IN ('PENDING','TRIGGER','NO_TRIGGER')),
 ADD COLUMN business_fallback_rule_revision text,
 ADD COLUMN business_fallback_reason text,
 ADD COLUMN business_classification_attempt_count integer NOT NULL DEFAULT 0
  CHECK(business_classification_attempt_count>=0),
 ADD COLUMN business_classification_next_attempt_at timestamptz,
 ADD COLUMN business_classification_error_code text,
 ADD CHECK(business_classification_error_code IS NULL OR
           business_classification_error_code ~ '^[A-Z][A-Z0-9_]{0,95}$'),
 ADD CHECK(NOT business_classification_pending OR
  (trigger_decision='PENDING' AND business_fallback_decision IS NOT NULL
   AND business_fallback_rule_revision IS NOT NULL
   AND business_fallback_reason IS NOT NULL
   AND business_classification_next_attempt_at IS NOT NULL
   AND business_classification_error_code IS NOT NULL));
CREATE INDEX bot_business_classification_pending
 ON public.bot_message_inbox(bot_id,business_classification_next_attempt_at,classification_deadline)
 WHERE trigger_decision='PENDING' AND business_classification_pending;

CREATE OR REPLACE FUNCTION public.forest_conversation_notice_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE inbox record; request record; conversation record;
BEGIN
 IF TG_OP='DELETE' THEN
  RAISE EXCEPTION 'CONVERSATION_NOTICE_IMMUTABLE' USING ERRCODE='55000';
 END IF;
 IF TG_OP='UPDATE' THEN
  IF (to_jsonb(NEW)-ARRAY['status','completed_at','claim_actor_id','claim_actor_epoch',
       'claim_host_incarnation','claim_input_batch_id']) IS DISTINCT FROM
     (to_jsonb(OLD)-ARRAY['status','completed_at','claim_actor_id','claim_actor_epoch',
       'claim_host_incarnation','claim_input_batch_id'])
   OR OLD.status IN ('COMPLETED','CANCELLED')
   OR NOT ((OLD.status='PENDING' AND NEW.status IN ('CLAIMED','CANCELLED'))
        OR (OLD.status='CLAIMED' AND NEW.status IN ('CLAIMED','COMPLETED','CANCELLED'))) THEN
   RAISE EXCEPTION 'CONVERSATION_NOTICE_IMMUTABLE' USING ERRCODE='55000';
  END IF;
  IF NEW.status IN ('CLAIMED','COMPLETED') THEN
   SELECT * INTO conversation FROM public.bot_conversations
    WHERE bot_id=NEW.bot_id AND conversation_id=NEW.conversation_id;
   IF conversation.bot_id IS NULL OR
      ROW(conversation.actor_id,conversation.actor_epoch,conversation.actor_host_incarnation)
      IS DISTINCT FROM
      ROW(NEW.claim_actor_id,NEW.claim_actor_epoch,NEW.claim_host_incarnation) THEN
    RAISE EXCEPTION 'CONVERSATION_NOTICE_ACTOR_FENCE' USING ERRCODE='55000';
   END IF;
  END IF;
  IF OLD.status='CLAIMED' AND NEW.status='COMPLETED' AND NOT EXISTS(
      SELECT 1 FROM public.bot_conversation_notice_results r
       WHERE r.bot_id=NEW.bot_id AND r.notice_id=NEW.notice_id) THEN
   RAISE EXCEPTION 'CONVERSATION_NOTICE_RESULT_REQUIRED' USING ERRCODE='55000';
  END IF;
  RETURN NEW;
 END IF;
 SELECT * INTO inbox FROM public.bot_message_inbox
  WHERE bot_id=NEW.bot_id AND event_id=NEW.event_id;
 IF inbox.bot_id IS NULL
  OR ROW(inbox.conversation_id,inbox.ingress_seq,inbox.payload_id) IS DISTINCT FROM
     ROW(NEW.conversation_id,NEW.ingress_seq,NEW.input_payload_id) THEN
  RAISE EXCEPTION 'CONVERSATION_NOTICE_PROOF_INVALID' USING ERRCODE='55000';
 END IF;
 IF NEW.kind='BUSINESS_CLASSIFICATION_FAILED' THEN
  IF inbox.trigger_decision<>'PENDING' OR NOT inbox.business_classification_pending THEN
   RAISE EXCEPTION 'CONVERSATION_NOTICE_PROOF_INVALID' USING ERRCODE='55000';
  END IF;
 ELSE
  SELECT * INTO request FROM public.bot_business_command_requests
   WHERE bot_id=NEW.bot_id AND request_id=NEW.request_id;
  IF request.bot_id IS NULL OR request.event_id<>NEW.event_id
   OR (NEW.kind='BUSINESS_COMMAND_DENIED' AND
       (request.status<>'DENIED' OR request.terminal_reason<>NEW.error_code))
   OR (NEW.kind='BUSINESS_COMMAND_FAILED' AND
       (request.status<>'FAILED' OR request.terminal_reason<>NEW.error_code)) THEN
   RAISE EXCEPTION 'CONVERSATION_NOTICE_PROOF_INVALID' USING ERRCODE='55000';
  END IF;
 END IF;
 RETURN NEW;
END;
$$;

CREATE TABLE public.bot_business_classification_attempts (
 bot_id text NOT NULL,
 event_id text NOT NULL,
 attempt_no integer NOT NULL CHECK(attempt_no>0),
 operation_id text NOT NULL,
 host_incarnation text NOT NULL,
 host_owner_epoch bigint NOT NULL CHECK(host_owner_epoch>0),
 error_code text NOT NULL CHECK(error_code ~ '^[A-Z][A-Z0-9_]{0,95}$'),
 retry_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,event_id,attempt_no),
 UNIQUE(bot_id,operation_id),
 FOREIGN KEY(bot_id,event_id) REFERENCES public.bot_message_inbox(bot_id,event_id)
);
CREATE FUNCTION public.forest_business_classification_attempt_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE inbox record; host record;
BEGIN
 SELECT * INTO inbox FROM public.bot_message_inbox
  WHERE bot_id=NEW.bot_id AND event_id=NEW.event_id;
 SELECT * INTO host FROM public.bot_host_lease WHERE bot_id=NEW.bot_id;
 IF inbox.bot_id IS NULL OR inbox.trigger_decision<>'PENDING'
  OR NOT inbox.business_classification_pending
  OR NEW.attempt_no<>inbox.business_classification_attempt_count+1
  OR ROW(host.incarnation,host.owner_epoch) IS DISTINCT FROM
     ROW(NEW.host_incarnation,NEW.host_owner_epoch)
  OR host.expires_at<=clock_timestamp() THEN
  RAISE EXCEPTION 'BUSINESS_CLASSIFICATION_ATTEMPT_PROOF_INVALID' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_business_classification_attempt_guard
 BEFORE INSERT ON public.bot_business_classification_attempts
 FOR EACH ROW EXECUTE FUNCTION public.forest_business_classification_attempt_guard();
CREATE TRIGGER bot_business_classification_attempts_immutable
 BEFORE UPDATE OR DELETE ON public.bot_business_classification_attempts
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE FUNCTION public.forest_business_classification_inbox_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.business_classification_attempt_count<OLD.business_classification_attempt_count
  OR (NOT OLD.business_classification_pending AND NEW.business_classification_pending)
  OR (OLD.business_fallback_decision IS NOT NULL AND
      ROW(NEW.business_fallback_decision,NEW.business_fallback_rule_revision,
          NEW.business_fallback_reason) IS DISTINCT FROM
      ROW(OLD.business_fallback_decision,OLD.business_fallback_rule_revision,
          OLD.business_fallback_reason))
  OR (OLD.business_classification_pending AND NEW.trigger_decision<>'PENDING'
      AND NEW.business_classification_pending) THEN
  RAISE EXCEPTION 'BUSINESS_CLASSIFICATION_STATE_INVALID' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_business_classification_inbox_guard
 BEFORE UPDATE ON public.bot_message_inbox FOR EACH ROW
 EXECUTE FUNCTION public.forest_business_classification_inbox_guard();
