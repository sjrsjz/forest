-- Durable attempts and Actor-ordered safe notices for business-command failures.
DO $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.bot_schema_writer_gate
               WHERE singleton AND minimum_generation>=15) THEN
  RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
 END IF;
END;
$$;

ALTER TABLE public.bot_business_command_requests
 ADD COLUMN execution_attempt_count integer NOT NULL DEFAULT 0 CHECK(execution_attempt_count>=0),
 ADD COLUMN next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 ADD COLUMN execution_deadline_at timestamptz NOT NULL
  DEFAULT (clock_timestamp()+interval '5 minutes'),
 ADD COLUMN last_error_code text,
 ADD CHECK(last_error_code IS NULL OR last_error_code ~ '^[A-Z][A-Z0-9_]{0,95}$'),
 ADD CHECK(next_attempt_at<=execution_deadline_at OR status IN ('COMPLETED','DENIED','FAILED','CANCELLED'));

CREATE OR REPLACE FUNCTION public.forest_business_command_request_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE inbox record; arguments record; policy record; conversation record;
BEGIN
 IF TG_OP='DELETE' THEN
  RAISE EXCEPTION 'BUSINESS_COMMAND_REQUEST_IMMUTABLE' USING ERRCODE='55000';
 END IF;
 IF TG_OP='UPDATE' THEN
  IF (to_jsonb(NEW)-ARRAY['status','terminal_reason','completed_at','claim_actor_id',
       'claim_actor_epoch','claim_host_incarnation','claim_input_batch_id',
       'execution_attempt_count','next_attempt_at','last_error_code']) IS DISTINCT FROM
     (to_jsonb(OLD)-ARRAY['status','terminal_reason','completed_at','claim_actor_id',
       'claim_actor_epoch','claim_host_incarnation','claim_input_batch_id',
       'execution_attempt_count','next_attempt_at','last_error_code'])
   OR NEW.execution_attempt_count<OLD.execution_attempt_count
   OR OLD.status IN ('COMPLETED','DENIED','FAILED','CANCELLED')
   OR NOT ((OLD.status='PENDING' AND NEW.status IN ('CLAIMED','FAILED','CANCELLED'))
        OR (OLD.status='CLAIMED' AND NEW.status IN ('CLAIMED','COMPLETED','FAILED'))) THEN
   RAISE EXCEPTION 'BUSINESS_COMMAND_REQUEST_IMMUTABLE' USING ERRCODE='55000';
  END IF;
  IF NEW.status IN ('CLAIMED','COMPLETED')
     OR (NEW.status='FAILED' AND NEW.claim_actor_id IS NOT NULL) THEN
   SELECT * INTO conversation FROM public.bot_conversations
    WHERE bot_id=NEW.bot_id AND conversation_id=NEW.conversation_id;
   IF conversation.bot_id IS NULL OR
      ROW(conversation.actor_id,conversation.actor_epoch,conversation.actor_host_incarnation)
      IS DISTINCT FROM
      ROW(NEW.claim_actor_id,NEW.claim_actor_epoch,NEW.claim_host_incarnation) THEN
    RAISE EXCEPTION 'BUSINESS_COMMAND_ACTOR_FENCE' USING ERRCODE='55000';
   END IF;
  END IF;
  IF OLD.status='CLAIMED' AND NEW.status='COMPLETED' AND NOT EXISTS(
      SELECT 1 FROM public.bot_business_command_results r
       WHERE r.bot_id=NEW.bot_id AND r.request_id=NEW.request_id
        AND r.disposition IN ('REPLY','CONSUMED')) THEN
   RAISE EXCEPTION 'BUSINESS_COMMAND_RESULT_REQUIRED' USING ERRCODE='55000';
  ELSIF OLD.status='CLAIMED' AND NEW.status='FAILED'
    AND NEW.terminal_reason='COMMAND_NOT_HANDLED' AND NOT EXISTS(
      SELECT 1 FROM public.bot_business_command_results r
       WHERE r.bot_id=NEW.bot_id AND r.request_id=NEW.request_id
        AND r.disposition='PASS') THEN
   RAISE EXCEPTION 'BUSINESS_COMMAND_RESULT_REQUIRED' USING ERRCODE='55000';
  END IF;
  RETURN NEW;
 END IF;
 SELECT * INTO inbox FROM public.bot_message_inbox
  WHERE bot_id=NEW.bot_id AND event_id=NEW.event_id;
 SELECT * INTO arguments FROM public.bot_payload_headers
  WHERE bot_id=NEW.bot_id AND payload_id=NEW.arguments_payload_id;
 SELECT * INTO policy FROM public.bot_command_policy_revisions
  WHERE (bot_id,binding_id,command_id,revision)=
        (NEW.bot_id,NEW.binding_id,NEW.command_id,NEW.command_policy_revision);
 IF inbox.bot_id IS NULL OR inbox.trigger_decision<>'PENDING'
  OR ROW(inbox.conversation_id,inbox.ingress_seq,inbox.payload_id,inbox.author->>'principal_ref',
         inbox.author->>'external_user_id') IS DISTINCT FROM
     ROW(NEW.conversation_id,NEW.ingress_seq,NEW.input_payload_id,NEW.principal_ref,
         NEW.external_user_id)
  OR arguments.bot_id IS NULL OR arguments.owner_kind<>'command.arguments'
  OR arguments.owner_id<>'business-command-arguments-v1:sha256:' || encode(sha256(convert_to(
       '[' || to_jsonb(NEW.request_id)::text || ']','UTF8')),'hex')
  OR arguments.digest<>NEW.arguments_payload_digest
  OR NEW.arguments_digest<>NEW.arguments_payload_digest OR arguments.state<>'ACTIVE'
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_bodies b
      WHERE b.bot_id=NEW.bot_id AND b.payload_id=NEW.arguments_payload_id
       AND jsonb_typeof(convert_from(b.content,'UTF8')::jsonb)='object'
       AND (convert_from(b.content,'UTF8')::jsonb)->>'request_id'=NEW.request_id
       AND (convert_from(b.content,'UTF8')::jsonb)->>'command_id'=NEW.command_id
       AND (convert_from(b.content,'UTF8')::jsonb)->>'name'=NEW.command_name
       AND (convert_from(b.content,'UTF8')::jsonb)->>'argument_mode'=NEW.argument_mode
       AND jsonb_typeof((convert_from(b.content,'UTF8')::jsonb)->'argument_text')='string')
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations d
      WHERE d.bot_id=NEW.bot_id AND d.payload_id=NEW.arguments_payload_id
       AND d.parent_payload_id=NEW.input_payload_id)
  OR policy.bot_id IS NULL
  OR ROW(policy.policy_mode,policy.values_payload_id,policy.values_payload_digest) IS DISTINCT FROM
     ROW(NEW.policy_mode,NEW.policy_payload_id,NEW.policy_payload_digest) THEN
  RAISE EXCEPTION 'BUSINESS_COMMAND_REQUEST_PROOF_INVALID' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;

CREATE TABLE public.bot_business_command_attempts (
 bot_id text NOT NULL,
 request_id text NOT NULL,
 attempt_no integer NOT NULL CHECK(attempt_no>0),
 operation_id text NOT NULL,
 actor_id text NOT NULL,
 actor_epoch bigint NOT NULL CHECK(actor_epoch>0),
 host_incarnation text NOT NULL,
 error_code text NOT NULL CHECK(error_code ~ '^[A-Z][A-Z0-9_]{0,95}$'),
 retryable boolean NOT NULL,
 retry_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,request_id,attempt_no),
 UNIQUE(bot_id,operation_id),
 FOREIGN KEY(bot_id,request_id)
  REFERENCES public.bot_business_command_requests(bot_id,request_id),
 CHECK(retryable=(retry_at IS NOT NULL))
);
CREATE FUNCTION public.forest_business_command_attempt_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE request record; conversation record;
BEGIN
 SELECT * INTO request FROM public.bot_business_command_requests
  WHERE bot_id=NEW.bot_id AND request_id=NEW.request_id;
 SELECT * INTO conversation FROM public.bot_conversations
  WHERE bot_id=request.bot_id AND conversation_id=request.conversation_id;
 IF request.bot_id IS NULL OR request.status<>'CLAIMED'
  OR NEW.attempt_no<>request.execution_attempt_count+1
  OR ROW(request.claim_actor_id,request.claim_actor_epoch,request.claim_host_incarnation)
     IS DISTINCT FROM ROW(NEW.actor_id,NEW.actor_epoch,NEW.host_incarnation)
  OR ROW(conversation.actor_id,conversation.actor_epoch,conversation.actor_host_incarnation)
     IS DISTINCT FROM ROW(NEW.actor_id,NEW.actor_epoch,NEW.host_incarnation) THEN
  RAISE EXCEPTION 'BUSINESS_COMMAND_ATTEMPT_PROOF_INVALID' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_business_command_attempt_guard
 BEFORE INSERT ON public.bot_business_command_attempts
 FOR EACH ROW EXECUTE FUNCTION public.forest_business_command_attempt_guard();
CREATE TRIGGER bot_business_command_attempts_immutable
 BEFORE UPDATE OR DELETE ON public.bot_business_command_attempts
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE TABLE public.bot_conversation_notices (
 bot_id text NOT NULL,
 notice_id text NOT NULL,
 event_id text NOT NULL,
 request_id text,
 conversation_id text NOT NULL,
 ingress_seq bigint NOT NULL CHECK(ingress_seq>0),
 input_payload_id text NOT NULL,
 kind text NOT NULL CHECK(kind IN (
  'BUSINESS_COMMAND_DENIED','BUSINESS_COMMAND_FAILED','BUSINESS_CLASSIFICATION_FAILED'
 )),
 error_code text NOT NULL CHECK(error_code ~ '^[A-Z][A-Z0-9_]{0,95}$'),
 status text NOT NULL DEFAULT 'PENDING'
  CHECK(status IN ('PENDING','CLAIMED','COMPLETED','CANCELLED')),
 claim_actor_id text,
 claim_actor_epoch bigint,
 claim_host_incarnation text,
 claim_input_batch_id text,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 completed_at timestamptz,
 PRIMARY KEY(bot_id,notice_id),
 UNIQUE(bot_id,event_id,kind),
 UNIQUE(bot_id,request_id),
 FOREIGN KEY(bot_id,event_id) REFERENCES public.bot_message_inbox(bot_id,event_id),
 FOREIGN KEY(bot_id,request_id)
  REFERENCES public.bot_business_command_requests(bot_id,request_id),
 FOREIGN KEY(bot_id,conversation_id)
  REFERENCES public.bot_conversations(bot_id,conversation_id),
 FOREIGN KEY(bot_id,input_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 FOREIGN KEY(bot_id,claim_input_batch_id)
  REFERENCES public.bot_context_input_batches(bot_id,input_batch_id),
 CHECK((request_id IS NULL)=(kind='BUSINESS_CLASSIFICATION_FAILED')),
 CHECK((status='COMPLETED')=(completed_at IS NOT NULL)),
 CHECK((claim_actor_id IS NULL AND claim_actor_epoch IS NULL
        AND claim_host_incarnation IS NULL AND claim_input_batch_id IS NULL)
    OR (claim_actor_id IS NOT NULL AND claim_actor_epoch>0
        AND claim_host_incarnation IS NOT NULL AND claim_input_batch_id IS NOT NULL)),
 CHECK(status NOT IN ('CLAIMED','COMPLETED') OR claim_actor_id IS NOT NULL)
);
CREATE INDEX bot_conversation_notice_pending
 ON public.bot_conversation_notices(bot_id,conversation_id,status,ingress_seq)
 WHERE status IN ('PENDING','CLAIMED');

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
 IF NEW.request_id IS NOT NULL THEN
  SELECT * INTO request FROM public.bot_business_command_requests
   WHERE bot_id=NEW.bot_id AND request_id=NEW.request_id;
 END IF;
 IF inbox.bot_id IS NULL
  OR ROW(inbox.conversation_id,inbox.ingress_seq,inbox.payload_id) IS DISTINCT FROM
     ROW(NEW.conversation_id,NEW.ingress_seq,NEW.input_payload_id)
  OR (NEW.kind='BUSINESS_COMMAND_DENIED' AND
      (request.bot_id IS NULL OR request.status<>'DENIED'
       OR request.event_id<>NEW.event_id OR request.terminal_reason<>NEW.error_code))
  OR (NEW.kind='BUSINESS_COMMAND_FAILED' AND
      (request.bot_id IS NULL OR request.status<>'FAILED'
       OR request.event_id<>NEW.event_id OR request.terminal_reason<>NEW.error_code)) THEN
  RAISE EXCEPTION 'CONVERSATION_NOTICE_PROOF_INVALID' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;

CREATE TABLE public.bot_conversation_notice_results (
 bot_id text NOT NULL,
 result_id text NOT NULL,
 operation_id text NOT NULL,
 notice_id text NOT NULL,
 conversation_id text NOT NULL,
 context_payload_id text NOT NULL,
 context_payload_digest text NOT NULL CHECK(context_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
 result_digest text NOT NULL CHECK(result_digest ~ '^sha256:[a-f0-9]{64}$'),
 response_seq bigint NOT NULL CHECK(response_seq>0),
 delivery jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,result_id),
 UNIQUE(bot_id,notice_id),
 UNIQUE(bot_id,conversation_id,operation_id),
 UNIQUE(bot_id,conversation_id,response_seq),
 FOREIGN KEY(bot_id,notice_id) REFERENCES public.bot_conversation_notices(bot_id,notice_id),
 FOREIGN KEY(bot_id,conversation_id) REFERENCES public.bot_conversations(bot_id,conversation_id),
 FOREIGN KEY(bot_id,context_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id)
);
CREATE FUNCTION public.forest_conversation_notice_result_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE notice record; context record; conversation record;
BEGIN
 SELECT * INTO notice FROM public.bot_conversation_notices
  WHERE bot_id=NEW.bot_id AND notice_id=NEW.notice_id;
 SELECT * INTO context FROM public.bot_payload_headers
  WHERE bot_id=NEW.bot_id AND payload_id=NEW.context_payload_id;
 SELECT * INTO conversation FROM public.bot_conversations
  WHERE bot_id=NEW.bot_id AND conversation_id=NEW.conversation_id;
 IF notice.bot_id IS NULL OR notice.status<>'CLAIMED'
  OR notice.conversation_id<>NEW.conversation_id
  OR ROW(notice.claim_actor_id,notice.claim_actor_epoch,notice.claim_host_incarnation)
     IS DISTINCT FROM
     ROW(conversation.actor_id,conversation.actor_epoch,conversation.actor_host_incarnation)
  OR context.bot_id IS NULL OR context.owner_kind<>'context.parts'
  OR context.owner_id<>NEW.result_id OR context.digest<>NEW.context_payload_digest
  OR context.state<>'ACTIVE'
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations d
      WHERE d.bot_id=NEW.bot_id AND d.payload_id=NEW.context_payload_id
       AND d.parent_payload_id=notice.input_payload_id) THEN
  RAISE EXCEPTION 'CONVERSATION_NOTICE_RESULT_PROOF_INVALID' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_conversation_notice_result_guard
 BEFORE INSERT ON public.bot_conversation_notice_results
 FOR EACH ROW EXECUTE FUNCTION public.forest_conversation_notice_result_guard();
CREATE TRIGGER bot_conversation_notice_results_immutable
 BEFORE UPDATE OR DELETE ON public.bot_conversation_notice_results
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_conversation_notice_guard
 BEFORE INSERT OR UPDATE OR DELETE ON public.bot_conversation_notices
 FOR EACH ROW EXECUTE FUNCTION public.forest_conversation_notice_guard();

ALTER TABLE public.bot_context_input_batches
 DROP CONSTRAINT bot_context_input_batches_kind_check;
ALTER TABLE public.bot_context_input_batches
 ADD CONSTRAINT bot_context_input_batches_kind_check CHECK(kind IN (
  'TRIGGER_INPUT','SILENT_DRAIN','RECOVERY_CLAIM','BUSINESS_COMMAND_INPUT',
  'CONVERSATION_NOTICE_INPUT'
 ));

ALTER TABLE public.bot_context_units DROP CONSTRAINT bot_context_units_kind_check;
ALTER TABLE public.bot_context_units
 ADD CONSTRAINT bot_context_units_kind_check CHECK(kind IN (
  'INPUT','REPLY','CONTROL','RUN_STEP','COMMAND_RESULT','SYSTEM_NOTICE'
  )),
 ADD COLUMN business_notice_id text,
 ADD UNIQUE(bot_id,business_notice_id),
 ADD FOREIGN KEY(bot_id,business_notice_id)
  REFERENCES public.bot_conversation_notice_results(bot_id,notice_id),
 ADD CHECK((kind='SYSTEM_NOTICE')=(business_notice_id IS NOT NULL));

CREATE FUNCTION public.forest_cancel_revoked_conversation_notice() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.state='ACTIVE' AND NEW.state='REVOKED' THEN
  UPDATE public.bot_conversation_notices SET status='CANCELLED'
   WHERE bot_id=NEW.bot_id AND input_payload_id=NEW.payload_id
    AND status IN ('PENDING','CLAIMED');
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_cancel_revoked_conversation_notice
 AFTER UPDATE ON public.bot_payload_headers FOR EACH ROW
 EXECUTE FUNCTION public.forest_cancel_revoked_conversation_notice();
