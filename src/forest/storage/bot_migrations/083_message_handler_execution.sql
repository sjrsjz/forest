-- Conversation Actor claims one frozen handler item per fair scheduling quantum.
DO $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.bot_schema_writer_gate
               WHERE singleton AND minimum_generation>=22) THEN
  RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
 END IF;
END;
$$;

ALTER TABLE public.bot_message_handler_chains
 ADD COLUMN claim_actor_id text,
 ADD COLUMN claim_actor_epoch bigint,
 ADD COLUMN claim_host_incarnation text,
 ADD COLUMN attempt_count integer NOT NULL DEFAULT 0 CHECK(attempt_count>=0),
 ADD COLUMN next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 ADD COLUMN last_error_code text,
 ADD CHECK(last_error_code IS NULL OR last_error_code ~ '^[A-Z][A-Z0-9_]{0,95}$'),
 ADD CHECK((claim_actor_id IS NULL AND claim_actor_epoch IS NULL AND claim_host_incarnation IS NULL)
        OR (claim_actor_id IS NOT NULL AND claim_actor_epoch>0 AND claim_host_incarnation IS NOT NULL));

DO $$
DECLARE selected_name text;
BEGIN
 SELECT conname INTO selected_name FROM pg_constraint
  WHERE conrelid='public.bot_message_handler_chains'::regclass AND contype='c'
   AND pg_get_constraintdef(oid) LIKE '%status = ''PENDING''%route_count > 0%'
 LIMIT 1;
 IF selected_name IS NULL THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_CHAIN_STATE_CONSTRAINT_MISSING' USING ERRCODE='55000';
 END IF;
 EXECUTE format('ALTER TABLE public.bot_message_handler_chains DROP CONSTRAINT %I',selected_name);
END;
$$;
ALTER TABLE public.bot_message_handler_chains
 ADD CONSTRAINT bot_message_handler_chain_state_check_v2 CHECK(
  (status='PENDING' AND route_count>0 AND current_index<route_count AND completed_at IS NULL)
  OR (status='PASSED' AND current_index=route_count AND completed_at IS NOT NULL)
  OR (status IN ('CONSUMED','REPLIED') AND current_index>0
      AND current_index<=route_count AND completed_at IS NOT NULL)
  OR (status IN ('FAILED','CANCELLED') AND completed_at IS NOT NULL)
 ),
 ADD CONSTRAINT bot_message_handler_chain_claim_check CHECK(
  (status='PENDING') OR claim_actor_id IS NULL
 );

CREATE TABLE public.bot_message_handler_attempts (
 bot_id text NOT NULL,
 chain_id text NOT NULL,
 attempt_no integer NOT NULL CHECK(attempt_no>0),
 route_index integer NOT NULL CHECK(route_index>=0),
 operation_id text NOT NULL,
 call_operation_id text NOT NULL,
 actor_id text NOT NULL,
 actor_epoch bigint NOT NULL CHECK(actor_epoch>0),
 host_incarnation text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,chain_id,attempt_no),
 UNIQUE(bot_id,operation_id),
 FOREIGN KEY(bot_id,chain_id) REFERENCES public.bot_message_handler_chains(bot_id,chain_id),
 FOREIGN KEY(bot_id,chain_id,route_index)
  REFERENCES public.bot_message_handler_chain_items(bot_id,chain_id,route_index)
);

CREATE TABLE public.bot_message_handler_results (
 bot_id text NOT NULL,
 result_id text NOT NULL,
 operation_id text NOT NULL,
 chain_id text NOT NULL,
 route_index integer NOT NULL CHECK(route_index>=0),
 attempt_no integer NOT NULL CHECK(attempt_no>0),
 conversation_id text NOT NULL,
 disposition text NOT NULL CHECK(disposition IN ('PASS','CONSUMED','REPLY')),
 call_operation_id text NOT NULL,
 call_result_payload_id text NOT NULL,
 call_result_digest text NOT NULL CHECK(call_result_digest ~ '^sha256:[a-f0-9]{64}$'),
 context_payload_id text,
 context_payload_digest text CHECK(context_payload_digest IS NULL OR context_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
 estimated_tokens bigint NOT NULL DEFAULT 0 CHECK(estimated_tokens>=0),
 result_digest text NOT NULL CHECK(result_digest ~ '^sha256:[a-f0-9]{64}$'),
 response_seq bigint CHECK(response_seq>0),
 delivery jsonb,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,result_id),
 UNIQUE(bot_id,chain_id,route_index),
 UNIQUE(bot_id,conversation_id,operation_id),
 FOREIGN KEY(bot_id,chain_id) REFERENCES public.bot_message_handler_chains(bot_id,chain_id),
 FOREIGN KEY(bot_id,chain_id,route_index)
  REFERENCES public.bot_message_handler_chain_items(bot_id,chain_id,route_index),
 FOREIGN KEY(bot_id,chain_id,attempt_no)
  REFERENCES public.bot_message_handler_attempts(bot_id,chain_id,attempt_no),
 FOREIGN KEY(bot_id,conversation_id) REFERENCES public.bot_conversations(bot_id,conversation_id),
 FOREIGN KEY(bot_id,call_result_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 FOREIGN KEY(bot_id,context_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 CHECK((disposition='REPLY')=(context_payload_id IS NOT NULL)),
 CHECK((disposition='REPLY')=(estimated_tokens>0)),
 CHECK((context_payload_id IS NULL)=(context_payload_digest IS NULL)),
 CHECK((disposition='REPLY')=(response_seq IS NOT NULL)),
 CHECK((response_seq IS NULL)=(delivery IS NULL))
);

CREATE TABLE public.bot_message_handler_failures (
 bot_id text NOT NULL,
 failure_id text NOT NULL,
 operation_id text NOT NULL,
 chain_id text NOT NULL,
 route_index integer NOT NULL CHECK(route_index>=0),
 attempt_no integer NOT NULL CHECK(attempt_no>0),
 conversation_id text NOT NULL,
 error_code text NOT NULL CHECK(error_code ~ '^[A-Z][A-Z0-9_]{0,95}$'),
 retryable boolean NOT NULL,
 retry_at timestamptz,
 context_payload_id text,
 context_payload_digest text CHECK(context_payload_digest IS NULL OR context_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
 estimated_tokens bigint NOT NULL DEFAULT 0 CHECK(estimated_tokens>=0),
 response_seq bigint CHECK(response_seq>0),
 delivery jsonb,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,failure_id),
 UNIQUE(bot_id,operation_id),
 UNIQUE(bot_id,chain_id,attempt_no),
 FOREIGN KEY(bot_id,chain_id) REFERENCES public.bot_message_handler_chains(bot_id,chain_id),
 FOREIGN KEY(bot_id,chain_id,route_index)
  REFERENCES public.bot_message_handler_chain_items(bot_id,chain_id,route_index),
 FOREIGN KEY(bot_id,chain_id,attempt_no)
  REFERENCES public.bot_message_handler_attempts(bot_id,chain_id,attempt_no),
 FOREIGN KEY(bot_id,conversation_id) REFERENCES public.bot_conversations(bot_id,conversation_id),
 FOREIGN KEY(bot_id,context_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 CHECK(retryable=(retry_at IS NOT NULL)),
 CHECK(retryable=(context_payload_id IS NULL)),
 CHECK((context_payload_id IS NOT NULL)=(estimated_tokens>0)),
 CHECK((context_payload_id IS NULL)=(context_payload_digest IS NULL)),
 CHECK((context_payload_id IS NULL)=(response_seq IS NULL)),
 CHECK((response_seq IS NULL)=(delivery IS NULL))
);

CREATE FUNCTION public.forest_message_handler_attempt_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE chain record; conversation record;
BEGIN
 SELECT * INTO chain FROM public.bot_message_handler_chains
  WHERE bot_id=NEW.bot_id AND chain_id=NEW.chain_id;
 SELECT * INTO conversation FROM public.bot_conversations
  WHERE bot_id=chain.bot_id AND conversation_id=chain.conversation_id;
 IF chain.bot_id IS NULL OR chain.status<>'PENDING' OR chain.claim_actor_id IS NOT NULL
  OR NEW.route_index<>chain.current_index OR NEW.attempt_no<>chain.attempt_count+1
  OR ROW(conversation.actor_id,conversation.actor_epoch,conversation.actor_host_incarnation)
     IS DISTINCT FROM ROW(NEW.actor_id,NEW.actor_epoch,NEW.host_incarnation) THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_ATTEMPT_PROOF_INVALID' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_message_handler_attempt_guard
 BEFORE INSERT ON public.bot_message_handler_attempts
 FOR EACH ROW EXECUTE FUNCTION public.forest_message_handler_attempt_guard();
CREATE TRIGGER bot_message_handler_attempt_immutable
 BEFORE UPDATE OR DELETE ON public.bot_message_handler_attempts
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE FUNCTION public.forest_message_handler_result_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE chain record; attempt record; item record; call record; context record; conversation record;
 context_valid boolean := true;
BEGIN
 SELECT * INTO chain FROM public.bot_message_handler_chains
  WHERE bot_id=NEW.bot_id AND chain_id=NEW.chain_id;
 SELECT * INTO attempt FROM public.bot_message_handler_attempts
  WHERE (bot_id,chain_id,attempt_no)=(NEW.bot_id,NEW.chain_id,NEW.attempt_no);
 SELECT * INTO item FROM public.bot_message_handler_chain_items
  WHERE (bot_id,chain_id,route_index)=(NEW.bot_id,NEW.chain_id,NEW.route_index);
 SELECT * INTO call FROM public.bot_calls
  WHERE bot_id=NEW.bot_id AND operation_id=NEW.call_operation_id;
 SELECT * INTO conversation FROM public.bot_conversations
  WHERE bot_id=NEW.bot_id AND conversation_id=NEW.conversation_id;
 IF NEW.context_payload_id IS NOT NULL THEN
 SELECT * INTO context FROM public.bot_payload_headers
   WHERE bot_id=NEW.bot_id AND payload_id=NEW.context_payload_id;
  context_valid := context.bot_id IS NOT NULL AND context.owner_kind='context.parts'
   AND context.owner_id=NEW.result_id AND context.digest=NEW.context_payload_digest
   AND context.state='ACTIVE' AND EXISTS(
    SELECT 1 FROM public.bot_payload_derivations d
    WHERE d.bot_id=NEW.bot_id AND d.payload_id=NEW.context_payload_id
     AND d.parent_payload_id=NEW.call_result_payload_id);
 END IF;
 IF chain.bot_id IS NULL OR chain.status<>'PENDING'
  OR chain.current_index<>NEW.route_index OR chain.attempt_count<>NEW.attempt_no
  OR ROW(chain.claim_actor_id,chain.claim_actor_epoch,chain.claim_host_incarnation)
     IS DISTINCT FROM ROW(conversation.actor_id,conversation.actor_epoch,
                          conversation.actor_host_incarnation)
  OR attempt.bot_id IS NULL OR attempt.route_index<>NEW.route_index
  OR NEW.call_operation_id<>attempt.call_operation_id
  OR call.bot_id IS NULL OR call.status<>'COMPLETED'
  OR ROW(call.component_id,call.generation_id,call.route_epoch,
         call.result_payload_id,call.result_digest) IS DISTINCT FROM
     ROW(item.component_id,item.generation_id,item.route_epoch,
         NEW.call_result_payload_id,NEW.call_result_digest)
  OR NOT context_valid THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_RESULT_PROOF_INVALID' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_message_handler_result_guard
 BEFORE INSERT ON public.bot_message_handler_results
 FOR EACH ROW EXECUTE FUNCTION public.forest_message_handler_result_guard();
CREATE TRIGGER bot_message_handler_result_immutable
 BEFORE UPDATE OR DELETE ON public.bot_message_handler_results
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE FUNCTION public.forest_message_handler_failure_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE chain record; attempt record; conversation record; context record;
 context_valid boolean := true;
BEGIN
 SELECT * INTO chain FROM public.bot_message_handler_chains
  WHERE bot_id=NEW.bot_id AND chain_id=NEW.chain_id;
 SELECT * INTO attempt FROM public.bot_message_handler_attempts
  WHERE (bot_id,chain_id,attempt_no)=(NEW.bot_id,NEW.chain_id,NEW.attempt_no);
 SELECT * INTO conversation FROM public.bot_conversations
  WHERE bot_id=NEW.bot_id AND conversation_id=NEW.conversation_id;
 IF NEW.context_payload_id IS NOT NULL THEN
 SELECT * INTO context FROM public.bot_payload_headers
   WHERE bot_id=NEW.bot_id AND payload_id=NEW.context_payload_id;
  context_valid := context.bot_id IS NOT NULL AND context.owner_kind='context.parts'
   AND context.owner_id=NEW.failure_id AND context.digest=NEW.context_payload_digest
   AND context.state='ACTIVE' AND EXISTS(
    SELECT 1 FROM public.bot_payload_derivations d
    WHERE d.bot_id=NEW.bot_id AND d.payload_id=NEW.context_payload_id
     AND d.parent_payload_id=chain.input_payload_id);
 END IF;
 IF chain.bot_id IS NULL OR chain.status<>'PENDING'
  OR chain.current_index<>NEW.route_index OR chain.attempt_count<>NEW.attempt_no
  OR attempt.bot_id IS NULL OR attempt.route_index<>NEW.route_index
  OR ROW(chain.claim_actor_id,chain.claim_actor_epoch,chain.claim_host_incarnation)
     IS DISTINCT FROM ROW(conversation.actor_id,conversation.actor_epoch,
                          conversation.actor_host_incarnation)
  OR NOT context_valid THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_FAILURE_PROOF_INVALID' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_message_handler_failure_guard
 BEFORE INSERT ON public.bot_message_handler_failures
 FOR EACH ROW EXECUTE FUNCTION public.forest_message_handler_failure_guard();
CREATE TRIGGER bot_message_handler_failure_immutable
 BEFORE UPDATE OR DELETE ON public.bot_message_handler_failures
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE OR REPLACE FUNCTION public.forest_message_handler_chain_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE inbox record; conversation record; result_disposition text; failure_retryable boolean;
 failure_retry_at timestamptz; has_attempt boolean;
BEGIN
 IF TG_OP='INSERT' THEN
  SELECT * INTO inbox FROM public.bot_message_inbox
   WHERE bot_id=NEW.bot_id AND event_id=NEW.event_id;
  IF inbox.bot_id IS NULL OR inbox.trigger_decision<>'PENDING'
   OR ROW(inbox.conversation_id,inbox.ingress_seq,inbox.payload_id,
          inbox.author->>'principal_ref',inbox.author->>'external_user_id') IS DISTINCT FROM
      ROW(NEW.conversation_id,NEW.ingress_seq,NEW.input_payload_id,
          NEW.principal_ref,NEW.external_user_id)
   OR NEW.chain_id<>'message-handler-chain:sha256:' || encode(sha256(convert_to(
      to_jsonb(NEW.event_id)::text,'UTF8')),'hex') THEN
   RAISE EXCEPTION 'MESSAGE_HANDLER_CHAIN_PROOF_INVALID' USING ERRCODE='55000';
  END IF;
  RETURN NEW;
 END IF;
 IF TG_OP='DELETE' THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_CHAIN_DELETE_FORBIDDEN' USING ERRCODE='55000';
 END IF;
 IF (to_jsonb(NEW)-ARRAY['fallback_decision','fallback_rule_revision','fallback_reason',
      'fallback_digest','status','current_index','revision','terminal_reason','completed_at',
      'claim_actor_id','claim_actor_epoch','claim_host_incarnation','attempt_count',
      'next_attempt_at','last_error_code']) IS DISTINCT FROM
    (to_jsonb(OLD)-ARRAY['fallback_decision','fallback_rule_revision','fallback_reason',
      'fallback_digest','status','current_index','revision','terminal_reason','completed_at',
      'claim_actor_id','claim_actor_epoch','claim_host_incarnation','attempt_count',
      'next_attempt_at','last_error_code'])
  OR NEW.revision<>OLD.revision+1 THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_CHAIN_IMMUTABLE' USING ERRCODE='55000';
 END IF;
 -- The sole legacy compatibility mutation fills a previously unknown fallback.
 IF OLD.fallback_decision='PENDING' AND NEW.status=OLD.status
  AND ROW(NEW.current_index,NEW.claim_actor_id,NEW.claim_actor_epoch,
          NEW.claim_host_incarnation,NEW.attempt_count,NEW.next_attempt_at,
          NEW.last_error_code,NEW.terminal_reason,NEW.completed_at) IS NOT DISTINCT FROM
      ROW(OLD.current_index,OLD.claim_actor_id,OLD.claim_actor_epoch,
          OLD.claim_host_incarnation,OLD.attempt_count,OLD.next_attempt_at,
          OLD.last_error_code,OLD.terminal_reason,OLD.completed_at)
  AND NEW.fallback_decision IN ('TRIGGER','NO_TRIGGER') THEN
  RETURN NEW;
 END IF;
 IF ROW(NEW.fallback_decision,NEW.fallback_rule_revision,NEW.fallback_reason,
        NEW.fallback_digest) IS DISTINCT FROM
    ROW(OLD.fallback_decision,OLD.fallback_rule_revision,OLD.fallback_reason,
        OLD.fallback_digest) THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_FALLBACK_INVALID' USING ERRCODE='55000';
 END IF;
 SELECT * INTO conversation FROM public.bot_conversations
  WHERE bot_id=NEW.bot_id AND conversation_id=NEW.conversation_id;
 -- Cancellation is allowed only before an item has begun.
 IF OLD.status IN ('PENDING','PASSED') AND OLD.claim_actor_id IS NULL
  AND NEW.status='CANCELLED' AND NEW.current_index=OLD.current_index
  AND NEW.attempt_count=OLD.attempt_count
  AND ROW(NEW.claim_actor_id,NEW.claim_actor_epoch,NEW.claim_host_incarnation,
          NEW.next_attempt_at,NEW.last_error_code) IS NOT DISTINCT FROM
      ROW(OLD.claim_actor_id,OLD.claim_actor_epoch,OLD.claim_host_incarnation,
          OLD.next_attempt_at,OLD.last_error_code)
  AND NEW.terminal_reason IN ('BUSINESS_COMMAND_OWNED','BUSINESS_CLASSIFICATION_FAILED',
      'POLICY_REVOKED') AND NEW.completed_at IS NOT NULL THEN
  RETURN NEW;
 END IF;
 -- Claim a new attempt or transfer an unfinished claim to the replacement Actor.
 IF OLD.status='PENDING' AND NEW.status='PENDING'
  AND NEW.current_index=OLD.current_index AND NEW.completed_at IS NULL
  AND NEW.claim_actor_id IS NOT NULL AND NEW.terminal_reason IS NOT DISTINCT FROM OLD.terminal_reason
  AND NEW.last_error_code IS NOT DISTINCT FROM OLD.last_error_code
  AND NEW.next_attempt_at IS NOT DISTINCT FROM OLD.next_attempt_at
  AND ROW(NEW.claim_actor_id,NEW.claim_actor_epoch,NEW.claim_host_incarnation)
      IS NOT DISTINCT FROM ROW(conversation.actor_id,conversation.actor_epoch,
                               conversation.actor_host_incarnation) THEN
  IF OLD.claim_actor_id IS NULL THEN
   SELECT EXISTS(SELECT 1 FROM public.bot_message_handler_attempts a
    WHERE (a.bot_id,a.chain_id,a.attempt_no,a.route_index)=
     (NEW.bot_id,NEW.chain_id,NEW.attempt_count,NEW.current_index)) INTO has_attempt;
   IF NEW.attempt_count=OLD.attempt_count+1 AND has_attempt THEN RETURN NEW; END IF;
  ELSIF NEW.attempt_count=OLD.attempt_count THEN
   RETURN NEW;
  END IF;
 END IF;
 SELECT r.disposition FROM public.bot_message_handler_results r
  WHERE (r.bot_id,r.chain_id,r.route_index,r.attempt_no)=
   (NEW.bot_id,NEW.chain_id,OLD.current_index,OLD.attempt_count)
  INTO result_disposition;
 SELECT f.retryable,f.retry_at FROM public.bot_message_handler_failures f
  WHERE (f.bot_id,f.chain_id,f.route_index,f.attempt_no)=
   (NEW.bot_id,NEW.chain_id,OLD.current_index,OLD.attempt_count)
  INTO failure_retryable,failure_retry_at;
 -- One Pass advances exactly one item and returns to the fair queue or completes.
 IF OLD.status='PENDING' AND OLD.claim_actor_id IS NOT NULL AND result_disposition='PASS'
  AND NEW.current_index=OLD.current_index+1 AND NEW.attempt_count=OLD.attempt_count
  AND NEW.claim_actor_id IS NULL AND NEW.last_error_code IS NULL
  AND NEW.next_attempt_at IS NOT DISTINCT FROM OLD.next_attempt_at
  AND ((NEW.status='PENDING' AND NEW.current_index<NEW.route_count AND NEW.completed_at IS NULL)
    AND NEW.terminal_reason IS NULL
    OR (NEW.status='PASSED' AND NEW.current_index=NEW.route_count
        AND NEW.terminal_reason='ALL_PASSED' AND NEW.completed_at IS NOT NULL)) THEN
  RETURN NEW;
 END IF;
 -- Consumed and Reply terminate after the current item.
 IF OLD.status='PENDING' AND OLD.claim_actor_id IS NOT NULL
  AND ((result_disposition='CONSUMED' AND NEW.status='CONSUMED')
    OR (result_disposition='REPLY' AND NEW.status='REPLIED'))
  AND NEW.current_index=OLD.current_index+1
  AND NEW.attempt_count=OLD.attempt_count AND NEW.claim_actor_id IS NULL
  AND NEW.next_attempt_at IS NOT DISTINCT FROM OLD.next_attempt_at
  AND NEW.terminal_reason=result_disposition AND NEW.completed_at IS NOT NULL THEN
  RETURN NEW;
 END IF;
 -- A retryable failure releases the claim without changing the item index.
 IF OLD.status='PENDING' AND OLD.claim_actor_id IS NOT NULL AND failure_retryable
  AND NEW.status='PENDING' AND NEW.current_index=OLD.current_index
  AND NEW.attempt_count=OLD.attempt_count AND NEW.claim_actor_id IS NULL
  AND NEW.last_error_code IS NOT NULL AND NEW.next_attempt_at=failure_retry_at
  AND NEW.terminal_reason IS NOT DISTINCT FROM OLD.terminal_reason
  AND NEW.completed_at IS NULL THEN
  RETURN NEW;
 END IF;
 -- A terminal failure closes the chain and never falls back to Agent.
 IF OLD.status='PENDING' AND OLD.claim_actor_id IS NOT NULL AND failure_retryable=false
  AND NEW.status='FAILED' AND NEW.current_index=OLD.current_index
  AND NEW.attempt_count=OLD.attempt_count AND NEW.claim_actor_id IS NULL
  AND NEW.last_error_code IS NOT NULL AND NEW.terminal_reason=NEW.last_error_code
  AND NEW.next_attempt_at IS NOT DISTINCT FROM OLD.next_attempt_at
  AND NEW.completed_at IS NOT NULL THEN
  RETURN NEW;
 END IF;
 RAISE EXCEPTION 'MESSAGE_HANDLER_CHAIN_TRANSITION_INVALID' USING ERRCODE='55000';
END;
$$;

ALTER TABLE public.bot_context_units
 DROP CONSTRAINT bot_context_units_kind_check,
 ADD CONSTRAINT bot_context_units_kind_check CHECK(kind IN (
  'INPUT','REPLY','CONTROL','RUN_STEP','COMMAND_RESULT','SYSTEM_NOTICE',
  'MESSAGE_HANDLER_RESULT'
 )),
 ADD COLUMN message_handler_chain_id text,
 ADD UNIQUE(bot_id,message_handler_chain_id),
 ADD FOREIGN KEY(bot_id,message_handler_chain_id)
  REFERENCES public.bot_message_handler_chains(bot_id,chain_id),
 ADD CHECK((kind='MESSAGE_HANDLER_RESULT')=(message_handler_chain_id IS NOT NULL));
