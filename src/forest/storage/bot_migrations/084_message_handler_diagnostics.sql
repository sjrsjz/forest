-- Purpose-bound Handler diagnostics and safe convergence after policy revocation.
DO $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.bot_schema_writer_gate
               WHERE singleton AND minimum_generation>=23) THEN
  RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
 END IF;
END;
$$;

-- Earlier policy migrations granted every issuer their write actions. Rebuild
-- those families from the issuer's durable base capabilities so an old reader
-- cannot become a planner or applier merely by crossing a schema boundary.
UPDATE public.bot_web_issuers i SET actions=COALESCE((
 SELECT jsonb_agg(action ORDER BY action) FROM (
  SELECT DISTINCT action FROM jsonb_array_elements_text(
   (i.actions - ARRAY[
    'command.policy.read','command.policy.plan','command.policy.apply',
    'command.execution.read','command.execution.recover',
    'command.limit.read','command.limit.plan','command.limit.apply',
    'message.handler.policy.read','message.handler.policy.plan',
    'message.handler.policy.apply','message.handler.execution.read',
    'message.handler.execution.recover'
   ]) ||
   CASE WHEN i.actions ? 'message.read' THEN
    '["command.policy.read","command.execution.read","command.limit.read",'
    '"message.handler.policy.read","message.handler.execution.read"]'::jsonb
    ELSE '[]'::jsonb END ||
   CASE WHEN i.actions ? 'config.plan' THEN
    '["command.policy.plan","command.limit.plan","message.handler.policy.plan"]'::jsonb
    ELSE '[]'::jsonb END ||
   CASE WHEN i.actions ?& ARRAY['config.apply','message.cancel'] THEN
    '["command.policy.apply","command.limit.apply",'
    '"message.handler.policy.apply"]'::jsonb ELSE '[]'::jsonb END ||
   CASE WHEN i.actions ? 'message.cancel' THEN
    '["command.execution.recover","message.handler.execution.recover"]'::jsonb
    ELSE '[]'::jsonb END
  ) AS action
 ) AS expanded
), '[]'::jsonb);
CREATE OR REPLACE FUNCTION public.forest_message_handler_chain_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE inbox record; conversation record; result_disposition text; failure_retryable boolean;
 failure_retry_at timestamptz; has_attempt boolean; call_status text;
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
 SELECT c.status INTO call_status FROM public.bot_message_handler_attempts a
  LEFT JOIN public.bot_calls c ON c.bot_id=a.bot_id AND c.operation_id=a.call_operation_id
  WHERE (a.bot_id,a.chain_id,a.attempt_no)=(OLD.bot_id,OLD.chain_id,OLD.attempt_count);
 -- Revocation after a claim suppresses a completed result or abandons work that
 -- was never durably admitted. An ADMITTED call must settle before cancellation.
 IF OLD.status='PENDING' AND OLD.claim_actor_id IS NOT NULL
  AND NEW.status='CANCELLED' AND NEW.current_index=OLD.current_index
  AND NEW.attempt_count=OLD.attempt_count AND NEW.claim_actor_id IS NULL
  AND ROW(NEW.next_attempt_at,NEW.last_error_code) IS NOT DISTINCT FROM
      ROW(OLD.next_attempt_at,OLD.last_error_code)
  AND NEW.completed_at IS NOT NULL
  AND ((call_status='COMPLETED' AND NEW.terminal_reason='POLICY_REVOKED_AFTER_CALL')
    OR (call_status IS DISTINCT FROM 'ADMITTED' AND call_status IS DISTINCT FROM 'COMPLETED'
        AND NEW.terminal_reason='POLICY_REVOKED_AFTER_CLAIM')) THEN
  RETURN NEW;
 END IF;
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


