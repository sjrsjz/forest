"""Frozen 063 Shell body import and post-cutover integrity guards."""

from psycopg.types.json import Jsonb

from forest.storage.payloads import PayloadStore
from forest.storage.shell_payloads import (
    arguments_envelope,
    arguments_metadata,
    delivery_metadata,
    outcome_metadata,
    owner_id,
)


async def _source(conn, store, row):
    source = await (await conn.execute(
        "SELECT payload_id FROM public.bot_message_inbox WHERE bot_id=%s AND event_id=%s",
        (store.bot_id, row["event_id"]),
    )).fetchone()
    if not source or not source["payload_id"]:
        raise ValueError("Legacy Shell command has no migrated source payload")
    return source["payload_id"]


async def migrate_in(conn, store):
    payloads = PayloadStore(store)
    after = None
    while True:
        rows = await (await conn.execute(
            "SELECT * FROM public.bot_shell_requests WHERE bot_id=%s "
            "AND (%s::text IS NULL OR command_id>%s) ORDER BY command_id LIMIT 64",
            (store.bot_id, after, after),
        )).fetchall()
        if not rows:
            break
        for row in rows:
            source = await _source(conn, store, row)
            envelope = arguments_envelope(
                row["command_name"], row["gate_version"], row["arguments"]
            )
            arguments_ref = await payloads.import_legacy_json_in(
                conn,
                owner_kind="shell.arguments",
                owner_id=owner_id("arguments", row["command_id"]),
                value=envelope,
                parents=(source,),
            )
            values = {
                "arguments_payload_id": arguments_ref.payload_id,
                "arguments_payload_digest": arguments_ref.digest,
                "arguments_metadata": Jsonb(arguments_metadata(
                    row["command_name"], row["gate_version"], row["arguments"]
                )),
            }
            if row["status"] == "COMPLETED":
                parents = [source, arguments_ref.payload_id]
                operation = await (await conn.execute(
                    "SELECT result_payload_id FROM public.bot_web_operations WHERE bot_id=%s "
                    "AND operation_id=%s",
                    (store.bot_id, "shell-action:" + row["command_id"]),
                )).fetchone()
                if operation and operation["result_payload_id"]:
                    parents.append(operation["result_payload_id"])
                outcome_ref = await payloads.import_legacy_json_in(
                    conn,
                    owner_kind="control.result",
                    owner_id=owner_id("outcome", row["command_id"]),
                    value={"command_id": row["command_id"], "outcome": row["outcome"]},
                    parents=tuple(parents),
                )
                delivery_parents = [outcome_ref.payload_id]
                if isinstance(row["delivery"], dict) and isinstance(
                    row["delivery"].get("intent_id"), str
                ):
                    parts = await (await conn.execute(
                        "SELECT payload_id FROM public.bot_send_parts WHERE bot_id=%s "
                        "AND intent_id=%s ORDER BY part_index",
                        (store.bot_id, row["delivery"]["intent_id"]),
                    )).fetchall()
                    delivery_parents.extend(part["payload_id"] for part in parts if part["payload_id"])
                delivery_ref = await payloads.import_legacy_json_in(
                    conn,
                    owner_kind="control.outbox",
                    owner_id=owner_id("delivery", row["command_id"]),
                    value={"command_id": row["command_id"], "delivery": row["delivery"]},
                    parents=tuple(delivery_parents),
                )
                values.update({
                    "outcome_payload_id": outcome_ref.payload_id,
                    "outcome_payload_digest": outcome_ref.digest,
                    "outcome_metadata": Jsonb(
                        outcome_metadata(row["outcome"]) if row["outcome"] is not None else {}
                    ),
                    "delivery_payload_id": delivery_ref.payload_id,
                    "delivery_payload_digest": delivery_ref.digest,
                    "delivery_metadata": Jsonb(delivery_metadata(
                        row["delivery"], legacy_untracked=True
                    )),
                    "legacy_result_untracked": True,
                })
            elif row["outcome"] is not None or row["delivery"] is not None:
                raise ValueError("Non-terminal legacy Shell request contains a partial result")
            await conn.execute(
                "UPDATE public.bot_shell_requests SET arguments=NULL,outcome=NULL,delivery=NULL,"
                "arguments_payload_id=%s,arguments_payload_digest=%s,arguments_metadata=%s,"
                "outcome_payload_id=%s,outcome_payload_digest=%s,outcome_metadata=%s,"
                "delivery_payload_id=%s,delivery_payload_digest=%s,delivery_metadata=%s,"
                "legacy_result_untracked=%s WHERE bot_id=%s AND command_id=%s",
                (
                    values["arguments_payload_id"], values["arguments_payload_digest"],
                    values["arguments_metadata"], values.get("outcome_payload_id"),
                    values.get("outcome_payload_digest"), values.get("outcome_metadata"),
                    values.get("delivery_payload_id"), values.get("delivery_payload_digest"),
                    values.get("delivery_metadata"), values.get("legacy_result_untracked", False),
                    store.bot_id, row["command_id"],
                ),
            )
        after = rows[-1]["command_id"]
    await conn.execute(FINAL_SQL)


FINAL_SQL = r"""
ALTER TABLE public.bot_shell_requests
 ALTER COLUMN arguments_payload_id SET NOT NULL,
 ALTER COLUMN arguments_payload_digest SET NOT NULL,
 ALTER COLUMN arguments_metadata SET NOT NULL,
 ADD CHECK(arguments IS NULL AND outcome IS NULL AND delivery IS NULL),
 ADD CHECK(arguments_payload_digest ~ '^sha256:[0-9a-f]{64}$'),
 ADD CHECK(outcome_payload_digest IS NULL OR outcome_payload_digest ~ '^sha256:[0-9a-f]{64}$'),
 ADD CHECK(delivery_payload_digest IS NULL OR delivery_payload_digest ~ '^sha256:[0-9a-f]{64}$'),
 ADD CHECK(jsonb_typeof(arguments_metadata)='object'),
 ADD CHECK((status='COMPLETED')=(outcome_payload_id IS NOT NULL
  AND outcome_payload_digest IS NOT NULL AND outcome_metadata IS NOT NULL
  AND delivery_payload_id IS NOT NULL AND delivery_payload_digest IS NOT NULL
  AND delivery_metadata IS NOT NULL)),
 ADD CHECK(status<>'CANCELLED' OR (terminal_reason IS NOT NULL AND completed_at IS NOT NULL));

CREATE FUNCTION public.forest_shell_payload_value(
 selected_bot text,selected_command text,selected_role text,
 selected_payload text,selected_digest text)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE kind text; prefix text; expected_owner text; expected_payload text; h record; envelope jsonb;
BEGIN
 IF selected_bot IS NULL OR selected_command IS NULL OR selected_command=''
  OR octet_length(selected_command)>256 OR selected_command ~ '[\x01-\x1f\x7f]'
  OR selected_role NOT IN ('arguments','outcome','delivery')
  OR selected_payload IS NULL OR selected_digest !~ '^sha256:[0-9a-f]{64}$' THEN
  RAISE EXCEPTION 'SHELL_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 kind := CASE selected_role WHEN 'arguments' THEN 'shell.arguments'
  WHEN 'outcome' THEN 'control.result' ELSE 'control.outbox' END;
 prefix := CASE selected_role WHEN 'arguments' THEN 'shell-arguments-v1:'
  WHEN 'outcome' THEN 'shell-outcome-v1:' ELSE 'shell-delivery-v1:' END;
 expected_owner := prefix || 'sha256:' || encode(sha256(convert_to(
  '[' || to_jsonb(selected_command)::text || ']','UTF8')),'hex');
 expected_payload := 'payload:sha256:' || encode(sha256(convert_to(
  '[' || to_jsonb(selected_bot)::text || ',' || to_jsonb(kind)::text || ','
   || to_jsonb(expected_owner)::text || ']','UTF8')),'hex');
 SELECT p.*,b.content INTO h FROM public.bot_payload_headers p
 LEFT JOIN public.bot_payload_bodies b USING(bot_id,payload_id)
 WHERE p.bot_id=selected_bot AND p.payload_id=selected_payload;
 IF NOT FOUND OR selected_payload IS DISTINCT FROM expected_payload
  OR h.owner_kind IS DISTINCT FROM kind OR h.owner_id IS DISTINCT FROM expected_owner
  OR h.encoding IS DISTINCT FROM 'json' OR h.digest IS DISTINCT FROM selected_digest
  OR NOT public.forest_job_payload_available_unlocked(selected_bot,selected_payload,selected_digest) THEN
  RAISE EXCEPTION 'CONTROL_CONTENT_REVOKED' USING ERRCODE='P0001';
 END IF;
 BEGIN envelope := convert_from(h.content,'UTF8')::jsonb;
 EXCEPTION WHEN invalid_text_representation OR character_not_in_repertoire OR untranslatable_character THEN
  RAISE EXCEPTION 'SHELL_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END;
 IF jsonb_typeof(envelope) IS DISTINCT FROM 'object' THEN
  RAISE EXCEPTION 'SHELL_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 IF selected_role='arguments' AND (
    NOT (envelope ?& ARRAY['gate_version','command_name','schema_digest','arguments'])
    OR envelope-ARRAY['gate_version','command_name','schema_digest','arguments']<>'{}'::jsonb
    OR jsonb_typeof(envelope->'gate_version')<>'string'
    OR jsonb_typeof(envelope->'command_name')<>'string'
    OR jsonb_typeof(envelope->'arguments')<>'object'
    OR (envelope->'schema_digest'<>'null'::jsonb
     AND (jsonb_typeof(envelope->'schema_digest')<>'string'
      OR envelope->>'schema_digest' !~ '^sha256:[0-9a-f]{64}$'))) THEN
  RAISE EXCEPTION 'SHELL_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 ELSIF selected_role='outcome' AND (
    NOT (envelope ?& ARRAY['command_id','outcome'])
    OR envelope-ARRAY['command_id','outcome']<>'{}'::jsonb
    OR envelope->>'command_id' IS DISTINCT FROM selected_command
    OR (envelope->'outcome'<>'null'::jsonb AND jsonb_typeof(envelope->'outcome')<>'object')) THEN
  RAISE EXCEPTION 'SHELL_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 ELSIF selected_role='delivery' AND (
    NOT (envelope ?& ARRAY['command_id','delivery'])
    OR envelope-ARRAY['command_id','delivery']<>'{}'::jsonb
    OR envelope->>'command_id' IS DISTINCT FROM selected_command
    OR (envelope->'delivery'<>'null'::jsonb AND jsonb_typeof(envelope->'delivery')<>'object')) THEN
  RAISE EXCEPTION 'SHELL_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 RETURN envelope;
END $$;

CREATE FUNCTION public.forest_shell_arguments_metadata(v jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE keys jsonb;
BEGIN
 SELECT COALESCE(jsonb_agg(key ORDER BY key COLLATE "C"),'[]'::jsonb)
 INTO keys FROM jsonb_object_keys(v->'arguments') key;
 RETURN jsonb_build_object('schema_digest',v->'schema_digest','argument_keys',keys);
END $$;

CREATE FUNCTION public.forest_shell_outcome_metadata(v jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
 IF v='null'::jsonb THEN RETURN '{}'::jsonb; END IF;
 IF jsonb_typeof(v)<>'object' OR jsonb_typeof(v->'code')<>'string'
  OR v->>'code' !~ '^[A-Z][A-Z0-9_]{0,127}$' THEN
  RAISE EXCEPTION 'SHELL_OUTCOME_INVALID' USING ERRCODE='23514';
 END IF;
 RETURN jsonb_build_object('code',v->'code');
END $$;

CREATE FUNCTION public.forest_shell_arguments_insert_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE envelope jsonb; source_payload text;
BEGIN
 envelope := public.forest_shell_payload_value(
  NEW.bot_id,NEW.command_id,'arguments',NEW.arguments_payload_id,NEW.arguments_payload_digest);
 SELECT payload_id INTO source_payload FROM public.bot_message_inbox
 WHERE bot_id=NEW.bot_id AND event_id=NEW.event_id;
 IF NEW.status<>'PENDING' OR NEW.legacy_result_untracked OR NEW.terminal_reason IS NOT NULL
  OR NEW.arguments IS NOT NULL OR envelope->>'gate_version' IS DISTINCT FROM NEW.gate_version
  OR envelope->>'command_name' IS DISTINCT FROM NEW.command_name
  OR NEW.arguments_metadata IS DISTINCT FROM public.forest_shell_arguments_metadata(envelope)
  OR (SELECT count(*) FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=NEW.arguments_payload_id)<>1
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=NEW.arguments_payload_id AND parent_payload_id=source_payload) THEN
  RAISE EXCEPTION 'SHELL_ARGUMENTS_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER bot_shell_arguments_insert_guard BEFORE INSERT ON public.bot_shell_requests
 FOR EACH ROW EXECUTE FUNCTION public.forest_shell_arguments_insert_guard();

CREATE OR REPLACE FUNCTION public.forest_shell_source_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR ROW(NEW.bot_id,NEW.command_id,NEW.event_id,NEW.conversation_id,NEW.binding_id,
     NEW.command_name,NEW.raw_digest,NEW.gate_version)
  IS DISTINCT FROM ROW(OLD.bot_id,OLD.command_id,OLD.event_id,OLD.conversation_id,OLD.binding_id,
     OLD.command_name,OLD.raw_digest,OLD.gate_version) THEN
  RAISE EXCEPTION 'Shell source and route are immutable' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER bot_shell_source_immutable BEFORE UPDATE OR DELETE ON public.bot_shell_requests
 FOR EACH ROW EXECUTE FUNCTION public.forest_shell_source_immutable();

CREATE OR REPLACE FUNCTION public.forest_shell_arguments_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF ROW(NEW.arguments,NEW.priority,NEW.arguments_payload_id,NEW.arguments_payload_digest,
     NEW.arguments_metadata)
  IS DISTINCT FROM ROW(OLD.arguments,OLD.priority,OLD.arguments_payload_id,OLD.arguments_payload_digest,
     OLD.arguments_metadata) THEN
  RAISE EXCEPTION 'Shell arguments and priority are immutable' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER bot_shell_arguments_immutable BEFORE UPDATE ON public.bot_shell_requests
 FOR EACH ROW EXECUTE FUNCTION public.forest_shell_arguments_immutable();

CREATE FUNCTION public.forest_shell_result_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE source_payload text; outcome_envelope jsonb; delivery_envelope jsonb;
 delivery_value jsonb; intent record; operation_result text;
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Shell request is immutable' USING ERRCODE='55000'; END IF;
 IF NEW.legacy_result_untracked IS DISTINCT FROM OLD.legacy_result_untracked THEN
  RAISE EXCEPTION 'Shell legacy result marker is immutable' USING ERRCODE='55000';
 END IF;
 IF OLD.status IN ('COMPLETED','CANCELLED') AND to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
  RAISE EXCEPTION 'Terminal Shell request is immutable' USING ERRCODE='55000';
 END IF;
 IF NEW.status='CANCELLED' THEN
  IF NEW.terminal_reason IS NULL OR NEW.completed_at IS NULL
   OR NEW.outcome_payload_id IS NOT NULL OR NEW.delivery_payload_id IS NOT NULL THEN
   RAISE EXCEPTION 'SHELL_TERMINAL_CONFLICT' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
 END IF;
 IF NEW.status<>'COMPLETED' THEN
  IF NEW.outcome_payload_id IS NOT NULL OR NEW.delivery_payload_id IS NOT NULL
   OR NEW.outcome_metadata IS NOT NULL OR NEW.delivery_metadata IS NOT NULL THEN
   RAISE EXCEPTION 'SHELL_TERMINAL_CONFLICT' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
 END IF;
 SELECT payload_id INTO source_payload FROM public.bot_message_inbox
 WHERE bot_id=NEW.bot_id AND event_id=NEW.event_id;
 outcome_envelope := public.forest_shell_payload_value(
  NEW.bot_id,NEW.command_id,'outcome',NEW.outcome_payload_id,NEW.outcome_payload_digest);
 delivery_envelope := public.forest_shell_payload_value(
  NEW.bot_id,NEW.command_id,'delivery',NEW.delivery_payload_id,NEW.delivery_payload_digest);
 delivery_value := delivery_envelope->'delivery';
 IF NEW.outcome IS NOT NULL OR NEW.delivery IS NOT NULL OR NEW.completed_at IS NULL
  OR outcome_envelope->>'command_id' IS DISTINCT FROM NEW.command_id
  OR delivery_envelope->>'command_id' IS DISTINCT FROM NEW.command_id
  OR NEW.outcome_metadata IS DISTINCT FROM public.forest_shell_outcome_metadata(outcome_envelope->'outcome')
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=NEW.outcome_payload_id AND parent_payload_id=source_payload)
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=NEW.outcome_payload_id AND parent_payload_id=NEW.arguments_payload_id)
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=NEW.delivery_payload_id AND parent_payload_id=NEW.outcome_payload_id) THEN
  RAISE EXCEPTION 'SHELL_RESULT_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 IF NEW.legacy_result_untracked THEN
  IF NEW.delivery_metadata IS DISTINCT FROM '{"legacy_untracked":true}'::jsonb THEN
   RAISE EXCEPTION 'SHELL_RESULT_IDENTITY_CONFLICT' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
 END IF;
 SELECT * INTO intent FROM public.bot_send_intents WHERE bot_id=NEW.bot_id
  AND intent_id=delivery_value->>'intent_id';
 IF NOT FOUND OR jsonb_typeof(delivery_value)<>'object'
  OR NEW.delivery_metadata IS DISTINCT FROM delivery_value
  OR ROW(intent.operation_id,intent.send_lane,intent.conversation_id,intent.response_seq,
      intent.command_id,intent.control_seq)
   IS DISTINCT FROM ROW(delivery_value->>'operation_id',delivery_value->>'send_lane',
      delivery_value->>'conversation_id',NULL::bigint,delivery_value->>'command_id',
      (delivery_value->>'control_seq')::bigint)
  OR intent.command_id IS DISTINCT FROM NEW.command_id OR intent.send_lane<>'CONTROL'
  OR NOT EXISTS(SELECT 1 FROM public.bot_send_parts p WHERE p.bot_id=NEW.bot_id
      AND p.intent_id=intent.intent_id)
  OR EXISTS(SELECT 1 FROM public.bot_send_parts p WHERE p.bot_id=NEW.bot_id
      AND p.intent_id=intent.intent_id AND NOT EXISTS(
       SELECT 1 FROM public.bot_payload_derivations d WHERE d.bot_id=p.bot_id
        AND d.payload_id=p.payload_id AND d.parent_payload_id=NEW.outcome_payload_id))
  OR EXISTS(SELECT 1 FROM public.bot_send_parts p WHERE p.bot_id=NEW.bot_id
      AND p.intent_id=intent.intent_id AND NOT EXISTS(
       SELECT 1 FROM public.bot_payload_derivations d WHERE d.bot_id=p.bot_id
        AND d.payload_id=NEW.delivery_payload_id AND d.parent_payload_id=p.payload_id)) THEN
  RAISE EXCEPTION 'SHELL_RESULT_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 SELECT result_payload_id INTO operation_result FROM public.bot_web_operations
 WHERE bot_id=NEW.bot_id AND operation_id='shell-action:' || NEW.command_id;
 IF operation_result IS NOT NULL AND public.forest_job_payload_available_unlocked(
    NEW.bot_id,operation_result,(SELECT digest FROM public.bot_payload_headers
     WHERE bot_id=NEW.bot_id AND payload_id=operation_result))
  AND NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=NEW.outcome_payload_id AND parent_payload_id=operation_result) THEN
  RAISE EXCEPTION 'SHELL_RESULT_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER bot_shell_result_guard BEFORE UPDATE OR DELETE ON public.bot_shell_requests
 FOR EACH ROW EXECUTE FUNCTION public.forest_shell_result_guard();

CREATE OR REPLACE FUNCTION public.forest_source_link_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE current_epoch bigint;
BEGIN
 SELECT deletion_epoch INTO current_epoch FROM public.bot_memory_clock WHERE bot_id=NEW.bot_id FOR UPDATE;
 IF current_epoch IS NULL OR NEW.accepted_deletion_epoch<>current_epoch OR NOT EXISTS(
  SELECT 1 FROM public.bot_message_echo_aliases e
   JOIN public.bot_message_inbox i ON i.bot_id=e.bot_id AND i.event_id=e.event_id
   JOIN public.bot_conversations c ON c.bot_id=i.bot_id AND c.conversation_id=i.conversation_id
   JOIN public.bot_bindings b ON b.bot_id=c.bot_id AND b.binding_id=c.binding_id
   JOIN public.bot_send_attempts a ON a.bot_id=e.bot_id AND a.attempt_id=e.attempt_id
   JOIN public.bot_send_intents s ON s.bot_id=a.bot_id AND s.intent_id=a.intent_id
   JOIN public.bot_memory_source_occurrences o ON o.bot_id=i.bot_id AND o.event_id=i.event_id
  WHERE e.bot_id=NEW.bot_id AND e.event_id=NEW.event_id AND a.attempt_id=NEW.attempt_id
   AND e.intent_id=a.intent_id
   AND a.wire_payload_id=NEW.parent_payload_id AND a.state='SENT'
   AND o.source_unit_id=NEW.dependent_source_unit_id
   AND i.author->>'is_self'='true' AND i.author->>'external_user_id'=b.self_id
   AND a.receipt->>'message_id'=i.platform_message_id AND e.platform_message_id=i.platform_message_id
   AND s.binding_id=c.binding_id AND s.conversation_id=c.conversation_id
   AND s.reply_route->>'self_id'=b.self_id AND s.reply_route->>'platform'=c.platform
   AND s.reply_route->>'channel_kind'=c.channel_kind AND s.reply_route->>'channel_id'=c.channel_id
   AND COALESCE(s.reply_route->>'thread_id','')=c.thread_id
   AND ((e.origin='chat_output' AND s.send_lane='CHAT' AND EXISTS(
     SELECT 1 FROM public.bot_conversation_replies r WHERE r.bot_id=e.bot_id AND r.reply_id=e.reply_id
      AND r.delivery->>'intent_id'=e.intent_id))
    OR (e.origin='control_output' AND s.send_lane='CONTROL' AND EXISTS(
     SELECT 1 FROM public.bot_shell_requests q WHERE q.bot_id=e.bot_id AND q.command_id=e.command_id
      AND q.command_id=s.command_id AND q.status='COMPLETED'
      AND q.delivery_metadata->>'intent_id'=e.intent_id)))
 ) THEN RAISE EXCEPTION 'SOURCE_LINK_PROOF_INVALID' USING ERRCODE='55000'; END IF;
 RETURN NEW;
END $$;
"""
