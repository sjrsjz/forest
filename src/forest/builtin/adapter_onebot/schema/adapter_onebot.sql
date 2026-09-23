CREATE TABLE platform_events (
    event_id text PRIMARY KEY CHECK (octet_length(event_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL UNIQUE CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    binding_ref text NOT NULL CHECK (octet_length(binding_ref) BETWEEN 1 AND 128),
    binding_epoch bigint NOT NULL CHECK (binding_epoch > 0),
    client_incarnation text NOT NULL CHECK (octet_length(client_incarnation) BETWEEN 1 AND 256),
    platform_message_ref text NOT NULL CHECK (octet_length(platform_message_ref) BETWEEN 1 AND 256),
    event_kind text NOT NULL CHECK (octet_length(event_kind) BETWEEN 1 AND 128),
    channel_kind text NOT NULL CHECK (channel_kind IN ('GROUP','PRIVATE')),
    channel_ref text NOT NULL CHECK (octet_length(channel_ref) BETWEEN 1 AND 256),
    conversation_ref text NOT NULL CHECK (octet_length(conversation_ref) BETWEEN 1 AND 256),
    author_subject_ref text NOT NULL CHECK (octet_length(author_subject_ref) BETWEEN 1 AND 256),
    raw_payload jsonb NOT NULL,
    normalized_payload jsonb NOT NULL,
    author_payload jsonb NOT NULL,
    received_at timestamptz NOT NULL,
    classification_deadline timestamptz NOT NULL,
    sent_at timestamptz NOT NULL,
    input_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (classification_deadline >= received_at)
);

CREATE INDEX platform_events_conversation_idx
ON platform_events(conversation_ref, received_at DESC, event_id DESC);

CREATE FUNCTION adapter_record_ingress(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    value jsonb := input->'input';
    event jsonb := value->'event';
    existing __forest_module_schema__.platform_events%ROWTYPE;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR input->'caller'->>'component_id' IS DISTINCT FROM 'adapter.onebot'
       OR value->>'operation_id' IS NULL OR event->>'event_id' IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
          jsonb_build_object('code','ONEBOT_INGRESS_INPUT_INVALID','message','OneBot ingress participant input is invalid','status',422));
    END IF;
    SELECT * INTO existing FROM __forest_module_schema__.platform_events
    WHERE event_id=event->>'event_id' OR operation_id=value->>'operation_id' FOR UPDATE;
    IF FOUND THEN
        IF existing.event_id=event->>'event_id' AND existing.operation_id=value->>'operation_id'
           AND existing.input_payload=input THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
              jsonb_build_object('event_id',existing.event_id,'binding_ref',existing.binding_ref,'binding_epoch',existing.binding_epoch));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
          jsonb_build_object('code','ONEBOT_EVENT_CONFLICT','message','OneBot event identity was reused','status',409));
    END IF;
    INSERT INTO __forest_module_schema__.platform_events(
      event_id,operation_id,binding_ref,binding_epoch,client_incarnation,platform_message_ref,
      event_kind,channel_kind,channel_ref,conversation_ref,author_subject_ref,raw_payload,
      normalized_payload,author_payload,received_at,classification_deadline,sent_at,input_payload
    ) VALUES (
      event->>'event_id',value->>'operation_id',value->>'binding_ref',(value->>'binding_epoch')::bigint,
      value->>'client_incarnation',event->>'platform_message_ref',event->>'event_kind',event->>'channel_kind',
      event->>'channel_ref',event->>'conversation_ref',event->>'author_subject_ref',event->'raw',
      event->'normalized',event->'author',(event->>'received_at')::timestamptz,
      (event->>'classification_deadline')::timestamptz,(event->>'sent_at')::timestamptz,input
    );
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
      jsonb_build_object('event_id',event->>'event_id','binding_ref',value->>'binding_ref','binding_epoch',(value->>'binding_epoch')::bigint));
EXCEPTION WHEN unique_violation THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','ONEBOT_EVENT_CONFLICT','message','OneBot event identity was reused','status',409));
WHEN invalid_text_representation OR datetime_field_overflow OR not_null_violation OR check_violation THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','ONEBOT_INGRESS_INPUT_INVALID','message','OneBot ingress participant input is invalid','status',422));
END;
$$;
