CREATE TABLE published_bindings (
    generation_id text NOT NULL CHECK (octet_length(generation_id) BETWEEN 1 AND 256),
    graph_id text NOT NULL CHECK (octet_length(graph_id) BETWEEN 1 AND 256),
    config_revision bigint NOT NULL CHECK (config_revision > 0),
    config_payload_digest text NOT NULL CHECK (octet_length(config_payload_digest) BETWEEN 1 AND 128),
    binding_ref text NOT NULL CHECK (octet_length(binding_ref) BETWEEN 1 AND 128),
    binding_epoch bigint NOT NULL CHECK (binding_epoch > 0),
    self_id text NOT NULL CHECK (self_id ~ '^[1-9][0-9]{0,18}$'),
    enabled boolean NOT NULL,
    PRIMARY KEY (generation_id, binding_ref),
    UNIQUE (generation_id, self_id)
);

CREATE TABLE platform_events (
    event_id text PRIMARY KEY CHECK (octet_length(event_id) BETWEEN 1 AND 256),
    generation_id text NOT NULL CHECK (octet_length(generation_id) BETWEEN 1 AND 256),
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
    published __forest_module_schema__.published_bindings%ROWTYPE;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR input->'caller'->>'component_id' IS DISTINCT FROM 'adapter.onebot'
       OR input#>>'{caller,generation_id}' IS DISTINCT FROM context->>'generation_id'
       OR value->>'operation_id' IS NULL OR event->>'event_id' IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
          jsonb_build_object('code','ONEBOT_INGRESS_INPUT_INVALID','message','OneBot ingress participant input is invalid','status',422));
    END IF;
    SELECT * INTO published FROM __forest_module_schema__.published_bindings
      WHERE generation_id=context->>'generation_id'
        AND binding_ref=value->>'binding_ref' FOR SHARE;
    IF NOT FOUND OR NOT published.enabled
       OR published.binding_epoch IS DISTINCT FROM (value->>'binding_epoch')::bigint THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
          jsonb_build_object('code','ONEBOT_BINDING_NOT_CURRENT','message','OneBot binding is not current','status',409));
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
      event_id,generation_id,operation_id,binding_ref,binding_epoch,client_incarnation,platform_message_ref,
      event_kind,channel_kind,channel_ref,conversation_ref,author_subject_ref,raw_payload,
      normalized_payload,author_payload,received_at,classification_deadline,sent_at,input_payload
    ) VALUES (
      event->>'event_id',context->>'generation_id',value->>'operation_id',value->>'binding_ref',(value->>'binding_epoch')::bigint,
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
WHEN invalid_text_representation OR numeric_value_out_of_range OR datetime_field_overflow OR not_null_violation OR check_violation THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','ONEBOT_INGRESS_INPUT_INVALID','message','OneBot ingress participant input is invalid','status',422));
END;
$$;


-- OneBot protocol ownership stays in this reloadable package. The kernel has
-- already compared raw_frame with the immutable Raw Payload in the same UoW.
CREATE FUNCTION onebot_normalize_id(value jsonb, message_id boolean DEFAULT false)
RETURNS text LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE raw text; number numeric;
BEGIN
    IF value IS NULL OR jsonb_typeof(value) NOT IN ('string','number') THEN
        RETURN NULL;
    END IF;
    raw:=value#>>'{}';
    IF raw !~ '^-?(0|[1-9][0-9]{0,18})$' THEN
        RETURN NULL;
    END IF;
    number:=raw::numeric;
    IF message_id THEN
        IF number < -2147483648 OR number >= 2147483648 THEN RETURN NULL; END IF;
    ELSIF number < 1 OR number >= 9223372036854775808 THEN
        RETURN NULL;
    END IF;
    RETURN number::bigint::text;
END;
$$;

CREATE FUNCTION adapter_prove_shell_origin(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    raw jsonb:=envelope#>'{input,raw_frame}';
    gate jsonb:=envelope#>'{prior,gate}';
    recorded __forest_module_schema__.platform_events%ROWTYPE;
    published __forest_module_schema__.published_bindings%ROWTYPE;
    raw_self text; raw_user text; raw_channel text; raw_message text;
    raw_sender text; anonymous jsonb; anonymous_id text; expected_principal text;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'adapter.onebot'
       OR envelope#>>'{caller,generation_id}' IS DISTINCT FROM context->>'generation_id'
       OR gate->>'protocol' IS DISTINCT FROM 'forest.shell-ingress-proof/1'
       OR jsonb_typeof(raw) IS DISTINCT FROM 'object'
       OR raw->>'post_type' IS DISTINCT FROM 'message'
       OR raw->>'message_type' IS DISTINCT FROM 'group' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
          jsonb_build_object('code','ONEBOT_SHELL_ORIGIN_INVALID','message','OneBot Shell origin is invalid','status',409));
    END IF;
    SELECT * INTO recorded FROM __forest_module_schema__.platform_events
      WHERE event_id=gate->>'event_ref';
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
          jsonb_build_object('code','ONEBOT_SHELL_ORIGIN_INVALID','message','OneBot Shell origin is invalid','status',409));
    END IF;
    SELECT * INTO published FROM __forest_module_schema__.published_bindings
      WHERE generation_id=context->>'generation_id'
        AND binding_ref=recorded.binding_ref FOR SHARE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
          jsonb_build_object('code','ONEBOT_SHELL_ORIGIN_INVALID','message','OneBot Shell origin is invalid','status',409));
    END IF;
    raw_self:=__forest_module_schema__.onebot_normalize_id(raw->'self_id');
    raw_user:=__forest_module_schema__.onebot_normalize_id(raw->'user_id');
    raw_channel:=__forest_module_schema__.onebot_normalize_id(raw->'group_id');
    raw_message:=__forest_module_schema__.onebot_normalize_id(raw->'message_id',true);
    raw_sender:=__forest_module_schema__.onebot_normalize_id(raw#>'{sender,user_id}');
    anonymous:=raw->'anonymous';
    IF anonymous IS NOT NULL AND anonymous <> 'null'::jsonb THEN
        anonymous_id:=__forest_module_schema__.onebot_normalize_id(anonymous->'id');
        expected_principal:='onebot:anonymous:'||anonymous_id;
    ELSIF raw->>'sub_type'='anonymous' THEN
        expected_principal:='onebot:anonymous:'||substring(recorded.event_id from 14 for 20);
    ELSE
        expected_principal:='qq:'||raw_user;
    END IF;
    IF raw_self IS NULL OR raw_user IS NULL OR raw_channel IS NULL
       OR raw_message IS NULL OR expected_principal IS NULL
       OR (raw#>'{sender,user_id}' IS NOT NULL
           AND raw#>'{sender,user_id}' <> 'null'::jsonb
           AND raw_sender IS DISTINCT FROM raw_user)
       OR NOT published.enabled
       OR published.self_id IS DISTINCT FROM raw_self
       OR published.binding_epoch IS DISTINCT FROM recorded.binding_epoch
       OR recorded.generation_id IS DISTINCT FROM context->>'generation_id'
       OR gate->>'self_id' IS DISTINCT FROM raw_self
       OR gate->>'event_ref' IS DISTINCT FROM recorded.event_id
       OR gate->>'binding_ref' IS DISTINCT FROM recorded.binding_ref
       OR gate->'binding_epoch' IS DISTINCT FROM to_jsonb(recorded.binding_epoch)
       OR gate->>'conversation_ref' IS DISTINCT FROM recorded.conversation_ref
       OR gate->>'principal_ref' IS DISTINCT FROM expected_principal
       OR gate->>'adapter_generation_id' IS DISTINCT FROM envelope#>>'{caller,generation_id}'
       OR envelope#>>'{prior,adapter,event_id}' IS DISTINCT FROM recorded.event_id
       OR envelope#>>'{prior,adapter,binding_ref}' IS DISTINCT FROM recorded.binding_ref
       OR envelope#>'{prior,adapter,binding_epoch}' IS DISTINCT FROM to_jsonb(recorded.binding_epoch)
       OR recorded.channel_kind IS DISTINCT FROM 'GROUP'
       OR recorded.channel_ref IS DISTINCT FROM raw_channel
       OR recorded.conversation_ref IS DISTINCT FROM 'onebot:group:'||raw_channel
       OR recorded.platform_message_ref IS DISTINCT FROM raw_message
       OR recorded.author_subject_ref IS DISTINCT FROM expected_principal
       OR recorded.event_kind IS DISTINCT FROM 'group.message'
       OR envelope#>>'{prior,conversation,conversation_id}' IS DISTINCT FROM recorded.conversation_ref THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
          jsonb_build_object('code','ONEBOT_SHELL_ORIGIN_INVALID','message','OneBot Shell origin is invalid','status',409));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
      jsonb_build_object('protocol','forest.onebot-shell-origin-proof/1',
        'event_ref',recorded.event_id,'binding_ref',recorded.binding_ref,
        'binding_epoch',recorded.binding_epoch,'conversation_ref',recorded.conversation_ref,
        'principal_ref',expected_principal,'self_id',raw_self,
        'adapter_generation_id',context->>'generation_id',
        'platform_message_ref',raw_message));
END;
$$;


-- This port is called only by the generic RuntimeGraph publication transaction.
-- It receives a verified module configuration and stores only public binding
-- identity facts; credentials and endpoints never enter the projection.
CREATE FUNCTION adapter_publish_configuration(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    config jsonb:=input->'value';
    binding jsonb;
    saved __forest_module_schema__.published_bindings%ROWTYPE;
    expected_count integer;
    actual_count integer;
    binding_self text;
BEGIN
    IF context->>'port_kind' IS DISTINCT FROM 'publication'
       OR context->>'component_id' IS DISTINCT FROM 'adapter.onebot'
       OR context->>'generation_id' IS DISTINCT FROM input->>'generation_id'
       OR input->>'protocol' IS DISTINCT FROM 'forest.configuration-publication/1'
       OR config->>'protocol' IS DISTINCT FROM 'forest.adapter.onebot/2'
       OR jsonb_typeof(config->'bindings') IS DISTINCT FROM 'array'
       OR jsonb_array_length(config->'bindings') NOT BETWEEN 1 AND 16
       OR (input->>'revision')::bigint < 1
       OR input->>'payload_digest' IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
          jsonb_build_object('code','ONEBOT_PUBLICATION_INVALID','message','OneBot configuration publication is invalid','status',409));
    END IF;
    expected_count:=jsonb_array_length(config->'bindings');
    FOR binding IN SELECT value FROM jsonb_array_elements(config->'bindings') LOOP
        binding_self:=__forest_module_schema__.onebot_normalize_id(binding->'self_id');
        IF binding_self IS NULL
           OR jsonb_typeof(binding->'enabled') IS DISTINCT FROM 'boolean'
           OR jsonb_typeof(binding->'binding_epoch') IS DISTINCT FROM 'number'
           OR length(binding->>'binding_ref') NOT BETWEEN 1 AND 128 THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
              jsonb_build_object('code','ONEBOT_PUBLICATION_INVALID','message','OneBot configuration publication is invalid','status',409));
        END IF;
        INSERT INTO __forest_module_schema__.published_bindings(
          generation_id,graph_id,config_revision,config_payload_digest,
          binding_ref,binding_epoch,self_id,enabled
        ) VALUES (
          input->>'generation_id',input->>'graph_id',(input->>'revision')::bigint,
          input->>'payload_digest',binding->>'binding_ref',
          (binding->>'binding_epoch')::bigint,binding_self,(binding->>'enabled')::boolean
        ) ON CONFLICT (generation_id,binding_ref) DO NOTHING;
        SELECT * INTO saved FROM __forest_module_schema__.published_bindings
          WHERE generation_id=input->>'generation_id'
            AND binding_ref=binding->>'binding_ref';
        IF NOT FOUND OR saved.graph_id IS DISTINCT FROM input->>'graph_id'
           OR saved.config_revision IS DISTINCT FROM (input->>'revision')::bigint
           OR saved.config_payload_digest IS DISTINCT FROM input->>'payload_digest'
           OR saved.binding_epoch IS DISTINCT FROM (binding->>'binding_epoch')::bigint
           OR saved.self_id IS DISTINCT FROM binding_self
           OR saved.enabled IS DISTINCT FROM (binding->>'enabled')::boolean THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
              jsonb_build_object('code','ONEBOT_PUBLICATION_CONFLICT','message','OneBot published binding differs','status',409));
        END IF;
    END LOOP;
    SELECT count(*) INTO actual_count FROM __forest_module_schema__.published_bindings
      WHERE generation_id=input->>'generation_id';
    IF actual_count<>expected_count THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
          jsonb_build_object('code','ONEBOT_PUBLICATION_CONFLICT','message','OneBot published binding count differs','status',409));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
      jsonb_build_object('generation_id',input->>'generation_id',
                         'config_revision',(input->>'revision')::bigint,
                         'binding_count',actual_count));
EXCEPTION WHEN unique_violation OR invalid_text_representation OR numeric_value_out_of_range
    OR not_null_violation OR check_violation THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','ONEBOT_PUBLICATION_INVALID','message','OneBot configuration publication is invalid','status',409));
END;
$$;
