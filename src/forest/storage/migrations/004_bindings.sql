-- Account ownership is a deployment fact; individual Bots cannot independently
-- decide that they both own the same external account.
CREATE TABLE platform_bindings (
    binding_id text PRIMARY KEY,
    bot_id text NOT NULL REFERENCES bot_catalog(bot_id),
    platform text NOT NULL CHECK (platform='onebot-v11'),
    self_id text NOT NULL,
    operation_id text NOT NULL UNIQUE,
    subject_id text NOT NULL REFERENCES web_accounts(id),
    input_digest text NOT NULL,
    payload jsonb NOT NULL,
    stage text NOT NULL DEFAULT 'RESERVED' CHECK (stage IN ('RESERVED','REGISTERED')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (platform,self_id)
);
CREATE FUNCTION forest_binding_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.binding_id,NEW.bot_id,NEW.platform,NEW.self_id,NEW.operation_id,
           NEW.subject_id,NEW.input_digest,NEW.payload,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.binding_id,OLD.bot_id,OLD.platform,OLD.self_id,OLD.operation_id,
           OLD.subject_id,OLD.input_digest,OLD.payload,OLD.created_at) THEN
        RAISE EXCEPTION 'Binding ownership is immutable' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER platform_binding_identity BEFORE UPDATE ON platform_bindings
    FOR EACH ROW EXECUTE FUNCTION forest_binding_identity();
CREATE TRIGGER platform_binding_no_delete BEFORE DELETE ON platform_bindings
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();
