CREATE TABLE public.bot_bindings (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    binding_id text NOT NULL,
    platform text NOT NULL CHECK (platform='onebot-v11'),
    self_id text NOT NULL,
    display_name text NOT NULL,
    registry_operation_id text NOT NULL,
    revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
    effective_revision bigint NOT NULL DEFAULT 0 CHECK (effective_revision BETWEEN 0 AND revision),
    binding_epoch bigint NOT NULL DEFAULT 1 CHECK (binding_epoch > 0),
    connection_epoch bigint NOT NULL DEFAULT 0 CHECK (connection_epoch >= 0),
    host_incarnation text,
    host_owner_epoch bigint,
    client_incarnation text,
    enabled boolean NOT NULL DEFAULT false,
    receive_enabled boolean NOT NULL DEFAULT false,
    config jsonb NOT NULL DEFAULT '{}'::jsonb,
    state text NOT NULL DEFAULT 'DISABLED' CHECK (state IN ('DISABLED','OFFLINE','ONLINE','DRAINING')),
    error_code text,
    PRIMARY KEY (bot_id,binding_id),
    UNIQUE (bot_id,platform,self_id)
);
CREATE TABLE public.bot_binding_plans (
    bot_id text NOT NULL,
    binding_id text NOT NULL,
    plan_id text NOT NULL,
    subject_id text NOT NULL,
    issuer_id text NOT NULL,
    expected_revision bigint NOT NULL,
    values jsonb NOT NULL,
    secret_ciphertext text NOT NULL,
    plan_digest text NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (bot_id,plan_id),
    FOREIGN KEY (bot_id,binding_id) REFERENCES public.bot_bindings(bot_id,binding_id)
);
CREATE TABLE public.bot_binding_revisions (
    bot_id text NOT NULL,
    binding_id text NOT NULL,
    revision bigint NOT NULL CHECK (revision>0),
    previous_revision bigint NOT NULL CHECK (previous_revision=revision-1),
    plan_id text NOT NULL,
    values jsonb NOT NULL,
    secret_ciphertext text NOT NULL,
    policy_revision bigint NOT NULL,
    operation_id text NOT NULL,
    PRIMARY KEY (bot_id,binding_id,revision),
    FOREIGN KEY (bot_id,binding_id) REFERENCES public.bot_bindings(bot_id,binding_id),
    FOREIGN KEY (bot_id,plan_id) REFERENCES public.bot_binding_plans(bot_id,plan_id)
);
CREATE TABLE public.bot_binding_credentials (
    bot_id text NOT NULL,
    operation_id text NOT NULL,
    binding_id text NOT NULL,
    subject_id text NOT NULL,
    plaintext_digest text NOT NULL,
    secret_ciphertext text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    FOREIGN KEY (bot_id,binding_id) REFERENCES public.bot_bindings(bot_id,binding_id)
);
CREATE TABLE public.bot_binding_clients (
    bot_id text NOT NULL,
    client_incarnation text NOT NULL,
    binding_id text NOT NULL,
    revision bigint NOT NULL,
    binding_epoch bigint NOT NULL,
    connection_epoch bigint NOT NULL,
    host_incarnation text NOT NULL,
    host_owner_epoch bigint NOT NULL,
    PRIMARY KEY (bot_id,client_incarnation),
    UNIQUE (bot_id,binding_id,binding_epoch),
    UNIQUE (bot_id,binding_id,connection_epoch),
    FOREIGN KEY (bot_id,binding_id,revision)
        REFERENCES public.bot_binding_revisions(bot_id,binding_id,revision)
);
CREATE TRIGGER bot_binding_credentials_immutable BEFORE UPDATE OR DELETE ON public.bot_binding_credentials
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_binding_clients_immutable BEFORE UPDATE OR DELETE ON public.bot_binding_clients
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_binding_plans_immutable BEFORE UPDATE OR DELETE ON public.bot_binding_plans
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_binding_revisions_immutable BEFORE UPDATE OR DELETE ON public.bot_binding_revisions
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE FUNCTION public.forest_binding_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.binding_id,NEW.platform,NEW.self_id,NEW.registry_operation_id)
       IS DISTINCT FROM ROW(OLD.bot_id,OLD.binding_id,OLD.platform,OLD.self_id,OLD.registry_operation_id) THEN
        RAISE EXCEPTION 'Bot binding identity is immutable' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER bot_binding_identity BEFORE UPDATE ON public.bot_bindings
    FOR EACH ROW EXECUTE FUNCTION public.forest_binding_identity();
UPDATE public.bot_web_issuers SET actions=actions||
    '["binding.create","binding.read","binding.plan","binding.apply","message.read","message.cancel","delivery.reconcile"]'::jsonb;
