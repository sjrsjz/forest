-- Host-owned management input is a deletable independent source domain.
-- Shell/automatic copies must additionally retain their real input parents.
-- This foundation does not migrate old control/configuration/Job bodies.
ALTER TABLE public.bot_payload_domains DROP CONSTRAINT bot_payload_domains_kind_check;
ALTER TABLE public.bot_payload_domains ADD CONSTRAINT bot_payload_domains_kind_check
 CHECK(kind IN ('SERVICE_INPUT','GENERATION_CONFIG','STATE_CONTRACT','CONTROL_INPUT'));
