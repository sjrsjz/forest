-- Exact effects and idempotency metadata are retained; original content is
-- imported and removed by the checksummed handler in this same transaction.
ALTER TABLE public.bot_send_intents ADD COLUMN lineage_digest text;
ALTER TABLE public.bot_send_parts
 ADD COLUMN payload_id text,
 ADD COLUMN receipt_payload_id text,
 ADD FOREIGN KEY(bot_id,payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ADD FOREIGN KEY(bot_id,receipt_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ALTER COLUMN payload DROP NOT NULL;
ALTER TABLE public.bot_send_attempts
 ADD COLUMN wire_payload_id text,
 ADD COLUMN receipt_payload_id text,
 ADD COLUMN deletion_epoch bigint NOT NULL DEFAULT 0 CHECK(deletion_epoch>=0),
 ADD FOREIGN KEY(bot_id,wire_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ADD FOREIGN KEY(bot_id,receipt_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ALTER COLUMN wire_body DROP NOT NULL;
ALTER TABLE public.bot_delivery_resolutions
 ADD COLUMN payload_id text,
 ADD COLUMN result_metadata jsonb NOT NULL DEFAULT '{}',
 ADD FOREIGN KEY(bot_id,payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ALTER COLUMN reason DROP NOT NULL,
 ALTER COLUMN result DROP NOT NULL;
DROP TRIGGER bot_send_parts_immutable ON public.bot_send_parts;
DROP TRIGGER bot_send_attempts_immutable ON public.bot_send_attempts;
DROP TRIGGER bot_delivery_resolutions_immutable ON public.bot_delivery_resolutions;
