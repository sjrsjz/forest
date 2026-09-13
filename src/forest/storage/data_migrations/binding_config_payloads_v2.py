"""Frozen 047 Binding values and display-name import; ciphertext stays separate."""

from forest.storage.migration_v1 import _digest, legacy_source_ref
from forest.storage.payloads import PayloadStore


async def _parent(conn, bot_id, kind, row):
    if kind == "REVISION":
        operation = await (
            await conn.execute(
                "SELECT result_payload_id FROM public.bot_web_operations WHERE bot_id=%s "
                "AND operation_id=%s AND action='binding.apply'",
                (bot_id, row["operation_id"]),
            )
        ).fetchone()
        if operation:
            return operation["result_payload_id"]
    identity = row["plan_id"] if kind == "PLAN" else _digest([row["binding_id"], row["revision"]])
    bridge = await (
        await conn.execute(
            "SELECT payload_id FROM public.bot_control_value_sources WHERE bot_id=%s "
            "AND family='binding' AND source_kind=%s AND source_id=%s",
            (bot_id, kind, identity),
        )
    ).fetchone()
    if bridge is None:
        raise ValueError("Binding configuration has no immutable 041 source")
    return bridge["payload_id"]


async def _initial_parent(conn, store, row, payloads):
    operation = await (
        await conn.execute(
            "SELECT * FROM public.bot_web_operations WHERE bot_id=%s AND operation_id=%s "
            "AND action='binding.create'",
            (store.bot_id, row["registry_operation_id"]),
        )
    ).fetchone()
    value = {key: row[key] for key in ("binding_id", "self_id", "display_name")}
    if operation and operation["input_payload_digest"] == _digest(
        {
            "action": "binding.create",
            "operation_id": row["registry_operation_id"],
            "value": value,
        }
    ):
        # Exact immutable input digest proves this name even after its body was
        # removed. Importing from that real parent preserves the removal.
        return operation["input_payload_id"]
    # Old SQL did not freeze display_name. A matching account/current value is
    # not proof of a historical create; retain an explicit conservative anchor.
    ref = await payloads.import_legacy_json_in(
        conn,
        owner_kind="control.config",
        owner_id="binding-initial-legacy:" + _digest([row["binding_id"]]),
        value=value,
        sources=(legacy_source_ref(store.bot_id),),
    )
    return ref.payload_id


async def migrate_in(conn, store):
    payloads = PayloadStore(store)
    after = ""
    while True:
        rows = await (
            await conn.execute(
                "SELECT * FROM public.bot_bindings WHERE bot_id=%s AND binding_id>%s ORDER BY binding_id LIMIT 64",
                (store.bot_id, after),
            )
        ).fetchall()
        if not rows:
            break
        for row in rows:
            parent = await _initial_parent(conn, store, row, payloads)
            ref = await payloads.import_legacy_json_in(
                conn,
                owner_kind="control.config",
                owner_id="config-values-v1:"
                + _digest(
                    [
                        "binding",
                        "INITIAL",
                        row["binding_id"],
                        row["registry_operation_id"],
                    ]
                ),
                value={
                    "family": "binding",
                    "kind": "INITIAL",
                    "resource": row["binding_id"],
                    "identity": row["registry_operation_id"],
                    "values": {"display_name": row["display_name"]},
                },
                parents=(parent,),
            )
            await conn.execute(
                "UPDATE public.bot_bindings SET display_name=NULL,initial_payload_id=%s,initial_payload_digest=%s "
                "WHERE bot_id=%s AND binding_id=%s",
                (ref.payload_id, ref.digest, store.bot_id, row["binding_id"]),
            )
        after = rows[-1]["binding_id"]
    for kind, table, key in (
        ("PLAN", "bot_binding_plans", "plan_id"),
        ("REVISION", "bot_binding_revisions", "revision"),
    ):
        after = None
        while True:
            rows = await (
                await conn.execute(
                    f"SELECT * FROM public.{table} WHERE bot_id=%s AND (%s::text IS NULL OR "
                    f"(binding_id,{key}::text)>(%s,%s)) ORDER BY binding_id,{key}::text LIMIT 64",
                    (store.bot_id, after[0] if after else None, *(after or (None, None))),
                )
            ).fetchall()
            if not rows:
                break
            for row in rows:
                identity = str(row[key])
                parent = await _parent(conn, store.bot_id, kind, row)
                if (
                    not isinstance(row["values"], dict)
                    or type(row["values"].get("enabled")) is not bool
                ):
                    raise ValueError("Legacy Binding settings have no explicit enabled decision")
                ref = await payloads.import_legacy_json_in(
                    conn,
                    owner_kind="control.config",
                    owner_id="config-values-v1:"
                    + _digest(
                        [
                            "binding",
                            kind,
                            row["binding_id"],
                            identity,
                        ]
                    ),
                    value={
                        "family": "binding",
                        "kind": kind,
                        "resource": row["binding_id"],
                        "identity": identity,
                        "values": row["values"],
                    },
                    parents=(parent,),
                )
                await conn.execute(
                    f"UPDATE public.{table} SET settings_enabled=%s,values=NULL,values_payload_id=%s,"
                    f"values_payload_digest=%s WHERE bot_id=%s AND binding_id=%s AND {key}=%s",
                    (
                        row["values"]["enabled"],
                        ref.payload_id,
                        ref.digest,
                        store.bot_id,
                        row["binding_id"],
                        row[key],
                    ),
                )
            after = (rows[-1]["binding_id"], str(rows[-1][key]))
    # Compare legacy effective content using the immutable imported bytes (not
    # the availability facade: an already erased configuration still migrates).
    # Its digest remains a proof without making suppressed content readable.
    rows = await (
        await conn.execute(
            "SELECT b.*,r.values_payload_id,r.values_payload_digest FROM public.bot_bindings b "
            "LEFT JOIN public.bot_binding_revisions r ON (r.bot_id,r.binding_id,r.revision)="
            "(b.bot_id,b.binding_id,b.effective_revision) WHERE b.bot_id=%s",
            (store.bot_id,),
        )
    ).fetchall()
    for row in rows:
        if row["effective_revision"]:
            expected = _digest(
                {
                    "family": "binding",
                    "kind": "REVISION",
                    "resource": row["binding_id"],
                    "identity": str(row["effective_revision"]),
                    "values": row["config"],
                }
            )
            if expected != row["values_payload_digest"]:
                raise ValueError("Legacy Binding effective settings differ from their revision")
        elif row["config"] != {}:
            raise ValueError("Legacy initial Binding contains unpublished effective settings")
        await conn.execute(
            "UPDATE public.bot_bindings SET config=NULL,config_payload_id=%s,config_payload_digest=%s "
            "WHERE bot_id=%s AND binding_id=%s",
            (
                row["values_payload_id"],
                row["values_payload_digest"],
                store.bot_id,
                row["binding_id"],
            ),
        )
    await conn.execute(FINAL_SQL)


FINAL_SQL = """
ALTER TABLE public.bot_bindings ALTER COLUMN initial_payload_id SET NOT NULL,
 ALTER COLUMN initial_payload_digest SET NOT NULL,
 ADD CHECK(display_name IS NULL AND config IS NULL),
 ADD CHECK(initial_payload_digest ~ '^sha256:[0-9a-f]{64}$'),
 ADD CHECK((effective_revision=0 AND config_payload_id IS NULL AND config_payload_digest IS NULL)
  OR (effective_revision>0 AND config_payload_id IS NOT NULL AND config_payload_digest IS NOT NULL
   AND config_payload_digest ~ '^sha256:[0-9a-f]{64}$'));
ALTER TABLE public.bot_binding_plans ALTER COLUMN values_payload_id SET NOT NULL,
 ALTER COLUMN values_payload_digest SET NOT NULL, ALTER COLUMN settings_enabled SET NOT NULL,
 ADD CHECK(values IS NULL), ADD CHECK(values_payload_digest ~ '^sha256:[0-9a-f]{64}$');
ALTER TABLE public.bot_binding_revisions ALTER COLUMN values_payload_id SET NOT NULL,
 ALTER COLUMN values_payload_digest SET NOT NULL, ALTER COLUMN settings_enabled SET NOT NULL,
 ADD CHECK(values IS NULL), ADD CHECK(values_payload_digest ~ '^sha256:[0-9a-f]{64}$');
CREATE TRIGGER bot_binding_plans_immutable BEFORE UPDATE OR DELETE ON public.bot_binding_plans
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_binding_revisions_immutable BEFORE UPDATE OR DELETE ON public.bot_binding_revisions
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE FUNCTION public.forest_binding_values_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE k text; ident text; value jsonb; p record; op record; h record; input jsonb; result jsonb;
BEGIN
 SELECT * INTO h FROM public.bot_bindings WHERE bot_id=NEW.bot_id AND binding_id=NEW.binding_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'BINDING_CONFIG_HEAD_REQUIRED' USING ERRCODE='23514'; END IF;
 k := CASE TG_TABLE_NAME WHEN 'bot_binding_plans' THEN 'PLAN' ELSE 'REVISION' END;
 ident := CASE k WHEN 'PLAN' THEN NEW.plan_id ELSE to_jsonb(NEW)->>'revision' END;
 value := public.forest_config_payload_values(NEW.bot_id,NEW.values_payload_id,NEW.values_payload_digest,
  'binding',k,NEW.binding_id,ident);
 IF jsonb_typeof(value) IS DISTINCT FROM 'object' OR jsonb_typeof(value->'enabled') IS DISTINCT FROM 'boolean'
  OR value->'enabled' IS DISTINCT FROM to_jsonb(NEW.settings_enabled) THEN
  RAISE EXCEPTION 'BINDING_CONFIG_METADATA_CONFLICT' USING ERRCODE='23514';
 END IF;
 IF k='REVISION' THEN
  SELECT * INTO p FROM public.bot_binding_plans WHERE bot_id=NEW.bot_id AND plan_id=NEW.plan_id;
  SELECT * INTO op FROM public.bot_web_operations WHERE bot_id=NEW.bot_id AND operation_id=NEW.operation_id;
  IF p.plan_id IS NULL OR op.operation_id IS NULL OR op.action<>'binding.apply'
   OR p.binding_id IS DISTINCT FROM NEW.binding_id
   OR ROW(p.subject_id,p.issuer_id) IS DISTINCT FROM ROW(op.subject_id,op.issuer_id)
   OR NEW.previous_revision IS DISTINCT FROM p.expected_revision
   OR h.revision IS DISTINCT FROM NEW.previous_revision
   OR NEW.secret_ciphertext IS DISTINCT FROM p.secret_ciphertext
   OR to_jsonb(NEW.policy_revision) IS DISTINCT FROM op.ticket_basis->'policy_revision' THEN
   RAISE EXCEPTION 'BINDING_CONFIG_REVISION_SOURCE_CONFLICT' USING ERRCODE='23514';
  END IF;
  IF value IS DISTINCT FROM public.forest_config_payload_values(NEW.bot_id,p.values_payload_id,p.values_payload_digest,
   'binding','PLAN',NEW.binding_id,p.plan_id) OR NOT EXISTS(
   SELECT 1 FROM public.bot_control_value_sources b WHERE b.bot_id=NEW.bot_id AND b.family='binding'
    AND b.source_kind='PLAN' AND b.source_id=p.plan_id) THEN
   RAISE EXCEPTION 'BINDING_CONFIG_REVISION_SOURCE_CONFLICT' USING ERRCODE='23514';
  END IF;
  input := public.forest_control_payload_value(NEW.bot_id,op.input_payload_id,op.input_payload_digest,
   'input',op.operation_id,op.action);
  result := public.forest_control_payload_value(NEW.bot_id,op.result_payload_id,op.result_payload_digest,
   'result',op.operation_id,op.action);
  IF input IS DISTINCT FROM jsonb_build_object('binding_id',NEW.binding_id,'plan_id',p.plan_id,
   'plan_digest',p.plan_digest,'expected_revision',p.expected_revision)
   OR result IS DISTINCT FROM jsonb_build_object('binding_id',NEW.binding_id,'revision',NEW.revision,
    'effective_revision',h.effective_revision,'status','ACCEPTED')
   OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
    AND payload_id=NEW.values_payload_id AND parent_payload_id=p.values_payload_id)
   OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
    AND payload_id=NEW.values_payload_id AND parent_payload_id=op.input_payload_id)
   OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
    AND payload_id=op.result_payload_id AND parent_payload_id=NEW.values_payload_id) THEN
   RAISE EXCEPTION 'BINDING_CONFIG_REVISION_SOURCE_CONFLICT' USING ERRCODE='23514';
  END IF;
  IF p.expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'PLAN_EXPIRED' USING ERRCODE='P0001'; END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER binding_plan_values_guard BEFORE INSERT ON public.bot_binding_plans
 FOR EACH ROW EXECUTE FUNCTION public.forest_binding_values_guard();
CREATE TRIGGER binding_revision_values_guard BEFORE INSERT ON public.bot_binding_revisions
 FOR EACH ROW EXECUTE FUNCTION public.forest_binding_values_guard();

CREATE FUNCTION public.forest_binding_plan_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.bot_control_value_sources b JOIN public.bot_web_operations w
  ON w.bot_id=b.bot_id AND w.operation_id=b.operation_id WHERE b.bot_id=NEW.bot_id AND b.family='binding'
  AND b.source_kind='PLAN' AND b.source_id=NEW.plan_id AND NOT b.legacy_untracked AND w.action='binding.plan'
  AND ROW(w.result_payload_id,w.result_payload_digest) IS NOT DISTINCT FROM ROW(b.payload_id,b.payload_digest)
  AND ROW(w.subject_id,w.issuer_id) IS NOT DISTINCT FROM ROW(NEW.subject_id,NEW.issuer_id)) THEN
  RAISE EXCEPTION 'BINDING_PLAN_RECEIPT_REQUIRED' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER binding_plan_complete AFTER INSERT ON public.bot_binding_plans
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.forest_binding_plan_complete();

CREATE FUNCTION public.forest_binding_initial_complete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE op record; value jsonb; input jsonb;
BEGIN
 IF NEW.revision<>0 OR NEW.effective_revision<>0 OR NEW.binding_epoch<>1 OR NEW.connection_epoch<>0
  OR NEW.enabled OR NEW.receive_enabled OR NEW.state<>'DISABLED' OR NEW.client_incarnation IS NOT NULL
  OR NEW.host_incarnation IS NOT NULL OR NEW.host_owner_epoch IS NOT NULL THEN
  RAISE EXCEPTION 'BINDING_INITIAL_STATE_INVALID' USING ERRCODE='23514';
 END IF;
 SELECT * INTO op FROM public.bot_web_operations WHERE bot_id=NEW.bot_id AND operation_id=NEW.registry_operation_id;
 IF NOT FOUND OR op.action<>'binding.create' THEN
  RAISE EXCEPTION 'BINDING_INITIAL_SOURCE_REQUIRED' USING ERRCODE='23514';
 END IF;
 value := public.forest_config_payload_values(NEW.bot_id,NEW.initial_payload_id,NEW.initial_payload_digest,
  'binding','INITIAL',NEW.binding_id,NEW.registry_operation_id);
 input := public.forest_control_payload_value(NEW.bot_id,op.input_payload_id,op.input_payload_digest,
  'input',op.operation_id,op.action);
 IF value IS DISTINCT FROM jsonb_build_object('display_name',input->'display_name')
  OR jsonb_typeof(value->'display_name') IS DISTINCT FROM 'string'
  OR input->>'binding_id' IS DISTINCT FROM NEW.binding_id OR input->>'self_id' IS DISTINCT FROM NEW.self_id
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
    AND payload_id=NEW.initial_payload_id AND parent_payload_id=op.input_payload_id)
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
    AND payload_id=op.result_payload_id AND parent_payload_id=NEW.initial_payload_id) THEN
  RAISE EXCEPTION 'BINDING_INITIAL_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER binding_initial_complete AFTER INSERT ON public.bot_bindings
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.forest_binding_initial_complete();

CREATE FUNCTION public.forest_binding_config_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r record; c record; h record; runtime record; value jsonb;
BEGIN
 IF ROW(NEW.initial_payload_id,NEW.initial_payload_digest) IS DISTINCT FROM ROW(OLD.initial_payload_id,OLD.initial_payload_digest) THEN
  RAISE EXCEPTION 'BINDING_INITIAL_REFERENCE_IMMUTABLE' USING ERRCODE='55000';
 END IF;
 IF NEW.revision IS DISTINCT FROM OLD.revision THEN
  SELECT * INTO r FROM public.bot_binding_revisions WHERE bot_id=NEW.bot_id AND binding_id=NEW.binding_id
   AND revision=NEW.revision;
  IF r.revision IS NULL OR NEW.revision<>OLD.revision+1 OR r.previous_revision<>OLD.revision THEN
   RAISE EXCEPTION 'BINDING_DESIRED_REVISION_CONFLICT' USING ERRCODE='23514';
  END IF;
 END IF;
 -- Released schemas allowed an ONLINE revision-zero Binding with no client
 -- record.  During upgrade the new Host must be able to adopt that runtime
 -- row so its already durable conversations remain operable.  This narrow
 -- compatibility path changes only Host ownership; every content, revision,
 -- connection and admission field stays byte-for-byte fixed.
 IF ROW(NEW.host_incarnation,NEW.host_owner_epoch) IS DISTINCT FROM
    ROW(OLD.host_incarnation,OLD.host_owner_epoch)
  AND OLD.client_incarnation IS NULL
  AND ROW(NEW.revision,NEW.effective_revision,NEW.config_payload_id,NEW.config_payload_digest,
    NEW.binding_epoch,NEW.connection_epoch,NEW.client_incarnation,NEW.enabled,
    NEW.receive_enabled,NEW.state,NEW.error_code) IS NOT DISTINCT FROM
   ROW(OLD.revision,OLD.effective_revision,OLD.config_payload_id,OLD.config_payload_digest,
    OLD.binding_epoch,OLD.connection_epoch,OLD.client_incarnation,OLD.enabled,
    OLD.receive_enabled,OLD.state,OLD.error_code) THEN
  SELECT * INTO h FROM public.bot_host_lease WHERE bot_id=NEW.bot_id FOR UPDATE;
  IF ROW(NEW.host_incarnation,NEW.host_owner_epoch) IS DISTINCT FROM ROW(h.incarnation,h.owner_epoch)
   OR h.expires_at<=clock_timestamp() THEN
   RAISE EXCEPTION 'STALE_HOST' USING ERRCODE='P0001';
  END IF;
  RETURN NEW;
 END IF;
 -- Pure disconnect/drain/error observations remain legal after body deletion.
 IF ROW(NEW.effective_revision,NEW.config_payload_id,NEW.config_payload_digest,NEW.binding_epoch,
  NEW.connection_epoch,NEW.client_incarnation,NEW.host_incarnation,NEW.host_owner_epoch) IS DISTINCT FROM
  ROW(OLD.effective_revision,OLD.config_payload_id,OLD.config_payload_digest,OLD.binding_epoch,
  OLD.connection_epoch,OLD.client_incarnation,OLD.host_incarnation,OLD.host_owner_epoch)
  OR (NEW.enabled AND NOT OLD.enabled) OR (NEW.receive_enabled AND NOT OLD.receive_enabled)
  OR (NEW.state='ONLINE' AND OLD.state<>'ONLINE') THEN
  SELECT * INTO runtime FROM public.bot_runtime_state WHERE bot_id=NEW.bot_id FOR UPDATE;
  SELECT * INTO h FROM public.bot_host_lease WHERE bot_id=NEW.bot_id FOR UPDATE;
  SELECT * INTO r FROM public.bot_binding_revisions WHERE bot_id=NEW.bot_id AND binding_id=NEW.binding_id
   AND revision=NEW.effective_revision;
  SELECT * INTO c FROM public.bot_binding_clients WHERE bot_id=NEW.bot_id AND client_incarnation=NEW.client_incarnation;
  IF r.revision IS NULL OR c.client_incarnation IS NULL
   OR NEW.effective_revision NOT IN (NEW.revision,OLD.effective_revision)
   OR ROW(NEW.config_payload_id,NEW.config_payload_digest) IS DISTINCT FROM ROW(r.values_payload_id,r.values_payload_digest)
   OR NEW.binding_epoch<>OLD.binding_epoch+1 OR NEW.connection_epoch<>OLD.connection_epoch+1
   OR ROW(c.binding_id,c.revision,c.binding_epoch,c.connection_epoch,c.host_incarnation,c.host_owner_epoch)
    IS DISTINCT FROM ROW(NEW.binding_id,NEW.effective_revision,NEW.binding_epoch,NEW.connection_epoch,NEW.host_incarnation,NEW.host_owner_epoch)
   OR ROW(NEW.host_incarnation,NEW.host_owner_epoch) IS DISTINCT FROM ROW(h.incarnation,h.owner_epoch)
   OR r.policy_revision IS DISTINCT FROM runtime.policy_revision OR NOT runtime.control_admission THEN
   RAISE EXCEPTION 'BINDING_CONFIGURATION_SUPERSEDED' USING ERRCODE='23514';
  END IF;
  value := public.forest_config_payload_values(NEW.bot_id,r.values_payload_id,r.values_payload_digest,
   'binding','REVISION',NEW.binding_id,r.revision::text);
  IF NEW.enabled IS DISTINCT FROM r.settings_enabled OR NEW.receive_enabled IS DISTINCT FROM r.settings_enabled
   OR NEW.state IS DISTINCT FROM (CASE r.settings_enabled WHEN true THEN 'ONLINE' ELSE 'DISABLED' END)
   OR value->'enabled' IS DISTINCT FROM to_jsonb(NEW.enabled) THEN
   RAISE EXCEPTION 'BINDING_CONFIGURATION_CONFLICT' USING ERRCODE='23514';
  END IF;
  IF h.expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'STALE_HOST' USING ERRCODE='P0001'; END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER binding_config_guard BEFORE UPDATE ON public.bot_bindings
 FOR EACH ROW EXECUTE FUNCTION public.forest_binding_config_guard();
"""
