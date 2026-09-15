"""Writer fence for retryable business-command classification, starting at 077."""

from forest.storage.migration_v15 import install_writer_gate_in as install_v15_gate_in

WRITER_GENERATION = 16


async def install_writer_gate_in(conn):
    await install_v15_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,16) WHERE singleton"
    )
