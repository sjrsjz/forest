"""Writer fence for durable business-command classification, starting at 074."""

from forest.storage.migration_v12 import install_writer_gate_in as install_v12_gate_in

WRITER_GENERATION = 13


async def install_writer_gate_in(conn):
    await install_v12_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,13) WHERE singleton"
    )
