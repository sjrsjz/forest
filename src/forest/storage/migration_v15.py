"""Writer fence for durable business-command failure handling, starting at 076."""

from forest.storage.migration_v14 import install_writer_gate_in as install_v14_gate_in

WRITER_GENERATION = 15


async def install_writer_gate_in(conn):
    await install_v14_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,15) WHERE singleton"
    )
