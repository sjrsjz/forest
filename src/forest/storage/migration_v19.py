"""Writer fence for durable ordinary-message handler registry, starting at 080."""

from forest.storage.migration_v18 import install_writer_gate_in as install_v18_gate_in

WRITER_GENERATION = 19


async def install_writer_gate_in(conn):
    await install_v18_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,19) WHERE singleton"
    )
