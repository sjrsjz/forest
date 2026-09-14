"""Writer fence for the Shell payload cutover, starting at 062."""

from forest.storage.migration_v3 import install_writer_gate_in as install_v3_gate_in

WRITER_GENERATION = 4


async def install_writer_gate_in(conn):
    """Retain every earlier gate and atomically exclude pre-062 writers."""
    await install_v3_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,4) WHERE singleton"
    )
