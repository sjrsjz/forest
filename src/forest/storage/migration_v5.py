"""Writer fence for the Shell context-control protocol, starting at 064."""

from forest.storage.migration_v4 import install_writer_gate_in as install_v4_gate_in

WRITER_GENERATION = 5


async def install_writer_gate_in(conn):
    """Retain every earlier gate and atomically exclude pre-064 writers."""
    await install_v4_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,5) WHERE singleton"
    )
