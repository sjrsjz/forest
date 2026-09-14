"""Writer fence for the Shell module-control protocol, starting at 066."""

from forest.storage.migration_v5 import install_writer_gate_in as install_v5_gate_in

WRITER_GENERATION = 6


async def install_writer_gate_in(conn):
    """Retain every earlier gate and atomically exclude pre-066 writers."""
    await install_v5_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,6) WHERE singleton"
    )
