"""Writer fence for safe-point Context reset, starting at 071."""

from forest.storage.migration_v10 import install_writer_gate_in as install_v10_gate_in

WRITER_GENERATION = 11


async def install_writer_gate_in(conn):
    """Retain every earlier gate and atomically exclude pre-071 writers."""
    await install_v10_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,11) WHERE singleton"
    )
