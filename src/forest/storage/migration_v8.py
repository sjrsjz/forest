"""Writer fence for qualified Shell priority admission, starting at 068."""

from forest.storage.migration_v7 import install_writer_gate_in as install_v7_gate_in

WRITER_GENERATION = 8


async def install_writer_gate_in(conn):
    """Retain every earlier gate and atomically exclude pre-068 writers."""
    await install_v7_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,8) WHERE singleton"
    )
